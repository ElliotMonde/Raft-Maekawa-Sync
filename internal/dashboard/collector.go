// Polls Raft/Worker nodes via gRPC and emits DashEvents to the Hub.
package dashboard

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	raftpb "raft-maekawa-sync/api/raft"
	"raft-maekawa-sync/internal/rpc"

	grpclib "google.golang.org/grpc"
)

type NodeConfig struct {
	ID   int32
	Addr string
}

type nodeSnapshot struct {
	role        string
	term        int32
	commitIndex int32
	logLength   int32
	taskIDs     map[string]raftpb.TaskStatus
}

type trackedTaskState struct {
	status     raftpb.TaskStatus
	assignedTo int32
}

type snapshotNode struct {
	ID          int32   `json:"id"`
	Role        string  `json:"role"`
	Term        int32   `json:"term"`
	LogLength   int32   `json:"log_length"`
	CommitIndex int32   `json:"commit_index"`
	Clock       int64   `json:"clock"`
	InCS        bool    `json:"in_cs"`
	Quorum      []int32 `json:"quorum"`
	VotedFor    int32   `json:"voted_for"`
}

type snapshotTask struct {
	ID         string `json:"id"`
	Data       string `json:"data"`
	Status     string `json:"status"`
	AssignedTo int32  `json:"assigned_to"`
}

type Collector struct {
	hub     *Hub
	clients map[int32]raftpb.RaftClient
	conns   map[int32]*grpclib.ClientConn // keyed by node ID for per-node close
	nodes   map[int32]NodeConfig          // all known nodes (static + dynamic)

	mu          sync.Mutex
	prev        map[int32]*nodeSnapshot
	globalTasks map[string]trackedTaskState   // global task status tracking
	leaderID    int32                        // caches last known Raft leader

	Manager NodeManager // optional; nil when not managing nodes
}

func isRaftNodeID(id int32) bool {
	return id >= 1 && id <= 3
}

func NewCollector(nodes []NodeConfig, hub *Hub) *Collector {
	c := &Collector{
		hub:         hub,
		clients:     make(map[int32]raftpb.RaftClient),
		conns:       make(map[int32]*grpclib.ClientConn),
		nodes:       make(map[int32]NodeConfig),
		prev:        make(map[int32]*nodeSnapshot),
		globalTasks: make(map[string]trackedTaskState),
		leaderID:    -1,
	}
	for _, n := range nodes {
		c.nodes[n.ID] = n
	}
	return c
}

// Dial opens gRPC connections to all statically-configured nodes.
func (c *Collector) Dial() {
	for _, n := range c.nodes {
		c.dialOne(n)
	}
}

func (c *Collector) dialOne(n NodeConfig) {
	if !isRaftNodeID(n.ID) {
		return
	}
	if old, ok := c.conns[n.ID]; ok {
		old.Close()
		delete(c.conns, n.ID)
		delete(c.clients, n.ID)
	}
	conn, err := rpc.Dial(n.Addr)
	if err != nil {
		log.Printf("dashboard: dial %d@%s: %v", n.ID, n.Addr, err)
		return
	}
	c.conns[n.ID] = conn
	c.clients[n.ID] = raftpb.NewRaftClient(conn)
}

// Close shuts down all gRPC connections.
func (c *Collector) Close() {
	for _, conn := range c.conns {
		conn.Close()
	}
}

// NodeList returns every known node config (static + dynamically added).
func (c *Collector) NodeList() []NodeConfig {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]NodeConfig, 0, len(c.nodes))
	for _, n := range c.nodes {
		out = append(out, n)
	}
	return out
}

// StartNode starts a managed node and resumes polling it.
func (c *Collector) StartNode(id int32, addr, dashAddr string) error {
	if c.Manager == nil {
		return fmt.Errorf("node manager not configured")
	}
	role := "worker"
	peers := c.workerPeerString(id)
	if isRaftNodeID(id) {
		role = "raft"
		peers = c.raftPeerString(id)
	}
	raftPeers := c.raftPeerString()
	if err := c.Manager.StartNode(id, role, addr, peers, raftPeers, dashAddr); err != nil {
		return err
	}
	// Give the process a moment to bind its gRPC port before dialing.
	time.Sleep(300 * time.Millisecond)
	n := NodeConfig{ID: id, Addr: addr}
	c.mu.Lock()
	c.nodes[id] = n
	c.mu.Unlock()
	c.dialOne(n)
	c.hub.Broadcast(DashEvent{Type: "node_up", NodeID: id})
	return nil
}

// StopNode stops a managed node and removes it from polling.
func (c *Collector) StopNode(id int32) error {
	if c.Manager == nil {
		return fmt.Errorf("node manager not configured")
	}
	if err := c.Manager.StopNode(id); err != nil {
		return err
	}
	c.mu.Lock()
	if conn, ok := c.conns[id]; ok {
		conn.Close()
		delete(c.conns, id)
	}
	delete(c.clients, id)
	c.prev[id] = &nodeSnapshot{role: "down"}
	if c.leaderID == id {
		c.leaderID = -1
	}
	c.mu.Unlock()
	c.hub.Broadcast(DashEvent{Type: "node_down", NodeID: id})
	return nil
}

// SubmitTask forwards a task to the current Raft leader.
func (c *Collector) SubmitTask(ctx context.Context, data string) (string, error) {
	c.mu.Lock()
	preferred := c.leaderID
	ids := make([]int32, 0, len(c.clients))
	if preferred > 0 {
		ids = append(ids, preferred)
	}
	for id := range c.clients {
		if id != preferred {
			ids = append(ids, id)
		}
	}
	c.mu.Unlock()

	for _, id := range ids {
		c.mu.Lock()
		client, ok := c.clients[id]
		c.mu.Unlock()
		if !ok {
			continue
		}
		rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		resp, err := client.SubmitTask(rctx, &raftpb.SubmitTaskRequest{Data: data})
		cancel()
		if err != nil {
			continue
		}
		if resp.Success {
			c.mu.Lock()
			c.leaderID = id
			c.mu.Unlock()
			return resp.TaskId, nil
		}
		if resp.LeaderId > 0 {
			c.mu.Lock()
			c.leaderID = resp.LeaderId
			client2, ok2 := c.clients[resp.LeaderId]
			c.mu.Unlock()
			if ok2 {
				rctx2, cancel2 := context.WithTimeout(ctx, 2*time.Second)
				resp2, err2 := client2.SubmitTask(rctx2, &raftpb.SubmitTaskRequest{Data: data})
				cancel2()
				if err2 == nil && resp2.Success {
					return resp2.TaskId, nil
				}
			}
		}
	}
	return "", fmt.Errorf("no leader available")
}

// SubmitTaskOnce sends a task to one specific Raft node without redirecting.
// This is useful for demonstrating honest failure semantics during elections.
func (c *Collector) SubmitTaskOnce(ctx context.Context, nodeID int32, data string) (string, error) {
	c.mu.Lock()
	client, ok := c.clients[nodeID]
	c.mu.Unlock()
	if !ok {
		return "", fmt.Errorf("node %d unavailable", nodeID)
	}

	rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	resp, err := client.SubmitTask(rctx, &raftpb.SubmitTaskRequest{Data: data})
	if err != nil {
		return "", err
	}
	if resp.Success {
		return resp.TaskId, nil
	}
	if resp.LeaderId > 0 {
		return "", fmt.Errorf("not the leader; try node %d", resp.LeaderId)
	}
	return "", fmt.Errorf("no leader available")
}

// KillNode stops a managed node when possible; otherwise it only marks it down in the dashboard view.
func (c *Collector) KillNode(nodeID int32) error {
	if c.Manager != nil && c.Manager.IsManaged(nodeID) {
		return c.StopNode(nodeID)
	}
	c.mu.Lock()
	if _, ok := c.clients[nodeID]; !ok {
		c.mu.Unlock()
		return fmt.Errorf("unknown node %d", nodeID)
	}
	c.prev[nodeID] = &nodeSnapshot{role: "down"}
	if c.leaderID == nodeID {
		c.leaderID = -1
	}
	c.mu.Unlock()
	c.hub.Broadcast(DashEvent{Type: "node_down", NodeID: nodeID})
	return nil
}

func (c *Collector) Run(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.poll(ctx)
		}
	}
}

func (c *Collector) poll(ctx context.Context) {
	c.mu.Lock()
	nodeIDs := make([]int32, 0, len(c.nodes))
	for id := range c.nodes {
		nodeIDs = append(nodeIDs, id)
	}
	c.mu.Unlock()

	for _, id := range nodeIDs {
		if !isRaftNodeID(id) {
			continue
		}
		c.mu.Lock()
		client, ok := c.clients[id]
		node, hasNode := c.nodes[id]
		c.mu.Unlock()
		if !ok {
			if hasNode {
				c.dialOne(node)
			}
			continue
		}

		rctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		resp, err := client.GetState(rctx, &raftpb.GetStateRequest{})
		cancel()

		c.mu.Lock()
		if err != nil {
			prev := c.prev[id]
			if prev == nil || prev.role != "down" {
				c.hub.Broadcast(DashEvent{Type: "node_down", NodeID: id})
				c.prev[id] = &nodeSnapshot{role: "down"}
			}
			c.mu.Unlock()
			continue
		}

		info := resp.NodeInfo
		prev := c.prev[id]

		if prev == nil || prev.role != info.Role || prev.term != info.Term {
			c.hub.Broadcast(DashEvent{
				Type:   "node_role",
				NodeID: info.NodeId,
				Role:   info.Role,
				Term:   int64(info.Term),
			})
		}

		if info.Role == "leader" {
			c.leaderID = info.NodeId
		}

		if prev != nil && info.LogLength > prev.logLength {
			for idx := prev.logLength + 1; idx <= info.LogLength; idx++ {
				c.hub.Broadcast(DashEvent{
					Type:     "log_replicated",
					NodeID:   info.NodeId,
					LogIndex: int64(idx),
					Term:     int64(info.Term),
				})
			}
		}

		if prev != nil && info.CommitIndex > prev.commitIndex {
			for idx := prev.commitIndex + 1; idx <= info.CommitIndex; idx++ {
				c.hub.Broadcast(DashEvent{
					Type:     "log_committed",
					NodeID:   info.NodeId,
					LogIndex: int64(idx),
					Term:     int64(info.Term),
				})
			}
		}

		newIDs := make(map[string]raftpb.TaskStatus, len(resp.Tasks))
		for _, t := range resp.Tasks {
			newIDs[t.TaskId] = t.Status
			prevTask, seen := c.globalTasks[t.TaskId]
			changed := !seen || prevTask.status != t.Status || prevTask.assignedTo != t.AssignedTo
			if changed {
				// If task jumped straight from PENDING to COMPLETED/FAILED without
				// us seeing IN_PROGRESS (poll interval missed it), synthesize task_assigned first.
				if (t.Status == raftpb.TaskStatus_COMPLETED || t.Status == raftpb.TaskStatus_FAILED) &&
					t.AssignedTo > 0 &&
					(!seen || prevTask.status != raftpb.TaskStatus_IN_PROGRESS || prevTask.assignedTo != t.AssignedTo) {
					c.hub.Broadcast(DashEvent{
						Type:   "task_assigned",
						From:   c.leaderID,
						To:     t.AssignedTo,
						NodeID: t.AssignedTo,
						TaskID: t.TaskId,
					})
				}

				evt := DashEvent{
					Type:   taskEvtType(t.Status),
					NodeID: t.AssignedTo,
					TaskID: t.TaskId,
				}
				if t.Status == raftpb.TaskStatus_IN_PROGRESS && t.AssignedTo > 0 {
					evt.From = c.leaderID
					evt.To = t.AssignedTo
				}
				c.hub.Broadcast(evt)
				c.globalTasks[t.TaskId] = trackedTaskState{status: t.Status, assignedTo: t.AssignedTo}
			}
		}

		c.prev[id] = &nodeSnapshot{
			role:        info.Role,
			term:        info.Term,
			commitIndex: info.CommitIndex,
			logLength:   info.LogLength,
			taskIDs:     newIDs,
		}
		c.mu.Unlock()
	}
}

func taskEvtType(s raftpb.TaskStatus) string {
	switch s {
	case raftpb.TaskStatus_PENDING:
		return "task_submitted"
	case raftpb.TaskStatus_IN_PROGRESS:
		return "task_assigned"
	case raftpb.TaskStatus_COMPLETED:
		return "task_done"
	default:
		return "task_failed"
	}
}

func (c *Collector) SnapshotPayload() map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Clear task tracking on snapshot (client reconnect/scenario change)
	c.globalTasks = make(map[string]trackedTaskState)

	nodeIDs := make([]int32, 0, len(c.nodes))
	for id := range c.nodes {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Slice(nodeIDs, func(i, j int) bool { return nodeIDs[i] < nodeIDs[j] })

	nodes := make([]snapshotNode, 0, len(nodeIDs))
	taskMap := make(map[string]snapshotTask)

	for _, id := range nodeIDs {
		prev := c.prev[id]
		node := snapshotNode{
			ID:       id,
			Role:     "follower",
			Term:     0,
			VotedFor: -1,
			Quorum:   []int32{},
		}
		if !isRaftNodeID(id) {
			node.Role = "worker"
		}
		if prev != nil {
			node.Role = prev.role
			node.Term = prev.term
			node.LogLength = prev.logLength
			node.CommitIndex = prev.commitIndex
			for taskID, status := range prev.taskIDs {
				task := taskMap[taskID]
				task.ID = taskID
				task.Status = snapshotTaskStatus(status)
				taskMap[taskID] = task
			}
		}
		nodes = append(nodes, node)
	}

	taskIDs := make([]string, 0, len(taskMap))
	for taskID := range taskMap {
		taskIDs = append(taskIDs, taskID)
	}
	sort.Strings(taskIDs)
	tasks := make([]snapshotTask, 0, len(taskIDs))
	for _, taskID := range taskIDs {
		tasks = append(tasks, taskMap[taskID])
	}

	return map[string]any{
		"nodes": nodes,
		"tasks": tasks,
	}
}

func (c *Collector) raftPeerString(excludeIDs ...int32) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	excluded := make(map[int32]struct{}, len(excludeIDs))
	for _, id := range excludeIDs {
		excluded[id] = struct{}{}
	}

	ids := make([]int32, 0, len(c.nodes))
	for id := range c.nodes {
		if !isRaftNodeID(id) {
			continue
		}
		if _, skip := excluded[id]; skip {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("%d=%s", id, c.nodes[id].Addr))
	}
	return strings.Join(parts, ",")
}

func (c *Collector) workerPeerString(selfID int32) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	ids := make([]int32, 0, len(c.nodes))
	for id := range c.nodes {
		if isRaftNodeID(id) || id == selfID {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("%d=%s", id, c.nodes[id].Addr))
	}
	return strings.Join(parts, ",")
}

func snapshotTaskStatus(s raftpb.TaskStatus) string {
	switch s {
	case raftpb.TaskStatus_PENDING:
		return "pending"
	case raftpb.TaskStatus_IN_PROGRESS:
		return "in_progress"
	case raftpb.TaskStatus_COMPLETED:
		return "completed"
	default:
		return "failed"
	}
}

// MaekawaHook returns an EventHook that can be wired to a maekawa.Worker.
func (c *Collector) MaekawaHook() func(evtType string, from, to int32, clock int64) {
	return func(evtType string, from, to int32, clock int64) {
		c.hub.Broadcast(DashEvent{
			Type:      evtType,
			From:      from,
			To:        to,
			NodeID:    from,
			Timestamp: clock,
		})
	}
}
