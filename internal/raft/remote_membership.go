package raft

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	raftpb "raft-maekawa-sync/api/raft"
	"raft-maekawa-sync/internal/models"
	"raft-maekawa-sync/internal/rpc"

	"google.golang.org/grpc"
)

type remoteTaskSnapshot struct {
	data       string
	status     raftpb.TaskStatus
	assignedTo int32
}

// RemoteMembership lets pure Maekawa workers talk to the Raft cluster without
// embedding a Raft node in the worker process.
type RemoteMembership struct {
	mu sync.RWMutex

	selfID int32
	peers  map[int32]string

	leaderID int32

	conns   map[int32]*grpc.ClientConn
	clients map[int32]raftpb.RaftClient

	activeMembers map[int32]bool
	tasks         map[string]remoteTaskSnapshot
}

func NewRemoteMembership(selfID int32, peers map[int32]string) *RemoteMembership {
	peersCopy := make(map[int32]string, len(peers))
	for id, addr := range peers {
		peersCopy[id] = addr
	}

	return &RemoteMembership{
		selfID:        selfID,
		peers:         peersCopy,
		leaderID:      -1,
		conns:         make(map[int32]*grpc.ClientConn),
		clients:       make(map[int32]raftpb.RaftClient),
		activeMembers: make(map[int32]bool),
		tasks:         make(map[string]remoteTaskSnapshot),
	}
}

func (m *RemoteMembership) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, conn := range m.conns {
		conn.Close()
	}
	m.conns = make(map[int32]*grpc.ClientConn)
	m.clients = make(map[int32]raftpb.RaftClient)
}

func (m *RemoteMembership) ActiveMembers() []int32 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	members := make([]int32, 0, len(m.activeMembers))
	for id, alive := range m.activeMembers {
		if alive {
			members = append(members, id)
		}
	}
	sort.Slice(members, func(i, j int) bool { return members[i] < members[j] })
	return members
}

func (m *RemoteMembership) IsAlive(id int32) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.activeMembers[id]
}

func (m *RemoteMembership) ClaimTask(taskID string, workerID int32) (bool, error) {
	return m.submitEvent(context.Background(), models.TaskEvent{
		Type:          models.EventClaimed,
		TaskID:        taskID,
		WorkerID:      workerID,
		EventUnixNano: time.Now().UnixNano(),
	})
}

func (m *RemoteMembership) ReportTaskSuccess(taskID string, workerID int32, result string) error {
	ok, err := m.submitEvent(context.Background(), models.TaskEvent{
		Type:          models.EventDone,
		TaskID:        taskID,
		WorkerID:      workerID,
		Result:        result,
		EventUnixNano: time.Now().UnixNano(),
	})
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("task %s was not accepted", taskID)
	}
	return nil
}

func (m *RemoteMembership) ReportTaskFailure(taskID string, workerID int32, reason string) error {
	ok, err := m.submitEvent(context.Background(), models.TaskEvent{
		Type:          models.EventFailed,
		TaskID:        taskID,
		WorkerID:      workerID,
		Reason:        reason,
		EventUnixNano: time.Now().UnixNano(),
	})
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("task %s was not accepted", taskID)
	}
	return nil
}

func (m *RemoteMembership) MarkWorkerUp(ctx context.Context) error {
	_, err := m.submitEvent(ctx, models.TaskEvent{
		Type:          models.EventWorkerUp,
		WorkerID:      m.selfID,
		EventUnixNano: time.Now().UnixNano(),
	})
	return err
}

func (m *RemoteMembership) MarkWorkerDown(ctx context.Context) error {
	_, err := m.submitEvent(ctx, models.TaskEvent{
		Type:          models.EventWorkerDown,
		WorkerID:      m.selfID,
		EventUnixNano: time.Now().UnixNano(),
	})
	return err
}

func (m *RemoteMembership) RunSync(ctx context.Context, applier TaskEventApplier) {
	heartbeatTicker := time.NewTicker(500 * time.Millisecond)
	pollTicker := time.NewTicker(250 * time.Millisecond)
	defer heartbeatTicker.Stop()
	defer pollTicker.Stop()
	sendHeartbeat := func() {
		hctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, _ = m.SendWorkerHeartbeat(hctx)
		cancel()
	}

	sendHeartbeat()

	for {
		if state, err := m.fetchState(ctx); err == nil {
			m.applySnapshot(state, applier)
		}

		select {
		case <-ctx.Done():
			dctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = m.MarkWorkerDown(dctx)
			cancel()
			return
		case <-heartbeatTicker.C:
			sendHeartbeat()
		case <-pollTicker.C:
		}
	}
}

func (m *RemoteMembership) SendWorkerHeartbeat(ctx context.Context) (bool, error) {
	m.mu.RLock()
	leaderID := m.leaderID
	m.mu.RUnlock()

	candidates := make([]int32, 0, len(m.peers))
	if leaderID > 0 {
		candidates = append(candidates, leaderID)
	}
	for peerID := range m.peers {
		if peerID != leaderID {
			candidates = append(candidates, peerID)
		}
	}

	lastErr := fmt.Errorf("leader unavailable")
	for _, peerID := range candidates {
		client, err := m.getPeerClient(peerID)
		if err != nil {
			lastErr = err
			continue
		}

		resp, err := client.WorkerHeartbeat(ctx, &raftpb.WorkerHeartbeatRequest{WorkerId: m.selfID})
		if err != nil {
			lastErr = err
			continue
		}

		if resp.LeaderId > 0 {
			m.mu.Lock()
			m.leaderID = resp.LeaderId
			m.mu.Unlock()
		}
		if resp.Success {
			return true, nil
		}
	}

	return false, lastErr
}

func (m *RemoteMembership) getPeerClient(peerID int32) (raftpb.RaftClient, error) {
	m.mu.Lock()
	if client, ok := m.clients[peerID]; ok {
		m.mu.Unlock()
		return client, nil
	}
	addr, ok := m.peers[peerID]
	m.mu.Unlock()
	if !ok {
		return nil, context.DeadlineExceeded
	}

	conn, err := rpc.Dial(addr)
	if err != nil {
		return nil, err
	}
	client := raftpb.NewRaftClient(conn)

	m.mu.Lock()
	m.conns[peerID] = conn
	m.clients[peerID] = client
	m.mu.Unlock()

	return client, nil
}

func (m *RemoteMembership) submitEvent(ctx context.Context, event models.TaskEvent) (bool, error) {
	payload, err := encodeInternalSubmit(event)
	if err != nil {
		return false, err
	}

	m.mu.RLock()
	leaderID := m.leaderID
	m.mu.RUnlock()

	candidates := make([]int32, 0, len(m.peers))
	if leaderID > 0 {
		candidates = append(candidates, leaderID)
	}
	for peerID := range m.peers {
		if peerID != leaderID {
			candidates = append(candidates, peerID)
		}
	}

	lastErr := fmt.Errorf("leader unavailable")
	for _, peerID := range candidates {
		client, err := m.getPeerClient(peerID)
		if err != nil {
			lastErr = err
			continue
		}

		resp, err := client.SubmitTask(ctx, &raftpb.SubmitTaskRequest{Data: payload})
		if err != nil {
			lastErr = err
			continue
		}

		if resp.LeaderId > 0 {
			m.mu.Lock()
			m.leaderID = resp.LeaderId
			m.mu.Unlock()
		}

		if resp.Success {
			return true, nil
		}

		if resp.LeaderId > 0 && resp.LeaderId != peerID {
			client, err = m.getPeerClient(resp.LeaderId)
			if err != nil {
				lastErr = err
				continue
			}
			resp, err = client.SubmitTask(ctx, &raftpb.SubmitTaskRequest{Data: payload})
			if err != nil {
				lastErr = err
				continue
			}
			if resp.Success {
				m.mu.Lock()
				m.leaderID = resp.LeaderId
				m.mu.Unlock()
				return true, nil
			}
			return false, nil
		}

		return false, nil
	}

	return false, lastErr
}

func (m *RemoteMembership) fetchState(ctx context.Context) (*raftpb.GetStateResponse, error) {
	m.mu.RLock()
	leaderID := m.leaderID
	m.mu.RUnlock()

	candidates := make([]int32, 0, len(m.peers))
	if leaderID > 0 {
		candidates = append(candidates, leaderID)
	}
	for peerID := range m.peers {
		if peerID != leaderID {
			candidates = append(candidates, peerID)
		}
	}

	var lastErr error = fmt.Errorf("raft state unavailable")
	for _, peerID := range candidates {
		client, err := m.getPeerClient(peerID)
		if err != nil {
			lastErr = err
			continue
		}

		rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		resp, err := client.GetState(rctx, &raftpb.GetStateRequest{})
		cancel()
		if err != nil {
			lastErr = err
			continue
		}

		if resp.NodeInfo != nil {
			m.mu.Lock()
			switch {
			case resp.NodeInfo.Role == "leader":
				m.leaderID = resp.NodeInfo.NodeId
			case resp.NodeInfo.LeaderId > 0:
				m.leaderID = resp.NodeInfo.LeaderId
			}
			m.mu.Unlock()
		}

		return resp, nil
	}

	return nil, lastErr
}

func (m *RemoteMembership) applySnapshot(resp *raftpb.GetStateResponse, applier TaskEventApplier) {
	if applier == nil || resp == nil {
		return
	}

	newActive := make(map[int32]bool, len(resp.ActiveNodes))
	for _, id := range resp.ActiveNodes {
		newActive[id] = true
	}

	newTasks := make(map[string]remoteTaskSnapshot, len(resp.Tasks))
	for _, task := range resp.Tasks {
		newTasks[task.TaskId] = remoteTaskSnapshot{
			data:       task.Data,
			status:     task.Status,
			assignedTo: task.AssignedTo,
		}
	}

	m.mu.Lock()
	prevActive := m.activeMembers
	prevTasks := m.tasks
	m.activeMembers = newActive
	m.tasks = newTasks
	m.mu.Unlock()

	memberIDs := make(map[int32]struct{}, len(prevActive)+len(newActive))
	for id := range prevActive {
		memberIDs[id] = struct{}{}
	}
	for id := range newActive {
		memberIDs[id] = struct{}{}
	}

	memberList := make([]int32, 0, len(memberIDs))
	for id := range memberIDs {
		memberList = append(memberList, id)
	}
	sort.Slice(memberList, func(i, j int) bool { return memberList[i] < memberList[j] })

	for _, id := range memberList {
		wasAlive := prevActive[id]
		isAlive := newActive[id]
		if wasAlive == isAlive {
			continue
		}
		eventType := models.EventWorkerDown
		if isAlive {
			eventType = models.EventWorkerUp
		}
		applyTaskEventToMaekawa(models.TaskEvent{Type: eventType, WorkerID: id}, applier)
	}

	taskIDs := make([]string, 0, len(prevTasks)+len(newTasks))
	seen := make(map[string]struct{}, len(prevTasks)+len(newTasks))
	for id := range prevTasks {
		if _, ok := seen[id]; !ok {
			taskIDs = append(taskIDs, id)
			seen[id] = struct{}{}
		}
	}
	for id := range newTasks {
		if _, ok := seen[id]; !ok {
			taskIDs = append(taskIDs, id)
			seen[id] = struct{}{}
		}
	}
	sort.Strings(taskIDs)

	for _, taskID := range taskIDs {
		prev, hadPrev := prevTasks[taskID]
		curr, hasCurr := newTasks[taskID]
		if !hasCurr {
			continue
		}

		changed := !hadPrev ||
			prev.status != curr.status ||
			prev.assignedTo != curr.assignedTo ||
			prev.data != curr.data
		if !changed {
			continue
		}

		event := models.TaskEvent{TaskID: taskID, WorkerID: curr.assignedTo}
		switch curr.status {
		case raftpb.TaskStatus_PENDING:
			event.Type = models.EventAssigned
			event.Task = &models.Task{ID: taskID, Data: curr.data}
		case raftpb.TaskStatus_IN_PROGRESS:
			event.Type = models.EventClaimed
		case raftpb.TaskStatus_COMPLETED:
			event.Type = models.EventDone
		default:
			event.Type = models.EventFailed
		}
		applyTaskEventToMaekawa(event, applier)
	}
}
