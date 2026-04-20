package raft

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	maekawapb "raft-maekawa-sync/api/maekawa"
	raftpb "raft-maekawa-sync/api/raft"
	"raft-maekawa-sync/internal/maekawa"
	"raft-maekawa-sync/internal/models"
	"raft-maekawa-sync/internal/rpc"
)

type combinedRuntimeNode struct {
	id      int32
	addr    string
	node    *Node
	worker  *maekawa.Worker
	server  *rpc.Server
	cancel  context.CancelFunc
	stopped atomic.Bool
}

type combinedRuntimeOptions struct {
	dataDir         string
	executorFactory func(id int32) maekawa.TaskExecutor
	startTaskLoops  bool
}

func (n *combinedRuntimeNode) Stop() {
	if !n.stopped.CompareAndSwap(false, true) {
		return
	}
	n.cancel()
	done := make(chan struct{})
	go func() {
		n.server.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}
}

func startCombinedRuntimeCluster(
	t *testing.T,
	count int,
	dataDir string,
	executorFactory func(id int32) maekawa.TaskExecutor,
) []*combinedRuntimeNode {
	return startCombinedRuntimeClusterWithOptions(t, count, combinedRuntimeOptions{
		dataDir:         dataDir,
		executorFactory: executorFactory,
		startTaskLoops:  true,
	})
}

func startCombinedRuntimeClusterWithOptions(
	t *testing.T,
	count int,
	opts combinedRuntimeOptions,
) []*combinedRuntimeNode {
	t.Helper()

	addrs := make(map[int32]string, count)
	activeIDs := make([]int32, 0, count)
	for id := int32(1); id <= int32(count); id++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("allocate port for node %d: %v", id, err)
		}
		addrs[id] = ln.Addr().String()
		activeIDs = append(activeIDs, id)
		ln.Close()
	}

	nodes := make([]*combinedRuntimeNode, 0, count)
	for id := int32(1); id <= int32(count); id++ {
		peers := make(map[int32]string, count-1)
		for peerID, addr := range addrs {
			if peerID != id {
				peers[peerID] = addr
			}
		}

		node := NewNode(id, addrs[id], peers, nil)
		node.electionMin = 80 * time.Millisecond
		node.electionMax = 160 * time.Millisecond
		node.heartbeatIntv = 40 * time.Millisecond
		node.workerHeartbeatTimeout = 250 * time.Millisecond
		node.workerHeartbeatCheckIntv = 25 * time.Millisecond
		node.SetManagedWorkers(activeIDs)
		if opts.dataDir != "" {
			if err := node.SetStoragePath(filepath.Join(opts.dataDir, fmt.Sprintf("node-%d.json", id))); err != nil {
				t.Fatalf("set storage path for node %d: %v", id, err)
			}
		}

		quorum := maekawa.RegridQuorum(id, activeIDs)
		if len(quorum) == 0 {
			quorum = []int32{id}
		}

		worker := maekawa.NewWorker(id, quorum, node)
		if opts.executorFactory != nil {
			worker.SetTaskExecutor(opts.executorFactory(id))
		}
		node.SetApplier(worker)

		server := rpc.NewServer()
		raftpb.RegisterRaftServer(server.GRPC(), node)
		maekawapb.RegisterMaekawaServer(server.GRPC(), worker)

		nodes = append(nodes, &combinedRuntimeNode{
			id:     id,
			addr:   addrs[id],
			node:   node,
			worker: worker,
			server: server,
			cancel: func() {},
		})
	}

	for _, runtimeNode := range nodes {
		if err := runtimeNode.worker.InitClients(addrs, runtimeNode.id); err != nil {
			t.Fatalf("init maekawa clients for node %d: %v", runtimeNode.id, err)
		}
		if err := runtimeNode.server.Start(runtimeNode.addr); err != nil {
			t.Fatalf("start combined runtime server for node %d: %v", runtimeNode.id, err)
		}
	}

	for _, runtimeNode := range nodes {
		runCtx, runCancel := context.WithCancel(context.Background())
		runtimeNode.cancel = runCancel
		go runtimeNode.node.Run(runCtx)
		go func(runtimeNode *combinedRuntimeNode) {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-runCtx.Done():
					return
				case <-ticker.C:
				}

				for _, candidate := range activeRuntimeNodes(nodes) {
					candidate.node.mu.Lock()
					isLeader := candidate.node.role == Leader
					candidate.node.mu.Unlock()
					if !isLeader {
						continue
					}
					_, _ = candidate.node.WorkerHeartbeat(runCtx, &raftpb.WorkerHeartbeatRequest{WorkerId: runtimeNode.id})
					break
				}
			}
		}(runtimeNode)
		if opts.startTaskLoops {
			go runtimeNode.worker.RunTaskLoop(runCtx)
		}
	}

	t.Cleanup(func() {
		for _, runtimeNode := range nodes {
			runtimeNode.Stop()
		}
	})

	time.Sleep(50 * time.Millisecond)
	return nodes
}

func activeRuntimeNodes(nodes []*combinedRuntimeNode) []*combinedRuntimeNode {
	active := make([]*combinedRuntimeNode, 0, len(nodes))
	for _, node := range nodes {
		if !node.stopped.Load() {
			active = append(active, node)
		}
	}
	return active
}

func waitForRuntimeLeader(t *testing.T, nodes []*combinedRuntimeNode, timeout time.Duration) *combinedRuntimeNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for _, runtimeNode := range activeRuntimeNodes(nodes) {
			runtimeNode.node.mu.Lock()
			isLeader := runtimeNode.node.role == Leader
			runtimeNode.node.mu.Unlock()
			if isLeader {
				return runtimeNode
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("no leader elected in combined runtime")
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func submitTaskOverRPC(t *testing.T, addr, data string) *raftpb.SubmitTaskResponse {
	t.Helper()
	conn, err := rpc.Dial(addr)
	if err != nil {
		t.Fatalf("dial raft node %s: %v", addr, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := raftpb.NewRaftClient(conn).SubmitTask(ctx, &raftpb.SubmitTaskRequest{Data: data})
	if err != nil {
		t.Fatalf("submit task to %s: %v", addr, err)
	}
	return resp
}

func waitForTaskStatusAcrossNodes(
	t *testing.T,
	nodes []*combinedRuntimeNode,
	taskID string,
	status raftpb.TaskStatus,
	timeout time.Duration,
) int32 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		allReady := true
		var assignedTo int32 = -1
		for _, runtimeNode := range activeRuntimeNodes(nodes) {
			resp, err := runtimeNode.node.GetState(context.Background(), &raftpb.GetStateRequest{})
			if err != nil {
				t.Fatalf("get state from node %d: %v", runtimeNode.id, err)
			}

			found := false
			for _, task := range resp.Tasks {
				if task.TaskId != taskID {
					continue
				}
				found = true
				if task.Status != status {
					allReady = false
					break
				}
				if assignedTo == -1 {
					assignedTo = task.AssignedTo
				}
				if task.AssignedTo != assignedTo {
					t.Fatalf("task %s assigned_to mismatch: got %d and %d", taskID, assignedTo, task.AssignedTo)
				}
			}
			if !found {
				allReady = false
				break
			}
			if !allReady {
				break
			}
		}

		if allReady {
			return assignedTo
		}
		if time.Now().After(deadline) {
			t.Fatalf("task %s did not reach status %s on all live nodes", taskID, status.String())
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func taskStatusOnNode(t *testing.T, runtimeNode *combinedRuntimeNode, taskID string) (bool, raftpb.TaskStatus, int32) {
	t.Helper()

	resp, err := runtimeNode.node.GetState(context.Background(), &raftpb.GetStateRequest{})
	if err != nil {
		t.Fatalf("get state from node %d: %v", runtimeNode.id, err)
	}
	for _, task := range resp.Tasks {
		if task.TaskId == taskID {
			return true, task.Status, task.AssignedTo
		}
	}
	return false, raftpb.TaskStatus_PENDING, 0
}

func waitForNewRuntimeLeader(
	t *testing.T,
	nodes []*combinedRuntimeNode,
	previousLeaderID int32,
	timeout time.Duration,
) *combinedRuntimeNode {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		for _, runtimeNode := range activeRuntimeNodes(nodes) {
			runtimeNode.node.mu.Lock()
			isLeader := runtimeNode.node.role == Leader
			runtimeNode.node.mu.Unlock()
			if isLeader && runtimeNode.id != previousLeaderID {
				return runtimeNode
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no new leader elected after leader %d stopped", previousLeaderID)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func waitForRuntimeQuorumsToExcludeNode(
	t *testing.T,
	nodes []*combinedRuntimeNode,
	excludedID int32,
	timeout time.Duration,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		allReady := true
		for _, runtimeNode := range activeRuntimeNodes(nodes) {
			quorum := runtimeNode.worker.CurrentQuorum()
			for _, member := range quorum {
				if member == excludedID {
					allReady = false
					break
				}
			}
			if !allReady {
				break
			}
		}
		if allReady {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("workers did not regrid away from node %d in time", excludedID)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestCombinedRuntimeSubmitTaskEndToEnd(t *testing.T) {
	var executions int32
	nodes := startCombinedRuntimeCluster(t, 3, "", func(id int32) maekawa.TaskExecutor {
		return func(_ context.Context, task *models.Task) (string, error) {
			atomic.AddInt32(&executions, 1)
			time.Sleep(30 * time.Millisecond)
			return fmt.Sprintf("done-by-%d:%s", id, task.ID), nil
		}
	})

	leader := waitForRuntimeLeader(t, nodes, 5*time.Second)
	resp := submitTaskOverRPC(t, leader.addr, "e2e-work")
	if !resp.Success {
		t.Fatalf("expected task submission to succeed, leader hint=%d", resp.LeaderId)
	}
	if resp.TaskId == "" {
		t.Fatal("expected task id from combined runtime submission")
	}

	assignedTo := waitForTaskStatusAcrossNodes(t, nodes, resp.TaskId, raftpb.TaskStatus_COMPLETED, 8*time.Second)
	if assignedTo <= 0 {
		t.Fatalf("expected assigned worker id, got %d", assignedTo)
	}

	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt32(&executions); got != 1 {
		t.Fatalf("task executed %d times, want exactly 1", got)
	}
}

func TestCombinedRuntimePersistsStateAcrossRestart(t *testing.T) {
	dataDir := t.TempDir()

	var firstRunExecutions int32
	nodes := startCombinedRuntimeCluster(t, 1, dataDir, func(id int32) maekawa.TaskExecutor {
		return func(_ context.Context, task *models.Task) (string, error) {
			atomic.AddInt32(&firstRunExecutions, 1)
			return fmt.Sprintf("done-by-%d:%s", id, task.ID), nil
		}
	})

	leader := waitForRuntimeLeader(t, nodes, 2*time.Second)
	resp := submitTaskOverRPC(t, leader.addr, "persist-me")
	if !resp.Success {
		t.Fatalf("expected single-node submission to succeed, leader hint=%d", resp.LeaderId)
	}
	waitForTaskStatusAcrossNodes(t, nodes, resp.TaskId, raftpb.TaskStatus_COMPLETED, 5*time.Second)
	if got := atomic.LoadInt32(&firstRunExecutions); got != 1 {
		t.Fatalf("task executed %d times before restart, want 1", got)
	}

	for _, node := range nodes {
		node.Stop()
	}

	restarted := startCombinedRuntimeClusterWithOptions(t, 1, combinedRuntimeOptions{
		dataDir:        dataDir,
		startTaskLoops: false,
	})

	waitForRuntimeLeader(t, restarted, 2*time.Second)
	waitForTaskStatusAcrossNodes(t, restarted, resp.TaskId, raftpb.TaskStatus_COMPLETED, 3*time.Second)
}

func TestCombinedRuntimeLeaderFailover(t *testing.T) {
	nodes := startCombinedRuntimeCluster(t, 3, "", func(id int32) maekawa.TaskExecutor {
		return func(_ context.Context, task *models.Task) (string, error) {
			time.Sleep(25 * time.Millisecond)
			return fmt.Sprintf("done-by-%d:%s", id, task.ID), nil
		}
	})

	initialLeader := waitForRuntimeLeader(t, nodes, 5*time.Second)
	first := submitTaskOverRPC(t, initialLeader.addr, "before-failover")
	if !first.Success {
		t.Fatalf("expected pre-failover submission to succeed, leader hint=%d", first.LeaderId)
	}
	waitForTaskStatusAcrossNodes(t, nodes, first.TaskId, raftpb.TaskStatus_COMPLETED, 8*time.Second)

	initialLeader.Stop()

	newLeader := waitForRuntimeLeader(t, nodes, 8*time.Second)
	waitForRuntimeQuorumsToExcludeNode(t, nodes, initialLeader.id, 5*time.Second)

	second := submitTaskOverRPC(t, newLeader.addr, "after-failover")
	if !second.Success {
		t.Fatalf("expected post-failover submission to succeed, leader hint=%d", second.LeaderId)
	}
	waitForTaskStatusAcrossNodes(t, nodes, second.TaskId, raftpb.TaskStatus_COMPLETED, 8*time.Second)
}

func TestCombinedRuntimeLeaderFailoverDuringAssignmentWindow(t *testing.T) {
	var executions int32
	claimGate := make(chan struct{})

	nodes := startCombinedRuntimeCluster(t, 3, "", func(id int32) maekawa.TaskExecutor {
		return func(_ context.Context, task *models.Task) (string, error) {
			atomic.AddInt32(&executions, 1)
			time.Sleep(25 * time.Millisecond)
			return fmt.Sprintf("done-by-%d:%s", id, task.ID), nil
		}
	})

	for _, runtimeNode := range nodes {
		runtimeNode.worker.SetBeforeClaimHook(func(task *models.Task) bool {
			<-claimGate
			return true
		})
	}

	leader := waitForRuntimeLeader(t, nodes, 5*time.Second)
	resp := submitTaskOverRPC(t, leader.addr, "failover-during-assignment")
	if !resp.Success {
		t.Fatalf("expected task submission to succeed, leader hint=%d", resp.LeaderId)
	}

	waitForTaskStatusAcrossNodes(t, nodes, resp.TaskId, raftpb.TaskStatus_PENDING, 5*time.Second)
	leader.Stop()
	waitForNewRuntimeLeader(t, nodes, leader.id, 8*time.Second)
	close(claimGate)

	waitForRuntimeQuorumsToExcludeNode(t, nodes, leader.id, 5*time.Second)
	for _, runtimeNode := range activeRuntimeNodes(nodes) {
		found, _, _ := taskStatusOnNode(t, runtimeNode, resp.TaskId)
		if !found {
			t.Fatalf("task %s missing on node %d after failover", resp.TaskId, runtimeNode.id)
		}
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		allFound := true
		allPending := true
		allCompleted := true
		allPendingUnassigned := true
		for _, runtimeNode := range activeRuntimeNodes(nodes) {
			found, status, assignedTo := taskStatusOnNode(t, runtimeNode, resp.TaskId)
			if !found {
				allFound = false
				break
			}
			if status != raftpb.TaskStatus_PENDING {
				allPending = false
			}
			if status != raftpb.TaskStatus_COMPLETED {
				allCompleted = false
			}
			if status == raftpb.TaskStatus_PENDING && assignedTo != 0 {
				allPendingUnassigned = false
			}
		}

		if allFound && allCompleted {
			if got := atomic.LoadInt32(&executions); got != 1 {
				t.Fatalf("interrupted task executed %d times, want exactly 1", got)
			}
			return
		}

		if allFound && allPending && allPendingUnassigned {
			if got := atomic.LoadInt32(&executions); got > 1 {
				t.Fatalf("interrupted task executed %d times while remaining pending", got)
			}
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("task %s did not settle to a safe post-failover state", resp.TaskId)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestCombinedRuntimeWorkerCrashBeforeClaim(t *testing.T) {
	var executions int32
	var crashedWorker int32
	var crashOnce sync.Once

	nodes := startCombinedRuntimeCluster(t, 3, "", func(id int32) maekawa.TaskExecutor {
		return func(_ context.Context, task *models.Task) (string, error) {
			atomic.AddInt32(&executions, 1)
			time.Sleep(25 * time.Millisecond)
			return fmt.Sprintf("done-by-%d:%s", id, task.ID), nil
		}
	})

	for _, runtimeNode := range nodes {
		runtimeNode := runtimeNode
		runtimeNode.worker.SetBeforeClaimHook(func(task *models.Task) bool {
			triggered := false
			crashOnce.Do(func() {
				atomic.StoreInt32(&crashedWorker, runtimeNode.id)
				triggered = true
				go runtimeNode.Stop()
			})
			return !triggered
		})
	}

	leader := waitForRuntimeLeader(t, nodes, 5*time.Second)
	resp := submitTaskOverRPC(t, leader.addr, "worker-crash-before-claim")
	if !resp.Success {
		t.Fatalf("expected task submission to succeed, leader hint=%d", resp.LeaderId)
	}

	assignedTo := waitForTaskStatusAcrossNodes(t, nodes, resp.TaskId, raftpb.TaskStatus_COMPLETED, 10*time.Second)
	if got := atomic.LoadInt32(&crashedWorker); got <= 0 {
		t.Fatal("expected one worker to crash before claiming")
	} else if assignedTo == got {
		t.Fatalf("task completed by crashed worker %d", got)
	}
	if got := atomic.LoadInt32(&executions); got != 1 {
		t.Fatalf("task executed %d times, want exactly 1", got)
	}
}

func TestCombinedRuntimeLeaderCrashAfterClaimBeforeCommit(t *testing.T) {
	var executions int32
	claimGate := make(chan struct{})
	var hookTriggered int32
	var crashOnce sync.Once

	nodes := startCombinedRuntimeCluster(t, 3, "", func(id int32) maekawa.TaskExecutor {
		return func(_ context.Context, task *models.Task) (string, error) {
			atomic.AddInt32(&executions, 1)
			time.Sleep(25 * time.Millisecond)
			return fmt.Sprintf("done-by-%d:%s", id, task.ID), nil
		}
	})

	for _, runtimeNode := range nodes {
		runtimeNode.worker.SetBeforeClaimHook(func(task *models.Task) bool {
			<-claimGate
			return true
		})
	}

	leader := waitForRuntimeLeader(t, nodes, 5*time.Second)
	resp := submitTaskOverRPC(t, leader.addr, "leader-crash-after-claim")
	if !resp.Success {
		t.Fatalf("expected task submission to succeed, leader hint=%d", resp.LeaderId)
	}

	waitForTaskStatusAcrossNodes(t, nodes, resp.TaskId, raftpb.TaskStatus_PENDING, 5*time.Second)
	leader.node.SetBeforeReplicateHook(func(event models.TaskEvent) bool {
		if event.Type != models.EventClaimed {
			return true
		}
		triggered := false
		crashOnce.Do(func() {
			triggered = true
			atomic.StoreInt32(&hookTriggered, 1)
			go leader.Stop()
		})
		return !triggered
	})

	close(claimGate)
	newLeader := waitForNewRuntimeLeader(t, nodes, leader.id, 8*time.Second)

	if atomic.LoadInt32(&hookTriggered) == 0 {
		t.Fatal("expected leader crash hook to trigger on claimed event")
	}
	waitForTaskStatusAcrossNodes(t, nodes, resp.TaskId, raftpb.TaskStatus_PENDING, 5*time.Second)
	for _, runtimeNode := range activeRuntimeNodes(nodes) {
		found, status, assignedTo := taskStatusOnNode(t, runtimeNode, resp.TaskId)
		if !found {
			t.Fatalf("task %s missing on node %d after leader crash", resp.TaskId, runtimeNode.id)
		}
		if status != raftpb.TaskStatus_PENDING {
			t.Fatalf("task %s reached unexpected status %s on node %d", resp.TaskId, status.String(), runtimeNode.id)
		}
		if assignedTo != 0 {
			t.Fatalf("task %s has committed assignee %d on node %d, want unclaimed", resp.TaskId, assignedTo, runtimeNode.id)
		}
	}

	waitForRuntimeQuorumsToExcludeNode(t, nodes, leader.id, 5*time.Second)
	followUp := submitTaskOverRPC(t, newLeader.addr, "post-claim-crash-follow-up")
	if !followUp.Success {
		t.Fatalf("expected follow-up submission to succeed, leader hint=%d", followUp.LeaderId)
	}
	waitForTaskStatusAcrossNodes(t, nodes, followUp.TaskId, raftpb.TaskStatus_COMPLETED, 8*time.Second)
	if got := atomic.LoadInt32(&executions); got < 1 || got > 2 {
		t.Fatalf("tasks executed %d times, want 1 or 2 total executions after recovery", got)
	}
}

func TestCombinedRuntimeConflictingClaimsRace(t *testing.T) {
	nodes := startCombinedRuntimeClusterWithOptions(t, 3, combinedRuntimeOptions{
		startTaskLoops: false,
	})

	leader := waitForRuntimeLeader(t, nodes, 5*time.Second)
	resp := submitTaskOverRPC(t, leader.addr, "claim-race")
	if !resp.Success {
		t.Fatalf("expected task submission to succeed, leader hint=%d", resp.LeaderId)
	}
	waitForTaskStatusAcrossNodes(t, nodes, resp.TaskId, raftpb.TaskStatus_PENDING, 5*time.Second)

	active := activeRuntimeNodes(nodes)
	if len(active) < 2 {
		t.Fatalf("need at least 2 active nodes for claim race, got %d", len(active))
	}
	claimants := active[:2]
	start := make(chan struct{})
	type claimResult struct {
		workerID int32
		ok       bool
		err      error
	}
	results := make(chan claimResult, len(claimants))

	for _, claimant := range claimants {
		claimant := claimant
		go func() {
			<-start
			ok, err := claimant.node.ClaimTask(resp.TaskId, claimant.id)
			results <- claimResult{workerID: claimant.id, ok: ok, err: err}
		}()
	}
	close(start)

	var winnerID int32
	successes := 0
	for range claimants {
		result := <-results
		if result.err != nil {
			t.Fatalf("claim from worker %d failed: %v", result.workerID, result.err)
		}
		if result.ok {
			successes++
			winnerID = result.workerID
		}
	}
	if successes != 1 {
		t.Fatalf("expected exactly one successful claim, got %d", successes)
	}

	var winnerNode *combinedRuntimeNode
	for _, runtimeNode := range nodes {
		if runtimeNode.id == winnerID {
			winnerNode = runtimeNode
			break
		}
	}
	if winnerNode == nil {
		t.Fatalf("winner node %d not found", winnerID)
	}

	if err := winnerNode.node.ReportTaskSuccess(resp.TaskId, winnerID, "ok"); err != nil {
		t.Fatalf("report success from winner %d: %v", winnerID, err)
	}

	assignedTo := waitForTaskStatusAcrossNodes(t, nodes, resp.TaskId, raftpb.TaskStatus_COMPLETED, 8*time.Second)
	if assignedTo != winnerID {
		t.Fatalf("completed task assigned to %d, want winner %d", assignedTo, winnerID)
	}
}

func TestCombinedRuntimeManyTaskFailoverRun(t *testing.T) {
	var mu sync.Mutex
	executions := make(map[string]int)

	nodes := startCombinedRuntimeCluster(t, 3, "", func(id int32) maekawa.TaskExecutor {
		return func(_ context.Context, task *models.Task) (string, error) {
			mu.Lock()
			executions[task.ID]++
			mu.Unlock()
			time.Sleep(80 * time.Millisecond)
			return fmt.Sprintf("done-by-%d:%s", id, task.ID), nil
		}
	})

	leader := waitForRuntimeLeader(t, nodes, 5*time.Second)
	completedBeforeFailover := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		resp := submitTaskOverRPC(t, leader.addr, fmt.Sprintf("batch-%d", i))
		if !resp.Success {
			t.Fatalf("expected task submission %d to succeed, leader hint=%d", i, resp.LeaderId)
		}
		completedBeforeFailover = append(completedBeforeFailover, resp.TaskId)
	}

	for _, taskID := range completedBeforeFailover {
		waitForTaskStatusAcrossNodes(t, nodes, taskID, raftpb.TaskStatus_COMPLETED, 8*time.Second)
	}

	leader.Stop()

	newLeader := waitForNewRuntimeLeader(t, nodes, leader.id, 8*time.Second)
	waitForRuntimeQuorumsToExcludeNode(t, nodes, leader.id, 5*time.Second)
	completedAfterFailover := make([]string, 0, 5)
	for i := 3; i < 8; i++ {
		resp := submitTaskOverRPC(t, newLeader.addr, fmt.Sprintf("batch-%d", i))
		if !resp.Success {
			t.Fatalf("expected post-failover task submission %d to succeed, leader hint=%d", i, resp.LeaderId)
		}
		completedAfterFailover = append(completedAfterFailover, resp.TaskId)
		waitForTaskStatusAcrossNodes(t, nodes, resp.TaskId, raftpb.TaskStatus_COMPLETED, 15*time.Second)
	}

	for _, taskID := range append(append([]string{}, completedBeforeFailover...), completedAfterFailover...) {
		for _, runtimeNode := range activeRuntimeNodes(nodes) {
			found, status, _ := taskStatusOnNode(t, runtimeNode, taskID)
			if !found || status != raftpb.TaskStatus_COMPLETED {
				t.Fatalf("task %s not completed on node %d", taskID, runtimeNode.id)
			}
		}
	}

	mu.Lock()
	defer mu.Unlock()
	for _, taskID := range append(append([]string{}, completedBeforeFailover...), completedAfterFailover...) {
		if executions[taskID] != 1 {
			t.Fatalf("task %s executed %d times, want exactly 1", taskID, executions[taskID])
		}
	}
}
