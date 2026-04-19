package raft

import (
	"context"
	"testing"
	"time"

	raftpb "raft-maekawa-sync/api/raft"
	"raft-maekawa-sync/internal/models"
	"raft-maekawa-sync/internal/rpc"
)

func TestApplyAssignedEvent(t *testing.T) {
	n := NewNode(1, ":5001", nil, nil)
	event := models.TaskEvent{
		Type:   models.EventAssigned,
		TaskID: "t1",
		Task:   &models.Task{ID: "t1", Data: "render_001"},
	}
	cmd, _ := event.Encode()

	n.mu.Lock()
	n.appendEntry(cmd)
	n.commitIndex = 1
	n.applyCommittedEntries()
	task := n.state.Tasks["t1"]
	n.mu.Unlock()

	if task == nil {
		t.Fatal("task should exist after assigned event")
	}
	if task.Data != "render_001" {
		t.Fatalf("wrong data: %s", task.Data)
	}
	if task.Status != models.EventAssigned {
		t.Fatal("wrong status")
	}
}

func TestApplyDoneAfterClaimed(t *testing.T) {
	n := NewNode(1, ":5001", nil, nil)
	events := []models.TaskEvent{
		{Type: models.EventAssigned, TaskID: "t1", Task: &models.Task{ID: "t1", Data: "x"}},
		{Type: models.EventClaimed, TaskID: "t1", WorkerID: 2},
		{Type: models.EventDone, TaskID: "t1", WorkerID: 2, Result: "ok"},
	}

	n.mu.Lock()
	for _, e := range events {
		cmd, _ := e.Encode()
		n.appendEntry(cmd)
	}
	n.commitIndex = 3
	n.applyCommittedEntries()
	task := n.state.Tasks["t1"]
	n.mu.Unlock()

	if task.Status != models.EventDone {
		t.Fatalf("expected done, got %s", task.Status)
	}
	if task.Result != "ok" {
		t.Fatal("wrong result")
	}
	if task.AssignedTo != 2 {
		t.Fatal("wrong worker")
	}
}

func TestApplyAssignedResetsStaleClaim(t *testing.T) {
	n := NewNode(1, ":5001", nil, nil)
	events := []models.TaskEvent{
		{Type: models.EventAssigned, TaskID: "t1", Task: &models.Task{ID: "t1", Data: "x"}},
		{Type: models.EventClaimed, TaskID: "t1", WorkerID: 2},
		{Type: models.EventAssigned, TaskID: "t1", Task: &models.Task{ID: "t1", Data: "x"}},
	}

	n.mu.Lock()
	for _, e := range events {
		cmd, _ := e.Encode()
		n.appendEntry(cmd)
	}
	n.commitIndex = int32(len(events))
	n.applyCommittedEntries()
	task := n.state.Tasks["t1"]
	n.mu.Unlock()

	if task.Status != models.EventAssigned {
		t.Fatalf("expected reassigned task to be pending, got %s", task.Status)
	}
	if task.AssignedTo != 0 {
		t.Fatalf("expected reassigned task to clear assignee, got %d", task.AssignedTo)
	}
}

func TestApplyWorkerUpDown(t *testing.T) {
	n := NewNode(1, ":5001", nil, nil)
	events := []models.TaskEvent{
		{Type: models.EventWorkerUp, WorkerID: 5},
		{Type: models.EventWorkerDown, WorkerID: 5},
	}

	n.mu.Lock()
	for _, e := range events {
		cmd, _ := e.Encode()
		n.appendEntry(cmd)
	}
	n.commitIndex = 2
	n.applyCommittedEntries()
	alive := n.state.ActiveWorkers[5]
	n.mu.Unlock()

	if alive {
		t.Fatal("worker 5 should be down")
	}
}

func TestApplyCallsApplier(t *testing.T) {
	fake := &fakeNotifier{}
	n := NewNode(1, ":5001", nil, fake)
	event := models.TaskEvent{Type: models.EventWorkerUp, WorkerID: 9}
	cmd, _ := event.Encode()

	n.mu.Lock()
	n.appendEntry(cmd)
	n.commitIndex = 1
	n.applyCommittedEntries()
	n.mu.Unlock()

	if len(fake.up) != 1 || fake.up[0] != 9 {
		t.Fatal("applier was not called with worker up event")
	}
}

func TestAppendEntriesHeartbeat(t *testing.T) {
	n := NewNode(1, ":5001", nil, nil)
	n.mu.Lock()
	n.currentTerm = 1
	n.mu.Unlock()

	resp, err := n.AppendEntries(context.Background(), &raftpb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: nil, LeaderCommit: 0,
	})
	if err != nil || !resp.Success {
		t.Fatal("heartbeat should succeed")
	}
}

func TestAppendEntriesRejectsStaleTerm(t *testing.T) {
	n := NewNode(1, ":5001", nil, nil)
	n.mu.Lock()
	n.currentTerm = 5
	n.mu.Unlock()

	resp, _ := n.AppendEntries(context.Background(), &raftpb.AppendEntriesRequest{Term: 3, LeaderId: 2})
	if resp.Success {
		t.Fatal("should reject stale leader")
	}
	if resp.Term != 5 {
		t.Fatal("should return our current term")
	}
}

func TestAppendEntriesConflictTruncate(t *testing.T) {
	n := NewNode(1, ":5001", nil, nil)
	n.mu.Lock()
	n.currentTerm = 2
	n.log = []*raftpb.LogEntry{&raftpb.LogEntry{Term: 1, Index: 1, Command: "stale"}}
	n.mu.Unlock()

	resp, _ := n.AppendEntries(context.Background(), &raftpb.AppendEntriesRequest{
		Term: 2, LeaderId: 2,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      []*raftpb.LogEntry{&raftpb.LogEntry{Term: 2, Index: 1, Command: "correct"}},
		LeaderCommit: 1,
	})
	if !resp.Success {
		t.Fatal("should succeed")
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.log) != 1 || n.log[0].Command != "correct" {
		t.Fatal("log not overwritten")
	}
	if n.commitIndex != 1 {
		t.Fatal("commitIndex should advance")
	}
}

func TestReplicationOneEntry(t *testing.T) {
	peers := map[int32]string{1: "127.0.0.1:15201", 2: "127.0.0.1:15202", 3: "127.0.0.1:15203"}
	nodes := make([]*Node, 0, 3)
	servers := make([]*rpc.Server, 0, 3)

	for id := int32(1); id <= 3; id++ {
		myPeers := map[int32]string{}
		for pid, addr := range peers {
			if pid != id {
				myPeers[pid] = addr
			}
		}

		n := NewNode(id, peers[id], myPeers, nil)
		s := rpc.NewServer()
		raftpb.RegisterRaftServer(s.GRPC(), n)
		if err := s.Start(peers[id]); err != nil {
			t.Fatalf("start server %d: %v", id, err)
		}
		nodes = append(nodes, n)
		servers = append(servers, s)
	}
	defer func() {
		for _, s := range servers {
			s.Stop()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, n := range nodes {
		go n.Run(ctx)
	}

	time.Sleep(3 * time.Second)

	var leader *Node
	for _, n := range nodes {
		n.mu.Lock()
		isLeader := n.role == Leader
		n.mu.Unlock()
		if isLeader {
			leader = n
			break
		}
	}
	if leader == nil {
		t.Fatal("expected a leader")
	}

	leader.mu.Lock()
	leader.appendEntry("x")
	leader.mu.Unlock()

	for peerID := range leader.peers {
		go leader.replicateToPeer(ctx, peerID)
	}

	time.Sleep(1 * time.Second)

	for i, n := range nodes {
		n.mu.Lock()
		logLen := len(n.log)
		commit := n.commitIndex
		n.mu.Unlock()
		if logLen != 1 {
			t.Fatalf("node %d log len=%d, want 1", i+1, logLen)
		}
		if commit != 1 {
			t.Fatalf("node %d commitIndex=%d, want 1", i+1, commit)
		}
	}
}

func TestSubmitTaskFollowerRedirects(t *testing.T) {
	n := NewNode(1, ":5001", map[int32]string{2: ":5002"}, nil)
	n.mu.Lock()
	n.leaderID = 2
	n.mu.Unlock()

	resp, _ := n.SubmitTask(context.Background(), &raftpb.SubmitTaskRequest{Data: "hello"})
	if resp.Success {
		t.Fatal("follower should not accept writes")
	}
	if resp.LeaderId != 2 {
		t.Fatal("should hint leader id")
	}
}

func TestSubmitTaskLeaderSucceeds(t *testing.T) {
	peers := map[int32]string{1: "127.0.0.1:15301", 2: "127.0.0.1:15302", 3: "127.0.0.1:15303"}
	nodes := make([]*Node, 0, 3)
	servers := make([]*rpc.Server, 0, 3)

	for id := int32(1); id <= 3; id++ {
		myPeers := map[int32]string{}
		for pid, addr := range peers {
			if pid != id {
				myPeers[pid] = addr
			}
		}

		n := NewNode(id, peers[id], myPeers, nil)
		s := rpc.NewServer()
		raftpb.RegisterRaftServer(s.GRPC(), n)
		if err := s.Start(peers[id]); err != nil {
			t.Fatalf("start server %d: %v", id, err)
		}
		nodes = append(nodes, n)
		servers = append(servers, s)
	}
	defer func() {
		for _, s := range servers {
			s.Stop()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, n := range nodes {
		go n.Run(ctx)
	}

	time.Sleep(3 * time.Second)

	var leader *Node
	for _, n := range nodes {
		n.mu.Lock()
		isLeader := n.role == Leader
		n.mu.Unlock()
		if isLeader {
			leader = n
			break
		}
	}
	if leader == nil {
		t.Fatal("expected a leader")
	}

	resp, err := leader.SubmitTask(ctx, &raftpb.SubmitTaskRequest{Data: "render_frame_042"})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Success {
		t.Fatal("leader should accept writes")
	}
	if resp.TaskId == "" {
		t.Fatal("task id should not be empty")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		allReplicated := true
		for _, n := range nodes {
			stateResp, err := n.GetState(ctx, &raftpb.GetStateRequest{})
			if err != nil {
				t.Fatal(err)
			}
			if len(stateResp.Tasks) != 1 || stateResp.Tasks[0].Status != raftpb.TaskStatus_PENDING {
				allReplicated = false
				break
			}
		}
		if allReplicated {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("task did not replicate to all nodes in time")
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestRecoverStaleClaimedTaskForDownWorker(t *testing.T) {
	n := NewNode(1, ":5001", nil, nil)
	n.taskClaimTimeout = 10 * time.Millisecond

	assigned := models.TaskEvent{
		Type:          models.EventAssigned,
		TaskID:        "t1",
		Task:          &models.Task{ID: "t1", Data: "render_001"},
		EventUnixNano: time.Now().Add(-50 * time.Millisecond).UnixNano(),
	}
	claimed := models.TaskEvent{
		Type:          models.EventClaimed,
		TaskID:        "t1",
		WorkerID:      5,
		EventUnixNano: time.Now().Add(-50 * time.Millisecond).UnixNano(),
	}

	n.mu.Lock()
	n.currentTerm = 1
	n.role = Leader
	n.leaderID = n.id
	for _, e := range []models.TaskEvent{assigned, claimed} {
		cmd, _ := e.Encode()
		n.appendEntry(cmd)
	}
	n.commitIndex = 2
	n.applyCommittedEntries()
	n.state.ActiveWorkers[5] = false
	n.mu.Unlock()

	n.recoverStaleTasks(context.Background())

	n.mu.Lock()
	task := n.state.Tasks["t1"]
	logLen := len(n.log)
	n.mu.Unlock()

	if logLen != 3 {
		t.Fatalf("expected recovery to append one entry, got log len %d", logLen)
	}
	if task.Status != models.EventAssigned {
		t.Fatalf("expected task to be requeued, got %s", task.Status)
	}
	if task.AssignedTo != 0 {
		t.Fatalf("expected task assignee to be cleared after recovery, got %d", task.AssignedTo)
	}
}

func TestDoesNotRecoverClaimedTaskForLiveWorker(t *testing.T) {
	n := NewNode(1, ":5001", nil, nil)
	n.taskClaimTimeout = 10 * time.Millisecond

	assigned := models.TaskEvent{
		Type:          models.EventAssigned,
		TaskID:        "t1",
		Task:          &models.Task{ID: "t1", Data: "render_001"},
		EventUnixNano: time.Now().Add(-50 * time.Millisecond).UnixNano(),
	}
	claimed := models.TaskEvent{
		Type:          models.EventClaimed,
		TaskID:        "t1",
		WorkerID:      5,
		EventUnixNano: time.Now().Add(-50 * time.Millisecond).UnixNano(),
	}

	n.mu.Lock()
	n.currentTerm = 1
	n.role = Leader
	n.leaderID = n.id
	for _, e := range []models.TaskEvent{assigned, claimed} {
		cmd, _ := e.Encode()
		n.appendEntry(cmd)
	}
	n.commitIndex = 2
	n.applyCommittedEntries()
	n.state.ActiveWorkers[5] = true
	n.mu.Unlock()

	n.recoverStaleTasks(context.Background())

	n.mu.Lock()
	task := n.state.Tasks["t1"]
	logLen := len(n.log)
	n.mu.Unlock()

	if logLen != 2 {
		t.Fatalf("expected no recovery entry for live worker, got log len %d", logLen)
	}
	if task.Status != models.EventClaimed {
		t.Fatalf("expected task to remain claimed, got %s", task.Status)
	}
}

func TestGetStateReflectsCommittedTask(t *testing.T) {
	n := NewNode(1, ":5001", nil, nil)
	event := models.TaskEvent{Type: models.EventAssigned, TaskID: "t99", Task: &models.Task{ID: "t99", Data: "y"}}
	cmd, _ := event.Encode()

	n.mu.Lock()
	n.appendEntry(cmd)
	n.commitIndex = 1
	n.applyCommittedEntries()
	n.mu.Unlock()

	resp, _ := n.GetState(context.Background(), &raftpb.GetStateRequest{})
	if len(resp.Tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(resp.Tasks))
	}
	if resp.Tasks[0].TaskId != "t99" {
		t.Fatal("wrong task id")
	}
	if resp.Tasks[0].Status != raftpb.TaskStatus_PENDING {
		t.Fatal("wrong status")
	}
}
