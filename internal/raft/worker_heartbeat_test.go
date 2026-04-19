package raft

import (
	"context"
	"testing"
	"time"

	raftpb "raft-maekawa-sync/api/raft"
)

func TestWorkerHeartbeatMarksTrackedWorkerUp(t *testing.T) {
	n := NewNode(1, ":5001", nil, nil)
	n.SetManagedWorkers([]int32{4})

	n.mu.Lock()
	n.role = Leader
	n.leaderID = n.id
	n.state.ActiveWorkers[4] = false
	n.workerHeartbeats[4] = time.Time{}
	n.mu.Unlock()

	resp, err := n.WorkerHeartbeat(context.Background(), &raftpb.WorkerHeartbeatRequest{WorkerId: 4})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Success {
		t.Fatalf("expected heartbeat success, got %+v", resp)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.state.ActiveWorkers[4] {
		t.Fatal("worker 4 should be marked up after heartbeat")
	}
	if n.workerHeartbeats[4].IsZero() {
		t.Fatal("worker 4 heartbeat timestamp should be recorded")
	}
}

func TestHeartbeatTimeoutMarksWorkerDown(t *testing.T) {
	n := NewNode(1, ":5001", nil, nil)
	n.SetManagedWorkers([]int32{4})

	n.mu.Lock()
	n.role = Leader
	n.leaderID = n.id
	n.workerHeartbeatTimeout = 20 * time.Millisecond
	n.workerHeartbeatCheckIntv = 5 * time.Millisecond
	n.state.ActiveWorkers[4] = true
	n.workerHeartbeats[4] = time.Now().Add(-time.Second)
	n.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.runWorkerHeartbeatLoop(ctx)

	deadline := time.Now().Add(200 * time.Millisecond)
	for {
		n.mu.Lock()
		alive := n.state.ActiveWorkers[4]
		n.mu.Unlock()
		if !alive {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("worker 4 should be marked down after heartbeat timeout")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
