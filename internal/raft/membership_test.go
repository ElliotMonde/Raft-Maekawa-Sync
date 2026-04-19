package raft

import (
	"context"
	"testing"
	"time"

	raftpb "raft-maekawa-sync/api/raft"
	"raft-maekawa-sync/internal/models"
	"raft-maekawa-sync/internal/rpc"
)

func TestMembershipClaimTaskOnlyOnce(t *testing.T) {
	nodes, servers, cancel := startTestCluster(t, 15401)
	defer cancel()
	defer stopServers(servers)

	leader := waitForLeader(t, nodes, 4*time.Second)
	resp, err := leader.SubmitTask(context.Background(), &raftpb.SubmitTaskRequest{Data: "work"})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Success {
		t.Fatal("leader should accept submitted task")
	}

	ok, err := leader.ClaimTask(resp.TaskId, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("first claim should succeed")
	}

	ok, err = leader.ClaimTask(resp.TaskId, 3)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("second claim should be rejected")
	}
}

func TestMembershipClaimTaskAfterDoneRejected(t *testing.T) {
	nodes, servers, cancel := startTestCluster(t, 15501)
	defer cancel()
	defer stopServers(servers)

	leader := waitForLeader(t, nodes, 4*time.Second)
	resp, err := leader.SubmitTask(context.Background(), &raftpb.SubmitTaskRequest{Data: "work"})
	if err != nil {
		t.Fatal(err)
	}

	ok, err := leader.ClaimTask(resp.TaskId, 2)
	if err != nil || !ok {
		t.Fatal("claim should succeed before done")
	}
	if err := leader.ReportTaskSuccess(resp.TaskId, 2, "ok"); err != nil {
		t.Fatal(err)
	}

	ok, err = leader.ClaimTask(resp.TaskId, 3)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("claim after done should be rejected")
	}
}

func TestMembershipActiveMembersReflectsEvents(t *testing.T) {
	n := NewNode(1, ":5001", nil, nil)
	e := models.TaskEvent{Type: models.EventWorkerUp, WorkerID: 7}
	cmd, _ := e.Encode()

	n.mu.Lock()
	n.appendEntry(cmd)
	n.commitIndex = 1
	n.applyCommittedEntries()
	n.mu.Unlock()

	members := n.ActiveMembers()
	found := false
	for _, id := range members {
		if id == 7 {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("worker 7 should be in active members")
	}
}

func startTestCluster(t *testing.T, basePort int) ([]*Node, []*rpc.Server, context.CancelFunc) {
	t.Helper()
	peers := map[int32]string{
		1: "127.0.0.1:" + itoa(basePort),
		2: "127.0.0.1:" + itoa(basePort+1),
		3: "127.0.0.1:" + itoa(basePort+2),
	}
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

	ctx, cancel := context.WithCancel(context.Background())
	for _, n := range nodes {
		go n.Run(ctx)
	}
	return nodes, servers, cancel
}

func waitForLeader(t *testing.T, nodes []*Node, timeout time.Duration) *Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for _, n := range nodes {
			n.mu.Lock()
			isLeader := n.role == Leader
			n.mu.Unlock()
			if isLeader {
				return n
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("no leader elected")
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func stopServers(servers []*rpc.Server) {
	for _, s := range servers {
		s.Stop()
	}
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	n := v
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	buf := [20]byte{}
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + (n % 10))
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}