package raft

import (
	"context"
	"testing"
	"time"

	raftpb "raft-maekawa-sync/api/raft"
	"raft-maekawa-sync/internal/rpc"
)

func TestRequestVoteGrantsFreshCandidate(t *testing.T) {
	n := NewNode(1, ":5001", map[int32]string{2: ":5002"}, nil)
	resp, err := n.RequestVote(context.Background(), &raftpb.RequestVoteRequest{
		Term: 1, CandidateId: 2, LastLogIndex: 0, LastLogTerm: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.VoteGranted {
		t.Fatal("expected vote granted")
	}
	if resp.Term != 1 {
		t.Fatalf("expected term 1, got %d", resp.Term)
	}
}

func TestRequestVoteRejectsStaleTerm(t *testing.T) {
	n := NewNode(1, ":5001", nil, nil)
	n.mu.Lock()
	n.currentTerm = 5
	n.mu.Unlock()

	resp, _ := n.RequestVote(context.Background(), &raftpb.RequestVoteRequest{
		Term: 3, CandidateId: 2,
	})
	if resp.VoteGranted {
		t.Fatal("should reject stale term")
	}
	if resp.Term != 5 {
		t.Fatal("should return our current term")
	}
}

func TestRequestVoteOnlyOneVotePerTerm(t *testing.T) {
	n := NewNode(1, ":5001", nil, nil)
	_, _ = n.RequestVote(context.Background(), &raftpb.RequestVoteRequest{Term: 1, CandidateId: 2})

	resp, _ := n.RequestVote(context.Background(), &raftpb.RequestVoteRequest{Term: 1, CandidateId: 3})
	if resp.VoteGranted {
		t.Fatal("should not grant second vote in same term")
	}
}

func TestRequestVoteStepDownOnHigherTerm(t *testing.T) {
	n := NewNode(1, ":5001", nil, nil)
	n.mu.Lock()
	n.role = Leader
	n.currentTerm = 3
	n.mu.Unlock()

	_, _ = n.RequestVote(context.Background(), &raftpb.RequestVoteRequest{Term: 7, CandidateId: 2})

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Follower {
		t.Fatal("should step down to follower")
	}
	if n.currentTerm != 7 {
		t.Fatal("should update term")
	}
}

func TestRequestVoteRejectsStaleLog(t *testing.T) {
	n := NewNode(1, ":5001", nil, nil)
	n.mu.Lock()
	n.currentTerm = 2
	n.log = append(n.log, &raftpb.LogEntry{Term: 2, Index: 1, Command: "x"})
	n.mu.Unlock()

	resp, _ := n.RequestVote(context.Background(), &raftpb.RequestVoteRequest{
		Term: 3, CandidateId: 2, LastLogTerm: 0, LastLogIndex: 0,
	})
	if resp.VoteGranted {
		t.Fatal("should reject candidate with stale log")
	}
}

func TestLeaderElection3Nodes(t *testing.T) {
	peers := map[int32]string{1: "127.0.0.1:15001", 2: "127.0.0.1:15002", 3: "127.0.0.1:15003"}
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

	leaders := 0
	for _, n := range nodes {
		n.mu.Lock()
		if n.role == Leader {
			leaders++
		}
		n.mu.Unlock()
	}
	if leaders != 1 {
		t.Fatalf("expected 1 leader, got %d", leaders)
	}
}

func TestHeartbeatPreventsNewElection(t *testing.T) {
	peers := map[int32]string{1: "127.0.0.1:15101", 2: "127.0.0.1:15102", 3: "127.0.0.1:15103"}
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

	time.Sleep(2 * time.Second)
	time.Sleep(2 * time.Second)

	leaders := 0
	for _, n := range nodes {
		n.mu.Lock()
		if n.role == Leader {
			leaders++
		}
		n.mu.Unlock()
	}
	if leaders != 1 {
		t.Fatalf("expected 1 leader after heartbeats, got %d", leaders)
	}
}

func TestSingleNodeElectionBecomesLeader(t *testing.T) {
	n := NewNode(1, "127.0.0.1:16001", nil, nil)
	n.electionMin = 10 * time.Millisecond
	n.electionMax = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Run(ctx)

	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		n.mu.Lock()
		isLeader := n.role == Leader
		n.mu.Unlock()
		if isLeader {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("single node did not become leader")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
