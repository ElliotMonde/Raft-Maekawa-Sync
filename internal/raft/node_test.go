package raft

import "testing"

func TestNodeDefaults(t *testing.T) {
	n := NewNode(1, "127.0.0.1:5001", map[int32]string{2: "127.0.0.1:5002"}, nil)
	if n.id != 1 {
		t.Fatal("wrong id")
	}
	if n.role != Follower {
		t.Fatal("expected follower")
	}
	if n.votedFor != -1 {
		t.Fatal("expected no vote yet")
	}
	if n.leaderID != -1 {
		t.Fatal("expected unknown leader")
	}
	if n.currentTerm != 0 {
		t.Fatal("term should start at 0")
	}
	if len(n.state.ActiveWorkers) != 0 {
		t.Fatal("workers should not be tracked until configured")
	}
}

func TestMajority(t *testing.T) {
	// 3-node cluster (1 self + 2 peers): majority = 2
	n := NewNode(1, ":5001", map[int32]string{2: ":5002", 3: ":5003"}, nil)
	if n.majority() != 2 {
		t.Fatalf("got %d, want 2", n.majority())
	}
}

func TestLastLogEmpty(t *testing.T) {
	n := NewNode(1, ":5001", nil, nil)
	if n.lastLogIndex() != 0 {
		t.Fatal("empty log should have index 0")
	}
	if n.lastLogTerm() != 0 {
		t.Fatal("empty log should have term 0")
	}
}

func TestBecomeFollower(t *testing.T) {
	n := NewNode(1, ":5001", nil, nil)
	n.mu.Lock()
	n.currentTerm = 5
	n.role = Leader
	n.mu.Unlock()

	n.becomeFollower(7, 2)

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.currentTerm != 7 {
		t.Fatal("term should update")
	}
	if n.role != Follower {
		t.Fatal("should be follower")
	}
	if n.votedFor != -1 {
		t.Fatal("vote should clear on term change")
	}
	if n.leaderID != 2 {
		t.Fatal("leaderID should be recorded")
	}
}
