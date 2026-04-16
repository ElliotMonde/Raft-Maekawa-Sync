package raft

import (
	"sync"
	"time"

	raftpb "raft-maekawa-sync/api/raft"
	"raft-maekawa-sync/internal/models"
	"google.golang.org/grpc"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type TaskRecord struct {
	ID         string
	Data       string
	Status     models.EventType
	AssignedTo int32
	Result     string
	Reason     string
}

type StateMachine struct {
	ActiveWorkers map[int32]bool
	Tasks         map[string]*TaskRecord
}

type Node struct {
	raftpb.UnimplementedRaftServer

	mu sync.Mutex

	id    int32
	addr  string
	peers map[int32]string

	role     Role
	leaderID int32

	currentTerm int32
	votedFor    int32
	log         []raftpb.LogEntry

	commitIndex int32
	lastApplied int32

	nextIndex  map[int32]int32
	matchIndex map[int32]int32

	electionReset time.Time
	electionMin   time.Duration
	electionMax   time.Duration
	heartbeatIntv time.Duration

	state   *StateMachine
	applier TaskEventApplier

	peerConns   map[int32]*grpc.ClientConn
	peerClients map[int32]raftpb.RaftClient
}

func NewNode(id int32, addr string, peers map[int32]string, applier TaskEventApplier) *Node {
	peersCopy := make(map[int32]string, len(peers))
	for peerID, peerAddr := range peers {
		peersCopy[peerID] = peerAddr
	}

	n := &Node{
		id:            id,
		addr:          addr,
		peers:         peersCopy,
		role:          Follower,
		leaderID:      -1,
		votedFor:      -1,
		electionReset: time.Now(),
		electionMin:   400 * time.Millisecond,
		electionMax:   800 * time.Millisecond,
		heartbeatIntv: 150 * time.Millisecond,
		state: &StateMachine{
			ActiveWorkers: map[int32]bool{id: true},
			Tasks:         make(map[string]*TaskRecord),
		},
		applier:     applier,
		peerConns:   make(map[int32]*grpc.ClientConn),
		peerClients: make(map[int32]raftpb.RaftClient),
	}

	return n
}

func (n *Node) lastLogIndex() int32 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return int32(len(n.log))
}

func (n *Node) lastLogTerm() int32 {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

func (n *Node) majority() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return (len(n.peers)+1)/2 + 1
}

func (n *Node) becomeFollower(term int32, leaderID int32) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.role = Follower
	n.currentTerm = term
	n.votedFor = -1
	n.leaderID = leaderID
	n.electionReset = time.Now()
}

func (n *Node) becomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.role = Leader
	n.leaderID = n.id
	n.nextIndex = make(map[int32]int32, len(n.peers))
	n.matchIndex = make(map[int32]int32, len(n.peers))
	next := int32(len(n.log)) + 1
	for peerID := range n.peers {
		n.nextIndex[peerID] = next
		n.matchIndex[peerID] = 0
	}
}
