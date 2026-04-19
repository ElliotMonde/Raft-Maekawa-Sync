package raft

import (
	"log"
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
	ID                string
	Data              string
	Status            models.EventType
	AssignedTo        int32
	Result            string
	Reason            string
	UpdatedAtUnixNano int64
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
	log         []*raftpb.LogEntry

	commitIndex int32
	lastApplied int32

	nextIndex  map[int32]int32
	matchIndex map[int32]int32

	electionReset            time.Time
	electionMin              time.Duration
	electionMax              time.Duration
	heartbeatIntv            time.Duration
	workerHeartbeatTimeout   time.Duration
	workerHeartbeatCheckIntv time.Duration
	taskClaimTimeout         time.Duration
	taskRecoveryCheckIntv    time.Duration

	state   *StateMachine
	applier TaskEventApplier

	storagePath         string
	pendingLiveness     map[int32]bool
	beforeReplicate     func(models.TaskEvent) bool
	replayedToApplier   bool
	peerConns           map[int32]*grpc.ClientConn
	peerClients         map[int32]raftpb.RaftClient
	workerHeartbeats    map[int32]time.Time
	pendingTaskRecovery map[string]bool
}

func NewNode(id int32, addr string, peers map[int32]string, applier TaskEventApplier) *Node {
	peersCopy := make(map[int32]string, len(peers))
	for peerID, peerAddr := range peers {
		peersCopy[peerID] = peerAddr
	}

	activeWorkers := make(map[int32]bool, len(peers)+1)
	activeWorkers[id] = true
	for peerID := range peersCopy {
		activeWorkers[peerID] = true
	}

	n := &Node{
		id:                       id,
		addr:                     addr,
		peers:                    peersCopy,
		role:                     Follower,
		leaderID:                 -1,
		votedFor:                 -1,
		electionReset:            time.Now(),
		electionMin:              400 * time.Millisecond,
		electionMax:              800 * time.Millisecond,
		heartbeatIntv:            150 * time.Millisecond,
		workerHeartbeatTimeout:   3 * time.Second,
		workerHeartbeatCheckIntv: 500 * time.Millisecond,
		taskClaimTimeout:         4 * time.Second,
		taskRecoveryCheckIntv:    500 * time.Millisecond,
		state: &StateMachine{
			ActiveWorkers: activeWorkers,
			Tasks:         make(map[string]*TaskRecord),
		},
		applier:             applier,
		pendingLiveness:     make(map[int32]bool),
		peerConns:           make(map[int32]*grpc.ClientConn),
		peerClients:         make(map[int32]raftpb.RaftClient),
		workerHeartbeats:    make(map[int32]time.Time),
		pendingTaskRecovery: make(map[string]bool),
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
	prevRole := n.role
	prevTerm := n.currentTerm
	prevLeader := n.leaderID
	n.role = Follower
	n.currentTerm = term
	n.votedFor = -1
	n.leaderID = leaderID
	n.electionReset = time.Now()
	_ = n.persistLocked()
	if prevRole != Follower || prevTerm != term || prevLeader != leaderID {
		log.Printf("raft node %d: became FOLLOWER, leader=%d, term=%d", n.id, leaderID, term)
	}
}

func (n *Node) becomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()
	prevRole := n.role
	prevTerm := n.currentTerm
	n.role = Leader
	n.leaderID = n.id
	n.nextIndex = make(map[int32]int32, len(n.peers))
	n.matchIndex = make(map[int32]int32, len(n.peers))
	next := int32(len(n.log)) + 1
	for peerID := range n.peers {
		n.nextIndex[peerID] = next
		n.matchIndex[peerID] = 0
	}
	if prevRole != Leader {
		log.Printf("raft node %d: became LEADER for term %d", n.id, prevTerm)
	}
}

func (n *Node) SetApplier(applier TaskEventApplier) {
	n.mu.Lock()
	n.applier = applier
	if applier == nil || n.replayedToApplier {
		n.mu.Unlock()
		return
	}
	events := make([]models.TaskEvent, 0, n.lastApplied)
	for idx := int32(0); idx < n.lastApplied; idx++ {
		event, err := models.DecodeTaskEvent(n.log[idx].Command)
		if err != nil {
			continue
		}
		events = append(events, *event)
	}
	n.replayedToApplier = true
	n.mu.Unlock()

	for _, event := range events {
		applyTaskEventToMaekawa(event, applier)
	}
}

func (n *Node) SetBeforeReplicateHook(hook func(event models.TaskEvent) bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.beforeReplicate = hook
}

func (n *Node) SetManagedWorkers(workerIDs []int32) {
	n.mu.Lock()
	defer n.mu.Unlock()

	activeWorkers := make(map[int32]bool, len(workerIDs))
	for _, workerID := range workerIDs {
		activeWorkers[workerID] = n.state.ActiveWorkers[workerID]
	}
	n.state.ActiveWorkers = activeWorkers
	_ = n.persistLocked()
}
