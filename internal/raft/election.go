package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"

	raftpb "raft-maekawa-sync/api/raft"
	"raft-maekawa-sync/internal/rpc"
)

func (n *Node) Run(ctx context.Context) {
	go n.runElectionTimer(ctx)
	<-ctx.Done()
}

func (n *Node) runElectionTimer(ctx context.Context) {
	for {
		delta := n.electionMax - n.electionMin
		timeout := n.electionMin
		if delta > 0 {
			timeout += time.Duration(rand.Int63n(int64(delta)))
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(timeout):
		}

		n.mu.Lock()
		elapsed := time.Since(n.electionReset)
		role := n.role
		n.mu.Unlock()

		if role != Leader && elapsed >= timeout {
			n.startElection(ctx)
		}
	}
}

func (n *Node) startElection(ctx context.Context) {
	n.mu.Lock()
	n.currentTerm++
	n.role = Candidate
	n.leaderID = -1
	n.votedFor = n.id
	n.electionReset = time.Now()

	term := n.currentTerm
	lastLogIndex := int32(len(n.log))
	lastLogTerm := int32(0)
	if lastLogIndex > 0 {
		lastLogTerm = n.log[lastLogIndex-1].Term
	}

	peerIDs := make([]int32, 0, len(n.peers))
	for peerID := range n.peers {
		peerIDs = append(peerIDs, peerID)
	}
	majority := (len(n.peers)+1)/2 + 1
	n.mu.Unlock()

	votes := 1
	var votesMu sync.Mutex
	var wg sync.WaitGroup

	for _, peerID := range peerIDs {
		wg.Add(1)
		go func(pid int32) {
			defer wg.Done()

			client, err := n.getPeerClient(pid)
			if err != nil {
				return
			}

			resp, err := client.RequestVote(ctx, &raftpb.RequestVoteRequest{
				Term:         term,
				CandidateId:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil || resp == nil {
				return
			}

			if resp.Term > term {
				n.becomeFollower(resp.Term, -1)
				return
			}

			if !resp.VoteGranted {
				return
			}

			votesMu.Lock()
			votes++
			currentVotes := votes
			votesMu.Unlock()

			if currentVotes < majority {
				return
			}

			n.mu.Lock()
			if n.role == Candidate && n.currentTerm == term {
				n.mu.Unlock()
				n.becomeLeader()
				go n.runHeartbeatLoop(ctx)
				return
			}
			n.mu.Unlock()
		}(peerID)
	}

	wg.Wait()
}

func (n *Node) runHeartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(n.heartbeatIntv)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.mu.Lock()
			if n.role != Leader {
				n.mu.Unlock()
				return
			}
			peerIDs := make([]int32, 0, len(n.peers))
			for peerID := range n.peers {
				peerIDs = append(peerIDs, peerID)
			}
			n.mu.Unlock()

			for _, peerID := range peerIDs {
				go n.replicateToPeer(ctx, peerID)
			}
		}
	}
}

func (n *Node) getPeerClient(peerID int32) (raftpb.RaftClient, error) {
	n.mu.Lock()
	if client, ok := n.peerClients[peerID]; ok {
		n.mu.Unlock()
		return client, nil
	}
	addr, ok := n.peers[peerID]
	n.mu.Unlock()
	if !ok {
		return nil, context.DeadlineExceeded
	}

	conn, err := rpc.Dial(addr)
	if err != nil {
		return nil, err
	}
	client := raftpb.NewRaftClient(conn)

	n.mu.Lock()
	n.peerConns[peerID] = conn
	n.peerClients[peerID] = client
	n.mu.Unlock()

	return client, nil
}

func (n *Node) isCandidateLogUpToDate(lastLogTerm, lastLogIndex int32) bool {
	ourLastTerm := n.lastLogTerm()
	if lastLogTerm > ourLastTerm {
		return true
	}
	if lastLogTerm < ourLastTerm {
		return false
	}
	return lastLogIndex >= n.lastLogIndex()
}

func (n *Node) RequestVote(_ context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	n.mu.Lock()
	if req.Term < n.currentTerm {
		term := n.currentTerm
		n.mu.Unlock()
		return &raftpb.RequestVoteResponse{Term: term, VoteGranted: false}, nil
	}
	higherTerm := req.Term > n.currentTerm
	n.mu.Unlock()

	if higherTerm {
		n.becomeFollower(req.Term, -1)
	}

	upToDate := n.isCandidateLogUpToDate(req.GetLastLogTerm(), req.GetLastLogIndex())

	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term < n.currentTerm {
		return &raftpb.RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}, nil
	}

	voteGranted := false
	if (n.votedFor == -1 || n.votedFor == req.GetCandidateId()) && upToDate {
		n.votedFor = req.GetCandidateId()
		n.electionReset = time.Now()
		voteGranted = true
	}

	return &raftpb.RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: voteGranted,
	}, nil
}
