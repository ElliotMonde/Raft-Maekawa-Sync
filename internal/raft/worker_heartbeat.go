package raft

import (
	"context"
	"time"

	raftpb "raft-maekawa-sync/api/raft"
)

func (n *Node) WorkerHeartbeat(ctx context.Context, req *raftpb.WorkerHeartbeatRequest) (*raftpb.WorkerHeartbeatResponse, error) {
	n.mu.Lock()
	if req.WorkerId <= 0 {
		leaderID := n.leaderID
		n.mu.Unlock()
		return &raftpb.WorkerHeartbeatResponse{Success: false, LeaderId: leaderID}, nil
	}

	if n.role != Leader {
		leaderID := n.leaderID
		n.mu.Unlock()
		return &raftpb.WorkerHeartbeatResponse{Success: false, LeaderId: leaderID}, nil
	}

	if _, tracked := n.state.ActiveWorkers[req.WorkerId]; !tracked {
		n.mu.Unlock()
		return &raftpb.WorkerHeartbeatResponse{Success: false, LeaderId: n.id}, nil
	}

	wasAlive := n.state.ActiveWorkers[req.WorkerId]
	n.workerHeartbeats[req.WorkerId] = time.Now()
	n.mu.Unlock()

	if !wasAlive {
		n.noteWorkerReachability(req.WorkerId, true)
	}

	return &raftpb.WorkerHeartbeatResponse{Success: true, LeaderId: n.id}, nil
}

func (n *Node) runWorkerHeartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(n.workerHeartbeatCheckIntv)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		now := time.Now()
		toMarkDown := make([]int32, 0)

		n.mu.Lock()
		if n.role != Leader {
			n.mu.Unlock()
			continue
		}
		for workerID, alive := range n.state.ActiveWorkers {
			if !alive {
				continue
			}
			lastBeat, ok := n.workerHeartbeats[workerID]
			if !ok {
				lastBeat = now
				n.workerHeartbeats[workerID] = lastBeat
			}
			if now.Sub(lastBeat) > n.workerHeartbeatTimeout {
				toMarkDown = append(toMarkDown, workerID)
			}
		}
		n.mu.Unlock()

		for _, workerID := range toMarkDown {
			n.noteWorkerReachability(workerID, false)
		}
	}
}

func (n *Node) resetWorkerHeartbeatsForLeadership() {
	n.mu.Lock()
	defer n.mu.Unlock()

	now := time.Now()
	n.workerHeartbeats = make(map[int32]time.Time, len(n.state.ActiveWorkers))
	for workerID, alive := range n.state.ActiveWorkers {
		if alive {
			n.workerHeartbeats[workerID] = now
		}
	}
}
