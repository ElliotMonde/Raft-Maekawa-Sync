package raft

import (
	"context"
	"fmt"
	"sort"
	"time"

	raftpb "raft-maekawa-sync/api/raft"
	"raft-maekawa-sync/internal/models"
)

func (n *Node) ActiveMembers() []int32 {
	n.mu.Lock()
	defer n.mu.Unlock()

	members := make([]int32, 0, len(n.state.ActiveWorkers))
	for id, alive := range n.state.ActiveWorkers {
		if alive {
			members = append(members, id)
		}
	}
	sort.Slice(members, func(i, j int) bool { return members[i] < members[j] })
	return members
}

func (n *Node) IsAlive(id int32) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state.ActiveWorkers[id]
}

func (n *Node) ClaimTask(taskID string, workerID int32) (bool, error) {
	event := models.TaskEvent{Type: models.EventClaimed, TaskID: taskID, WorkerID: workerID}
	return n.commitOrForwardEvent(context.Background(), event)
}

func (n *Node) ReportTaskSuccess(taskID string, workerID int32, result string) error {
	event := models.TaskEvent{Type: models.EventDone, TaskID: taskID, WorkerID: workerID, Result: result}
	accepted, err := n.commitOrForwardEvent(context.Background(), event)
	if err != nil {
		return err
	}
	if !accepted {
		return fmt.Errorf("task %s was not accepted", taskID)
	}
	return nil
}

func (n *Node) ReportTaskFailure(taskID string, workerID int32, reason string) error {
	event := models.TaskEvent{Type: models.EventFailed, TaskID: taskID, WorkerID: workerID, Reason: reason}
	accepted, err := n.commitOrForwardEvent(context.Background(), event)
	if err != nil {
		return err
	}
	if !accepted {
		return fmt.Errorf("task %s was not accepted", taskID)
	}
	return nil
}

func (n *Node) commitEvent(ctx context.Context, event models.TaskEvent) error {
	accepted, _, err := n.commitTaskEventAsLeader(ctx, event)
	if err != nil {
		return err
	}
	if !accepted {
		return fmt.Errorf("event %s was not accepted", event.Type)
	}
	return nil
}

func (n *Node) commitOrForwardEvent(ctx context.Context, event models.TaskEvent) (bool, error) {
	n.mu.Lock()
	isLeader := n.role == Leader
	leaderID := n.leaderID
	n.mu.Unlock()

	if isLeader {
		accepted, _, err := n.commitTaskEventAsLeader(ctx, event)
		return accepted, err
	}

	payload, err := encodeInternalSubmit(event)
	if err != nil {
		return false, err
	}
	candidates := make([]int32, 0, len(n.peers)+1)
	if leaderID > 0 {
		candidates = append(candidates, leaderID)
	}
	for peerID := range n.peers {
		if peerID != leaderID {
			candidates = append(candidates, peerID)
		}
	}

	lastErr := fmt.Errorf("leader unavailable")
	for _, peerID := range candidates {
		client, err := n.getPeerClient(peerID)
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := client.SubmitTask(ctx, &raftpb.SubmitTaskRequest{Data: payload})
		if err != nil {
			lastErr = err
			continue
		}
		if resp.LeaderId > 0 {
			n.mu.Lock()
			n.leaderID = resp.LeaderId
			n.mu.Unlock()
		}
		if resp.Success {
			return true, nil
		}
		if resp.LeaderId > 0 && resp.LeaderId != peerID {
			leaderID = resp.LeaderId
			client, err = n.getPeerClient(leaderID)
			if err != nil {
				lastErr = err
				continue
			}
			resp, err = client.SubmitTask(ctx, &raftpb.SubmitTaskRequest{Data: payload})
			if err != nil {
				lastErr = err
				continue
			}
			if resp.Success {
				return true, nil
			}
			return false, nil
		}
		return false, nil
	}
	return false, lastErr
}

func (n *Node) notePeerReachability(peerID int32, alive bool) {
	if peerID == n.id {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	current := n.state.ActiveWorkers[peerID]
	if current == alive {
		n.mu.Unlock()
		return
	}
	if pending, ok := n.pendingLiveness[peerID]; ok && pending == alive {
		n.mu.Unlock()
		return
	}
	n.pendingLiveness[peerID] = alive
	n.mu.Unlock()

	eventType := models.EventWorkerDown
	if alive {
		eventType = models.EventWorkerUp
	}
	_, err := n.commitOrForwardEvent(ctx, models.TaskEvent{Type: eventType, WorkerID: peerID})

	n.mu.Lock()
	delete(n.pendingLiveness, peerID)
	n.mu.Unlock()

	if err != nil {
		return
	}
}
