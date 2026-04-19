package raft

import (
	"context"
	"fmt"
	"sort"
	"time"

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
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return false, fmt.Errorf("not the leader")
	}
	task, ok := n.state.Tasks[taskID]
	if !ok || task.Status != models.EventAssigned {
		n.mu.Unlock()
		return false, nil
	}
	n.mu.Unlock()

	event := models.TaskEvent{Type: models.EventClaimed, TaskID: taskID, WorkerID: workerID}
	if err := n.commitEvent(context.Background(), event); err != nil {
		return false, err
	}
	return true, nil
}

func (n *Node) ReportTaskSuccess(taskID string, workerID int32, result string) error {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return fmt.Errorf("not the leader")
	}
	n.mu.Unlock()

	event := models.TaskEvent{Type: models.EventDone, TaskID: taskID, WorkerID: workerID, Result: result}
	return n.commitEvent(context.Background(), event)
}

func (n *Node) ReportTaskFailure(taskID string, workerID int32, reason string) error {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return fmt.Errorf("not the leader")
	}
	n.mu.Unlock()

	event := models.TaskEvent{Type: models.EventFailed, TaskID: taskID, WorkerID: workerID, Reason: reason}
	return n.commitEvent(context.Background(), event)
}

func (n *Node) commitEvent(ctx context.Context, event models.TaskEvent) error {
	cmd, err := event.Encode()
	if err != nil {
		return err
	}

	n.mu.Lock()
	n.appendEntry(cmd)
	newIndex := int32(len(n.log))
	peerIDs := make([]int32, 0, len(n.peers))
	for peerID := range n.peers {
		peerIDs = append(peerIDs, peerID)
	}
	if len(n.peers) == 0 {
		n.commitIndex = newIndex
		n.applyCommittedEntries()
	}
	n.mu.Unlock()

	for _, peerID := range peerIDs {
		go n.replicateToPeer(ctx, peerID)
	}

	if !n.waitForCommit(ctx, newIndex, 5*time.Second) {
		return fmt.Errorf("commit timeout")
	}
	return nil
}