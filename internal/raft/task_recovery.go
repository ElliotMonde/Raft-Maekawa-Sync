package raft

import (
	"context"
	"time"

	"raft-maekawa-sync/internal/models"
)

func (n *Node) runTaskRecoveryLoop(ctx context.Context) {
	ticker := time.NewTicker(n.taskRecoveryCheckIntv)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.recoverStaleTasks(ctx)
		}
	}
}

func (n *Node) recoverStaleTasks(ctx context.Context) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}

	type recovery struct {
		taskID string
		data   string
	}

	candidates := make([]recovery, 0)
	for taskID, task := range n.state.Tasks {
		if n.pendingTaskRecovery[taskID] || !n.canRecoverClaimedTaskLocked(task) {
			continue
		}
		n.pendingTaskRecovery[taskID] = true
		candidates = append(candidates, recovery{taskID: taskID, data: task.Data})
	}
	n.mu.Unlock()

	for _, candidate := range candidates {
		accepted, _, err := n.commitTaskEventAsLeader(ctx, models.TaskEvent{
			Type:          models.EventAssigned,
			TaskID:        candidate.taskID,
			Task:          &models.Task{ID: candidate.taskID, Data: candidate.data},
			EventUnixNano: time.Now().UnixNano(),
		})
		if err != nil || !accepted {
			n.mu.Lock()
			delete(n.pendingTaskRecovery, candidate.taskID)
			n.mu.Unlock()
		}
	}
}
