package raft

import "raft-maekawa-sync/internal/models"

// TaskEventApplier is implemented by the local Maekawa worker and injected into Raft.
// Raft calls this only from committed-log apply paths.
type TaskEventApplier interface {
	ApplyTaskEvent(event models.TaskEvent)
}

// applyTaskEventToMaekawa routes a committed TaskEvent to the local Maekawa worker.
func applyTaskEventToMaekawa(event models.TaskEvent, applier TaskEventApplier) {
	if applier == nil {
		return
	}
	applier.ApplyTaskEvent(event)
}
