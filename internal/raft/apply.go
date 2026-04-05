package raft

import "raft-maekawa/internal/models"

// MaekawaNotifier is implemented by the local Maekawa worker and injected into Raft.
// Raft calls these only from committed-log apply paths.
type MaekawaNotifier interface {
	NotifyWorkerDown(workerID int)
	NotifyWorkerUp(workerID int)
	NotifyWorkerRemoved(workerID int)
	NotifyWorkerAdded(workerID int)
}

// applyTaskEventToMaekawa routes a committed TaskEvent to the local Maekawa notifier.
// Unknown event types are ignored.
func applyTaskEventToMaekawa(event models.TaskEvent, notifier MaekawaNotifier) {
	if notifier == nil {
		return
	}

	switch event.Type {
	case models.EventWorkerDown:
		notifier.NotifyWorkerDown(event.WorkerID)
	case models.EventWorkerUp:
		notifier.NotifyWorkerUp(event.WorkerID)
	case models.EventWorkerRemoved:
		notifier.NotifyWorkerRemoved(event.WorkerID)
	case models.EventWorkerAdded:
		notifier.NotifyWorkerAdded(event.WorkerID)
	}
}
