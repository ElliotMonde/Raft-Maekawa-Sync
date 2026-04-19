package maekawa

import (
	"context"
	"fmt"
	"raft-maekawa-sync/internal/models"
)

// does the actual work.
type TaskExecutor func(ctx context.Context, task *models.Task) (string, error)

func (w *Worker) RunTaskLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-w.taskQueue:
			w.Mu.Lock()
			canceled := w.canceledTasks[task.ID]
			w.Mu.Unlock()
			if canceled {
				continue
			}

			if err := w.RequestForGlobalLock(ctx); err != nil {
				continue
			}
			w.handleTaskExecution(ctx, task)
			w.exitGlobalCS()
		}
	}
}

func (w *Worker) handleTaskExecution(ctx context.Context, task *models.Task) {
	w.Mu.Lock()
	executor := w.executor
	beforeClaim := w.beforeClaim
	w.Mu.Unlock()

	if executor == nil {
		return
	}

	if beforeClaim != nil && !beforeClaim(task) {
		return
	}

	won, err := w.membership.ClaimTask(task.ID, w.ID)
	if err != nil || !won {
		return
	}

	var result string
	var execErr error

	func() {
		defer func() {
			if r := recover(); r != nil {
				execErr = fmt.Errorf("task execution paniced: %v", r)
			}
		}()
		result, execErr = executor(ctx, task)
	}()

	if execErr != nil {
		_ = w.membership.ReportTaskFailure(task.ID, w.ID, execErr.Error())
	} else {
		_ = w.membership.ReportTaskSuccess(task.ID, w.ID, result)
	}
}

func (w *Worker) removeFromLocalQueue(taskID string) {
	w.Mu.Lock()
	defer w.Mu.Unlock()
	w.canceledTasks[taskID] = true
}

func (w *Worker) restoreTask(taskID string) {
	w.Mu.Lock()
	defer w.Mu.Unlock()
	delete(w.canceledTasks, taskID)
}

func (w *Worker) ApplyTaskEvent(event models.TaskEvent) {
	switch event.Type {
	case models.EventAssigned:
		if event.Task == nil {
			return
		}
		w.restoreTask(event.Task.ID)
		// Enqueue newly assigned tasks from the Raft commit stream.
		select {
		case w.taskQueue <- event.Task:
		default:
			// Keep this non-blocking to avoid stalling Raft apply path.
		}

	case models.EventClaimed:
		if event.WorkerID != w.ID {
			w.removeFromLocalQueue(event.TaskID)
		}

	case models.EventWorkerUp, models.EventWorkerDown, models.EventWorkerAdded, models.EventWorkerRemoved:
		// Trigger the regridding logic we wrote earlier
		w.OnMembershipChange(w.membership.ActiveMembers())

	case models.EventDone, models.EventFailed, models.EventCanceled:
		// Optional: Remove this task from your local taskQueue
		// if it's still sitting there to save some Maekawa messages.
		w.removeFromLocalQueue(event.TaskID)
	}
}
