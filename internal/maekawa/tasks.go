package maekawa

import (
	"context"
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
    won, err := w.membership.ClaimTask(task.ID, w.ID)
    if err != nil || !won {
        return
    }

    result, execErr := w.executor(ctx, task)

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

func (w *Worker) ApplyTaskEvent(event models.TaskEvent) {
    switch event.Type {
    case models.EventWorkerUp, models.EventWorkerDown:
        // Trigger the regridding logic we wrote earlier
        w.OnMembershipChange(w.membership.ActiveMembers())
        
    case models.EventDone, models.EventCanceled:
        // Optional: Remove this task from your local taskQueue 
        // if it's still sitting there to save some Maekawa messages.
        w.removeFromLocalQueue(event.TaskID)
    }
}