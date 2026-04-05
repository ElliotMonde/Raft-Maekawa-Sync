package maekawa

import (
	"context"
	"errors"
	"fmt"
	"time"

	"raft-maekawa/internal/models"
)

// TaskEventReporter is the outbound bridge from a worker back to Raft.
// A real Raft client should commit the event and return once it is accepted.
type TaskEventReporter interface {
	SubmitTaskEvent(ctx context.Context, event models.TaskEvent) error
}

// TaskExecutor performs the task payload after this worker has won Maekawa.
type TaskExecutor func(ctx context.Context, task *models.Task) (string, error)

type taskCommitState struct {
	wonBy           int
	wonApplied      bool
	wonCommitted    chan struct{}
	terminalType    models.EventType
	terminalWorker  int
	terminalApplied bool
	terminalCommit  chan struct{}
}

func (w *Worker) SetTaskEventReporter(reporter TaskEventReporter) {
	w.mu.Lock()
	w.reporter = reporter
	w.mu.Unlock()
}

func (w *Worker) SetTaskExecutor(executor TaskExecutor) {
	w.mu.Lock()
	w.executor = executor
	w.mu.Unlock()
}

// ApplyTaskEvent is the Raft-facing apply hook. Membership events update quorum
// state, assigned tasks are queued for contention, and terminal task events
// cancel duplicates that are still waiting to enter the CS.
func (w *Worker) ApplyTaskEvent(event models.TaskEvent) {
	switch event.Type {
	case models.EventAssigned:
		task := taskFromEvent(event)
		if task == nil {
			w.log.Warn("ignoring assigned event without task payload", "task", event.TaskID)
			return
		}
		w.enqueueAssignedTask(task)
	case models.EventWon:
		w.recordCommittedEvent(event)
		// Another worker won the CS and Raft committed the claim.
		// For all losers this is terminal: mark the task so that any queued,
		// in-flight, or executing attempt is suppressed. The winner may later
		// follow up with EventDone/EventFailed, or Raft may cancel the task.
		if event.WorkerID != w.ID {
			w.markTaskTerminal(event)
		}
	case models.EventDone, models.EventFailed, models.EventCanceled:
		w.recordCommittedEvent(event)
		w.markTaskTerminal(event)
	case models.EventWorkerDown:
		w.NotifyWorkerDown(event.WorkerID)
	case models.EventWorkerUp:
		w.NotifyWorkerUp(event.WorkerID)
	case models.EventWorkerRemoved:
		w.NotifyWorkerRemoved(event.WorkerID)
	case models.EventWorkerAdded:
		w.NotifyWorkerAdded(event.WorkerID)
	}
}

// RunTaskLoop processes tasks that arrived from committed EventAssigned entries.
// Each worker contends for the task, executes it if it wins, then reports the
// outcome back through the configured TaskEventReporter.
func (w *Worker) RunTaskLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-w.taskQueue:
			if task == nil || w.taskTerminal(task.ID) {
				continue
			}
			if err := w.processAssignedTask(ctx, task); err != nil &&
				!errors.Is(err, context.Canceled) &&
				!errors.Is(err, context.DeadlineExceeded) {
				w.log.Warn("assigned task failed", "task", task.ID, "err", err)
			}
		}
	}
}

func (w *Worker) processAssignedTask(ctx context.Context, task *models.Task) error {
	if err := w.RequestCS(ctx, task); err != nil {
		if !w.taskTerminal(task.ID) {
			w.clearQueuedTask(task.ID)
		}
		return err
	}

	// Won the CS — report the win to Raft before executing.
	// Raft must commit EventWon so all workers learn who the winner is and stop
	// their own in-flight attempts. Only after Raft accepts do we start executing.
	//
	// TODO(raft): replace SubmitTaskEvent with a real Raft propose-and-wait call.
	// The reporter commits the event cluster-wide; every node's apply path calls
	// ApplyTaskEvent(EventWon) which calls markTaskTerminal for the losers.
	winEvent := models.TaskEvent{
		Type:     models.EventWon,
		TaskID:   task.ID,
		WorkerID: w.ID,
	}
	if err := w.submitTaskEvent(ctx, winEvent); err != nil {
		// Raft rejected the win claim (e.g. another winner already committed).
		// Release the CS without executing — we are not the accepted winner.
		if !w.taskTerminal(task.ID) {
			w.clearQueuedTask(task.ID)
		}
		w.ReleaseCS()
		return fmt.Errorf("win claim rejected for task %s: %w", task.ID, err)
	}
	if err := w.waitForCommittedEvent(ctx, winEvent); err != nil {
		if !w.taskTerminal(task.ID) {
			w.clearQueuedTask(task.ID)
		}
		w.ReleaseCS()
		return err
	}

	// A terminal event can arrive after EventWon is committed but before the
	// executor is installed. In that case, skip execution entirely.
	if terminalEvent, ok := w.committedTerminalEvent(task.ID); ok {
		w.markTaskTerminal(terminalEvent)
		w.ReleaseCS()
		w.markTaskTerminal(terminalEvent)
		return nil
	}
	if w.taskPermanentlyTerminal(task.ID) {
		w.ReleaseCS()
		return nil
	}

	// Raft accepted our win. Now execute.
	execCtx, execCancel := context.WithCancel(ctx)
	w.mu.Lock()
	task.OwnerID = w.ID
	task.Status = models.TaskInProgress
	task.StartedAt = time.Now().UnixNano()
	w.execCancel = execCancel
	w.mu.Unlock()

	result, execErr := w.executeTask(execCtx, task)
	execCancel()

	w.mu.Lock()
	w.execCancel = nil
	st := w.taskStatus[task.ID]
	alreadyTerminal := st == models.TaskDone || st == models.TaskFailed || st == models.TaskCanceled || st == models.TaskWon
	task.FinishedAt = time.Now().UnixNano()
	if execErr == nil {
		task.Status = models.TaskDone
	} else {
		task.Status = models.TaskFailed
	}
	w.mu.Unlock()

	// If Raft already broadcast a terminal event from elsewhere (e.g. a replay),
	// do not emit a second report — just release and return.
	if alreadyTerminal {
		w.ReleaseCS()
		return nil
	}

	doneEvent := models.TaskEvent{
		Type:     models.EventDone,
		TaskID:   task.ID,
		WorkerID: w.ID,
		Result:   result,
	}
	if execErr != nil {
		doneEvent.Type = models.EventFailed
		doneEvent.Result = ""
		doneEvent.Reason = execErr.Error()
	}

	reportErr := w.submitTaskEvent(ctx, doneEvent)
	if reportErr != nil {
		// Raft did not durably accept the terminal event — do not finalize locally.
		// Reset local tracking so the task can be re-enqueued and retried when
		// the next EventAssigned arrives or the caller re-submits.
		w.mu.Lock()
		delete(w.queuedTasks, task.ID)
		delete(w.taskStatus, task.ID)
		w.resetTaskCommitStateLocked(task.ID)
		w.mu.Unlock()
		w.ReleaseCS()
		w.mu.Lock()
		delete(w.queuedTasks, task.ID)
		delete(w.taskStatus, task.ID)
		w.resetTaskCommitStateLocked(task.ID)
		w.mu.Unlock()
		return reportErr
	}
	if err := w.waitForCommittedEvent(ctx, doneEvent); err != nil {
		w.mu.Lock()
		delete(w.queuedTasks, task.ID)
		delete(w.taskStatus, task.ID)
		w.resetTaskCommitStateLocked(task.ID)
		w.mu.Unlock()
		w.ReleaseCS()
		w.mu.Lock()
		delete(w.queuedTasks, task.ID)
		delete(w.taskStatus, task.ID)
		w.resetTaskCommitStateLocked(task.ID)
		w.mu.Unlock()
		return err
	}

	w.markTaskTerminal(doneEvent)
	w.ReleaseCS()
	return execErr
}

func (w *Worker) executeTask(ctx context.Context, task *models.Task) (string, error) {
	w.mu.Lock()
	executor := w.executor
	w.mu.Unlock()
	if executor == nil {
		return "", nil
	}
	return executor(ctx, cloneTask(task))
}

func (w *Worker) submitTaskEvent(ctx context.Context, event models.TaskEvent) error {
	w.mu.Lock()
	reporter := w.reporter
	w.mu.Unlock()
	if reporter == nil {
		return errors.New("no task event reporter configured")
	}
	return reporter.SubmitTaskEvent(ctx, event)
}

func (w *Worker) enqueueAssignedTask(task *models.Task) {
	task = cloneTask(task)
	if task == nil || task.ID == "" {
		return
	}

	w.mu.Lock()
	st := w.taskStatus[task.ID]
	// TaskDone, TaskFailed, and TaskCanceled are permanent — never re-enqueue.
	if st == models.TaskDone || st == models.TaskFailed || st == models.TaskCanceled {
		w.mu.Unlock()
		return
	}
	// TaskWon means another worker previously won this task ID, but if Raft is
	// re-assigning it (winner failed, retry), allow re-enqueue by resetting state.
	if st == models.TaskWon {
		delete(w.taskStatus, task.ID)
		delete(w.queuedTasks, task.ID)
	}
	w.resetTaskCommitStateLocked(task.ID)
	if w.queuedTasks[task.ID] || (w.currentTask != nil && w.currentTask.ID == task.ID) {
		w.mu.Unlock()
		return
	}
	task.Status = models.TaskPending
	w.taskStatus[task.ID] = models.TaskPending
	w.queuedTasks[task.ID] = true
	w.mu.Unlock()

	select {
	case w.taskQueue <- task:
	default:
		go func() { w.taskQueue <- task }()
	}
}

func (w *Worker) markTaskTerminal(event models.TaskEvent) {
	if event.TaskID == "" {
		return
	}

	var status models.TaskStatus
	switch event.Type {
	case models.EventFailed:
		status = models.TaskFailed
	case models.EventCanceled:
		status = models.TaskCanceled
	case models.EventWon:
		// TaskWon suppresses further attempts on this worker but is not permanent:
		// a later EventAssigned for the same ID (e.g. winner failed, Raft reassigns)
		// will reset status to TaskPending and allow re-enqueue.
		status = models.TaskWon
	default:
		status = models.TaskDone
	}

	w.mu.Lock()
	if existing, ok := w.taskStatus[event.TaskID]; ok && taskPermanentStatus(existing) && existing != status {
		w.mu.Unlock()
		return
	}
	w.taskStatus[event.TaskID] = status
	delete(w.queuedTasks, event.TaskID)

	if w.currentTask != nil && w.currentTask.ID == event.TaskID {
		switch w.state {
		case StateWanting:
			// Still collecting votes — cancel the pending RequestCS.
			w.csErr = fmt.Errorf("worker %d: task %s already closed by worker %d", w.ID, event.TaskID, event.WorkerID)
			select {
			case <-w.csEnter:
			default:
				close(w.csEnter)
			}
		case StateHeld:
			// Already executing — cancel the executor so it stops promptly.
			if w.execCancel != nil {
				w.execCancel()
			}
		}
	}
	w.mu.Unlock()
}

// taskTerminal returns true if the task should be dropped from RunTaskLoop.
// TaskWon counts as suppressed (drop it from the queue) but is not permanently
// terminal — enqueueAssignedTask can accept the same ID again after a retry.
func (w *Worker) taskTerminal(taskID string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.taskStatus[taskID]
	return st == models.TaskDone || st == models.TaskFailed || st == models.TaskCanceled || st == models.TaskWon
}

func (w *Worker) taskPermanentlyTerminal(taskID string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.taskStatus[taskID]
	return st == models.TaskDone || st == models.TaskFailed || st == models.TaskCanceled
}

func (w *Worker) clearQueuedTask(taskID string) {
	w.mu.Lock()
	delete(w.queuedTasks, taskID)
	if w.taskStatus[taskID] == models.TaskPending || w.taskStatus[taskID] == models.TaskInProgress {
		delete(w.taskStatus, taskID)
	}
	w.resetTaskCommitStateLocked(taskID)
	w.mu.Unlock()
}

func (w *Worker) recordCommittedEvent(event models.TaskEvent) {
	if event.TaskID == "" {
		return
	}

	w.mu.Lock()
	state := w.taskCommitStateLocked(event.TaskID)
	switch event.Type {
	case models.EventWon:
		state.wonBy = event.WorkerID
		if !state.wonApplied {
			state.wonApplied = true
			close(state.wonCommitted)
		}
	case models.EventDone, models.EventFailed, models.EventCanceled:
		if !state.terminalApplied {
			state.terminalType = event.Type
			state.terminalWorker = event.WorkerID
			state.terminalApplied = true
			close(state.terminalCommit)
		}
	}
	w.mu.Unlock()
}

func (w *Worker) waitForCommittedEvent(ctx context.Context, event models.TaskEvent) error {
	if event.TaskID == "" {
		return nil
	}

	var waitCh chan struct{}
	w.mu.Lock()
	state := w.taskCommitStateLocked(event.TaskID)
	switch event.Type {
	case models.EventWon:
		if state.wonApplied && state.wonBy == event.WorkerID {
			w.mu.Unlock()
			return nil
		}
		waitCh = state.wonCommitted
	case models.EventDone, models.EventFailed, models.EventCanceled:
		if state.terminalApplied && state.terminalType == event.Type && state.terminalWorker == event.WorkerID {
			w.mu.Unlock()
			return nil
		}
		waitCh = state.terminalCommit
	default:
		w.mu.Unlock()
		return nil
	}
	w.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-waitCh:
			w.mu.Lock()
			state := w.taskCommitStateLocked(event.TaskID)
			var (
				matched bool
				err     error
			)
			switch event.Type {
			case models.EventWon:
				matched = state.wonApplied && state.wonBy == event.WorkerID
				if state.wonApplied && !matched {
					err = fmt.Errorf("task %s win committed for worker %d, not %d", event.TaskID, state.wonBy, event.WorkerID)
				} else if !matched {
					waitCh = state.wonCommitted
				}
			case models.EventDone, models.EventFailed, models.EventCanceled:
				matched = state.terminalApplied && state.terminalType == event.Type && state.terminalWorker == event.WorkerID
				if state.terminalApplied && !matched {
					err = fmt.Errorf("task %s terminal event committed as %v by worker %d", event.TaskID, state.terminalType, state.terminalWorker)
				} else if !matched {
					waitCh = state.terminalCommit
				}
			}
			w.mu.Unlock()
			if matched {
				return nil
			}
			if err != nil {
				return err
			}
		}
	}
}

func (w *Worker) taskCommitStateLocked(taskID string) *taskCommitState {
	state := w.taskCommits[taskID]
	if state != nil {
		return state
	}
	state = &taskCommitState{
		wonCommitted:   make(chan struct{}),
		terminalCommit: make(chan struct{}),
	}
	w.taskCommits[taskID] = state
	return state
}

func (w *Worker) resetTaskCommitStateLocked(taskID string) {
	delete(w.taskCommits, taskID)
}

func (w *Worker) committedTerminalEvent(taskID string) (models.TaskEvent, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	state := w.taskCommits[taskID]
	if state == nil || !state.terminalApplied {
		return models.TaskEvent{}, false
	}
	switch state.terminalType {
	case models.EventDone, models.EventFailed, models.EventCanceled:
		return models.TaskEvent{
			Type:     state.terminalType,
			TaskID:   taskID,
			WorkerID: state.terminalWorker,
		}, true
	default:
		return models.TaskEvent{}, false
	}
}

func cloneTask(task *models.Task) *models.Task {
	if task == nil {
		return nil
	}
	clone := *task
	return &clone
}

func taskFromEvent(event models.TaskEvent) *models.Task {
	if event.Task != nil {
		task := cloneTask(event.Task)
		if task.ID == "" {
			task.ID = event.TaskID
		}
		return task
	}
	if event.TaskID == "" {
		return nil
	}
	return &models.Task{ID: event.TaskID}
}

func taskPermanentStatus(status models.TaskStatus) bool {
	return status == models.TaskDone || status == models.TaskFailed || status == models.TaskCanceled
}
