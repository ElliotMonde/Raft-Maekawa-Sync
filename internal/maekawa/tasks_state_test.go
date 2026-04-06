
//go:build ignore

package maekawa

import (
	"context"
	"testing"
	"time"

	"raft-maekawa-sync/internal/models"
)

// TestEventWonSuppressesQueuedTask: task is enqueued but not yet started when
// EventWon arrives from another worker. RunTaskLoop must drop it without executing.
func TestEventWonSuppressesQueuedTask(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	// Stop a quorum peer so w0 can never win — RequestCS will block forever.
	// This gives us time to inject EventWon before the task exits the queue.
	workers[1].Stop()

	w0 := workers[0]
	reporter := newFakeTaskReporter()
	w0.SetTaskEventReporter(reporter)
	executed := make(chan struct{})
	w0.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
		close(executed)
		return "", nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w0.RunTaskLoop(ctx) }()

	task := &models.Task{ID: "won-suppress", Description: "should be suppressed"}
	w0.ApplyTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: task.ID, Task: task})

	// Give RunTaskLoop time to dequeue and block in RequestCS.
	time.Sleep(150 * time.Millisecond)

	// Another worker wins — broadcast EventWon to w0.
	w0.ApplyTaskEvent(models.TaskEvent{Type: models.EventWon, TaskID: task.ID, WorkerID: 3})

	// Executor must never run.
	select {
	case <-executed:
		t.Fatal("executor ran after EventWon — task was not suppressed")
	case <-time.After(500 * time.Millisecond):
	}

	// No events should have been reported (w0 did not win).
	reporter.mu.Lock()
	n := len(reporter.events)
	reporter.mu.Unlock()
	if n != 0 {
		t.Fatalf("expected no reported events, got %d", n)
	}

	// taskStatus should be TaskWon (suppressed, not permanently terminal).
	w0.mu.Lock()
	st := w0.taskStatus[task.ID]
	w0.mu.Unlock()
	if st != models.TaskWon {
		t.Errorf("taskStatus = %v, want TaskWon", st)
	}
}

// TestEventWonThenReassignAllowsRetry: after EventWon marks a task TaskWon,
// a subsequent EventAssigned for the same task ID (winner failed, Raft retries)
// must be accepted and processed normally.
func TestEventWonThenReassignAllowsRetry(t *testing.T) {
	workers, _ := startWorkers(t, 1)
	defer stopWorkers(workers)

	w := workers[0]
	reporter := newFakeTaskReporter()
	reporter.submit = func(_ context.Context, event models.TaskEvent) error {
		w.ApplyTaskEvent(event)
		return nil
	}
	w.SetTaskEventReporter(reporter)
	w.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
		return "retry-ok", nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.RunTaskLoop(ctx) }()

	taskID := "retry-task"

	// Step 1: another worker wins — mark this task as TaskWon on w.
	// (w.ID == 0, so WorkerID=1 means "someone else won")
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventWon, TaskID: taskID, WorkerID: 1})

	w.mu.Lock()
	st := w.taskStatus[taskID]
	w.mu.Unlock()
	if st != models.TaskWon {
		t.Fatalf("after EventWon: taskStatus = %v, want TaskWon", st)
	}

	// Step 2: Raft reassigns the same task (winner failed).
	task := &models.Task{ID: taskID, Description: "reassigned"}
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: taskID, Task: task})

	// Step 3: w should now compete and report EventWon then EventDone.
	deadline := time.After(3 * time.Second)
	for _, want := range []models.EventType{models.EventWon, models.EventDone} {
		select {
		case event := <-reporter.ch:
			if event.Type != want {
				t.Fatalf("got event %v, want %v", event.Type, want)
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %v after reassignment", want)
		}
	}
}

// TestEventWonDoesNotAllowReenqueueAfterDone: EventDone is a permanent terminal
// state — even after a TaskWon → EventDone sequence, a new EventAssigned must
// not re-enqueue the task.
func TestEventWonDoesNotAllowReenqueueAfterDone(t *testing.T) {
	workers, _ := startWorkers(t, 1)
	defer stopWorkers(workers)

	w := workers[0]
	taskID := "perm-done"

	// Mark the task permanently done.
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventDone, TaskID: taskID, WorkerID: 0})

	w.mu.Lock()
	st := w.taskStatus[taskID]
	w.mu.Unlock()
	if st != models.TaskDone {
		t.Fatalf("taskStatus = %v, want TaskDone", st)
	}

	// Attempt to re-enqueue — must be rejected.
	task := &models.Task{ID: taskID, Description: "should not re-enqueue"}
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: taskID, Task: task})

	w.mu.Lock()
	queued := w.queuedTasks[taskID]
	st = w.taskStatus[taskID]
	w.mu.Unlock()

	if queued {
		t.Error("task was re-enqueued after EventDone — permanent terminal violated")
	}
	if st != models.TaskDone {
		t.Errorf("taskStatus changed after rejected enqueue: %v", st)
	}
}

func TestEventFailedDoesNotAllowReenqueue(t *testing.T) {
	workers, _ := startWorkers(t, 1)
	defer stopWorkers(workers)

	w := workers[0]
	taskID := "perm-failed"

	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventFailed, TaskID: taskID, WorkerID: 0, Reason: "boom"})

	w.mu.Lock()
	st := w.taskStatus[taskID]
	w.mu.Unlock()
	if st != models.TaskFailed {
		t.Fatalf("taskStatus = %v, want TaskFailed", st)
	}

	task := &models.Task{ID: taskID, Description: "should not re-enqueue after fail"}
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: taskID, Task: task})

	w.mu.Lock()
	queued := w.queuedTasks[taskID]
	st = w.taskStatus[taskID]
	w.mu.Unlock()

	if queued {
		t.Error("task was re-enqueued after EventFailed")
	}
	if st != models.TaskFailed {
		t.Errorf("taskStatus changed after rejected failed enqueue: %v", st)
	}
}

func TestEventCanceledDoesNotAllowReenqueue(t *testing.T) {
	workers, _ := startWorkers(t, 1)
	defer stopWorkers(workers)

	w := workers[0]
	taskID := "perm-canceled"

	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventCanceled, TaskID: taskID, WorkerID: 42, Reason: "requester canceled"})

	w.mu.Lock()
	st := w.taskStatus[taskID]
	w.mu.Unlock()
	if st != models.TaskCanceled {
		t.Fatalf("taskStatus = %v, want TaskCanceled", st)
	}

	task := &models.Task{ID: taskID, Description: "should not re-enqueue after cancel"}
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: taskID, Task: task})

	w.mu.Lock()
	queued := w.queuedTasks[taskID]
	st = w.taskStatus[taskID]
	w.mu.Unlock()

	if queued {
		t.Error("task was re-enqueued after EventCanceled")
	}
	if st != models.TaskCanceled {
		t.Errorf("taskStatus changed after rejected canceled enqueue: %v", st)
	}
}

func TestDoneEventCancelsPendingAssignedTask(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]
	victim := workers[1]
	victim.Stop() // block one quorum vote so RequestCS waits

	reporter := newFakeTaskReporter()
	w0.SetTaskEventReporter(reporter)
	w0.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
		return "unexpected:" + task.ID, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w0.RunTaskLoop(ctx) }()

	task := &models.Task{ID: "assigned-cancel", Description: "cancel pending contender"}
	w0.ApplyTaskEvent(models.TaskEvent{
		Type:   models.EventAssigned,
		TaskID: task.ID,
		Task:   task,
	})

	time.Sleep(200 * time.Millisecond)
	w0.ApplyTaskEvent(models.TaskEvent{
		Type:     models.EventDone,
		TaskID:   task.ID,
		WorkerID: 4,
	})

	deadline := time.After(3 * time.Second)
	for {
		w0.mu.Lock()
		state := w0.state
		current := w0.currentTask
		w0.mu.Unlock()

		if state == StateReleased && current == nil {
			break
		}

		select {
		case <-deadline:
			t.Fatal("pending assigned task was not cancelled")
		case <-time.After(20 * time.Millisecond):
		}
	}

	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if len(reporter.events) != 0 {
		t.Fatalf("unexpected reported events after cancellation: %v", reporter.events)
	}
}

func TestCanceledEventCancelsPendingAssignedTask(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]
	victim := workers[1]
	victim.Stop() // block one quorum vote so RequestCS waits

	reporter := newFakeTaskReporter()
	w0.SetTaskEventReporter(reporter)
	w0.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
		return "unexpected:" + task.ID, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w0.RunTaskLoop(ctx) }()

	task := &models.Task{ID: "assigned-cancel-user", Description: "cancel pending contender"}
	w0.ApplyTaskEvent(models.TaskEvent{
		Type:   models.EventAssigned,
		TaskID: task.ID,
		Task:   task,
	})

	time.Sleep(200 * time.Millisecond)
	w0.ApplyTaskEvent(models.TaskEvent{
		Type:     models.EventCanceled,
		TaskID:   task.ID,
		WorkerID: 17,
		Reason:   "requester canceled",
	})

	deadline := time.After(3 * time.Second)
	for {
		w0.mu.Lock()
		state := w0.state
		current := w0.currentTask
		status := w0.taskStatus[task.ID]
		w0.mu.Unlock()

		if state == StateReleased && current == nil && status == models.TaskCanceled {
			break
		}

		select {
		case <-deadline:
			t.Fatal("pending assigned task was not canceled")
		case <-time.After(20 * time.Millisecond):
		}
	}

	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if len(reporter.events) != 0 {
		t.Fatalf("unexpected reported events after cancellation: %v", reporter.events)
	}
}

func TestTerminalReportRejectedResetsTaskState(t *testing.T) {
	workers, _ := startWorkers(t, 1)
	defer stopWorkers(workers)

	w := workers[0]
	reporter := newFakeTaskReporter()
	reporter.submit = func(_ context.Context, event models.TaskEvent) error {
		switch event.Type {
		case models.EventWon:
			w.ApplyTaskEvent(event)
			return nil
		case models.EventDone, models.EventFailed:
			return context.Canceled
		default:
			return nil
		}
	}
	w.SetTaskEventReporter(reporter)
	w.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
		return "ok:" + task.ID, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.RunTaskLoop(ctx) }()

	task := &models.Task{ID: "reject-terminal", Description: "terminal commit rejected"}
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: task.ID, Task: task})

	waitForWorkerReleased(t, w, 3*time.Second)
	waitForTaskStateCleared(t, w, task.ID, 3*time.Second)

	w.mu.Lock()
	_, queued := w.queuedTasks[task.ID]
	_, status := w.taskStatus[task.ID]
	_, commit := w.taskCommits[task.ID]
	w.mu.Unlock()

	if queued {
		t.Fatal("queuedTasks entry remained after terminal rejection")
	}
	if status {
		t.Fatal("taskStatus entry remained after terminal rejection")
	}
	if commit {
		t.Fatal("taskCommitState remained after terminal rejection")
	}

	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if len(reporter.events) < 2 {
		t.Fatalf("expected EventWon and terminal report attempt, got %v", reporter.events)
	}
}
