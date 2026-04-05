package maekawa

import (
	"context"
	"sync"
	"testing"
	"time"

	"raft-maekawa/internal/models"
)

func TestRunTaskLoopReportsDoneForAssignedTask(t *testing.T) {
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
		return "ok:" + task.ID, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.RunTaskLoop(ctx) }()

	task := &models.Task{ID: "assigned-1", Description: "from raft"}
	w.ApplyTaskEvent(models.TaskEvent{
		Type:   models.EventAssigned,
		TaskID: task.ID,
		Task:   task,
	})

	// Expect EventWon (win claim) followed by EventDone (completion).
	deadline := time.After(3 * time.Second)
	wantTypes := []models.EventType{models.EventWon, models.EventDone}
	for _, want := range wantTypes {
		select {
		case event := <-reporter.ch:
			if event.Type != want {
				t.Fatalf("got event type %v, want %v", event.Type, want)
			}
			if event.TaskID != task.ID {
				t.Fatalf("task id = %q, want %q", event.TaskID, task.ID)
			}
			if event.WorkerID != w.ID {
				t.Fatalf("worker id = %d, want %d", event.WorkerID, w.ID)
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %v event", want)
		}
	}
}

func TestExecutionWaitsForCommittedWin(t *testing.T) {
	workers, _ := startWorkers(t, 1)
	defer stopWorkers(workers)

	w := workers[0]
	reporter := newFakeTaskReporter()
	wonSubmitted := make(chan struct{}, 1)
	releaseWon := make(chan struct{})
	reporter.submit = func(_ context.Context, event models.TaskEvent) error {
		switch event.Type {
		case models.EventWon:
			select {
			case wonSubmitted <- struct{}{}:
			default:
			}
			<-releaseWon
			w.ApplyTaskEvent(event)
		case models.EventDone, models.EventFailed:
			w.ApplyTaskEvent(event)
		}
		return nil
	}
	w.SetTaskEventReporter(reporter)

	executed := make(chan struct{}, 1)
	w.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
		select {
		case executed <- struct{}{}:
		default:
		}
		return "ok:" + task.ID, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.RunTaskLoop(ctx) }()

	task := &models.Task{ID: "gated-win", Description: "must wait for committed win"}
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: task.ID, Task: task})

	select {
	case <-wonSubmitted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for EventWon proposal")
	}

	select {
	case <-executed:
		t.Fatal("executor started before committed EventWon was applied")
	case <-time.After(300 * time.Millisecond):
	}

	close(releaseWon)

	select {
	case <-executed:
	case <-time.After(3 * time.Second):
		t.Fatal("executor did not start after committed EventWon")
	}
}

func TestRejectedWinDoesNotExecute(t *testing.T) {
	workers, _ := startWorkers(t, 1)
	defer stopWorkers(workers)

	w := workers[0]
	reporter := newFakeTaskReporter()
	reporter.submit = func(_ context.Context, event models.TaskEvent) error {
		if event.Type == models.EventWon {
			return context.Canceled
		}
		return nil
	}
	w.SetTaskEventReporter(reporter)

	executed := make(chan struct{}, 1)
	w.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
		select {
		case executed <- struct{}{}:
		default:
		}
		return "unexpected:" + task.ID, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.RunTaskLoop(ctx) }()

	task := &models.Task{ID: "reject-win", Description: "must not execute"}
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: task.ID, Task: task})

	select {
	case <-executed:
		t.Fatal("executor ran after rejected win claim")
	case <-time.After(500 * time.Millisecond):
	}

	waitForWorkerReleased(t, w, 3*time.Second)
}

func TestRunTaskLoopProcessesMultipleAssignedTasksSequentially(t *testing.T) {
	workers, _ := startWorkers(t, 1)
	defer stopWorkers(workers)

	w := workers[0]
	reporter := newFakeTaskReporter()
	reporter.submit = func(_ context.Context, event models.TaskEvent) error {
		w.ApplyTaskEvent(event)
		return nil
	}
	w.SetTaskEventReporter(reporter)

	var mu sync.Mutex
	executed := make([]string, 0, 2)
	w.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
		mu.Lock()
		executed = append(executed, task.ID)
		mu.Unlock()
		return "ok:" + task.ID, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.RunTaskLoop(ctx) }()

	for _, taskID := range []string{"multi-1", "multi-2"} {
		task := &models.Task{ID: taskID, Description: "sequential local task"}
		w.ApplyTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: task.ID, Task: task})
	}

	deadline := time.After(5 * time.Second)
	doneSeen := 0
	for doneSeen < 2 {
		select {
		case event := <-reporter.ch:
			if event.Type == models.EventDone {
				doneSeen++
			}
		case <-deadline:
			t.Fatal("timed out waiting for both EventDone events")
		}
	}

	waitForWorkerReleased(t, w, 3*time.Second)

	mu.Lock()
	defer mu.Unlock()
	if len(executed) != 2 {
		t.Fatalf("executed %d tasks, want 2; got=%v", len(executed), executed)
	}
	if executed[0] != "multi-1" || executed[1] != "multi-2" {
		t.Fatalf("execution order = %v, want [multi-1 multi-2]", executed)
	}
}

func TestEventWonCancelsExecutingWorker(t *testing.T) {
	workers, _ := startWorkers(t, 1)
	defer stopWorkers(workers)

	w := workers[0]
	reporter := newFakeTaskReporter()
	reporter.submit = func(_ context.Context, event models.TaskEvent) error {
		if event.Type == models.EventWon {
			w.ApplyTaskEvent(event)
		}
		return nil
	}
	w.SetTaskEventReporter(reporter)

	started := make(chan struct{}, 1)
	canceled := make(chan struct{}, 1)
	w.SetTaskExecutor(func(ctx context.Context, task *models.Task) (string, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		select {
		case canceled <- struct{}{}:
		default:
		}
		return "", ctx.Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.RunTaskLoop(ctx) }()

	task := &models.Task{ID: "exec-cancel", Description: "cancel while executing"}
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: task.ID, Task: task})

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("executor never started")
	}

	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventWon, TaskID: task.ID, WorkerID: 99})

	select {
	case <-canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("executor was not canceled after foreign EventWon")
	}

	waitForWorkerReleased(t, w, 3*time.Second)

	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if len(reporter.events) != 1 || reporter.events[0].Type != models.EventWon {
		t.Fatalf("unexpected reported events after execution cancel: %v", reporter.events)
	}
}

func TestEventCanceledAfterCommittedWinBeforeExecuteSkipsExecution(t *testing.T) {
	workers, _ := startWorkers(t, 1)
	defer stopWorkers(workers)

	w := workers[0]
	reporter := newFakeTaskReporter()
	reporter.submit = func(_ context.Context, event models.TaskEvent) error {
		if event.Type == models.EventWon {
			w.ApplyTaskEvent(event)
			w.ApplyTaskEvent(models.TaskEvent{
				Type:     models.EventCanceled,
				TaskID:   event.TaskID,
				WorkerID: 99,
				Reason:   "requester canceled",
			})
		}
		return nil
	}
	w.SetTaskEventReporter(reporter)

	executed := make(chan struct{}, 1)
	w.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
		select {
		case executed <- struct{}{}:
		default:
		}
		return "unexpected:" + task.ID, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.RunTaskLoop(ctx) }()

	task := &models.Task{ID: "cancel-after-win", Description: "cancel after committed win"}
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: task.ID, Task: task})

	waitForWorkerReleased(t, w, 3*time.Second)
	waitForTaskStatus(t, w, task.ID, models.TaskCanceled, 3*time.Second)

	select {
	case <-executed:
		t.Fatal("executor started after committed cancellation")
	default:
	}

	w.mu.Lock()
	st := w.taskStatus[task.ID]
	w.mu.Unlock()
	if st != models.TaskCanceled {
		t.Fatalf("taskStatus = %v, want TaskCanceled", st)
	}

	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if len(reporter.events) != 1 || reporter.events[0].Type != models.EventWon {
		t.Fatalf("unexpected reported events after cancellation: %v", reporter.events)
	}
}

func TestEventCanceledCancelsExecutingWorker(t *testing.T) {
	workers, _ := startWorkers(t, 1)
	defer stopWorkers(workers)

	w := workers[0]
	reporter := newFakeTaskReporter()
	reporter.submit = func(_ context.Context, event models.TaskEvent) error {
		if event.Type == models.EventWon {
			w.ApplyTaskEvent(event)
		}
		return nil
	}
	w.SetTaskEventReporter(reporter)

	started := make(chan struct{}, 1)
	canceled := make(chan struct{}, 1)
	w.SetTaskExecutor(func(ctx context.Context, task *models.Task) (string, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		select {
		case canceled <- struct{}{}:
		default:
		}
		return "", ctx.Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.RunTaskLoop(ctx) }()

	task := &models.Task{ID: "cancel-executing", Description: "cancel while executing"}
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: task.ID, Task: task})

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("executor did not start")
	}

	w.ApplyTaskEvent(models.TaskEvent{
		Type:     models.EventCanceled,
		TaskID:   task.ID,
		WorkerID: 77,
		Reason:   "user canceled",
	})

	select {
	case <-canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("executor was not canceled after EventCanceled")
	}

	waitForWorkerReleased(t, w, 3*time.Second)
	waitForTaskStatus(t, w, task.ID, models.TaskCanceled, 3*time.Second)

	w.mu.Lock()
	st := w.taskStatus[task.ID]
	w.mu.Unlock()
	if st != models.TaskCanceled {
		t.Fatalf("taskStatus = %v, want TaskCanceled", st)
	}

	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if len(reporter.events) != 1 || reporter.events[0].Type != models.EventWon {
		t.Fatalf("unexpected reported events after cancel: %v", reporter.events)
	}
}
