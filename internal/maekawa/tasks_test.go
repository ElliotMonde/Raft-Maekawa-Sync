package maekawa

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"raft-maekawa/internal/models"
)

type fakeTaskReporter struct {
	mu     sync.Mutex
	events []models.TaskEvent
	ch     chan models.TaskEvent
	submit func(context.Context, models.TaskEvent) error
}

func newFakeTaskReporter() *fakeTaskReporter {
	return &fakeTaskReporter{ch: make(chan models.TaskEvent, 8)}
}

func (r *fakeTaskReporter) SubmitTaskEvent(ctx context.Context, event models.TaskEvent) error {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()

	select {
	case r.ch <- event:
	default:
	}
	if r.submit != nil {
		return r.submit(ctx, event)
	}
	return nil
}

type fakeClusterReporter struct {
	mu       sync.Mutex
	workers  []*Worker
	down     map[int]bool
	removed  map[int]bool
	winners  map[string]int
	terminal map[string]models.EventType
	events   chan models.TaskEvent
}

func newFakeClusterReporter(workers []*Worker) *fakeClusterReporter {
	return &fakeClusterReporter{
		workers:  workers,
		down:     make(map[int]bool),
		removed:  make(map[int]bool),
		winners:  make(map[string]int),
		terminal: make(map[string]models.EventType),
		events:   make(chan models.TaskEvent, 32),
	}
}

func (r *fakeClusterReporter) SubmitTaskEvent(_ context.Context, event models.TaskEvent) error {
	return r.CommitTaskEvent(event)
}

func (r *fakeClusterReporter) CommitTaskEvent(event models.TaskEvent) error {
	r.mu.Lock()

	switch event.Type {
	case models.EventWon:
		if r.down[event.WorkerID] || r.removed[event.WorkerID] {
			r.mu.Unlock()
			return fmt.Errorf("worker %d is unavailable", event.WorkerID)
		}
		if committedWinner, ok := r.winners[event.TaskID]; ok {
			if committedWinner != event.WorkerID {
				r.mu.Unlock()
				return fmt.Errorf("task %s already won by worker %d", event.TaskID, committedWinner)
			}
		} else {
			r.winners[event.TaskID] = event.WorkerID
		}
	case models.EventDone, models.EventFailed:
		if r.down[event.WorkerID] || r.removed[event.WorkerID] {
			r.mu.Unlock()
			return fmt.Errorf("worker %d is unavailable", event.WorkerID)
		}
		committedWinner, ok := r.winners[event.TaskID]
		if !ok {
			r.mu.Unlock()
			return fmt.Errorf("task %s has no committed winner", event.TaskID)
		}
		if committedWinner != event.WorkerID {
			r.mu.Unlock()
			return fmt.Errorf("task %s terminal event from worker %d, want %d", event.TaskID, event.WorkerID, committedWinner)
		}
		if committed, ok := r.terminal[event.TaskID]; ok && committed != event.Type {
			r.mu.Unlock()
			return fmt.Errorf("task %s already terminal as %v", event.TaskID, committed)
		}
		r.terminal[event.TaskID] = event.Type
	case models.EventCanceled:
		if committed, ok := r.terminal[event.TaskID]; ok && committed != event.Type {
			r.mu.Unlock()
			return fmt.Errorf("task %s already terminal as %v", event.TaskID, committed)
		}
		r.terminal[event.TaskID] = event.Type
	case models.EventWorkerDown:
		r.down[event.WorkerID] = true
		for taskID, winnerID := range r.winners {
			if winnerID == event.WorkerID {
				delete(r.winners, taskID)
				delete(r.terminal, taskID)
			}
		}
	case models.EventWorkerUp:
		delete(r.down, event.WorkerID)
	case models.EventWorkerRemoved:
		r.removed[event.WorkerID] = true
		r.down[event.WorkerID] = true
		for taskID, winnerID := range r.winners {
			if winnerID == event.WorkerID {
				delete(r.winners, taskID)
				delete(r.terminal, taskID)
			}
		}
	case models.EventWorkerAdded:
		delete(r.removed, event.WorkerID)
		delete(r.down, event.WorkerID)
	}

	workers := append([]*Worker(nil), r.workers...)
	r.mu.Unlock()

	for _, w := range workers {
		w.ApplyTaskEvent(event)
	}
	select {
	case r.events <- event:
	default:
	}
	return nil
}

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

func TestSharedAssignedTaskWithAcceptedRaftFlow(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	reporter := newFakeClusterReporter(workers)

	var execMu sync.Mutex
	executedBy := make([]int, 0, 4)
	executedCh := make(chan int, 4)

	for _, w := range workers {
		w.SetTaskEventReporter(reporter)
		wid := w.ID
		w.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
			execMu.Lock()
			executedBy = append(executedBy, wid)
			execMu.Unlock()
			select {
			case executedCh <- wid:
			default:
			}
			time.Sleep(50 * time.Millisecond)
			return fmt.Sprintf("done-by-%d", wid), nil
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, w := range workers {
		w := w
		go func() { _ = w.RunTaskLoop(ctx) }()
	}

	task := &models.Task{ID: "shared-accepted", Description: "all workers contend for same task"}
	event := models.TaskEvent{Type: models.EventAssigned, TaskID: task.ID, Task: task}
	for _, w := range workers {
		w.ApplyTaskEvent(event)
	}

	var winner int
	select {
	case winner = <-executedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("no worker executed after shared EventAssigned")
	}

	deadline := time.After(5 * time.Second)
	var sawDone bool
	for !sawDone {
		select {
		case committed := <-reporter.events:
			if committed.TaskID != task.ID {
				continue
			}
			if committed.Type == models.EventDone {
				sawDone = true
				if committed.WorkerID != winner {
					t.Fatalf("EventDone committed by worker %d, want %d", committed.WorkerID, winner)
				}
			}
		case <-deadline:
			t.Fatal("timed out waiting for committed EventDone")
		}
	}

	time.Sleep(300 * time.Millisecond)

	execMu.Lock()
	defer execMu.Unlock()
	if len(executedBy) != 1 {
		t.Fatalf("executors ran %d times, want exactly 1; workers=%v", len(executedBy), executedBy)
	}
	if executedBy[0] != winner {
		t.Fatalf("recorded executor worker %d, want %d", executedBy[0], winner)
	}

	for _, w := range workers {
		w.mu.Lock()
		status := w.taskStatus[task.ID]
		w.mu.Unlock()
		if status != models.TaskDone {
			t.Fatalf("worker %d taskStatus = %v, want TaskDone", w.ID, status)
		}
	}
}

func TestSharedAssignedTasksProgressAcrossMultipleRounds(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	reporter := newFakeClusterReporter(workers)

	var execMu sync.Mutex
	executedByTask := make(map[string][]int)

	for _, w := range workers {
		w.SetTaskEventReporter(reporter)
		wid := w.ID
		w.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
			execMu.Lock()
			executedByTask[task.ID] = append(executedByTask[task.ID], wid)
			execMu.Unlock()
			time.Sleep(40 * time.Millisecond)
			return fmt.Sprintf("done-by-%d", wid), nil
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, w := range workers {
		w := w
		go func() { _ = w.RunTaskLoop(ctx) }()
	}

	taskIDs := []string{"round-task-0", "round-task-1", "round-task-2"}
	for _, taskID := range taskIDs {
		task := &models.Task{ID: taskID, Description: "sequential shared task"}
		event := models.TaskEvent{Type: models.EventAssigned, TaskID: task.ID, Task: task}
		for _, w := range workers {
			w.ApplyTaskEvent(event)
		}

		deadline := time.After(5 * time.Second)
		for {
			select {
			case committed := <-reporter.events:
				if committed.TaskID == taskID && committed.Type == models.EventDone {
					goto nextTask
				}
			case <-deadline:
				t.Fatalf("timed out waiting for EventDone for %s", taskID)
			}
		}
	nextTask:
		waitForAllWorkersReleased(t, workers, 3*time.Second)
	}

	time.Sleep(300 * time.Millisecond)

	execMu.Lock()
	defer execMu.Unlock()
	for _, taskID := range taskIDs {
		executors := executedByTask[taskID]
		if len(executors) != 1 {
			t.Fatalf("task %s executed %d times, want 1; workers=%v", taskID, len(executors), executors)
		}
	}
}

func TestAcceptedWinnerDownAllowsReassignment(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	reporter := newFakeClusterReporter(workers)

	var execCount int32
	firstStarted := make(chan int, 1)
	secondStarted := make(chan int, 1)

	for _, w := range workers {
		w.SetTaskEventReporter(reporter)
		wid := w.ID
		w.SetTaskExecutor(func(ctx context.Context, task *models.Task) (string, error) {
			n := atomic.AddInt32(&execCount, 1)
			if n == 1 {
				select {
				case firstStarted <- wid:
				default:
				}
				<-ctx.Done()
				return "", ctx.Err()
			}

			select {
			case secondStarted <- wid:
			default:
			}
			return fmt.Sprintf("done-by-%d", wid), nil
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, w := range workers {
		w := w
		go func() { _ = w.RunTaskLoop(ctx) }()
	}

	taskID := "winner-down-reassign"
	task := &models.Task{ID: taskID, Description: "accepted winner fails and task is retried"}
	if err := reporter.CommitTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: task.ID, Task: task}); err != nil {
		t.Fatalf("commit initial assignment: %v", err)
	}

	var failedWinner int
	select {
	case failedWinner = <-firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for accepted winner to start executing")
	}

	if err := reporter.CommitTaskEvent(models.TaskEvent{Type: models.EventWorkerDown, WorkerID: failedWinner}); err != nil {
		t.Fatalf("commit worker down: %v", err)
	}

	retryTask := &models.Task{ID: taskID, Description: "reassigned after winner failure", RetryCount: 1}
	if err := reporter.CommitTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: retryTask.ID, Task: retryTask}); err != nil {
		t.Fatalf("commit reassignment: %v", err)
	}

	var replacement int
	select {
	case replacement = <-secondStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for reassigned task to start on a survivor")
	}

	if replacement == failedWinner {
		t.Fatalf("reassigned task started on failed winner %d", replacement)
	}

	deadline := time.After(5 * time.Second)
	for {
		select {
		case event := <-reporter.events:
			if event.TaskID == taskID && event.Type == models.EventDone {
				if event.WorkerID != replacement {
					t.Fatalf("EventDone committed by worker %d, want replacement %d", event.WorkerID, replacement)
				}
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for reassigned task EventDone")
		}
	}
}

func TestEventWorkerDownCancelsAcceptedExecutingWinner(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	reporter := newFakeClusterReporter(workers)

	started := make(chan int, 1)
	canceled := make(chan int, 1)

	for _, w := range workers {
		w.SetTaskEventReporter(reporter)
		wid := w.ID
		w.SetTaskExecutor(func(ctx context.Context, task *models.Task) (string, error) {
			select {
			case started <- wid:
			default:
			}
			<-ctx.Done()
			select {
			case canceled <- wid:
			default:
			}
			return "", ctx.Err()
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, w := range workers {
		w := w
		go func() { _ = w.RunTaskLoop(ctx) }()
	}

	task := &models.Task{ID: "winner-down-cancel", Description: "accepted winner should cancel on down"}
	if err := reporter.CommitTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: task.ID, Task: task}); err != nil {
		t.Fatalf("commit assignment: %v", err)
	}

	var winner int
	select {
	case winner = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for accepted winner to start")
	}

	if err := reporter.CommitTaskEvent(models.TaskEvent{Type: models.EventWorkerDown, WorkerID: winner}); err != nil {
		t.Fatalf("commit worker down: %v", err)
	}

	select {
	case got := <-canceled:
		if got != winner {
			t.Fatalf("canceled worker = %d, want winner %d", got, winner)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("accepted winner executor was not canceled by EventWorkerDown")
	}
}

func TestAcceptedWinnerRemovedAllowsReassignment(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	reporter := newFakeClusterReporter(workers)

	var execCount int32
	firstStarted := make(chan int, 1)
	secondStarted := make(chan int, 1)

	for _, w := range workers {
		w.SetTaskEventReporter(reporter)
		wid := w.ID
		w.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
			n := atomic.AddInt32(&execCount, 1)
			if n == 1 {
				select {
				case firstStarted <- wid:
				default:
				}
				time.Sleep(200 * time.Millisecond)
				return fmt.Sprintf("stale-done-by-%d", wid), nil
			}

			select {
			case secondStarted <- wid:
			default:
			}
			return fmt.Sprintf("done-by-%d", wid), nil
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, w := range workers {
		w := w
		go func() { _ = w.RunTaskLoop(ctx) }()
	}

	taskID := "winner-removed-reassign"
	task := &models.Task{ID: taskID, Description: "accepted winner removed and task retried"}
	if err := reporter.CommitTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: task.ID, Task: task}); err != nil {
		t.Fatalf("commit initial assignment: %v", err)
	}

	var removedWinner int
	select {
	case removedWinner = <-firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for initial winner to start")
	}

	if err := reporter.CommitTaskEvent(models.TaskEvent{Type: models.EventWorkerRemoved, WorkerID: removedWinner}); err != nil {
		t.Fatalf("commit worker removed: %v", err)
	}

	retryTask := &models.Task{ID: taskID, Description: "reassigned after removal", RetryCount: 1}
	if err := reporter.CommitTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: retryTask.ID, Task: retryTask}); err != nil {
		t.Fatalf("commit reassignment: %v", err)
	}

	var replacement int
	select {
	case replacement = <-secondStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for reassigned worker after removal")
	}

	if replacement == removedWinner {
		t.Fatalf("reassigned task started on removed winner %d", replacement)
	}

	deadline := time.After(5 * time.Second)
	for {
		select {
		case event := <-reporter.events:
			if event.TaskID == taskID && event.Type == models.EventDone {
				if event.WorkerID != replacement {
					t.Fatalf("EventDone committed by worker %d, want replacement %d", event.WorkerID, replacement)
				}
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for reassigned EventDone after removal")
		}
	}
}

func TestDuplicateAssignedWhileQueuedOrInProgressDoesNotDoubleExecute(t *testing.T) {
	workers, _ := startWorkers(t, 1)
	defer stopWorkers(workers)

	w := workers[0]
	reporter := newFakeTaskReporter()
	reporter.submit = func(_ context.Context, event models.TaskEvent) error {
		w.ApplyTaskEvent(event)
		return nil
	}
	w.SetTaskEventReporter(reporter)

	var execCount int32
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	w.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
		atomic.AddInt32(&execCount, 1)
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return "ok:" + task.ID, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.RunTaskLoop(ctx) }()

	task := &models.Task{ID: "dup-assigned-live", Description: "duplicate assigned while active"}
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: task.ID, Task: task})
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: task.ID, Task: task})

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("executor never started")
	}

	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: task.ID, Task: task})
	close(release)

	waitForWorkerReleased(t, w, 3*time.Second)

	if got := atomic.LoadInt32(&execCount); got != 1 {
		t.Fatalf("duplicate EventAssigned caused %d executions, want 1", got)
	}
}

func TestDuplicateCommittedTaskEventsAreIdempotent(t *testing.T) {
	workers, _ := startWorkers(t, 1)
	defer stopWorkers(workers)

	w := workers[0]
	taskID := "dup-committed"

	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventWon, TaskID: taskID, WorkerID: 7})
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventWon, TaskID: taskID, WorkerID: 7})

	w.mu.Lock()
	st := w.taskStatus[taskID]
	w.mu.Unlock()
	if st != models.TaskWon {
		t.Fatalf("after duplicate EventWon taskStatus = %v, want TaskWon", st)
	}

	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventDone, TaskID: taskID, WorkerID: 7, Result: "ok"})
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventDone, TaskID: taskID, WorkerID: 7, Result: "ok"})

	w.mu.Lock()
	st = w.taskStatus[taskID]
	w.mu.Unlock()
	if st != models.TaskDone {
		t.Fatalf("after duplicate EventDone taskStatus = %v, want TaskDone", st)
	}
}

func TestDuplicateCanceledEventsAreIdempotent(t *testing.T) {
	workers, _ := startWorkers(t, 1)
	defer stopWorkers(workers)

	w := workers[0]
	taskID := "dup-canceled"

	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventCanceled, TaskID: taskID, WorkerID: 8, Reason: "user canceled"})
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventCanceled, TaskID: taskID, WorkerID: 8, Reason: "user canceled"})

	w.mu.Lock()
	st := w.taskStatus[taskID]
	queued := w.queuedTasks[taskID]
	w.mu.Unlock()

	if queued {
		t.Fatal("task remained queued after duplicate EventCanceled")
	}
	if st != models.TaskCanceled {
		t.Fatalf("after duplicate EventCanceled taskStatus = %v, want TaskCanceled", st)
	}
}

func TestDoneAfterCanceledIgnored(t *testing.T) {
	workers, _ := startWorkers(t, 1)
	defer stopWorkers(workers)

	w := workers[0]
	taskID := "done-after-canceled"

	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventCanceled, TaskID: taskID, WorkerID: 9, Reason: "user canceled"})
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventDone, TaskID: taskID, WorkerID: 9, Result: "late"})

	w.mu.Lock()
	st := w.taskStatus[taskID]
	w.mu.Unlock()
	if st != models.TaskCanceled {
		t.Fatalf("taskStatus = %v, want TaskCanceled", st)
	}
}

func TestCanceledAfterDoneIgnored(t *testing.T) {
	workers, _ := startWorkers(t, 1)
	defer stopWorkers(workers)

	w := workers[0]
	taskID := "canceled-after-done"

	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventDone, TaskID: taskID, WorkerID: 4, Result: "ok"})
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventCanceled, TaskID: taskID, WorkerID: 9, Reason: "late cancel"})

	w.mu.Lock()
	st := w.taskStatus[taskID]
	w.mu.Unlock()
	if st != models.TaskDone {
		t.Fatalf("taskStatus = %v, want TaskDone", st)
	}
}

func TestReplayCommittedTaskHistoryIsStable(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w := workers[0]
	taskID := "replay-history"
	sequence := []models.TaskEvent{
		{Type: models.EventAssigned, TaskID: taskID, Task: &models.Task{ID: taskID, Description: "first attempt"}},
		{Type: models.EventWon, TaskID: taskID, WorkerID: 2},
		{Type: models.EventWorkerDown, WorkerID: 2},
		{Type: models.EventAssigned, TaskID: taskID, Task: &models.Task{ID: taskID, Description: "retry", RetryCount: 1}},
		{Type: models.EventDone, TaskID: taskID, WorkerID: 5, Result: "ok"},
	}

	for _, event := range sequence {
		w.ApplyTaskEvent(event)
	}

	w.mu.Lock()
	initialStatus := w.taskStatus[taskID]
	initialQueued := w.queuedTasks[taskID]
	initialLive := w.liveWorkers[2]
	w.mu.Unlock()

	if initialStatus != models.TaskDone {
		t.Fatalf("after first replay taskStatus = %v, want TaskDone", initialStatus)
	}
	if initialQueued {
		t.Fatal("task remained queued after first replay")
	}
	if initialLive {
		t.Fatal("worker 2 should remain marked down after first replay")
	}

	for _, event := range sequence {
		w.ApplyTaskEvent(event)
	}

	w.mu.Lock()
	finalStatus := w.taskStatus[taskID]
	finalQueued := w.queuedTasks[taskID]
	finalLive := w.liveWorkers[2]
	w.mu.Unlock()

	if finalStatus != models.TaskDone {
		t.Fatalf("after second replay taskStatus = %v, want TaskDone", finalStatus)
	}
	if finalQueued {
		t.Fatal("task became queued after replay")
	}
	if finalLive {
		t.Fatal("worker 2 should remain marked down after replay")
	}
}

func TestLateStaleTerminalFromOldWinnerRejectedAfterReassignment(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	reporter := newFakeClusterReporter(workers)

	var execCount int32
	firstStarted := make(chan int, 1)
	secondStarted := make(chan int, 1)
	firstFinished := make(chan int, 1)

	for _, w := range workers {
		w.SetTaskEventReporter(reporter)
		wid := w.ID
		w.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
			n := atomic.AddInt32(&execCount, 1)
			if n == 1 {
				select {
				case firstStarted <- wid:
				default:
				}
				time.Sleep(250 * time.Millisecond)
				select {
				case firstFinished <- wid:
				default:
				}
				return fmt.Sprintf("late-stale-done-by-%d", wid), nil
			}

			select {
			case secondStarted <- wid:
			default:
			}
			return fmt.Sprintf("done-by-%d", wid), nil
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, w := range workers {
		w := w
		go func() { _ = w.RunTaskLoop(ctx) }()
	}

	taskID := "late-stale-terminal"
	task := &models.Task{ID: taskID, Description: "old winner reports late after reassignment"}
	if err := reporter.CommitTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: task.ID, Task: task}); err != nil {
		t.Fatalf("commit initial assignment: %v", err)
	}

	var failedWinner int
	select {
	case failedWinner = <-firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for initial winner start")
	}

	if err := reporter.CommitTaskEvent(models.TaskEvent{Type: models.EventWorkerDown, WorkerID: failedWinner}); err != nil {
		t.Fatalf("commit worker down: %v", err)
	}

	retryTask := &models.Task{ID: taskID, Description: "retry after winner failure", RetryCount: 1}
	if err := reporter.CommitTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: retryTask.ID, Task: retryTask}); err != nil {
		t.Fatalf("commit retry assignment: %v", err)
	}

	var replacement int
	select {
	case replacement = <-secondStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for replacement start")
	}

	if replacement == failedWinner {
		t.Fatalf("replacement equals failed winner %d", replacement)
	}

	select {
	case got := <-firstFinished:
		if got != failedWinner {
			t.Fatalf("stale finisher = %d, want failed winner %d", got, failedWinner)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for stale old winner to finish")
	}

	deadline := time.After(5 * time.Second)
	for {
		select {
		case event := <-reporter.events:
			if event.TaskID == taskID && event.Type == models.EventDone {
				if event.WorkerID != replacement {
					t.Fatalf("final EventDone committed by worker %d, want replacement %d", event.WorkerID, replacement)
				}
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for replacement EventDone after stale old terminal")
		}
	}
}

func TestLateStaleTerminalFromOldWinnerRejectedAfterCancel(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	reporter := newFakeClusterReporter(workers)

	started := make(chan int, 1)
	for _, w := range workers {
		w.SetTaskEventReporter(reporter)
		wid := w.ID
		w.SetTaskExecutor(func(ctx context.Context, task *models.Task) (string, error) {
			select {
			case started <- wid:
			default:
			}
			<-ctx.Done()
			return "", ctx.Err()
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, w := range workers {
		w := w
		go func() { _ = w.RunTaskLoop(ctx) }()
	}

	taskID := "late-stale-after-cancel"
	task := &models.Task{ID: taskID, Description: "winner canceled before stale terminal"}
	if err := reporter.CommitTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: task.ID, Task: task}); err != nil {
		t.Fatalf("commit assignment: %v", err)
	}

	var canceledWinner int
	select {
	case canceledWinner = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for winner to start")
	}

	if err := reporter.CommitTaskEvent(models.TaskEvent{
		Type:     models.EventCanceled,
		TaskID:   taskID,
		WorkerID: 99,
		Reason:   "requester canceled",
	}); err != nil {
		t.Fatalf("commit cancel: %v", err)
	}

	if err := reporter.CommitTaskEvent(models.TaskEvent{
		Type:     models.EventDone,
		TaskID:   taskID,
		WorkerID: canceledWinner,
		Result:   "late",
	}); err == nil {
		t.Fatal("expected stale EventDone after cancel to be rejected")
	}

	waitForAllWorkersReleased(t, workers, 5*time.Second)
	for _, w := range workers {
		waitForTaskStatus(t, w, taskID, models.TaskCanceled, 5*time.Second)
	}
}

func TestConcurrentSharedAssignedTasksCompleteExactlyOnce(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	reporter := newFakeClusterReporter(workers)

	var execMu sync.Mutex
	executedByTask := make(map[string][]int)

	for _, w := range workers {
		w.SetTaskEventReporter(reporter)
		wid := w.ID
		w.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
			execMu.Lock()
			executedByTask[task.ID] = append(executedByTask[task.ID], wid)
			execMu.Unlock()
			time.Sleep(30 * time.Millisecond)
			return fmt.Sprintf("done-by-%d", wid), nil
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, w := range workers {
		w := w
		go func() { _ = w.RunTaskLoop(ctx) }()
	}

	taskIDs := []string{"concurrent-0", "concurrent-1", "concurrent-2", "concurrent-3"}
	for _, taskID := range taskIDs {
		task := &models.Task{ID: taskID, Description: "shared concurrent task"}
		if err := reporter.CommitTaskEvent(models.TaskEvent{Type: models.EventAssigned, TaskID: taskID, Task: task}); err != nil {
			t.Fatalf("commit assignment for %s: %v", taskID, err)
		}
	}

	doneByTask := make(map[string]int)
	deadline := time.After(10 * time.Second)
	for len(doneByTask) < len(taskIDs) {
		select {
		case event := <-reporter.events:
			if event.Type != models.EventDone {
				continue
			}
			if _, seen := doneByTask[event.TaskID]; seen {
				t.Fatalf("duplicate EventDone observed for task %s", event.TaskID)
			}
			doneByTask[event.TaskID] = event.WorkerID
		case <-deadline:
			t.Fatal("timed out waiting for concurrent shared tasks to finish")
		}
	}

	waitForAllWorkersReleased(t, workers, 5*time.Second)

	execMu.Lock()
	defer execMu.Unlock()
	for _, taskID := range taskIDs {
		executors := executedByTask[taskID]
		if len(executors) != 1 {
			t.Fatalf("task %s executed %d times, want 1; workers=%v", taskID, len(executors), executors)
		}
	}
}

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

func waitForWorkerReleased(t *testing.T, w *Worker, timeout time.Duration) {
	t.Helper()

	deadline := time.After(timeout)
	for {
		w.mu.Lock()
		state := w.state
		current := w.currentTask
		w.mu.Unlock()

		if state == StateReleased && current == nil {
			return
		}

		select {
		case <-deadline:
			t.Fatal("worker did not return to released state")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func waitForAllWorkersReleased(t *testing.T, workers []*Worker, timeout time.Duration) {
	t.Helper()
	for _, w := range workers {
		waitForWorkerReleased(t, w, timeout)
	}
}

func waitForTaskStateCleared(t *testing.T, w *Worker, taskID string, timeout time.Duration) {
	t.Helper()

	deadline := time.After(timeout)
	for {
		w.mu.Lock()
		_, queued := w.queuedTasks[taskID]
		_, status := w.taskStatus[taskID]
		_, commit := w.taskCommits[taskID]
		w.mu.Unlock()

		if !queued && !status && !commit {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("task state for %s was not fully cleared", taskID)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func waitForTaskStatus(t *testing.T, w *Worker, taskID string, want models.TaskStatus, timeout time.Duration) {
	t.Helper()

	deadline := time.After(timeout)
	for {
		w.mu.Lock()
		st, ok := w.taskStatus[taskID]
		w.mu.Unlock()

		if ok && st == want {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("task %s did not reach status %v", taskID, want)
		case <-time.After(20 * time.Millisecond):
		}
	}
}
