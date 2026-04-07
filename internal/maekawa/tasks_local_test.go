// tests enabled

package maekawa

import (
	"context"
	"testing"
	"time"

	"raft-maekawa-sync/internal/models"
)

// TestRunTaskLoopReportsDoneForAssignedTask: a single worker executes a task
// and reports success via the membership interface.
func TestRunTaskLoopReportsDoneForAssignedTask(t *testing.T) {
	mem := newTaskMembership([]int32{0})
	w := NewWorker(0, QuorumFor(0, 1), mem)

	w.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
		return "ok:" + task.ID, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { w.RunTaskLoop(ctx) }()

	task := &models.Task{ID: "assigned-1", Data: "from raft"}
	w.taskQueue <- task

	mem.waitForDone(t, task.ID, 3*time.Second)
}

// TestWorkerSkipsCanceledTaskBeforeExecution: a task canceled before it is
// dequeued must never be executed.
func TestWorkerSkipsCanceledTaskBeforeExecution(t *testing.T) {
	mem := newTaskMembership([]int32{0})
	w := NewWorker(0, QuorumFor(0, 1), mem)

	executed := make(chan struct{}, 1)
	w.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
		select {
		case executed <- struct{}{}:
		default:
		}
		return "unexpected", nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { w.RunTaskLoop(ctx) }()

	taskID := "cancel-me"
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventCanceled, TaskID: taskID})
	w.taskQueue <- &models.Task{ID: taskID, Data: ""}

	select {
	case <-executed:
		t.Fatal("executor ran for canceled task")
	case <-time.After(500 * time.Millisecond):
	}
}

// TestWorkerAppliesDoneEventSkipsTask: receiving EventDone for a task marks it
// as canceled locally; subsequent dequeue of the same taskID is skipped.
func TestWorkerAppliesDoneEventSkipsTask(t *testing.T) {
	mem := newTaskMembership([]int32{0})
	w := NewWorker(0, QuorumFor(0, 1), mem)

	executed := make(chan string, 1)
	w.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
		executed <- task.ID
		return "ok", nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { w.RunTaskLoop(ctx) }()

	taskID := "done-received"
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventDone, TaskID: taskID, WorkerID: 3})
	w.taskQueue <- &models.Task{ID: taskID, Data: ""}

	select {
	case id := <-executed:
		t.Fatalf("task %q was executed after EventDone was applied (expected skip)", id)
	case <-time.After(400 * time.Millisecond):
	}
}

// TestRunTaskLoopProcessesMultipleTasksSequentially: two tasks enqueued serially
// each complete before the next starts.
func TestRunTaskLoopProcessesMultipleTasksSequentially(t *testing.T) {
	mem := newTaskMembership([]int32{0})
	w := NewWorker(0, QuorumFor(0, 1), mem)

	order := make([]string, 0, 2)
	doneSignal := make(chan struct{})
	w.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
		order = append(order, task.ID)
		if len(order) == 2 {
			close(doneSignal)
		}
		return "ok", nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { w.RunTaskLoop(ctx) }()

	for _, id := range []string{"multi-1", "multi-2"} {
		w.taskQueue <- &models.Task{ID: id, Data: ""}
	}

	select {
	case <-doneSignal:
	case <-time.After(5 * time.Second):
		t.Fatal("not all tasks completed")
	}

	if len(order) != 2 || order[0] != "multi-1" || order[1] != "multi-2" {
		t.Errorf("execution order = %v, want [multi-1 multi-2]", order)
	}
}

// TestRunTaskLoopContextCancellationStops: cancelling the parent context stops
// RunTaskLoop cleanly.
func TestRunTaskLoopContextCancellationStops(t *testing.T) {
	mem := newTaskMembership([]int32{0})
	w := NewWorker(0, QuorumFor(0, 1), mem)
	w.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
		return "ok", nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		w.RunTaskLoop(ctx)
		close(stopped)
	}()

	cancel()

	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("RunTaskLoop did not stop after context cancellation")
	}
}
