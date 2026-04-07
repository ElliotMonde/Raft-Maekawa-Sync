// tests enabled

package maekawa

import (
	"context"
	"testing"
	"time"

	"raft-maekawa-sync/internal/models"
)

// TestApplyTaskEventCanceledEntriesMap verifies the canceledTasks map is updated.
func TestApplyTaskEventCanceledEntriesMap(t *testing.T) {
	mem := newTaskMembership([]int32{0})
	w := NewWorker(0, QuorumFor(0, 1), mem)

	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventCanceled, TaskID: "cancel-state-1"})

	w.Mu.Lock()
	canceled := w.canceledTasks["cancel-state-1"]
	w.Mu.Unlock()

	if !canceled {
		t.Error("canceledTasks[cancel-state-1] should be true after EventCanceled")
	}
}

// TestApplyTaskEventDoneEntriesMap verifies EventDone sets the canceledTasks flag.
func TestApplyTaskEventDoneEntriesMap(t *testing.T) {
	mem := newTaskMembership([]int32{0})
	w := NewWorker(0, QuorumFor(0, 1), mem)

	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventDone, TaskID: "done-state-1"})

	w.Mu.Lock()
	canceled := w.canceledTasks["done-state-1"]
	w.Mu.Unlock()

	if !canceled {
		t.Error("canceledTasks[done-state-1] should be true after EventDone")
	}
}

// TestApplyTaskEventWorkerUpDownTriggersRegrid verifies membership change events
// update the quorum (validated via quorum always containing self).
func TestApplyTaskEventWorkerUpDownTriggersRegrid(t *testing.T) {
	mem := newTaskMembership([]int32{0, 1, 2, 3, 4, 5, 6, 7, 8})
	w := NewWorker(0, QuorumFor(0, 9), mem)

	w.Mu.Lock()
	before := make([]int32, len(w.quorum))
	copy(before, w.quorum)
	w.Mu.Unlock()

	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventWorkerDown, WorkerID: 8})

	w.Mu.Lock()
	after := make([]int32, len(w.quorum))
	copy(after, w.quorum)
	w.Mu.Unlock()

	found := false
	for _, id := range after {
		if id == w.ID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("self %d not in quorum after EventWorkerDown: %v", w.ID, after)
	}
	t.Logf("quorum before: %v, after EventWorkerDown: %v", before, after)
}

// TestCanceledTaskIdempotency verifies applying EventCanceled twice is safe.
func TestCanceledTaskIdempotency(t *testing.T) {
	mem := newTaskMembership([]int32{0})
	w := NewWorker(0, QuorumFor(0, 1), mem)

	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventCanceled, TaskID: "idem-cancel"})
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventCanceled, TaskID: "idem-cancel"})

	w.Mu.Lock()
	c := w.canceledTasks["idem-cancel"]
	w.Mu.Unlock()
	if !c {
		t.Error("canceledTasks should be true after duplicate EventCanceled")
	}
}

// TestTaskNotExecutedWhenCanceledBeforeDequeue: if canceled before dequeue
// RunTaskLoop skips execution.
func TestTaskNotExecutedWhenCanceledBeforeDequeue(t *testing.T) {
	mem := newTaskMembership([]int32{0})
	w := NewWorker(0, QuorumFor(0, 1), mem)

	executed := make(chan struct{}, 1)
	w.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
		select {
		case executed <- struct{}{}:
		default:
		}
		return "", nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { w.RunTaskLoop(ctx) }()

	taskID := "no-exec"
	w.ApplyTaskEvent(models.TaskEvent{Type: models.EventCanceled, TaskID: taskID})
	w.taskQueue <- &models.Task{ID: taskID, Data: ""}

	select {
	case <-executed:
		t.Fatal("executor ran for canceled task")
	case <-time.After(400 * time.Millisecond):
	}
}

// TestClaimTaskBySingleWinner: with two competing workers, only one wins.
func TestClaimTaskBySingleWinner(t *testing.T) {
	ids := []int32{0, 1}
	mem := newTaskMembership(ids)

	won1, err1 := mem.ClaimTask("exclusive-task", 0)
	won2, err2 := mem.ClaimTask("exclusive-task", 1)

	if err1 != nil || err2 != nil {
		t.Fatalf("ClaimTask errors: %v, %v", err1, err2)
	}
	if won1 == won2 {
		t.Errorf("both workers claimed the same task — exclusivity violated")
	}
}
