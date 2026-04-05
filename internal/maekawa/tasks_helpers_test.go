package maekawa

import (
	"context"
	"fmt"
	"sync"
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
