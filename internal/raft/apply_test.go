package raft

import (
	"testing"

	"raft-maekawa-sync/internal/models"
)

type fakeNotifier struct {
	down     []int32
	up       []int32
	removed  []int32
	added    []int32
	assigned []models.TaskEvent
	done     []models.TaskEvent
	failed   []models.TaskEvent
	canceled []models.TaskEvent
}

func (f *fakeNotifier) ApplyTaskEvent(event models.TaskEvent) {
	switch event.Type {
	case models.EventWorkerDown:
		f.down = append(f.down, event.WorkerID)
	case models.EventWorkerUp:
		f.up = append(f.up, event.WorkerID)
	case models.EventWorkerRemoved:
		f.removed = append(f.removed, event.WorkerID)
	case models.EventWorkerAdded:
		f.added = append(f.added, event.WorkerID)
	case models.EventAssigned:
		f.assigned = append(f.assigned, event)
	case models.EventDone:
		f.done = append(f.done, event)
	case models.EventFailed:
		f.failed = append(f.failed, event)
	case models.EventCanceled:
		f.canceled = append(f.canceled, event)
	}
}

func TestApplyTaskEventToMaekawa(t *testing.T) {
	f := &fakeNotifier{}

	applyTaskEventToMaekawa(models.TaskEvent{Type: models.EventWorkerDown, WorkerID: 1}, f)
	applyTaskEventToMaekawa(models.TaskEvent{Type: models.EventWorkerUp, WorkerID: 2}, f)
	applyTaskEventToMaekawa(models.TaskEvent{Type: models.EventWorkerRemoved, WorkerID: 3}, f)
	applyTaskEventToMaekawa(models.TaskEvent{Type: models.EventWorkerAdded, WorkerID: 4}, f)

	if len(f.down) != 1 || f.down[0] != 1 {
		t.Fatalf("down calls = %v, want [1]", f.down)
	}
	if len(f.up) != 1 || f.up[0] != 2 {
		t.Fatalf("up calls = %v, want [2]", f.up)
	}
	if len(f.removed) != 1 || f.removed[0] != 3 {
		t.Fatalf("removed calls = %v, want [3]", f.removed)
	}
	if len(f.added) != 1 || f.added[0] != 4 {
		t.Fatalf("added calls = %v, want [4]", f.added)
	}
}

func TestApplyTaskEventToMaekawaNilNotifierSafe(t *testing.T) {
	applyTaskEventToMaekawa(models.TaskEvent{Type: models.EventWorkerDown, WorkerID: 1}, nil)
}

func TestApplyTaskEventToMaekawaForwardsTaskEvents(t *testing.T) {
	f := &fakeNotifier{}
	applyTaskEventToMaekawa(models.TaskEvent{Type: models.EventAssigned, WorkerID: 9}, f)
	applyTaskEventToMaekawa(models.TaskEvent{Type: models.EventCanceled, WorkerID: 8}, f)

	if len(f.assigned) != 1 || f.assigned[0].WorkerID != 9 {
		t.Fatalf("assigned calls = %v, want WorkerID 9", f.assigned)
	}
	if len(f.canceled) != 1 || f.canceled[0].WorkerID != 8 {
		t.Fatalf("canceled calls = %v, want WorkerID 8", f.canceled)
	}
}
