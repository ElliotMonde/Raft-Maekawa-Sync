package raft

import (
	"testing"

	"raft-maekawa/internal/models"
)

type fakeNotifier struct {
	down    []int
	up      []int
	removed []int
	added   []int
}

func (f *fakeNotifier) NotifyWorkerDown(workerID int)    { f.down = append(f.down, workerID) }
func (f *fakeNotifier) NotifyWorkerUp(workerID int)      { f.up = append(f.up, workerID) }
func (f *fakeNotifier) NotifyWorkerRemoved(workerID int) { f.removed = append(f.removed, workerID) }
func (f *fakeNotifier) NotifyWorkerAdded(workerID int)   { f.added = append(f.added, workerID) }

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

func TestApplyTaskEventToMaekawaIgnoresNonMembershipEvents(t *testing.T) {
	f := &fakeNotifier{}
	applyTaskEventToMaekawa(models.TaskEvent{Type: models.EventAssigned, WorkerID: 9}, f)

	if len(f.down)+len(f.up)+len(f.removed)+len(f.added) != 0 {
		t.Fatalf("expected no notifier calls for non-membership event")
	}
}
