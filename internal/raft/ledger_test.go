package raft

import (
	"testing"

	"raft-maekawa-sync/internal/models"
)

func TestApplyAssignedEvent(t *testing.T) {
	n := NewNode(1, ":5001", nil, nil)
	event := models.TaskEvent{
		Type:   models.EventAssigned,
		TaskID: "t1",
		Task:   &models.Task{ID: "t1", Data: "render_001"},
	}
	cmd, _ := event.Encode()

	n.mu.Lock()
	n.appendEntry(cmd)
	n.commitIndex = 1
	n.applyCommittedEntries()
	task := n.state.Tasks["t1"]
	n.mu.Unlock()

	if task == nil {
		t.Fatal("task should exist after assigned event")
	}
	if task.Data != "render_001" {
		t.Fatalf("wrong data: %s", task.Data)
	}
	if task.Status != models.EventAssigned {
		t.Fatal("wrong status")
	}
}

func TestApplyDoneAfterClaimed(t *testing.T) {
	n := NewNode(1, ":5001", nil, nil)
	events := []models.TaskEvent{
		{Type: models.EventAssigned, TaskID: "t1", Task: &models.Task{ID: "t1", Data: "x"}},
		{Type: models.EventClaimed, TaskID: "t1", WorkerID: 2},
		{Type: models.EventDone, TaskID: "t1", WorkerID: 2, Result: "ok"},
	}

	n.mu.Lock()
	for _, e := range events {
		cmd, _ := e.Encode()
		n.appendEntry(cmd)
	}
	n.commitIndex = 3
	n.applyCommittedEntries()
	task := n.state.Tasks["t1"]
	n.mu.Unlock()

	if task.Status != models.EventDone {
		t.Fatalf("expected done, got %s", task.Status)
	}
	if task.Result != "ok" {
		t.Fatal("wrong result")
	}
	if task.AssignedTo != 2 {
		t.Fatal("wrong worker")
	}
}

func TestApplyWorkerUpDown(t *testing.T) {
	n := NewNode(1, ":5001", nil, nil)
	events := []models.TaskEvent{
		{Type: models.EventWorkerUp, WorkerID: 5},
		{Type: models.EventWorkerDown, WorkerID: 5},
	}

	n.mu.Lock()
	for _, e := range events {
		cmd, _ := e.Encode()
		n.appendEntry(cmd)
	}
	n.commitIndex = 2
	n.applyCommittedEntries()
	alive := n.state.ActiveWorkers[5]
	n.mu.Unlock()

	if alive {
		t.Fatal("worker 5 should be down")
	}
}

func TestApplyCallsApplier(t *testing.T) {
	fake := &fakeNotifier{}
	n := NewNode(1, ":5001", nil, fake)
	event := models.TaskEvent{Type: models.EventWorkerUp, WorkerID: 9}
	cmd, _ := event.Encode()

	n.mu.Lock()
	n.appendEntry(cmd)
	n.commitIndex = 1
	n.applyCommittedEntries()
	n.mu.Unlock()

	if len(fake.up) != 1 || fake.up[0] != 9 {
		t.Fatal("applier was not called with worker up event")
	}
}