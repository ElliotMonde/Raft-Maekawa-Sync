package models

import (
    "encoding/json"
    "fmt"
)

// EventType identifies what a committed Raft log entry represents.
type EventType string

const (
    // Task lifecycle
    EventAssigned EventType = "assigned" // Raft broadcast a new task to all workers
    EventClaimed  EventType = "claimed"  // A worker won the CS and claimed the task
    EventDone     EventType = "done"     // The claiming worker finished execution
    EventFailed   EventType = "failed"   // The claiming worker failed execution
    EventCanceled EventType = "canceled" // Task was canceled before completion

    // Membership — Raft commits these when node health changes
    EventWorkerDown    EventType = "worker_down"
    EventWorkerUp      EventType = "worker_up"
    EventWorkerRemoved EventType = "worker_removed"
    EventWorkerAdded   EventType = "worker_added"
)

// Task is the unit of work the cluster executes.
// Only protocol-required fields — application logic belongs above this layer.
type Task struct {
    ID   string // unique identifier, matches raft.Task.task_id on the wire
    Data string // opaque payload handed to the TaskExecutor
}

// TaskEvent is a decoded Raft log entry broadcast to all workers after commit.
type TaskEvent struct {
    Type     EventType
    TaskID   string
    WorkerID int32 // which worker this event concerns
    Task     *Task // non-nil only for EventAssigned
    Result   string // populated for EventDone
    Reason   string // populated for EventFailed and EventCanceled
    EventUnixNano int64 // wall-clock timestamp used for stale-task recovery
}

// Encode serialises a TaskEvent for storage in a Raft LogEntry.command field.
func (e TaskEvent) Encode() (string, error) {
    b, err := json.Marshal(e)
    if err != nil {
        return "", fmt.Errorf("encode task event: %w", err)
    }
    return string(b), nil
}

// DecodeTaskEvent deserialises a LogEntry.command string back into a TaskEvent.
func DecodeTaskEvent(command string) (*TaskEvent, error) {
    var e TaskEvent
    if err := json.Unmarshal([]byte(command), &e); err != nil {
        return nil, fmt.Errorf("decode task event: %w", err)
    }
    return &e, nil
}
