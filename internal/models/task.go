package models

// licycle of task
type TaskStatus uint8

const (
	TaskPending    TaskStatus = iota // submitted, not yet assigned
	TaskInProgress                   // worker won quorum, executing
	TaskDone                         // completed successfully
	TaskFailed                       // worker failed or timed out
)

// task object that is being passed around
type Task struct {
	ID          string     `json:"id"`
	Description string     `json:"description"`
	RequesterID int        `json:"requester_id"`
	OwnerID     int        `json:"owner_id"` // worker that won the quorum
	Status      TaskStatus `json:"status"`
	Reward      float64    `json:"reward"`      // paid to worker on success
	Penalty     float64    `json:"penalty"`     // slashed from worker on failure
	RetryCount  int        `json:"retry_count"` // how many times task has been re-queued
	MaxRetries  int        `json:"max_retries"` // permanently fail after this many retries
	CreatedAt   int64      `json:"created_at"`  // unix nanoseconds
	StartedAt   int64      `json:"started_at"`
	FinishedAt  int64      `json:"finished_at"`
}

type EventType uint8

const (
	EventAssigned      EventType = iota + 1 // worker won quorum, task started
	EventDone                               // worker completed successfully
	EventFailed                             // worker failed or timed out
	EventWorkerDown                         // worker crashed, remove from live quorum
	EventWorkerUp                           // worker recovered, add back to live quorum
	EventWorkerRemoved                      // worker removed from active membership
	EventWorkerAdded                        // worker added back to active membership
)

// committed to raft log, task metadata
type TaskEvent struct {
	Type     EventType `json:"type"`
	TaskID   string    `json:"task_id"`
	WorkerID int       `json:"worker_id"`
	Result   string    `json:"result,omitempty"` // optional output or hash
	Reason   string    `json:"reason,omitempty"` // why task failed (e.g. "insufficient quorum")
}

// track work's standing in the marketplace
type WorkerStats struct {
	WorkerID       int     `json:"worker_id"`
	Reputation     float64 `json:"reputation"`
	TotalCompleted int     `json:"total_completed"`
}

// track requester's wallet
type RequesterStats struct {
	RequesterID int     `json:"requester_id"`
	Balance     float64 `json:"balance"`
}
