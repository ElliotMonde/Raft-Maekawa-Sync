# Implementation: Raft + Maekawa Integration

This document covers how the two layers connect, the data flow across their boundary, and the failure scenarios that require both layers to cooperate.

---

## The Integration Boundary

Raft and Maekawa are decoupled through two interfaces:

### `ClusterMembership` (Maekawa → Raft)

Defined in `internal/maekawa/membership.go`. The Maekawa worker calls these methods; the Raft node implements them.

```go
type ClusterMembership interface {
    ActiveMembers() []int32
    IsAlive(id int32) bool
    ClaimTask(taskID string, workerID int32) (bool, error)
    ReportTaskSuccess(taskID string, workerID int32, result string) error
    ReportTaskFailure(taskID string, workerID int32, reason string) error
}
```

`ClaimTask` and `ReportTask*` are not local calls — they commit events through Raft. If the node receiving the call is a follower, it forwards to the leader via `SubmitTask` RPC.

### `TaskEventApplier` (Raft → Maekawa)

Defined in `internal/raft/apply.go`. The Raft apply path calls this after committing each entry.

```go
type TaskEventApplier interface {
    ApplyTaskEvent(event models.TaskEvent)
}
```

`applyTaskEventToMaekawa(event, applier)` is called **outside** the Raft `mu` lock, after `applyEvent` has already updated the Raft state machine. This avoids lock inversion: Maekawa's `ApplyTaskEvent` may call back into `ClusterMembership` methods that need the Raft lock.

---

## Data Flow: Task Assigned → Executed → Completed

```
Client
  │
  ▼ SubmitTask(data) [gRPC]
Raft Leader
  │  encode EventAssigned{task_id, task}
  │  append to log, replicate to majority
  │  commit
  │
  ├─► applyEvent(EventAssigned)          [Raft state machine]
  │     state.Tasks[task_id] = PENDING
  │
  └─► applyTaskEventToMaekawa(EventAssigned, applier)
        Worker.ApplyTaskEvent(EventAssigned)
          restoreTask(task_id)           [clear any stale cancel]
          enqueueTask(task)              [non-blocking push to taskQueue]

Worker.RunTaskLoop (goroutine)
  │  dequeue task from taskQueue
  │  check canceledTasks → not canceled, proceed
  │
  ▼ RequestForGlobalLock(ctx)            [Maekawa protocol]
    tick(), send RequestLock to quorum
    wait for all grants via grantChan
    inCS = true
  │
  ▼ handleTaskExecution(ctx, task)
    ClaimTask(task_id, workerID)         [via ClusterMembership → Raft]
      ├─ if leader: commitTaskEventAsLeader(EventClaimed)
      │    pendingClaims[task_id] = workerID
      │    append, replicate, commit
      │    applyEvent(EventClaimed) → AssignedTo = workerID
      │    applyTaskEventToMaekawa(EventClaimed)
      │      other workers: removeFromLocalQueue(task_id)
      └─ returns (true, nil)
  │
  ▼ executor(ctx, task)                  [user-provided function]
    returns (result, nil)
  │
  ▼ ReportTaskSuccess(task_id, workerID, result)
    commitTaskEventAsLeader(EventDone)
    append, replicate, commit
    applyEvent(EventDone) → status=COMPLETED
  │
  ▼ exitGlobalCS()
    inCS = false
    send ReleaseLock to all quorum members
```

---

## Wiring: How the Layers Are Connected at Startup

### Docker / Production (`cmd/worker/main.go`, `cmd/raft/main.go`)

The Raft node and Maekawa worker run in separate processes. The `RemoteMembership` adapter (`internal/raft/remote_membership.go`) lets the worker call into Raft over gRPC without directly importing the Raft node type.

```
cmd/worker process:
  membership := raft.NewRemoteMembership(workerID, raftPeers)
  worker     := maekawa.NewWorker(workerID, quorum, membership)
  worker.SetTaskExecutor(...)
  worker.InitClients(workerPeers, workerID)

  go membership.RunSync(ctx, worker)   // polls Raft for committed events,
                                        // calls worker.ApplyTaskEvent
  go worker.RunTaskLoop(ctx)
  rpc.RegisterMaekawaServer(worker)
  rpc.Start(addr)
```

`RunSync` periodically calls `GetState` on the Raft cluster and delivers any new committed events to the Maekawa worker. This is the apply bridge in the production path.

### Integration Tests (`internal/raft/runtime_integration_test.go`)

In tests, Raft and Maekawa run in the same process. The `Node` directly implements `ClusterMembership` and the `Worker` is registered as the `TaskEventApplier` via `node.SetApplier(worker)`.

```
node   := raft.NewNode(id, addr, peers, nil)
worker := maekawa.NewWorker(id, quorum, node)   // node implements ClusterMembership
node.SetApplier(worker)                          // worker is the TaskEventApplier
```

Events committed by Raft are delivered directly to `worker.ApplyTaskEvent` in the same process — no polling needed.

---

## Safety Invariants

### 1. Maekawa CS is necessary but not sufficient for task ownership

Entering the Maekawa critical section gives a worker *permission to attempt* `ClaimTask`. It does not make the worker the official owner. Official ownership requires a successful Raft commit of `EventClaimed`.

If `ClaimTask` returns `(false, nil)`, the worker lost the Raft-level race (another worker's claim was already in `pendingClaims`). The worker must release the CS and not execute the task.

### 2. Raft commit is the only source of truth

Task status is never derived from Maekawa state. The dashboard, tests, and worker decisions all query the Raft state machine via `GetState`. A task is COMPLETED only when `EventDone` is committed and applied.

### 3. No duplicate execution across crashes

If a leader crashes after accepting `ClaimTask` but before committing:
- The entry is not in any follower's log (not replicated to majority).
- The new leader's state machine still shows the task as PENDING.
- `pendingClaims` on the old leader is lost with it — the guard is not needed because the log entry itself was never committed.
- The task returns to competition. Workers whose `RequestForGlobalLock` was aborted by the regrid re-enqueue and retry.

If a worker crashes after claiming but before reporting done:
- `task_recovery.go` detects the stale `EventClaimed` (AssignedTo = dead worker, timeout elapsed).
- Leader commits a fresh `EventAssigned` to reset the task.
- All workers receive `EventAssigned` via `ApplyTaskEvent` and re-enqueue.

### 4. Only one claim can succeed per task per lifetime

`shouldCommitEventLocked` checks:
- `task.Status == EventAssigned` (no claim already committed)
- `pendingClaims[taskID]` is unset or already set to this worker (no other claim in flight)

The `pendingClaims` reservation is set before the log entry is appended and released on commit, abort, or timeout. This prevents two concurrent `ClaimTask` calls from both appending entries — the second sees the reservation and is rejected before touching the log.

---

## Failure Scenarios

### Leader Failover During Normal Operation

1. Leader crashes. Followers time out and elect a new leader.
2. New leader reconstructs the full state machine from its log.
3. Worker heartbeat loop restarts on the new leader; workers resume heartbeating.
4. In-progress tasks either: (a) already committed — shown as IN_PROGRESS until recovery timeout, then reset; (b) not committed — already PENDING in new leader's state.
5. New tasks can be submitted immediately.

### Worker Crash Before Claim

1. Worker W3 crashes. Leader detects via heartbeat timeout → commits `EventWorkerDown(3)`.
2. All workers receive `EventWorkerDown` → `OnMembershipChange` → regrid quorum.
3. If W3's quorum now excludes the crashed node, remaining workers can still form quorums.
4. Pending task is still PENDING; surviving workers compete via Maekawa with the new quorum.

### Worker Crash After Claim, Before Done

1. W3 successfully claimed task T → `EventClaimed` committed, `AssignedTo=3`.
2. W3 crashes before calling `ReportTaskSuccess`.
3. Leader marks W3 down via heartbeat timeout → commits `EventWorkerDown(3)`.
4. `runTaskRecoveryLoop` detects: T is IN_PROGRESS, W3 is down, `taskClaimTimeout` elapsed.
5. Leader commits fresh `EventAssigned(T)` → task resets to PENDING.
6. Surviving workers receive `EventAssigned` → re-enqueue T → compete via Maekawa.

### Conflicting Concurrent Claims

1. W1 and W2 both exit Maekawa CS at nearly the same time (possible if quorum sizes differ or Maekawa round completes nearly simultaneously on two nodes).
2. Both call `ClaimTask`.
3. W1 arrives first at the leader: `pendingClaims[T] = 1`, log entry appended.
4. W2 arrives: `pendingClaims[T] = 1 ≠ 2` → rejected. Returns `(false, nil)`.
5. W2 exits CS, releases lock back to quorum.
6. W1's entry commits. `AssignedTo=1`. W1 executes.

### Membership Change Mid-Lock-Acquisition

1. W1 is waiting for grants from quorum `{1, 2, 3}`.
2. W3 crashes → `EventWorkerDown(3)` committed → `ApplyTaskEvent(EventWorkerDown)` called on W1's worker.
3. `OnMembershipChange([1,2])` runs: new quorum computed, `grantChan ← false`.
4. `RequestForGlobalLock` receives `false` from `grantChan` → returns error.
5. `RunTaskLoop` calls `retryTaskLater(task)` → re-enqueues after 25 ms.
6. W1 retries with new quorum `{1,2}` — can form quorum and proceed.

---

## Component Dependency Map

```
cmd/raft          cmd/worker          cmd/dashboard
    │                  │                   │
    ▼                  ▼                   ▼
internal/raft     RemoteMembership    internal/dashboard
    │   ▲              │                   │
    │   │   ClusterMembership              │ polls GetState
    │   └──────────────┤                   │
    │                  ▼                   │
    │           internal/maekawa ◄─────────┘ (event hook)
    │                  │
    └── TaskEventApplier (SetApplier)
    │
    ▼
internal/models  (TaskEvent, Task, EventType)
    ▲
    │
api/raft   api/maekawa   (proto-generated gRPC stubs)
```

All cross-process communication is gRPC. The `internal/models` package is the only shared data type between Raft and Maekawa — it is a pure data layer with no logic.
