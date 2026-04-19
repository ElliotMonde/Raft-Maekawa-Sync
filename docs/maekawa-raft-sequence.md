# Maekawa And Raft Interaction Note

This note reflects the current code on `remote-main`.

## 1. What Maekawa Currently Implements

### Roles

- **Requester worker**: the worker trying to enter the global critical section.
- **Voter worker**: each quorum member that can grant at most one vote at a time.
- **Membership source**: an injected interface that answers liveness and records task outcomes.

### Quorum Structure

- Quorums are computed as a row-plus-column grid quorum using [`QuorumFor`](../internal/maekawa/quorum.go).
- On membership change, each worker recomputes its quorum with [`RegridQuorum`](../internal/maekawa/quorum.go) and resets in-flight lock state.

### Maekawa RPC Types

Defined in [`api/maekawa/maekawa.proto`](../api/maekawa/maekawa.proto):

- `RequestLock(LockRequest)`  
  Data: `node_id`, `timestamp`
- `ReleaseLock(ReleaseRequest)`  
  Data: `node_id`, `timestamp`
- `Inquire(InquireRequest)`  
  Data: `sender_id`, `timestamp`
- `Yield(YieldRequest)`  
  Data: `sender_id`, `timestamp`
- `Grant(GrantRequest)`  
  Data: `sender_id`, `timestamp`

### Worker State Used In The Protocol

Important fields in [`internal/maekawa/worker.go`](../internal/maekawa/worker.go):

- `votedFor`, `currentReq`: who this worker currently voted for
- `requestQueue`: queued competing requests ordered by `(timestamp, node_id)`
- `votesReceived`, `grantsReceived`: requester-side vote tracking
- `ownReqTimestamp`: identifies the current lock round
- `committed`, `inCS`: requester-side entry state
- `yieldedTo`, `pendingInquiries`: deadlock-resolution bookkeeping

## 2. Main Maekawa Sequence

This is the actual lock-acquisition path in [`RequestForGlobalLock`](../internal/maekawa/worker.go), [`RequestLock`](../internal/maekawa/worker.go), [`Grant`](../internal/maekawa/worker.go), and [`ReleaseLock`](../internal/maekawa/worker.go).

```mermaid
sequenceDiagram
    autonumber
    participant W as Requesting Worker Wi
    participant Q1 as Quorum Member Q1
    participant Q2 as Quorum Member Q2
    participant Qn as Quorum Member Qn

    W->>W: tick() and set ownReqTimestamp
    W->>W: verify all quorum members are alive

    par request each quorum member
        W->>Q1: RequestLock(node_id=Wi, timestamp=t)
        W->>Q2: RequestLock(node_id=Wi, timestamp=t)
        W->>Qn: RequestLock(node_id=Wi, timestamp=t)
    end

    alt voter has not voted yet
        Q1-->>W: LockResponse(granted=true)
        W->>W: Grant(sender_id=Q1, timestamp=t)
    else voter already voted
        Q1-->>W: LockResponse(granted=false)
        Q1->>Q1: queue request by (timestamp, node_id)
    end

    Note over W: Each granted=true response is converted locally into a Grant message

    Q2-->>W: LockResponse(granted=true)
    W->>W: Grant(sender_id=Q2, timestamp=t)
    Qn-->>W: LockResponse(granted=true)
    W->>W: Grant(sender_id=Qn, timestamp=t)

    W->>W: votesReceived++
    alt votesReceived == len(quorum)
        W->>W: committed=true
        W->>W: grantChan <- true
        W->>W: inCS=true
    end

    Note over W: Worker executes task while holding global CS

    W->>Q1: ReleaseLock(node_id=Wi, timestamp=t)
    W->>Q2: ReleaseLock(node_id=Wi, timestamp=t)
    W->>Qn: ReleaseLock(node_id=Wi, timestamp=t)

    Q1->>Q1: clear vote for Wi
    alt queued request exists
        Q1->>NextRequester: Grant(sender_id=Q1, timestamp=next.timestamp)
    end
```

### What Each Message Means

- `RequestLock`: "Please allocate your single vote to me for lock round `timestamp`."
- `LockResponse(granted=true)`: immediate approval from that quorum member.
- `Grant`: the actual requester-side accounting signal for one received vote.
- `ReleaseLock`: "I am done; you may reassign your vote."

## 3. Deadlock Handling Sequence

This is implemented in [`internal/maekawa/deadlock.go`](../internal/maekawa/deadlock.go).

When a voter already voted for an older request, and a higher-priority request arrives, the voter sends `Inquire` to the current holder. If that holder is still waiting and not yet committed/in CS, it may `Yield`.

```mermaid
sequenceDiagram
    autonumber
    participant V as Voter V
    participant A as Current Vote Holder A
    participant B as Higher Priority Requester B

    B->>V: RequestLock(node_id=B, timestamp=tB)
    Note over V: V already voted for A at timestamp tA

    alt tB < tA or tie-break by lower node_id
        V->>A: Inquire(sender_id=V, timestamp=tA)
        A->>A: if not committed and not inCS
        alt A already has V's grant
            A->>V: Yield(sender_id=A, timestamp=tA)
            V->>V: push A back into queue
            V->>B: Grant(sender_id=V, timestamp=tB)
        else A has not yet received that grant
            A->>A: remember pending inquiry for V
            Note over A: when Grant from V later arrives, immediately Yield it back
        end
    else lower priority request
        V->>V: enqueue B and keep vote with A
    end
```

### Current Priority Rule

Ordering is by:

1. lower Lamport-style `timestamp`
2. lower `node_id` as tie-break

That ordering is enforced both in the request heap and in the `RequestLock` comparison logic.

## 4. Membership Change Interaction

When the membership layer reports a worker-up or worker-down event, the Maekawa worker calls `OnMembershipChange(...)`.

Effects:

- rebuild `alive` set
- if current vote holder disappeared, evict it and grant the next waiter
- recompute quorum by regridding
- clear vote-tracking state
- abort any in-flight lock acquisition via `grantChan <- false`

That means a membership change is treated as a hard reset for the current lock round.

## 5. Current Raft Integration Boundary

### What Exists

The only real Raft-to-Maekawa bridge on this branch is [`applyTaskEventToMaekawa`](../internal/raft/apply.go), which forwards committed `models.TaskEvent` records into `Worker.ApplyTaskEvent(...)`.

Defined task event types in [`internal/models/task.go`](../internal/models/task.go):

- `assigned`
- `claimed`
- `done`
- `failed`
- `canceled`
- `worker_down`
- `worker_up`
- `worker_removed`
- `worker_added`

### What `ApplyTaskEvent(...)` Currently Does

Current logic in [`internal/maekawa/tasks.go`](../internal/maekawa/tasks.go):

- on `worker_up` or `worker_down`: call `OnMembershipChange(...)`
- on `done` or `canceled`: mark the task as locally canceled so it will be skipped if still queued

### What Is Not Wired Yet

Important gaps on this branch:

- `cmd/worker/main.go` is still a startup stub, so Raft and Maekawa are not actually wired together yet.
- `internal/raft/node.go`, `internal/raft/election.go`, and `internal/raft/ledger.go` are effectively empty.
- `ApplyTaskEvent(...)` does **not** currently handle `assigned`, so committed Raft task assignments are not yet being enqueued into `taskQueue`.
- `ClaimTask`, `ReportTaskSuccess`, and `ReportTaskFailure` are only interface calls from Maekawa into the membership layer; there is no concrete Raft-backed implementation yet in this branch.

## 6. Intended End-To-End Raft + Maekawa Flow

This is the intended architecture implied by the interfaces and event types, even though the full Raft side is not implemented yet.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client / Requester
    participant RL as Raft Leader
    participant RF as Raft Followers
    participant W1 as Worker 1 Local Maekawa
    participant W2 as Worker 2 Local Maekawa
    participant Wn as Worker N Local Maekawa

    C->>RL: SubmitTask(data)
    RL->>RL: create TaskEvent{type=assigned, task_id, task}
    RL->>RF: AppendEntries(LogEntry(command=encoded TaskEvent))
    RF-->>RL: AppendEntriesResponse(success)
    RL->>RL: commit assigned event

    RL->>W1: apply committed EventAssigned
    RL->>W2: apply committed EventAssigned
    RL->>Wn: apply committed EventAssigned

    W1->>W1: enqueue task locally
    W2->>W2: enqueue task locally
    Wn->>Wn: enqueue task locally

    par workers compete via Maekawa
        W1->>W1: RequestForGlobalLock()
        W2->>W2: RequestForGlobalLock()
        Wn->>Wn: RequestForGlobalLock()
    end

    Note over W1,Wn: exactly one worker should win execution for a task

    Winner->>RL: ClaimTask(task_id, worker_id) through membership layer
    RL->>RL: append EventClaimed
    RL->>RF: replicate claimed event
    RL->>All Workers: apply EventClaimed

    Winner->>Winner: execute task payload

    alt success
        Winner->>RL: ReportTaskSuccess(task_id, worker_id, result)
        RL->>RL: append EventDone
        RL->>RF: replicate done event
        RL->>All Workers: apply EventDone
    else failure
        Winner->>RL: ReportTaskFailure(task_id, worker_id, reason)
        RL->>RL: append EventFailed
        RL->>RF: replicate failed event
        RL->>All Workers: apply EventFailed
    end
```

## 7. What The Raft Leader Needs To Do

Given the current Maekawa structure, the Raft leader should own the following responsibilities.

### Task Intake And Ordering

- Accept `SubmitTask(data)` requests.
- Generate a globally unique `task_id`.
- Append an `EventAssigned` log entry carrying the task payload.
- Replicate it with `AppendEntries`.
- Only expose the task to workers after the assignment entry is committed.

### Membership As Source Of Truth

- Maintain the authoritative list of active workers.
- Commit `worker_up`, `worker_down`, `worker_added`, and `worker_removed` events.
- On apply, deliver those committed events to each local Maekawa worker so it can regrid quorum membership.

### Backing The `ClusterMembership` Interface

The leader-backed Raft layer needs a concrete implementation of:

- `ActiveMembers() []int32`
- `IsAlive(id int32) bool`
- `ClaimTask(taskID, workerID)`
- `ReportTaskSuccess(taskID, workerID, result)`
- `ReportTaskFailure(taskID, workerID, reason)`

Semantically:

- `ClaimTask(...)` must be linearized through Raft so only one worker becomes the official winner for a task.
- `ReportTaskSuccess(...)` must turn into a committed `done` event.
- `ReportTaskFailure(...)` must turn into a committed `failed` event.

### Apply Path Duties

- Decode committed log entries into `models.TaskEvent`.
- Call `applyTaskEventToMaekawa(...)` only after commit.
- Extend `Worker.ApplyTaskEvent(...)` to handle `EventAssigned` by pushing the task into `taskQueue`.
- Decide what `EventClaimed` should do locally, for example mark non-winning workers to stop retrying that task.

### Safety Guarantees The Leader Must Preserve

- Maekawa only gives temporary permission to execute; Raft must still be the final source of truth for task ownership and completion.
- A worker should never be treated as the winner just because it entered the Maekawa critical section.
- The official winner is the worker whose `ClaimTask` is accepted and committed by Raft.
- Completion or failure must only become externally visible after the corresponding Raft log entry is committed.

### Recovery And Failure Handling

- If a worker dies before claiming, Raft should eventually commit a membership event and let Maekawa regrid.
- If a worker dies after entering CS but before reporting success/failure, the leader needs a recovery policy:
  - requeue the task, or
  - mark it failed, or
  - wait for lease/timeout expiry before reassignment
- If leadership changes, the new leader must reconstruct pending tasks and worker membership from the committed log.

## 8. Practical Summary

Right now:

- Maekawa mutual exclusion and deadlock-resolution messaging are mostly implemented.
- Raft is only represented by protobufs plus a tiny committed-event callback.
- The real missing bridge is: committed `assigned/claimed/done/failed/membership` events need to drive the Maekawa worker queue and membership view.

If you want, the next step can be to turn this note into a shorter report-ready diagram set, or I can start wiring the missing `EventAssigned -> taskQueue` and Raft-backed `ClusterMembership` skeleton.
