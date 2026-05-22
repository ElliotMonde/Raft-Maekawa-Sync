# Implementation: Raft Consensus

**Package:** `internal/raft/`

Raft is implemented natively from scratch. No third-party consensus library is used. The implementation follows the original Raft paper (Ongaro & Ousterhout 2014) with extensions for task management and worker liveness.

---

## Design Goals

1. All nodes agree on the same ordered sequence of task events (replicated log).
2. Exactly one leader accepts writes at any time.
3. A minority of node failures (up to ⌊N/2⌋) do not affect availability.
4. Committed state survives leader crashes (persistent storage).
5. Task claims are linearized — only one worker can officially own a task.
6. Worker liveness is tracked and drives Maekawa quorum regridding.

---

## Node Structure (`node.go`)

```
Node {
    // Identity
    id, addr, peers map[int32]string

    // Raft state
    role             Follower | Candidate | Leader
    currentTerm      int32
    votedFor         int32
    log              []*LogEntry
    commitIndex      int32
    lastApplied      int32

    // Leader-only replication state
    nextIndex        map[int32]int32   // next log index to send each peer
    matchIndex       map[int32]int32   // highest log index known replicated on each peer

    // Task and membership state machine
    state            *StateMachine {
        ActiveWorkers map[int32]bool
        Tasks         map[string]*TaskRecord
    }

    // In-flight claim guard (prevents concurrent claims on same task)
    pendingClaims    map[string]int32

    // Worker liveness
    workerHeartbeats map[int32]time.Time
    pendingLiveness  map[int32]bool

    // Timers
    electionMin/Max, heartbeatIntv
    workerHeartbeatTimeout, workerHeartbeatCheckIntv
    taskClaimTimeout, taskRecoveryCheckIntv
}
```

`ActiveWorkers` starts **empty**. Workers are registered via `SetManagedWorkers([]int32)`, which initialises unknown workers to `alive=true`. This means the Raft node never claims a worker is alive based on peer topology alone — liveness is tracked exclusively through heartbeats.

---

## Leader Election (`election.go`)

### Election Timer

Each node runs `runElectionTimer` in a goroutine. On every tick:
- If the node is not a Leader **and** `time.Since(electionReset) >= timeout`, call `startElection`.
- Timeout is re-randomised each iteration from `[electionMin, electionMax]` to avoid split votes.

### `startElection(ctx)`

1. Increment `currentTerm`, transition to Candidate, vote for self, persist.
2. Send `RequestVote` RPCs to all peers concurrently.
3. First goroutine to push `votes` past majority wins.
4. On majority: `becomeLeader()`, reset worker heartbeat timestamps, start heartbeat loop.
5. If any response carries a higher term: immediately `becomeFollower`.

### `RequestVote(req)`

Grant only if all three hold:
- `req.Term >= currentTerm`
- `votedFor == -1` or `votedFor == req.CandidateId` (one vote per term)
- Candidate log is at least as up-to-date: higher `lastLogTerm`, or equal term with `lastLogIndex >= ours`

Persist `votedFor` to disk before responding.

### `becomeLeader()` / `becomeFollower(term, leaderID)`

`becomeLeader`: initialise `nextIndex[peer] = len(log)+1` and `matchIndex[peer] = 0` for all peers.  
`becomeFollower`: reset `role`, update `currentTerm`, clear `votedFor`, reset `electionReset`. Persist.

---

## Log Replication (`ledger.go`)

### Heartbeat Loop

The leader calls `replicateToPeer(ctx, peerID)` for all peers on every `heartbeatIntv` tick. Heartbeats and log replication use the same `AppendEntries` RPC — an empty `Entries` slice is a pure heartbeat.

### `replicateToPeer(ctx, peerID)`

1. Lock: snapshot `term`, `nextIndex[peer]`, `prevLogIndex/Term`, log entries from `nextIndex` onward, `commitIndex`.
2. Unlock. Send `AppendEntries` to peer.
3. On success: update `matchIndex[peer]`, advance `nextIndex[peer]`, call `tryAdvanceCommitIndex`.
4. On failure with higher term: `becomeFollower`.
5. On rejection (log inconsistency): decrement `nextIndex[peer]` and retry next heartbeat.

### `AppendEntries(req)` (follower side)

1. Reject if `req.Term < currentTerm`.
2. Upgrade term if `req.Term > currentTerm`; reset to Follower.
3. Check `prevLogIndex/prevLogTerm` consistency; if mismatch, truncate and reject.
4. Append new entries, truncating any conflicting suffix.
5. Advance `commitIndex` to `min(req.LeaderCommit, len(log))`.
6. Call `applyCommittedEntries()`.
7. Persist if anything changed.

### `tryAdvanceCommitIndex()`

Iterates candidate indices from `len(log)` down to `commitIndex+1`. For each:
- Must be from the current term (Raft safety: leader only counts own-term entries for majority).
- Count how many peers have `matchIndex >= idx` (plus leader self).
- If count >= majority: advance `commitIndex`, call `applyCommittedEntries`, persist, return.

### `applyCommittedEntries()`

For each index in `(lastApplied, commitIndex]`:
1. Decode `models.TaskEvent` from the log entry command.
2. Call `applyEvent(event)` — mutates the in-memory state machine (holds `mu`).
3. Release `mu`, call `applyTaskEventToMaekawa(event, applier)` (avoids lock inversion with Maekawa).
4. Re-acquire `mu`.

---

## State Machine (`ledger.go`, `apply.go`)

`applyEvent(event)` transitions the in-memory `StateMachine`:

| Event | State Transition |
|---|---|
| `EventAssigned` | Create `TaskRecord{Status: EventAssigned}`, clear `pendingClaims[taskID]`, clear recovery flag |
| `EventClaimed` | Set `record.AssignedTo = event.WorkerID`, `Status = EventClaimed`, clear `pendingClaims[taskID]` |
| `EventDone` | Set `Status = EventDone`, `Result`, clear `pendingClaims` and recovery flag |
| `EventFailed` | Set `Status = EventFailed`, `Reason`, clear `pendingClaims` and recovery flag |
| `EventCanceled` | Set `Status = EventCanceled`, `Reason`, clear `pendingClaims` and recovery flag |
| `EventWorkerUp/Added` | `ActiveWorkers[id] = true` |
| `EventWorkerDown/Removed` | `ActiveWorkers[id] = false` |

`GetState` snapshots this state machine into the proto `GetStateResponse` for dashboard and test polling.

---

## Task Submission (`ledger.go`)

### `SubmitTask(req)`

External clients POST tasks here. If `req.Data` carries the internal prefix (`__raft_internal_event__:`), it is decoded as a forwarded `TaskEvent` from a follower. Otherwise a fresh `EventAssigned` is created with a unique `task-<nodeID>-<unixNano>` ID.

Delegates to `commitTaskEventAsLeader`.

### `commitTaskEventAsLeader(ctx, event)`

```
1. Lock. If not leader: return (false, leaderID, nil).
2. shouldCommitEventLocked(event) — validate transition is legal.
3. If EventClaimed: pendingClaims[taskID] = workerID  (in-flight claim guard).
4. appendEntry(encoded command).
5. If single-node cluster: immediately advance commitIndex (no peers to wait for).
6. Snapshot beforeReplicate hook, persist, unlock.
7. If beforeReplicate hook returns false: clear pendingClaims, return false.
8. Fan out replicateToPeer to all peers (goroutines).
9. waitForCommit(ctx, newIndex, 5s).
10. If timeout: clear pendingClaims, return false.
11. Return true.
```

### `shouldCommitEventLocked(event)` — Transition Guard

| Event | Guard |
|---|---|
| `EventAssigned` | Task must not exist OR be a stale claim eligible for recovery |
| `EventClaimed` | Task must be `EventAssigned`; no other worker's claim must be in `pendingClaims` |
| `EventDone/EventFailed` | Task must be `EventClaimed` and `AssignedTo == event.WorkerID` |
| `EventCanceled` | Task must not already be done or failed |
| Worker events | Always allowed |

The `pendingClaims` guard is the key serialization point for concurrent claims: the first worker to reach step 3 reserves the slot; any subsequent `ClaimTask` call for the same task fails at step 2 before touching the log.

---

## Claim Forwarding (`membership.go`)

Followers cannot commit directly. `commitOrForwardEvent` handles both cases:

```
if isLeader:
    commitTaskEventAsLeader(ctx, event)
else:
    encode event as internal SubmitTask payload
    try leader first, then iterate other peers
    update cached leaderID from response hint
```

`ClaimTask`, `ReportTaskSuccess`, `ReportTaskFailure` all go through this path.

---

## Worker Heartbeat and Liveness (`worker_heartbeat.go`)

Workers send `WorkerHeartbeat(workerID)` to the leader every ~100 ms. The leader:
1. Records `workerHeartbeats[workerID] = time.Now()`.
2. If the worker was previously tracked as down: `noteWorkerReachability(id, true)` → commits `EventWorkerUp`.

`runWorkerHeartbeatLoop` runs on the leader every `workerHeartbeatCheckIntv`:
- For each managed worker: if `time.Since(lastHeartbeat) > workerHeartbeatTimeout` → `noteWorkerReachability(id, false)` → commits `EventWorkerDown`.

`noteWorkerReachability` deduplicates: it checks `state.ActiveWorkers[id]` and `pendingLiveness[id]` before committing, so a single crash produces at most one `EventWorkerDown`.

**Important:** `notePeerReachability` (called by the Raft RPC transport) is intentionally a no-op. Raft peer transport failures are not a reliable signal for worker liveness and were causing false-positive `EventWorkerDown` commits in earlier versions.

---

## Task Recovery (`task_recovery.go`)

`runTaskRecoveryLoop` runs on the leader every `taskRecoveryCheckIntv`. For each task in the state machine:
- If `Status == EventClaimed` and `AssignedTo` worker is marked down and `time.Since(UpdatedAtUnixNano) >= taskClaimTimeout`:
  - Commit a fresh `EventAssigned` for the same task, resetting it to PENDING.

`canRecoverClaimedTaskLocked`:
```
task.Status == EventClaimed
AND task.AssignedTo != 0
AND ActiveWorkers[task.AssignedTo] == false
AND time.Since(task.UpdatedAtUnixNano) >= taskClaimTimeout
```

The re-committed `EventAssigned` clears `AssignedTo` in the state machine and re-enqueues the task on all workers via `applyTaskEventToMaekawa`.

---

## Persistent Storage (`storage.go`)

`persistLocked()` serialises and atomically writes:
- `currentTerm`
- `votedFor`
- All log entries

to a JSON file at `storagePath`. Called before responding to any RPC that changes durable state.

On startup, `SetStoragePath` loads the file and rebuilds the in-memory state machine by replaying all log entries up to `lastApplied` before registering the applier. `SetApplier` replays committed events to the Maekawa worker if it is registered after some entries are already applied.

---

## gRPC Surface (`api/raft/raft.proto`)

| RPC | Caller | Purpose |
|---|---|---|
| `RequestVote` | Candidate → peers | Solicit votes during election |
| `AppendEntries` | Leader → followers | Replicate log entries; also heartbeat |
| `SubmitTask` | Client or follower → leader | Submit or forward a task event |
| `GetState` | Dashboard / tests → any node | Snapshot the state machine |
| `WorkerHeartbeat` | Worker → leader | Liveness ping |
