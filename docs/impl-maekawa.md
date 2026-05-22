# Implementation: Maekawa's Mutual Exclusion

**Package:** `internal/maekawa/`

Maekawa's algorithm is implemented natively from scratch. No third-party mutex, distributed lock, or consensus library is used.

---

## Design Goals

1. At most one worker executes a task at any moment (mutual exclusion).
2. No central lock server — all coordination is peer-to-peer.
3. Any two quorums from the same active set share at least one member (the intersection enforces ME).
4. The system continues operating when workers join or leave.
5. Deadlock is prevented, not just detected.

---

## Quorum Structure (`quorum.go`)

Workers are arranged in a conceptual √N × √N grid. Each worker's quorum is its entire row plus its entire column. The grid layout guarantees that any two quorums share at least one member (the cell at their row/column intersection).

```
For N=9, IDs 1–9 arranged in a 3×3 grid:

  1  2  3
  4  5  6
  7  8  9

Quorum(5) = row {4,5,6} ∪ col {2,5,8} = {2,4,5,6,8}   (size 5)
Quorum(1) = row {1,2,3} ∪ col {1,4,7} = {1,2,3,4,7}   (size 5)
Intersection(Q(5), Q(1)) = {2,4} ≠ ∅  ✓
```

`QuorumFor(id, allIDs)` computes the quorum for a single node.  
`RegridQuorum(id, activeIDs)` recomputes after any membership change.

Both functions sort the active ID list before laying out the grid so the quorum is deterministic regardless of the order IDs arrive.

---

## Worker State (`worker.go`)

Each `Worker` struct holds two logical roles simultaneously:

**As a voter (receives RequestLock from peers):**
- `votedFor int32` — which node this voter's single vote is currently allocated to (-1 = free)
- `currentReq *LockRequest` — the full request associated with `votedFor`
- `requestQueue` — priority min-heap of pending requests ordered by `(timestamp, node_id)`
- `isPinned bool` — whether an `Inquire` has already been sent this round (prevents duplicate inquiries)

**As a requester (acquiring the global lock):**
- `ownReqTimestamp int64` — Lamport timestamp of the current lock round (-1 = not requesting)
- `votesReceived int` — how many quorum members have granted this round
- `grantsReceived map[int32]bool` — which quorum members granted (dedup guard)
- `committed bool` — all grants received; waiting to enter CS
- `inCS bool` — currently inside critical section
- `grantChan chan bool` — signals `RequestForGlobalLock` to proceed (true) or abort (false)
- `yieldedTo map[int32]int64` — voters this requester yielded to this round
- `pendingInquiries map[int32]int64` — inquiries received before the sender's grant arrived

---

## Lamport Clock (`clock.go`)

```
tick()         → clock++; return clock
sync(received) → clock = max(clock, received) + 1
updateClock(t) → sync(t)
```

Every `RequestLock` send calls `tick()` to stamp the request.  
Every `RequestLock` receive calls `updateClock(req.Timestamp)` to stay causally consistent.  
Ordering: lower timestamp wins; ties broken by lower node ID.

---

## Lock Protocol (`worker.go`)

### Acquiring the lock — `RequestForGlobalLock(ctx)`

1. Reset all requester-side state for the new round.
2. Drain any stale signal from `grantChan`.
3. Verify all quorum members are alive (fail fast if the quorum is already broken).
4. Snapshot the quorum list, release the mutex.
5. Call `sendLockRequest(peerID, timestamp)` concurrently for every quorum member.
6. Block on `grantChan` (also polls liveness every 25 ms; aborts if any member goes down).

### `RequestLock` (voter side)

```
if votedFor == -1:
    allocate vote → return granted=true
else:
    if new request has higher priority and isPinned==false:
        send Inquire to current holder
        isPinned = true
    push new request into requestQueue
    return granted=false
```

### `Grant` (requester side, called locally when a granted=true response arrives)

```
if ownReqTimestamp < 0 or inCS or timestamp ≠ ownReqTimestamp:
    discard (stale or wrong round)
if grantsReceived[sender]:
    discard (duplicate)
if pendingInquiries[sender] matches:
    immediately yield back (late-arriving grant after Inquire)
grantsReceived[sender] = true
votesReceived++
if votesReceived == len(quorum):
    committed = true; grantChan ← true
```

### Releasing the lock — `exitGlobalCS()`

1. `tick()` (increment clock on release).
2. Reset all requester-side state.
3. Send `ReleaseLock(node_id, reqTimestamp)` to every quorum member concurrently.

### `ReleaseLock` (voter side)

```
if req matches votedFor and currentReq.Timestamp:
    clear vote (votedFor=-1, currentReq=nil, isPinned=false)
    if requestQueue non-empty:
        pop next request, sendGrant(next)
else:
    remove matching entry from requestQueue (queued but not yet granted)
```

---

## Deadlock Prevention (`deadlock.go`)

Without intervention, two workers can deadlock:
- W1 needs votes from {A, B}; has A's vote.
- W2 needs votes from {A, B}; has B's vote.
- Neither can proceed; neither releases.

The INQUIRE/YIELD protocol breaks this:

**`sendInquire(targetID, timestamp)`** — sent by a voter to the current holder when a higher-priority request arrives and `isPinned` is false.

**`Inquire(req)`** (called on the current holder):
```
if not requesting this round, or different round → ignore
if inCS or committed → ignore (will release naturally)
if grantsReceived[sender]:
    yield back: yieldedTo[sender]=t, votesReceived--, grantsReceived[sender]=false
    sendYield(sender, t)
else:
    pendingInquiries[sender] = t   (yield when grant eventually arrives)
```

**`Yield(req)`** (called on the voter):
```
if req.SenderId ≠ votedFor → ignore
push currentReq back into requestQueue
clear vote (votedFor=-1, currentReq=nil, isPinned=false)
pop next from requestQueue → sendGrant(next)
```

The `yieldedTo` map prevents a node from double-yielding the same vote in the same round. The `pendingInquiries` map handles the race where `Inquire` arrives before the voter's own `Grant`.

---

## Membership Changes (`membership.go`)

`OnMembershipChange(newActiveNodes []int32)` is called whenever a `EventWorkerDown/Up` event is committed by Raft.

Steps (all under `Mu` lock):
1. Rebuild `alive` set from `newActiveNodes`.
2. If `votedFor` node is now dead: evict it, grant next from queue.
3. Call `RegridQuorum(id, newActiveNodes)` → new `quorum`.
4. Hard-reset all requester and voter state to zero.
5. Signal `grantChan ← false` to abort any in-flight `RequestForGlobalLock`.
6. Replace `requestQueue` with a fresh empty heap (stale requests from dead members cannot be serviced).

The task loop handles the abort signal in `RunTaskLoop` by calling `retryTaskLater`, which re-enqueues the task after 25 ms once the regrid has settled.

---

## Task Integration (`tasks.go`)

`RunTaskLoop(ctx)` is the main goroutine per worker:

```
for each task dequeued from taskQueue:
    if canceled: skip
    err = RequestForGlobalLock(ctx)
    if err: retryTaskLater(task); continue
    handleTaskExecution(ctx, task)
    exitGlobalCS()
```

`handleTaskExecution`:
1. Call optional `beforeClaim` hook (used in tests to inject timing).
2. Call `membership.ClaimTask(taskID, workerID)` — this goes through Raft; returns `(won, err)`.
3. If won: run `executor(ctx, task)`.
4. Call `membership.ReportTaskSuccess` or `ReportTaskFailure`.

`enqueueTask(task)` is the idempotent entry point:
- Checks `canceledTasks[id]` and `queuedTasks[id]` under `Mu` before sending to `taskQueue`.
- If the channel is full, rolls back `queuedTasks[id]` (non-blocking send, no task loss).

`ApplyTaskEvent(event)` is called by the Raft apply path:
- `EventAssigned`: `restoreTask` + `enqueueTask` (non-blocking, safe to call from apply loop).
- `EventClaimed` (by another worker): `removeFromLocalQueue` — stop competing for this task.
- `EventDone / EventFailed / EventCanceled`: `removeFromLocalQueue` — task is finished.
- `EventWorkerUp/Down`: `OnMembershipChange(membership.ActiveMembers())`.

---

## gRPC Surface (`api/maekawa/maekawa.proto`)

| RPC | Direction | Purpose |
|---|---|---|
| `RequestLock(LockRequest)` | requester → voter | Request the voter's single vote |
| `ReleaseLock(ReleaseRequest)` | requester → voter | Release the vote; voter grants next |
| `Grant(GrantRequest)` | voter → requester | Voter notifies requester of a re-grant (after Yield) |
| `Inquire(InquireRequest)` | voter → current holder | Ask holder if it will yield its vote |
| `Yield(YieldRequest)` | holder → voter | Holder gives back the vote |

All RPCs are implemented natively. No gRPC streaming; all are unary calls with a 3-second timeout and retry.
