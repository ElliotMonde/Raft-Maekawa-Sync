# Technical Write-Up

## 1. What Has Been Implemented

This project implements a distributed task execution system that combines two independently implemented algorithms:

- **Raft** — for distributed consensus, leader election, log replication, and task state management
- **Maekawa's Mutual Exclusion** — for decentralised lock arbitration so exactly one worker executes each task

Both algorithms are implemented natively from scratch with no drop-in third-party consensus or mutex libraries.

---

### 1.1 Distributed Synchronization — Maekawa's Algorithm

**Location:** `internal/maekawa/`

Maekawa's algorithm provides mutual exclusion without a central lock server. Instead of broadcasting to all nodes, each worker only needs permission from its *quorum* — a carefully chosen subset of peers with the property that any two quorums intersect. This means at most one worker can hold the global lock at any moment.

#### Quorum Structure

Quorums are computed as a row-plus-column grid (`internal/maekawa/quorum.go`):

- Workers are arranged conceptually in a √N × √N grid.
- Each worker's quorum is its entire row plus its entire column.
- Any two quorums share at least one member (the intersection cell), which enforces mutual exclusion.

For a 3-worker cluster the quorums are `{1,2,3}`, `{1,2,3}`, `{1,2,3}` (degenerate — all three intersect). For a 9-worker cluster each quorum has size 5 out of 9.

When a worker joins or leaves, every surviving worker calls `RegridQuorum` to recompute its quorum from the new active set.

#### Lock Protocol

1. **Request** — The requester increments its Lamport clock, records `ownReqTimestamp`, and sends `RequestLock(node_id, timestamp)` to all quorum members in parallel.
2. **Grant** — A voter that has not yet voted replies immediately with `LockResponse(granted=true)`. If it has already voted, it queues the request ordered by `(timestamp, node_id)`.
3. **Entry** — The requester counts grants. When it has received one from every quorum member, it sets `inCS = true` and proceeds to execute.
4. **Release** — After execution the requester sends `ReleaseLock` to all quorum members. Each voter clears its vote and grants the next queued request (if any).

#### Deadlock Prevention (INQUIRE / YIELD)

Without additional logic, two workers waiting on overlapping quorums can deadlock. The resolution (`internal/maekawa/deadlock.go`):

- When a voter holds a vote for worker A and receives a higher-priority request from worker B, it sends `Inquire(timestamp_A)` to A.
- If A has not yet committed (is still waiting for other votes), it sends `Yield` back to the voter and clears that vote from its own count.
- The voter then grants B and pushes A back into the request queue.
- If A is already in the critical section, it ignores `Inquire` — it will release naturally.

Priority is `(lower timestamp, lower node_id)`, consistent with Lamport clock ordering.

#### Lamport Clock

Each worker maintains a Lamport clock (`internal/maekawa/clock.go`). The clock is incremented on every send and synchronised on every receive (`max(local, received) + 1`). This gives a causal ordering of all lock requests across the cluster.

#### Membership-Aware Locking

When the active worker set changes (a node joins or crashes):

- The worker rebuilds its `alive` set.
- If the current vote holder has crashed, its vote is evicted and the next queued request is granted.
- The quorum is recomputed via `RegridQuorum`.
- Any in-flight lock acquisition is aborted via a channel signal — the task is retried after the regrid settles.

---

### 1.2 Distributed Resource Management — Raft Consensus

**Location:** `internal/raft/`

Raft provides a replicated state machine so all nodes agree on task assignment, ownership, and completion. It is the source of truth; Maekawa only arbitrates *who gets to try first*, but the actual winner is the one whose `ClaimTask` Raft commits.

#### Leader Election (`election.go`)

- Every node starts as a Follower with a randomised election timeout (80–160 ms in tests, 400–800 ms in production).
- If no heartbeat is received before the timeout, the node transitions to Candidate, increments its term, votes for itself, and sends `RequestVote` RPCs to all peers.
- A node grants a vote if: the candidate's term is at least as large as its own, it has not already voted this term, and the candidate's log is at least as up-to-date as its own.
- A candidate that receives a majority of votes becomes Leader and immediately sends heartbeats.
- On receiving any message with a higher term, a node immediately reverts to Follower.

#### Log Replication (`ledger.go`)

- The leader appends client commands as log entries tagged with the current term.
- It sends `AppendEntries` RPCs to all followers carrying the new entries plus a consistency check (`prevLogIndex`, `prevLogTerm`).
- A follower that fails the consistency check truncates its log and retries at an earlier index.
- Once a majority of nodes have acknowledged an entry, the leader advances `commitIndex` and applies the entry to the state machine.
- Followers advance their `commitIndex` when the leader's `leaderCommit` field exceeds their own.

#### Persistent State (`storage.go`)

Each node persists `currentTerm`, `votedFor`, and the full log to a JSON file before responding to any RPC that could affect these values. On restart the node loads this state, reconstructs the in-memory state machine by replaying committed log entries, and rejoins the cluster as a Follower.

#### Task State Machine (`ledger.go`, `node.go`)

All task lifecycle transitions are encoded as `models.TaskEvent` records committed through Raft. The state machine handles:

| Event | Transition |
|---|---|
| `EventAssigned` | New task appears with status PENDING; workers enqueue it |
| `EventClaimed` | Task moves to IN_PROGRESS with a specific `AssignedTo` worker |
| `EventDone` | Task moves to COMPLETED with a result string |
| `EventFailed` | Task moves to FAILED with a reason |
| `EventCanceled` | Task is canceled from any non-terminal state |
| `EventWorkerUp/Down` | Worker liveness updated; triggers Maekawa regrid |

#### Claim Linearization

Only one worker can become the official owner of a task. The mechanism:

1. A worker calls `ClaimTask(taskID, workerID)`.
2. The leader checks `shouldCommitEventLocked`: the task must be PENDING (`EventAssigned`) and no other claim must already be in flight (`pendingClaims` guard).
3. The `pendingClaims[taskID]` reservation is set before replication begins, so a second concurrent claim from a different worker is rejected immediately at step 2 without touching the log.
4. If replication fails or the leader crashes before commit, the reservation is cleared and the task returns to PENDING so another worker can claim it.

#### Worker Heartbeat and Liveness (`worker_heartbeat.go`)

Workers send `WorkerHeartbeat` RPCs to the current leader every 100 ms. The leader tracks last-seen timestamps and commits `EventWorkerDown` for any worker silent for more than the heartbeat timeout (250 ms in tests, 3 s in production). This is the *only* signal that drives worker liveness — peer reachability from Raft AppendEntries RPCs is intentionally ignored to avoid false positives.

#### Task Recovery (`task_recovery.go`)

If a worker crashes after claiming a task but before reporting done/failed, the task stays IN_PROGRESS forever without recovery. The leader periodically checks for IN_PROGRESS tasks assigned to dead workers. Once the claim timeout elapses, it re-commits `EventAssigned` to reset the task to PENDING so a healthy worker can reclaim it.

---

### 1.3 Decentralized Features

#### No Central Lock Server

Maekawa mutual exclusion requires no central arbiter. Workers communicate directly with their quorum peers. There is no lock manager process; any worker can request, grant, or release locks.

#### Peer-to-Peer Communication

All inter-node communication is direct gRPC — workers contact quorum peers directly, Raft nodes exchange votes and log entries directly. There is no message broker or relay.

#### Dynamic Membership

The active worker set is not hardcoded. It is maintained in the Raft log. When a worker fails or recovers, a `EventWorkerDown`/`EventWorkerUp` event is committed and replicated to all nodes, which then recompute their Maekawa quorums from the new membership. The system continues operating with the surviving members.

#### Symmetric Worker Roles

Every worker is simultaneously a potential requester (can claim tasks) and a voter (grants lock requests to peers). There is no designated lock holder or coordinator role among workers.

#### Dashboard Visualization

A real-time dashboard (`internal/dashboard/`, `web/`) streams Raft node state, Maekawa message events, and task progress via WebSocket. It can also start/stop worker containers via the Docker socket, providing a live view of the decentralized system in operation.

---

## 2. How to Test and Deploy

### Prerequisites

- Go 1.22+
- Docker and Docker Compose (for the cluster deployment)
- `protoc` with `protoc-gen-go` and `protoc-gen-go-grpc` (only needed to regenerate protos)

---

### Running Tests

Run the full test suite:

```bash
make test
```

Run with the race detector:

```bash
make race
```

Run only the Maekawa tests:

```bash
make test-maekawa
```

Run only the Raft tests:

```bash
make test-raft
```

Run a specific test with repetition (used to catch flaky timing bugs):

```bash
GOCACHE=/tmp/go-build go test ./internal/raft \
  -run TestCombinedRuntimeLeaderCrashAfterClaimBeforeCommit \
  -count=20 -timeout=300s
```

---

### Local Development (without Docker)

Build all binaries:

```bash
make build
```

Start a 3-node Raft cluster manually (three terminals):

```bash
./bin/raft --id 1 --addr 127.0.0.1:5001 \
  --peers "2=127.0.0.1:5002,3=127.0.0.1:5003" \
  --workers "1,2,3"

./bin/raft --id 2 --addr 127.0.0.1:5002 \
  --peers "1=127.0.0.1:5001,3=127.0.0.1:5003" \
  --workers "1,2,3"

./bin/raft --id 3 --addr 127.0.0.1:5003 \
  --peers "1=127.0.0.1:5001,2=127.0.0.1:5002" \
  --workers "1,2,3"
```

Start 3 Maekawa workers (three more terminals):

```bash
./bin/worker --id 1 --addr 127.0.0.1:6001 \
  --peers "2=127.0.0.1:6002,3=127.0.0.1:6003" \
  --raft "1=127.0.0.1:5001,2=127.0.0.1:5002,3=127.0.0.1:5003"

./bin/worker --id 2 --addr 127.0.0.1:6002 \
  --peers "1=127.0.0.1:6001,3=127.0.0.1:6003" \
  --raft "1=127.0.0.1:5001,2=127.0.0.1:5002,3=127.0.0.1:5003"

./bin/worker --id 3 --addr 127.0.0.1:6003 \
  --peers "1=127.0.0.1:6001,2=127.0.0.1:6002" \
  --raft "1=127.0.0.1:5001,2=127.0.0.1:5002,3=127.0.0.1:5003"
```

Submit a task:

```bash
./bin/requester --raft 127.0.0.1:5001 --data "hello-world"
```

If the node is not the leader, the response will include the leader ID. Retry against the correct address.

---

### Docker Cluster Deployment

Start the full 3-node Raft + 3-worker + dashboard cluster:

```bash
make docker-up
```

Open the dashboard:

```
http://localhost:8080
```

Submit a task from the host machine:

```bash
go run ./cmd/requester --raft 127.0.0.1:5001 --data "demo-task"
```

Submit a task from inside Docker:

```bash
make docker-request DATA="demo-task"
```

Target a specific Raft node:

```bash
make docker-request RAFT=raft-node2:5002 DATA="demo-task"
```

Watch logs:

```bash
make docker-logs
```

Stop the cluster:

```bash
make docker-down
```

Raft nodes persist their state in named Docker volumes. Restarting a node with `docker compose restart raft-node1` will replay its log and rejoin the cluster without losing committed state.

---

### Regenerating Protobuf Files

```bash
make proto
```

This regenerates `api/raft/*.pb.go` and `api/maekawa/*.pb.go` from the `.proto` source files.
