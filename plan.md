# Plan: Full Implementation — Foundation → Maekawa → Raft

## Context
PeerSync is a distributed resource orchestration system (GPU render farm marketplace). Workers compete for tasks via Maekawa's quorum-based mutual exclusion; outcomes are committed to a Raft replicated log which also runs the Economic Enforcer state machine (reputation, rewards, slashing). All source files are currently empty stubs. The user wants the full end-to-end plan: foundation first, then Maekawa standalone, then Raft integration. gRPC + protobuf is the chosen transport.

---

## Phase 1 — Foundation (no logic yet, just scaffolding)

### Step 1 — `go.mod`
```
go mod init github.com/phonavitra/raft-maekawa-sync
go get google.golang.org/grpc
go get google.golang.org/protobuf
go get google.golang.org/grpc/cmd/protoc-gen-go-grpc
```

### Step 2 — `internal/models/task.go`
```go
type TaskStatus uint8  // Pending, InProgress, Done, Failed
type Task struct {
    ID, Description string
    OwnerID         int    // worker assigned to task
    Status          TaskStatus
    Reward          float64
    CreatedAt, StartedAt, FinishedAt int64
}
```

### Step 3 — `proto/maekawa.proto` (new file)
```proto
syntax = "proto3";
package maekawa;
option go_package = "github.com/phonavitra/raft-maekawa-sync/proto/maekawapb";

enum MsgType { REQUEST=0; REPLY=1; RELEASE=2; INQUIRE=3; YIELD=4; }

message MaekawaMsg {
  MsgType type          = 1;
  int32   sender_id     = 2;
  int64   clock         = 3;
  string  task_id       = 4;
  int64   inquire_clock = 5;  // only for INQUIRE
}
message Ack { bool ok = 1; }

service MaekawaService {
  rpc Send(MaekawaMsg) returns (Ack);
}
```

### Step 4 — `proto/raft.proto` (new file)
```proto
syntax = "proto3";
package raft;
option go_package = "github.com/phonavitra/raft-maekawa-sync/proto/raftpb";

message LogEntry {
  int64  index    = 1;
  int64  term     = 2;
  bytes  data     = 3;  // JSON-encoded Task or economic event
}
message RequestVoteReq  { int64 term=1; int32 candidate_id=2; int64 last_log_index=3; int64 last_log_term=4; }
message RequestVoteResp { int64 term=1; bool  vote_granted=2; }
message AppendEntriesReq  { int64 term=1; int32 leader_id=2; int64 prev_log_index=3; int64 prev_log_term=4; repeated LogEntry entries=5; int64 leader_commit=6; }
message AppendEntriesResp { int64 term=1; bool  success=2; }

service RaftService {
  rpc RequestVote(RequestVoteReq)   returns (RequestVoteResp);
  rpc AppendEntries(AppendEntriesReq) returns (AppendEntriesResp);
}
```

Run codegen:
```
protoc --go_out=. --go-grpc_out=. proto/maekawa.proto proto/raft.proto
```

---

## Phase 2 — Maekawa (standalone, no Raft needed)

### Step 5 — `internal/maekawa/quorum.go`
3×3 grid coterie for N=9. Node i's quorum = its row ∪ its column.
- Quorum size = 5 (2×√9 − 1)
- Any two quorums share ≥1 node (row/col must cross at a grid cell)
- Self included in quorum set (implicit self-vote; no self-REQUEST sent)

```go
func QuorumFor(nodeID, n int) []int   // returns sorted quorum member IDs
type RequestHeap []MaekawaMsg         // min-heap by (Clock, SenderID) — container/heap
```
**Write unit tests immediately:** verify quorum sizes, intersection property, self-inclusion.

---

### Step 6 — `internal/network/maekawa_server.go`
gRPC server for Maekawa messages:

```go
type MaekawaServer struct {
    pb.UnimplementedMaekawaServiceServer
    handler func(*pb.MaekawaMsg) error
}
func (s *MaekawaServer) Send(ctx, msg) (*pb.Ack, error)
func StartMaekawaServer(addr string, handler func(*pb.MaekawaMsg) error) (*grpc.Server, error)
```

---

### Step 7 — `internal/network/maekawa_client.go`
gRPC client, one persistent connection per peer:

```go
type MaekawaClient struct {
    conns  map[int]*grpc.ClientConn
    stubs  map[int]pb.MaekawaServiceClient
    peers  map[int]string  // nodeID -> "host:port"
}
func NewMaekawaClient(peers map[int]string) *MaekawaClient
func (c *MaekawaClient) Send(targetID int, msg *pb.MaekawaMsg) error
func (c *MaekawaClient) Close()
```

---

### Step 8 — `internal/network/rpc_wrapper.go`
Typed helpers so Worker never touches raw protobuf:

```go
type MaekawaRPC struct { selfID int; client *MaekawaClient }
func (r *MaekawaRPC) SendRequest(targetID int, taskID string, clock int64) error
func (r *MaekawaRPC) SendReply(targetID int, taskID string, clock int64) error
func (r *MaekawaRPC) SendRelease(targetID int, taskID string, clock int64) error
func (r *MaekawaRPC) SendInquire(targetID int, taskID string, clock, inquiredClock int64) error
func (r *MaekawaRPC) SendYield(targetID int, taskID string, clock int64) error
```

---

### Step 9 — `internal/maekawa/worker.go`
Core state machine. Every worker is simultaneously a **requester** (wants CS) and a **voter** (grants votes to others).

**States:** `Released → Wanting → Held`

**Key struct fields:**
```go
state           WorkerState
replyCount      int          // starts at 1 (implicit self-vote)
currentReqClock int64        // clock of the REQUEST we sent (for stale INQUIRE detection)
voter           VoterState   // locked, lockedFor, lockedClock, inquired, waitQueue (heap)
clock           int64        // Lamport clock (monotonically increasing)
csEnter         chan struct{} // closed when replyCount == len(Quorum) → CS acquired
```

**Key methods:**
- `RequestCS(task)` — tick clock, broadcast REQUEST to quorum peers (not self), block on `<-csEnter`
- `ReleaseCS()` — broadcast RELEASE to quorum peers, reset state, make new csEnter
- `HandleMessage(msg)` — updateClock first, then dispatch by msg.Type

**CRITICAL concurrency rule:** Collect messages-to-send while holding `mu`, then release `mu` before calling gRPC. Never hold the lock during I/O.

**Message dispatch:**

| Incoming | Condition | Action |
|---|---|---|
| REQUEST | voter not locked | REPLY; lock voter for sender |
| REQUEST | voter locked, sender has lower (clock,id) = higher priority | `maybeInquire`; enqueue sender |
| REQUEST | voter locked, sender has higher (clock,id) | enqueue sender only |
| REPLY | state==Wanting | replyCount++; if ==len(Quorum): close csEnter |
| RELEASE | from current lock holder | `handleRelease`: unlock; REPLY to WaitQueue head |
| INQUIRE | state==Wanting AND replyCount < len(Quorum) | YIELD; replyCount-- |
| INQUIRE | state==Held OR replyCount==len(Quorum) | Ignore (already in CS) |
| YIELD | voter got yield in response to INQUIRE | `handleYield`: requeue, REPLY to WaitQueue min |

---

### Step 10 — `internal/maekawa/deadlock.go`
Methods on `*Worker` for the Sanders deadlock-resolution extension:

```go
func (w *Worker) maybeInquire(req pb.MaekawaMsg)   // send INQUIRE to current lock holder (once only)
func (w *Worker) handleYield(msg pb.MaekawaMsg)    // voter: retract grant, give to next in queue
func (w *Worker) handleRelease(msg pb.MaekawaMsg)  // voter: unlock, grant next in queue
func (w *Worker) shouldYield() bool                // requester: haven't collected all votes yet?
```

**Pitfall — stale INQUIRE:** Check `msg.InquireClock == w.currentReqClock` before yielding. A stale INQUIRE (from a previous request round) must be ignored.

**Pitfall — race between YIELD and CS entry:** INQUIRE arrives after replyCount already == len(Quorum) → ignore.

---

### Step 11 — `cmd/worker/main.go`
```
1. Read WORKER_ID (int), WORKER_PEERS ("0=host:port,1=host:port,...") from env
2. NewWorker(id, 9, peers, peers[id])
3. worker.Start()     — gRPC server listening before any requests
4. Probe loop until all peers reachable (avoids race on startup)
5. Simulate 3 rounds per worker:
   task := &Task{ID: fmt.Sprintf("task-%d-%d", id, i)}
   worker.RequestCS(task)
   time.Sleep(500ms + rand jitter)   // simulate work
   worker.ReleaseCS()
6. os/signal for graceful shutdown
```

---

## Phase 2 Test: Maekawa Standalone

### Unit tests — `internal/maekawa/worker_test.go`
Use `mockRPC` (implements MaekawaRPC interface, records calls instead of sending).
- Single worker: immediate CS entry (replyCount starts=1, quorum={self})
- Lamport clock strictly increases through REQUEST→REPLY→RELEASE
- RequestHeap: out-of-order push, verify pop order by (clock, id)

### Integration test — `internal/maekawa/integration_test.go`
```go
func TestNineWorkersMutualExclusion(t *testing.T) {
    // 9 workers on 127.0.0.1 with OS-assigned ports
    // All 9 goroutines call RequestCS simultaneously
    var csCount int64  // atomic; must never exceed 1
    // All 9 must complete (liveness — no permanent deadlock)
}
```
The INQUIRE/YIELD path will activate automatically because all 9 start simultaneously.

---

## Fault Tolerance Design (Maekawa ↔ Raft)

### How Raft detects dead workers and unblocks Maekawa

Two failure scenarios must be handled. Both are driven by Raft — Maekawa never declares a worker dead on its own.

---

**Scenario A — Worker dies before `RequestCS` is called**

```
Worker W_x crashes
  ↓
Raft leader stops receiving AppendEntries ACKs from W_x
  ↓
missedBeats[W_x]++ each heartbeat interval (~150ms)
  ↓
missedBeats[W_x] == maxMissedBeats (3) → ~450ms after crash
  ↓
Leader submits EventWorkerDown{WorkerID: W_x} to Raft log
  ↓
Majority ACK → committed → all nodes call applyEntry
  ↓
applyEntry calls n.maekawa.NotifyWorkerDown(W_x)
  ↓
Maekawa: liveWorkers[W_x] = false
  ↓
Any subsequent RequestCS sees live quorum < fullQuorumSize (5)
  → returns error immediately, task retried via Raft
```

---

**Scenario B — Worker dies after `RequestCS` was sent (mid-flight)**

```
Worker W0 sends REQUESTs to quorum {1,2,3,6}
W1 crashes before sending REPLY
W0 blocks on <-csEnter — will wait forever without intervention
  ↓
Raft leader detects W1 down (same heartbeat path as Scenario A)
  ↓
Leader commits EventWorkerDown{WorkerID: 1}
  ↓
applyEntry → NotifyWorkerDown(1)
  ↓
NotifyWorkerDown sees W0.state == StateWanting AND W1 ∈ W0.Quorum
  → sets csErr = "quorum member 1 died mid-request"
  → closes csEnter
  ↓
RequestCS unblocks, reads csErr, returns error
Task retried via Raft (no penalty — quorum failure, not worker fault)
```

---

**Scenario C — Maekawa deadlock (worker stuck waiting, not a crash)**

A Maekawa deadlock cannot happen with the Sanders INQUIRE/YIELD extension correctly implemented. But if a worker is slow (e.g. long GC pause, overloaded), it may delay replies, causing others to time out via Raft heartbeat. This is treated the same as Scenario B — Raft declares it dead, `NotifyWorkerDown` cancels the waiting requester.

There is **no separate Maekawa timeout** — Raft's heartbeat is the single source of truth for liveness.

---

**Worker recovery (`EventWorkerUp`)**

```
W_x restarts, rejoins cluster
  ↓
W_x responds to AppendEntries again
  ↓
missedBeats[W_x] = 0; deadWorkers[W_x] = false
  ↓
Leader submits EventWorkerUp{WorkerID: W_x}
  ↓
applyEntry → n.maekawa.NotifyWorkerUp(W_x)
  ↓
Maekawa: liveWorkers[W_x] = true
Next RequestCS includes W_x in live quorum
```

---

**Why `applyEntry` calls `NotifyWorkerDown` (not `replicateLog` directly)**

If the leader called `NotifyWorkerDown` at detection time, only the leader's local Maekawa worker would update. Followers would remain unaware until they received the committed log entry — inconsistent state. By routing through the Raft log, all 9 Maekawa workers call `NotifyWorkerDown` at the same committed log index, in the same order, guaranteed by Raft.

---

**`MaekawaNotifier` interface (avoids import cycle)**

```go
// defined in internal/raft/node.go
type MaekawaNotifier interface {
    NotifyWorkerDown(workerID int)
    NotifyWorkerUp(workerID int)
  NotifyWorkerRemoved(workerID int)
  NotifyWorkerAdded(workerID int)
}
```

Raft cannot import `internal/maekawa` (maekawa imports raft via Raft client → cycle). The interface breaks the cycle: Raft defines it, the caller (`cmd/worker/main.go`) wires in the concrete `*maekawa.Worker`.

---

**Key constants**

| Constant | Value | Where |
|---|---|---|
| `maxMissedBeats` | 3 | `internal/raft/ledger.go` |
| `heartbeatInterval` | 50ms | `internal/raft/node.go` |
| `reconfigTimeout` | 5–30s | `internal/raft/ledger.go` |
| `fullQuorumSize` | 5 (2×gridSize−1) | `internal/maekawa/worker.go` |
| Dead declared after | ~150ms (3×50ms) | Raft heartbeat loop |
| Removed after | `reconfigTimeout` | Raft heartbeat loop |

---

## Phase 3 — Raft Consensus

### Step 12 — `internal/network/raft_server.go`
gRPC server for Raft RPCs (same pattern as Maekawa server):

```go
type RaftServer struct {
    pb.UnimplementedRaftServiceServer
    node *raft.Node
}
func (s *RaftServer) RequestVote(ctx, req) (*pb.RequestVoteResp, error)
func (s *RaftServer) AppendEntries(ctx, req) (*pb.AppendEntriesResp, error)
func StartRaftServer(addr string, node *raft.Node) (*grpc.Server, error)
```

---

### Step 13 — `internal/network/raft_client.go`
gRPC client for peer Raft nodes:

```go
type RaftClient struct {
    conns map[int]*grpc.ClientConn
    stubs map[int]pb.RaftServiceClient
    peers map[int]string
}
func (c *RaftClient) SendRequestVote(targetID int, req *pb.RequestVoteReq) (*pb.RequestVoteResp, error)
func (c *RaftClient) SendAppendEntries(targetID int, req *pb.AppendEntriesReq) (*pb.AppendEntriesResp, error)
```

---

### Step 14 — `internal/raft/node.go`
Core Raft node. Three roles: **Follower**, **Candidate**, **Leader**.

**Key struct fields:**
```go
id           int
peers        map[int]string
role         Role          // Follower, Candidate, Leader
currentTerm  int64
votedFor     int           // -1 = none
log          []pb.LogEntry
commitIndex  int64
lastApplied  int64

// Leader only:
nextIndex      map[int]int64
matchIndex     map[int]int64
missedBeats    map[int]int    // consecutive missed heartbeat ACKs per peer
deadWorkers    map[int]bool   // peers already declared dead (avoid re-committing EventWorkerDown)

electionTimer  *time.Timer
heartbeatTick  *time.Ticker

// Reference to the local Maekawa worker — called in applyEntry
maekawa        MaekawaNotifier  // interface: NotifyWorkerDown/Up/Removed/Added
```

**`MaekawaNotifier` interface** (defined in `node.go`, implemented by `*maekawa.Worker`):
```go
type MaekawaNotifier interface {
    NotifyWorkerDown(workerID int)
    NotifyWorkerUp(workerID int)
  NotifyWorkerRemoved(workerID int)
  NotifyWorkerAdded(workerID int)
}
```
This keeps Raft decoupled from the Maekawa package — no import cycle.

**Key methods:**
- `Start()` — start timers, gRPC server
- `becomeFollower(term)` — reset timer
- `becomeCandidate()` — increment term, vote for self, send RequestVote to peers
- `becomeLeader()` — initialize nextIndex/matchIndex/missedBeats, start heartbeat ticker
- `Submit(entry []byte) error` — client-facing: append to log, replicate (leader only)
- `apply(entry pb.LogEntry)` — calls Economic Enforcer + MaekawaNotifier

---

### Step 15 — `internal/raft/election.go`
Leader election (Term Manager + Election Engine components):

```go
func (n *Node) startElection()          // becomeCandidate, broadcast RequestVote
func (n *Node) handleRequestVote(req)   // vote if term >= currentTerm and log up-to-date
func (n *Node) resetElectionTimer()     // random timeout 150–300ms
```

**Split vote resolution:** Randomized timeouts (150–300ms) make simultaneous elections unlikely. On split, all candidates time out and start a new term.

---

### Step 16 — `internal/raft/ledger.go`
Log replication + commit + state machine apply (Log Replicator + Economic Enforcer):

```go
func (n *Node) replicateLog()             // leader: send AppendEntries to all peers; track missedBeats
func (n *Node) handleAppendEntries(req)   // follower: validate, append, update commitIndex
func (n *Node) advanceCommitIndex()       // leader: check if majority matchIndex >= N → commit
func (n *Node) applyCommitted()           // apply log[lastApplied+1..commitIndex] to state machine

// Economic Enforcer (state machine):
func (n *Node) applyEntry(entry pb.LogEntry)  // decode entry, update task status, reputation, reward/slash
```

**Heartbeat failure detection inside `replicateLog`:**
```
for each peer:
    resp, err := SendAppendEntries(peer, heartbeat)
    if err != nil or !resp.success:
        missedBeats[peer]++
        if missedBeats[peer] >= maxMissedBeats (3) and !deadWorkers[peer]:
            deadWorkers[peer] = true
            Submit(encode EventWorkerDown{WorkerID: peer})  // commit to Raft log
    if missedBeats[peer] * heartbeatInterval >= reconfigTimeout and !removedWorkers[peer]:
      removedWorkers[peer] = true
      Submit(encode EventWorkerRemoved{WorkerID: peer}) // commit membership change
    else:
        missedBeats[peer] = 0
    if removedWorkers[peer]:
      // worker is physically back, but do NOT auto-join to avoid thrash
      // require explicit operator/API-triggered EventWorkerAdded
      continue
        if deadWorkers[peer]:
            deadWorkers[peer] = false
            Submit(encode EventWorkerUp{WorkerID: peer})    // commit recovery
```
`maxMissedBeats = 3` means a worker is declared dead after ~3 × heartbeat interval (150ms) = ~450ms.

**Two-stage failure handling:**
- Stage 1 (`EventWorkerDown`): temporary liveness failure; member stays in cluster config.
- Stage 2 (`EventWorkerRemoved`): prolonged failure beyond `reconfigTimeout`; member removed from active config to prevent starvation.
- Rejoin path: explicit `EventWorkerAdded` (operator/API), then normal `EventWorkerUp` heartbeats.

**Failure/Reconfiguration timeline (single worker `Wk`):**
```
T+0ms      : Wk stops responding to heartbeats
T+450ms    : missedBeats reaches 3
             -> submit+commit EventWorkerDown{Wk}
             -> applyEntry: NotifyWorkerDown(Wk)

T+reconfig : if still down for reconfigTimeout (e.g., 5-30s)
             -> submit+commit EventWorkerRemoved{Wk}
             -> applyEntry: NotifyWorkerRemoved(Wk)

T+recover  : Wk physically comes back
             -> operator/API submits EventWorkerAdded{Wk}
             -> applyEntry: NotifyWorkerAdded(Wk)
             -> next successful heartbeats can produce EventWorkerUp{Wk}
```

**Economic Enforcer logic inside `applyEntry`:**
- `EventWorkerDown` → `n.maekawa.NotifyWorkerDown(event.WorkerID)` + update WorkerStats
- `EventWorkerUp`   → `n.maekawa.NotifyWorkerUp(event.WorkerID)`
- `EventWorkerRemoved` → `n.maekawa.NotifyWorkerRemoved(event.WorkerID)` (active membership shrink)
- `EventWorkerAdded`   → `n.maekawa.NotifyWorkerAdded(event.WorkerID)` (active membership grow)
- `EventDone`       → worker reputation += reward, requester balance -= reward
- `EventFailed`     → worker reputation -= penalty (slashing); if RetryCount < MaxRetries → re-queue task
- `EventAssigned`   → task status = InProgress

**Why `applyEntry` calls `NotifyWorkerDown` (not `replicateLog` directly):**
Raft guarantees all nodes apply the same log in the same order. If the leader called `NotifyWorkerDown` directly on detection, followers wouldn't know until they received the committed entry. By going through the log, all 9 Maekawa workers get `NotifyWorkerDown` at the same logical point — consistent state.

---

### Step 17 — `cmd/raft/main.go`
```
1. Read NODE_ID, RAFT_PEERS from env
2. raft.NewNode(id, peers)
3. node.Start()
4. Block on signal
```

---

## Phase 4 — Integration: Maekawa → Raft handoff

### Step 18 — Worker calls Raft after CS acquired
In `cmd/worker/main.go` (or `internal/maekawa/worker.go` via callback):

```
worker.RequestCS(task)
    → CS acquired
    → worker executes task (simulated sleep)
    → raftClient.Submit(encode TaskDone{taskID, workerID, result})
    → wait for commit confirmation
worker.ReleaseCS()
```

The Raft leader receives the Submit, appends to log, replicates to followers. Once majority ACK → commit → `applyEntry` runs Economic Enforcer.

---

### Step 19 — `cmd/requester/main.go`
```
1. Connect to Raft leader
2. Submit task via gRPC: raftClient.Submit(encode Task{...})
3. Poll/wait for task status = Done in the state machine
```

---

### Step 20 — `docker-compose.yml`
```yaml
services:
  raft0..raft2: image: raft binary, RAFT_PEERS env
  worker0..worker8: image: worker binary, WORKER_ID + WORKER_PEERS env
  requester: image: requester binary
  dashboard: image: dashboard binary
```

---

## Phase 4.5 — Standby Worker (active-standby replica)

### Design

Each worker optionally has a **standby** — another worker that watches the Raft log for its primary and takes over automatically if the primary dies mid-task. This avoids waiting for a requester to re-submit; the standby claims the task immediately.

This is **not** CS shadowing. The standby requests its own fresh quorum via Maekawa when it takes over. The task is re-executed (same as Option A retry), but faster — no round-trip to the requester.

**Combined redundancy model:**
```
Option A (Raft retry):    task survives any worker failure, re-queued by requester
Option B (standby):       task survives primary failure, claimed immediately by standby
Both together:            standby tries first; if standby also fails → Raft retry via requester
```

---

### What needs to be added

**`internal/maekawa/worker.go`** — add standby identity field:
```go
StandbyFor int  // ID of the primary this worker is standby for (-1 = not a standby)
```

**`internal/maekawa/standby.go`** (new file) — standby logic:
```go
// StandbyNotifier is called by Raft applyEntry when both conditions are true:
//   1. EventAssigned was committed for task T with WorkerID == w.StandbyFor
//   2. EventWorkerDown was committed for w.StandbyFor
// This means the primary died mid-task — standby should take over.
func (w *Worker) TakeOver(task *models.Task) {
    // request fresh CS via Maekawa
    // re-execute task
    // commit EventDone or EventFailed to Raft
}
```

**`internal/raft/ledger.go`** — extend `applyEntry`:
```go
case models.EventWorkerDown:
    n.maekawa.NotifyWorkerDown(event.WorkerID)
    // check if any in-progress task was assigned to the dead worker
    // if yes, notify that worker's standby via StandbyNotifier
```

**`MaekawaNotifier` interface** — extend with standby method:
```go
type MaekawaNotifier interface {
    NotifyWorkerDown(workerID int)
    NotifyWorkerUp(workerID int)
    TakeOver(task *models.Task)  // called on standby when primary dies mid-task
}
```

---

### Standby assignment (static, configured at startup)

For 9 workers in a 3×3 grid, standby pairs are assigned by column rotation:
```
W0 → standby W3 (same column, next row)
W1 → standby W4
W2 → standby W5
W3 → standby W6
W4 → standby W7
W5 → standby W8
W6 → standby W0  (wrap around)
W7 → standby W1
W8 → standby W2
```
Configured via `STANDBY_FOR` env var in `cmd/worker/main.go`.

---

### Full failure flow with standby

```
W1 wins quorum → EventAssigned{TaskID, WorkerID:1} committed
W1 executes task → crashes
  ↓
Raft detects W1 down (3 missed heartbeats ~450ms)
Leader commits EventWorkerDown{WorkerID:1}
  ↓
applyEntry on all nodes:
  → NotifyWorkerDown(1)           — Maekawa quorum updated
  → sees EventAssigned for W1 still pending (no EventDone/EventFailed)
  → calls standby W4.TakeOver(task)
  ↓
W4 requests CS via Maekawa (fresh quorum)
W4 re-executes task
W4 commits EventDone{TaskID, WorkerID:4}
  ↓
Economic Enforcer: W4 gets reward, W1 gets no penalty (hardware fault, not negligence)
RetryCount++ on task (tracked in Raft state machine)
```

---

### Retry limit

If standby also fails (or no standby configured):
```
RetryCount < MaxRetries → task re-queued, next available worker picks it up
RetryCount >= MaxRetries → EventFailed committed, penalty applied to last worker
```
`MaxRetries` is already in the `Task` model.

---

### New file

| File | Owns |
|---|---|
| [internal/maekawa/standby.go](internal/maekawa/standby.go) | `TakeOver`, standby state tracking |

---

## Phase 5 — Dashboard (optional, implement last)

### Step 21 — `internal/dashboard/collector.go` + `api.go`
- Collector polls Raft nodes for log/commit state and workers for Maekawa state
- API exposes `/state`, `/log`, `/workers` as JSON over HTTP

### Step 22 — `cmd/dashboard/main.go`
Start HTTP server, serve frontend assets.

---

## Critical Files Summary

| File | Phase |
|---|---|
| [internal/maekawa/quorum.go](internal/maekawa/quorum.go) | 2 |
| [internal/maekawa/worker.go](internal/maekawa/worker.go) | 2 |
| [internal/maekawa/deadlock.go](internal/maekawa/deadlock.go) | 2 |
| [internal/network/maekawa_server.go](internal/network/maekawa_server.go) | 2 |
| [internal/network/maekawa_client.go](internal/network/maekawa_client.go) | 2 |
| [internal/raft/node.go](internal/raft/node.go) | 3 |
| [internal/raft/election.go](internal/raft/election.go) | 3 |
| [internal/raft/ledger.go](internal/raft/ledger.go) | 3 |
| [internal/network/raft_server.go](internal/network/raft_server.go) | 3 |
| [internal/network/raft_client.go](internal/network/raft_client.go) | 3 |
| [internal/maekawa/standby.go](internal/maekawa/standby.go) | 4.5 |

## Verification

**Phase 2 (Maekawa — safety + liveness):**
1. `go build ./...` — all packages compile
2. `go test ./internal/maekawa/ -run TestQuorum` — quorum math correct
3. `go test ./internal/maekawa/ -run TestNineWorkersMutualExclusion -v` — safety + liveness
4. `go test ./internal/maekawa/ -race -count=5` — no data races across 5 runs (19 tests)

**Phase 2 (Maekawa — fault tolerance):**
5. `TestNotifyWorkerDownUp` — liveWorkers map updated correctly
6. `TestRequestCSFailsWhenQuorumTooSmall` — upfront quorum check works (Scenario A)
7. `TestRequestCSCancelledByWorkerDown` — mid-flight cancel works (Scenario B)
8. `TestReleaseSafeAfterCancelledRequestCS` — ReleaseCS is a no-op after error
9. `TestWorkerDownOutsideQuorumHasNoEffect` — outsider death has no effect
10. `TestMutualExclusionWithOneWorkerDown` — affected workers refuse, unaffected proceed safely
11. `TestConcurrentNotifyWorkerDown` — 8 concurrent NotifyWorkerDown calls, no panic
12. `TestNotifyWorkerDownRacesWithReply` — 20 iterations of maximum-race timing
13. `TestStaleInquireIgnored` — stale delayed INQUIRE from old round is ignored
14. `TestDuplicateNotifyWorkerDownIdempotent` — duplicate down notification is safe/idempotent
15. `TestBroadcastNotifyWorkerDownUpConsistency` — simulated Raft apply on all workers converges state

**Maekawa Coverage Matrix (what is done vs pending):**

| Scenario | Status | Test / Reason |
|---|---|---|
| Mutual exclusion under contention | ✅ Covered | `TestNineWorkersMutualExclusion`, `TestNineWorkersMultipleRounds` |
| Deadlock resolution (INQUIRE/YIELD) | ✅ Covered | `TestNineWorkersMultipleRounds`, `TestInquireIgnoredWhenInCS` |
| Quorum member down mid-request | ✅ Covered | `TestRequestCSCancelledByWorkerDown` |
| CS holder crash + force-release | ✅ Covered | `TestCSHolderDiesVotersForceRelease`, `TestCSHolderDiesNoWaiters` |
| Down/Up notifications + races | ✅ Covered | `TestConcurrentNotifyWorkerDown`, `TestNotifyWorkerDownRacesWithReply`, `TestBroadcastNotifyWorkerDownUpConsistency` |
| Stale delayed control message | ✅ Covered | `TestStaleInquireIgnored` |
| Duplicate down event apply | ✅ Covered | `TestDuplicateNotifyWorkerDownIdempotent` |
| Raft leader crash mid-commit | ⏳ Pending (Raft) | Requires real log replication + commit index handling |
| Network partition with divergent commit visibility | ⏳ Pending (Raft/network) | Needs partition harness across Raft+Maekawa |
| Crash recovery with persisted voter queue/lock state | ⏳ Pending (persistence) | Current Maekawa worker state is in-memory only |

**Phase 3 (Raft):**
13. `go test ./internal/raft/ -run TestLeaderElection` — one leader elected
14. `go test ./internal/raft/ -run TestLogReplication` — committed entries appear on all nodes
15. `go test ./internal/raft/ -run TestWorkerDownCommitted` — missed heartbeats → EventWorkerDown committed
16. Kill the leader, verify re-election within 300ms

**Phase 4 (Integration):**
17. `docker-compose up` — 9 workers + 3 raft nodes + 1 requester
18. Submit task from requester; observe logs: Maekawa winner → Raft commit → Economic Enforcer apply
19. Kill one worker mid-task; verify Raft commits EventWorkerDown, task retried, no penalty applied

**Phase 4.5 (Standby):**
20. Kill primary W1 mid-task; verify standby W4 claims task within ~500ms
21. Kill both primary W1 and standby W4; verify task falls back to Raft retry (Option A)
22. Verify RetryCount increments correctly; task permanently fails after MaxRetries
