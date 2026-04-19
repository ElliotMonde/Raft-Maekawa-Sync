# Plan: Interactive Distributed System Visualization Frontend

## Context

The Raft-Maekawa-Sync project has a fully implemented Raft consensus layer and Maekawa mutual exclusion layer, but the frontend (`/web/`) is a bare vanilla Vite+TypeScript scaffold and the dashboard backend (`/internal/dashboard/`, `/cmd/dashboard/`) is entirely empty stubs. The goal is to build an interactive visualization UI that animates the four key distributed system scenarios — Leader Election, Log Replication, Leader Failover, and Maekawa Mutual Exclusion — drawing from the behavior proven in the Go test suite. The system is headed toward Docker deployment, so the dashboard runs as a separate service that speaks gRPC to the worker/raft nodes.

---

## Architecture Overview

```
[Raft/Worker nodes] ←gRPC→ [Dashboard binary] ←WebSocket→ [Browser frontend]
   (cmd/worker)              (cmd/dashboard)                 (web/ Vite+React)
```

The dashboard binary:
- Polls all Raft nodes via `GetState` gRPC RPC every 200ms
- Intercepts/proxies Maekawa messages to observe lock protocol events
- Exposes a WebSocket at `ws://localhost:8080/ws` pushing JSON events
- Serves the built frontend static files at `GET /`
- Exposes `POST /api/scenario` to trigger scenario actions (submit task, kill node, etc.)

---

## Phase 1: Add React to the Frontend

**Files to change:** `web/package.json`, `web/tsconfig.json`, `web/index.html`, `web/src/`

1. Add React 19 + ReactDOM, Framer Motion (animations), React Flow (node graph), Zustand (state)
2. Convert `web/src/main.ts` → `web/src/main.tsx`, mount `<App />`
3. Update `tsconfig.json` to add `"jsx": "react-jsx"`
4. Update `web/index.html` script tag to point to `main.tsx`

**Dependencies to install:**
```
react react-dom @types/react @types/react-dom
framer-motion
@xyflow/react
zustand
```

---

## Phase 2: Dashboard Backend

### 2a. WebSocket event server — `internal/dashboard/api.go`

Define the event types pushed to the frontend:
```go
type EventType string
const (
  EvtNodeRole       EventType = "node_role"       // follower→candidate→leader
  EvtVoteRequest    EventType = "vote_request"     // candidate → peer
  EvtVoteGranted    EventType = "vote_granted"     // peer → candidate
  EvtHeartbeat      EventType = "heartbeat"        // leader → follower
  EvtLogReplicated  EventType = "log_replicated"   // leader → follower entry
  EvtLogCommitted   EventType = "log_committed"    // entry committed
  EvtTaskSubmitted  EventType = "task_submitted"
  EvtTaskAssigned   EventType = "task_assigned"
  EvtTaskDone       EventType = "task_done"
  EvtLockRequest    EventType = "lock_request"     // Maekawa REQUEST
  EvtLockGrant      EventType = "lock_grant"       // Maekawa GRANT
  EvtLockDefer      EventType = "lock_defer"       // Maekawa DEFER
  EvtLockInquire    EventType = "lock_inquire"     // Maekawa INQUIRE
  EvtLockYield      EventType = "lock_yield"       // Maekawa YIELD
  EvtLockRelease    EventType = "lock_release"     // Maekawa RELEASE
  EvtCSEnter        EventType = "cs_enter"         // worker enters CS
  EvtCSExit         EventType = "cs_exit"          // worker exits CS
  EvtNodeDown       EventType = "node_down"        // node killed
  EvtNodeUp         EventType = "node_up"          // node revived
  EvtMembershipChange EventType = "membership_change"
)

type DashEvent struct {
  Type      EventType `json:"type"`
  From      int32     `json:"from,omitempty"`
  To        int32     `json:"to,omitempty"`
  NodeID    int32     `json:"node_id,omitempty"`
  Role      string    `json:"role,omitempty"`     // "leader"|"follower"|"candidate"
  Term      int64     `json:"term,omitempty"`
  TaskID    string    `json:"task_id,omitempty"`
  LogIndex  int64     `json:"log_index,omitempty"`
  Timestamp int64     `json:"timestamp,omitempty"` // Lamport clock
  Payload   any       `json:"payload,omitempty"`
}
```

HTTP/WebSocket handlers (using `net/http` + `github.com/gorilla/websocket`):
- `GET /ws` — WebSocket upgrade; clients subscribe to event stream
- `POST /api/scenario` — receives `{ scenario, action, params }` to trigger actions
- `GET /` — serve built frontend from `web/dist/`

### 2b. State collector — `internal/dashboard/collector.go`

```go
type Collector struct {
  nodes    []NodeConfig  // id + gRPC addr of each Raft/Worker node
  clients  map[int32]raftpb.RaftClient
  broadcast chan DashEvent
}
```

- Every 200ms: call `GetState` on each Raft node → diff against last snapshot → emit `node_role`, `heartbeat`, `log_replicated`, `log_committed`, `task_*` events
- **For Maekawa observability**: add an optional thin interceptor. Since Maekawa messages are direct gRPC between workers, the dashboard can't passively observe them. Instead, workers call a `ReportEvent(DashEvent)` gRPC on the dashboard when they send/receive Maekawa messages. Add this optional callback to `maekawa/worker.go` as a hook: `worker.SetEventHook(func(DashEvent))`.

### 2c. Dashboard binary — `cmd/dashboard/main.go`

Flags:
- `--addr` (default `:8080`) — HTTP/WebSocket listen address  
- `--nodes` — comma-separated `id=grpcAddr` pairs of all Raft/Worker nodes
- `--web-dir` — path to built frontend (default `./web/dist`)

Startup:
1. Parse node list, dial each via `rpc.Dial()`
2. Start `Collector` goroutine
3. Start HTTP server with WebSocket hub

---

## Phase 3: Frontend Visualization

### Component Tree

```
App
├── ScenarioSidebar        — pick which scenario to run
├── ControlBar             — Play / Pause / Reset / Speed
└── VisualizationCanvas
    ├── RaftClusterView    — shows for Raft scenarios
    │   ├── NodeCard × N   — shows role, term, log entries
    │   └── MessageArrow   — animated packet flying between nodes
    └── MaekawaView        — shows for Maekawa scenario
        ├── WorkerNode × N — shows CS state, Lamport clock, quorum
        ├── MessageArrow   — REQUEST/GRANT/DEFER/RELEASE packets
        └── QuorumHighlight — overlay showing which nodes are in quorum
```

### Scenario Panels

**1. Leader Election**
- Initial state: 3 nodes, all `follower`, term=0
- User clicks "Start" → election timeout fires on node 1
- Animate: node 1 → `candidate`, sends `RequestVote` arrows to nodes 2 & 3
- Animate: grant arrows return, node 1 → `leader`, heartbeats pulse out
- Inspired by: `TestLeaderElection3Nodes`, `TestHeartbeatPreventsNewElection`

**2. Log Replication**
- Show 3 nodes with a log strip on each
- User types a task payload and clicks "Submit"
- Animate: task enters leader's log at index N (pending), replication arrows fly to followers
- Animate: followers append, ACKs return, commit index advances on all nodes
- Task status changes from `PENDING → IN_PROGRESS → COMPLETED`
- Inspired by: `TestReplicationOneEntry`, `TestSubmitTaskLeaderSucceeds`

**3. Leader Failover**
- Start with a running 3-node cluster with pending tasks
- User clicks "Kill Leader" — leader node goes red/dim
- Animate: remaining two nodes hold election, new leader emerges
- Tasks complete on new leader; show task completion crossing the failover boundary
- Inspired by: `TestCombinedRuntimeLeaderFailover`, `TestCombinedRuntimeManyTaskFailoverRun`

**4. Maekawa Mutual Exclusion**
- Show 9 worker nodes in a 3×3 grid with quorum overlays
- User selects how many workers contend (2, 5, or 9)
- Animate: REQUEST packets fly to quorum members, GRANT/DEFER responses return
- Highlight node currently in CS (green glow); others show WAITING
- INQUIRE/YIELD deadlock prevention protocol shown when applicable
- User can "Kill CS Holder" mid-execution → show voters eventually releasing → next waiter wins
- Lamport clock counter shown per node, incrementing with each message
- Inspired by: `TestTwoWorkersMutualExclusion`, `TestNineWorkersMutualExclusion`, `TestNoStarvation`, `TestCSHolderDiesVotersEventuallyRelease`, `TestInquireYieldRoundtrip`

### State Management (Zustand store)

```typescript
interface AppStore {
  scenario: 'election' | 'replication' | 'failover' | 'maekawa'
  nodes: Map<number, NodeState>
  events: DashEvent[]
  tasks: Map<string, TaskState>
  wsConnected: boolean
  speed: number  // 0.5x, 1x, 2x
}
```

WebSocket listener feeds events into the store; components reactively re-render and trigger Framer Motion animations.

### Animation Design

- **Message packets**: small colored circles that travel along SVG paths between nodes using Framer Motion `animate` with `x/y` keyframes
  - Red = VOTE_REQUEST / LOCK_REQUEST
  - Green = VOTE_GRANTED / LOCK_GRANT  
  - Yellow = DEFER / INQUIRE
  - Orange = YIELD
  - Blue = HEARTBEAT / APPEND_ENTRIES
- **Node state glow**: Framer Motion `animate` on box-shadow
  - Gold = leader / in CS
  - Gray = follower / idle
  - Orange = candidate / requesting
  - Red/dim = down
- **Log strip**: append animation slides new entry in from right

---

## Phase 4: Maekawa Worker Event Hook

**File:** `internal/maekawa/worker.go`

Add an optional hook field:
```go
type EventHook func(from, to int32, evtType string, clock int64)

// On Worker struct:
EventHook EventHook  // set by dashboard; nil = no-op
```

Call the hook at the key send points inside `worker.go`:
- After sending `RequestLock` → hook("lock_request", from, to, clock)
- After sending `Grant` → hook("lock_grant", from, to, clock)
- After sending `Inquire` → hook("lock_inquire", from, to, clock)
- After yielding → hook("lock_yield", from, to, clock)
- When entering CS → hook("cs_enter", id, -1, clock)
- When releasing → hook("lock_release", id, -1, clock)

In Docker setup, workers call dashboard's gRPC `ReportEvent` endpoint instead of in-process hook.

---

## Phase 5: Docker-Ready Design

`docker-compose.yml` structure (to be created later):
```yaml
services:
  worker1: { build: ., command: ./worker --id 1 --addr worker1:5001 --peers ... }
  worker2: { ... }
  worker3: { ... }
  dashboard:
    build: .
    command: ./dashboard --nodes 1=worker1:5001,2=worker2:5001,3=worker3:5001
    ports: ["8080:8080"]
    environment:
      WEB_DIR: /app/web/dist
```

Dashboard binary connects to workers by service name. Frontend always connects to `ws://<dashboard-host>:8080/ws`.

---

## Critical Files

| File | Action |
|------|--------|
| `web/package.json` | Add React, Framer Motion, React Flow, Zustand |
| `web/tsconfig.json` | Add `"jsx": "react-jsx"` |
| `web/index.html` | Change script src to `main.tsx` |
| `web/src/main.tsx` | New: React root mount |
| `web/src/App.tsx` | New: top-level layout |
| `web/src/store.ts` | New: Zustand store + WebSocket listener |
| `web/src/components/` | New: NodeCard, MessageArrow, RaftView, MaekawaView, ScenarioSidebar |
| `internal/dashboard/api.go` | Implement: event types, WebSocket hub, HTTP handlers |
| `internal/dashboard/collector.go` | Implement: gRPC polling, state diffing, event emission |
| `cmd/dashboard/main.go` | Implement: CLI flags, start collector + HTTP server |
| `internal/maekawa/worker.go` | Add: optional EventHook field + hook call sites |
| `go.mod` | Add: `github.com/gorilla/websocket` |

---

## Reuse Existing Code

- `internal/rpc/client.go` → `rpc.Dial()` — reuse for dashboard→worker gRPC connections
- `internal/rpc/server.go` → `rpc.Server` — reuse if dashboard exposes a gRPC `ReportEvent` endpoint
- `api/raft/raft.proto` → `GetState` RPC — primary polling mechanism for Raft state
- `internal/models/task.go` → `TaskEvent`, `EventType` constants — reuse in dashboard event types
- `internal/raft/node.go` → `StateMachine`, `TaskRecord` — match frontend task state shape

---

## Verification

1. **Backend only**: `go run ./cmd/dashboard --nodes 1=127.0.0.1:5001` → open `ws://localhost:8080/ws` in wscat, verify JSON events stream when a worker cluster is running
2. **Frontend dev**: `cd web && npm run dev` → connect to dashboard WebSocket, verify store updates and components render
3. **Scenario: Election** → 3 workers running → trigger election → verify `node_role` events animate correctly in browser
4. **Scenario: Maekawa** → 9 workers → run contention scenario → verify exactly one node shows CS glow at a time (mirrors `TestNineWorkersMutualExclusion`)
5. **Scenario: Failover** → kill leader node → verify new election animates and task completion continues
6. **End-to-end**: `make build` → run all binaries + dashboard → open browser → step through all 4 scenario panels
