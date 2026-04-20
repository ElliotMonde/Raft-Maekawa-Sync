# Frontend Guide

This guide reflects the current dashboard and runtime layout:

- `N1-N3` are pure Raft nodes.
- `W1-W3` are pure Maekawa workers backed by the Raft cluster.
- The dashboard always shows both halves of the system at the same time:
  - left pane: Raft
  - right pane: Maekawa

## 1. Start The System

### Docker demo path

```bash
make docker-down
make docker-up
```

Open:

```text
http://localhost:8080
```

Submit a task from the terminal:

```bash
make docker-request DATA="demo-task"
```

Important:

- submit tasks to a Raft node, not a worker
- default requester target is `raft-node1:5001`
- if that node is not the leader, the requester will tell you which node to retry

Tear down:

```bash
make docker-down
```

### Local frontend dev

Build the web app once:

```bash
cd web
npm install
npm run build
```

Run the dashboard and cluster however you prefer, then for hot reload:

```bash
cd web
npm run dev
```

Vite serves the UI on `http://localhost:5173` and proxies `/ws` and `/api` to the dashboard on `:8080`.

## 2. What The Dashboard Shows

The UI is a live observer of the split architecture.

### Left pane: Raft Cluster

Shows nodes `N1-N3` only.

Each node displays:

- node id
- current role: `LEADER`, `FOLLOWER`, `CANDIDATE`, or `DOWN`
- term
- voted-for hint when relevant
- replicated log strip below the node

The task ribbon at the bottom of the pane shows task lifecycle as seen by Raft:

- `PENDING`
- `IN PROGRESS`
- `COMPLETED`
- `FAILED`

### Right pane: Maekawa Workers

Shows workers `W1-W3`, which correspond to backend ids `4, 5, 6`.

Each worker displays:

- worker label `W1-W3`
- current state: `IDLE`, `WAITING`, `IN CS`, or `DOWN`
- Lamport clock
- vote ownership when it has granted a vote

The pane also shows:

- quorum grid layout
- quorum highlight around the current CS holder
- Maekawa message arrows between workers

### Cross-panel overlay

The animated overlay between the two panes visualizes only cross-boundary events:

- `task_assigned`: Raft to worker
- `task_done`: worker to Raft
- `task_failed`: worker to Raft

This is the cleanest place to see the separation between consensus and execution.

## 3. How To Navigate

The sidebar has two tabs.

### `Scenarios`

This tab is a launcher for pre-baked demo actions. It does not change the layout. It simply triggers submissions or failures so you can watch the system respond in the two panes and event log.

### `Controls`

This tab is for manual exploration.

It contains:

- task submission form
- quick batch submit buttons
- animation speed buttons
- reset view button
- Raft node controls
- worker controls
- Lamport clock readout

## 4. Sidebar Controls

### Submit Task

Posts a task into the Raft layer through the dashboard API. The task then appears in the Raft ribbon, is assigned through committed state, and eventually executes through a worker.

### `× 3`

Submits 3 tasks quickly.

### `× 5`

Submits 5 tasks quickly.

### Animation speed

Scales arrow and event animation timing:

- `0.5x`
- `1x`
- `2x`

### `Reset View`

Clears client-side dashboard state:

- event log
- task ribbon state
- animated messages
- node snapshot in the browser store

It does not reset the actual cluster. The backend keeps running.

### Kill / Start buttons

Current behavior:

- Raft nodes can be killed from the UI
- Raft nodes are not restarted from the UI
- workers can be killed from the UI
- workers can be restarted from the UI

That matches the current frontend wiring:

- worker restart calls the dashboard `start_worker` action
- worker restart only passes worker peers, not Raft peers
- Raft restart is intentionally not exposed in the UI

## 5. Event Log

The event log is the fastest way to confirm what actually happened.

Filters:

- `All`
- `Raft`
- `Maekawa`
- `Tasks`

Common Raft events:

- `node_role`
- `vote_request`
- `vote_granted`
- `heartbeat`
- `log_replicated`
- `log_committed`
- `task_submitted`
- `task_assigned`
- `task_done`
- `task_failed`
- `node_down`
- `node_up`

Common Maekawa events:

- `lock_request`
- `lock_grant`
- `lock_defer`
- `lock_inquire`
- `lock_yield`
- `lock_release`
- `cs_enter`
- `cs_exit`

Worker labels in the log are normalized to `W1-W3` even though the backend ids are `4-6`.

## 6. Scenario Buttons

These are the actual scenario buttons in `web/src/components/ScenarioRunner.tsx`.

### Raft Cluster

#### `Single Task`

What it does:

- submits one task: `demo-task-001`

What to watch:

- leader accepts task
- replicated log entry appears on `N1-N3`
- task transitions through `assigned`, `claimed`, `done`
- one worker enters CS and executes it

#### `Parallel Tasks ×5`

What it does:

- submits five tasks in parallel

What to watch:

- multiple assigned tasks appear quickly
- workers contend for CS
- task ribbon drains over time as tasks complete

#### `Burst Tasks ×9`

What it does:

- submits nine tasks in parallel

What to watch:

- sustained queue pressure
- many Raft task events
- repeated worker contention and serialization through Maekawa

### Raft Election

#### `Leader Failure → Re-election`

What it does:

- kills node `1`, which is often but not always the leader in a fresh run

What to watch:

- one remaining Raft node becomes candidate
- a new leader is elected
- event log shows `node_role` transitions and a higher term

#### `Leader Failure During Tasks`

What it does:

- submits three tasks
- waits briefly
- kills node `1`

What to watch:

- leadership changes during in-flight work
- task processing resumes under the new leader
- Raft stays the source of truth for claim/done state

#### `Submit During No Leader`

What it does:

- kills the current leader
- tries one-shot submit through a follower (should fail)
- waits for new leader election
- retries submit (should succeed)

What to watch:

- failed submit during failover window
- new leader eventually elected
- task accepted and processes on new leader

### Maekawa Mutex

#### `Mutual Exclusion: 2 Workers`

What it does:

- submits two tasks that cause worker contention

What to watch:

- `REQUEST`, `GRANT`, `DEFER`, `RELEASE`
- only one worker in CS at a time

#### `Mutual Exclusion: 9 Workers`

What it does:

- submits nine tasks

Reality check:

- the current Docker demo has 3 workers, not 9
- this still creates high contention, but it is not a 3x3 worker grid unless you build a larger cluster

#### `CS Holder Dies`

What it does:

- submits a task
- waits
- kills worker `4` (`W1`)

What to watch:

- heartbeat timeout marks the worker down in Raft
- Maekawa membership regrids
- queued work can continue on surviving workers

#### `INQUIRE/YIELD Deadlock Prevention`

What it does:

- submits five contending tasks

What to watch:

- `INQUIRE` and `YIELD` arrows in the Maekawa pane
- deadlock-prevention behavior under competing timestamps

#### `No Starvation Test`

What it does:

- submits eight tasks in quick sequence

What to watch:

- all tasks eventually finish
- no worker gets stuck waiting forever

#### `Worker Heartbeat Timeout`

What it does:

- kills a worker node
- waits for heartbeat timeout to mark it `DOWN`
- submits another task to show regridding

What to watch:

- worker transitions to `DOWN` state
- Maekawa quorums automatically regrid
- new task processes on reduced worker set

#### `Claimed Worker Dies -> Task Recovered`

What it does:

- submits a task and waits for a worker to claim it
- kills that claiming worker
- waits for task recovery timeout
- watches task reassign to a surviving worker or complete

What to watch:

- task initially in `in_progress` on one worker
- worker transitions to `DOWN`
- task recovered and either completed or moved to another worker
- failed task does not leave the system hung

### Combined / E2E

#### `Full End-to-End`

What it does:

- submits three named tasks

What to watch:

- full system path:
  - submit to leader
  - replicate in Raft
  - assign to worker
  - Maekawa CS entry
  - result flows back to Raft
  - task completes

#### `Regrid (Kill + Recover Worker)`

What it does:

- kills worker `4`
- waits
- submits a task
- prompts you to restart the worker manually from the UI

What to watch:

- reduced membership
- regridded worker quorums
- `worker_down` then later `worker_up`

#### `Raft Node Restart`

What it does:

- stops a Raft follower
- keeps submitting tasks through the leader
- restarts the follower
- watches it rejoin and catch up

What to watch:

- follower goes `DOWN`
- leadership remains stable
- new follower starts and transitions to `FOLLOWER` role
- log replication catches it up on rejoining

## 7. Recommended Test Flow

1. Start the stack with `make docker-up`.
2. Open the dashboard.
3. Run `Single Task`.
4. Run `Parallel Tasks ×5`.
5. Kill one worker from the `Controls` tab.
6. Submit another task and show that work still completes.
7. Restart that worker from the UI.
8. Run `Leader Failure → Re-election`.
9. Submit another task to show the cluster still works.

## 8. What The Go Tests Cover

The frontend is only a visualizer. The correctness checks live in Go tests.

### `internal/raft`

#### `election_test.go`

Covers leader election and vote rules:

- fresh candidates can win
- stale terms are rejected
- only one vote per term is granted
- nodes step down on higher term
- heartbeat prevents unnecessary elections

Representative tests:

- `TestLeaderElection3Nodes`
- `TestHeartbeatPreventsNewElection`
- `TestSingleNodeElectionBecomesLeader`

#### `ledger_test.go`

Covers Raft log replication and task lifecycle application:

- append entries heartbeats
- stale term rejection
- conflict truncation
- one-entry replication
- submit-to-leader success
- follower redirect
- committed task state exposed by `GetState`

Representative tests:

- `TestReplicationOneEntry`
- `TestSubmitTaskLeaderSucceeds`
- `TestSubmitTaskFollowerRedirects`
- `TestGetStateReflectsCommittedTask`

#### `membership_test.go`

Covers the Raft-backed task and membership adapter:

- only one worker can officially claim a task
- done tasks cannot be claimed again
- active membership reflects committed events
- followers forward lifecycle updates to leader

Representative tests:

- `TestMembershipClaimTaskOnlyOnce`
- `TestMembershipClaimTaskAfterDoneRejected`
- `TestMembershipActiveMembersReflectsEvents`
- `TestFollowerForwardsTaskLifecycleToLeader`

#### `apply_test.go`

Covers forwarding committed Raft events into the worker-side applier.

Representative tests:

- `TestApplyTaskEventToMaekawa`
- `TestApplyTaskEventToMaekawaForwardsTaskEvents`

#### `worker_heartbeat_test.go`

Covers worker liveness tracking from the Raft side.

Representative tests:

- `TestWorkerHeartbeatMarksTrackedWorkerUp`
- `TestHeartbeatTimeoutMarksWorkerDown`

#### `runtime_integration_test.go`

Covers end-to-end integrated runtime behavior:

- submit-task success
- restart persistence
- leader failover
- failover during assignment window
- worker crash before claim
- leader crash after claim before commit
- conflicting claim races
- many-task failover runs

Representative tests:

- `TestCombinedRuntimeSubmitTaskEndToEnd`
- `TestCombinedRuntimeLeaderFailover`
- `TestCombinedRuntimeWorkerCrashBeforeClaim`

### `internal/maekawa`

#### `quorum_test.go`

Covers static quorum properties:

- quorum size
- self inclusion
- quorum intersection
- sorted membership
- regrid after add/remove

#### `integration_safety_test.go`

Covers core mutual exclusion behavior:

- single worker no-contention path
- sequential access
- requester waits correctly
- two-worker and nine-worker mutual exclusion
- Lamport clock monotonicity and synchronization

Representative tests:

- `TestTwoWorkersMutualExclusion`
- `TestNineWorkersMutualExclusion`
- `TestLamportClockMonotonic`

#### `integration_message_test.go`

Covers message-level protocol safety:

- duplicate grant handling
- duplicate release handling
- stale grant rejection
- inquire/yield roundtrip
- invalid node id handling

Representative tests:

- `TestDuplicateGrantIgnoredWhenInCS`
- `TestInquireYieldRoundtrip`

#### `integration_membership_test.go`

Covers membership-driven quorum changes:

- regridding after membership changes
- vote reset
- aborting in-flight requests
- preserving mutual exclusion after change

Representative tests:

- `TestMembershipChangeRegridsQuorum`
- `TestOnMembershipChangeAbortsMidFlightRequest`
- `TestMembershipChangeMutualExclusionPreserved`

#### `integration_fault_test.go`

Covers crash and degraded-cluster behavior:

- worker down/up
- quorum too small
- canceled requests
- holder death
- safe release after cancellation
- request timeout/cancellation behavior

Representative tests:

- `TestMembershipDownUp`
- `TestMutualExclusionWithOneWorkerDown`
- `TestCSHolderDiesVotersEventuallyRelease`
- `TestWorkerRevived`

#### `integration_fairness_test.go`

Covers fairness and contention pressure:

- barrier start with 2 workers
- barrier start with 9 workers
- churn while contending
- no starvation

Representative tests:

- `TestBarrierStart2Workers`
- `TestBarrierStart9Workers`
- `TestNoStarvation`

#### `tasks_local_test.go`, `tasks_cluster_test.go`, `tasks_state_test.go`, `tasks_helpers_test.go`

Covers worker task execution behavior:

- assigned tasks execute and report `done`
- canceled or completed tasks are skipped
- multiple tasks run sequentially
- shared task executes once across workers
- membership changes affect the task loop
- task event application updates local state correctly

Representative tests:

- `TestRunTaskLoopReportsDoneForAssignedTask`
- `TestSharedTaskOnlyExecutedOnce`
- `TestApplyTaskEventWorkerUpDownTriggersRegrid`
- `TestClaimTaskBySingleWinner`

## 9. Practical Notes

- The frontend is a live visualization, not the source of truth.
- `Reset View` only resets browser state.
- The UI labels workers as `W1-W3`, but the backend ids are `4-6`.
- Tasks should be submitted to Raft nodes.
- Worker restart is supported from the UI; Raft restart is not.

## 10. Useful Commands

```bash
make docker-up
make docker-down
make docker-logs
make docker-request DATA="demo-task"
make test
make test-raft
make test-maekawa
```
