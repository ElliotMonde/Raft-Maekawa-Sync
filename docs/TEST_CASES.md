# Test Cases and Results

All tests pass with `make test` and `make race`. Results below reflect a clean run on the final codebase.

```
ok  raft-maekawa-sync/internal/raft      21.7s
ok  raft-maekawa-sync/internal/maekawa  19.6s
```

---

## Raft — Unit Tests (`internal/raft/`)

### Election

| Test | Path | What It Verifies |
|---|---|---|
| `TestRequestVoteGrantsFreshCandidate` | Happy | A follower with no prior vote grants to a candidate with an equal or higher term and up-to-date log |
| `TestRequestVoteRejectsStaleTerm` | Sad | Vote is rejected when the candidate's term is lower than the receiver's current term |
| `TestRequestVoteOnlyOneVotePerTerm` | Sad | A node that already voted in a term rejects a second candidate in that same term |
| `TestRequestVoteStepDownOnHigherTerm` | Sad | A candidate that receives a vote response with a higher term steps down to Follower |
| `TestRequestVoteRejectsStaleLog` | Sad | A vote is rejected when the candidate's log is shorter or older than the voter's log |
| `TestLeaderElection3Nodes` | Happy | A 3-node cluster elects exactly one leader within the election timeout |
| `TestHeartbeatPreventsNewElection` | Happy | Continuous heartbeats from the leader prevent followers from starting a new election |
| `TestSingleNodeElectionBecomesLeader` | Happy | A single-node cluster elects itself leader immediately |

### Log Replication

| Test | Path | What It Verifies |
|---|---|---|
| `TestAppendEntriesHeartbeat` | Happy | An empty AppendEntries (heartbeat) resets the follower's election timer and returns success |
| `TestAppendEntriesRejectsStaleTerm` | Sad | AppendEntries is rejected when the request term is lower than the receiver's current term |
| `TestAppendEntriesConflictTruncate` | Sad | A follower with a conflicting log entry at a given index truncates and replaces it with the leader's version |
| `TestReplicationOneEntry` | Happy | A single log entry submitted to the leader is replicated to all followers and committed |
| `TestSubmitTaskLeaderSucceeds` | Happy | Submitting a task to the leader returns success and the task appears committed in state |
| `TestSubmitTaskFollowerRedirects` | Sad | Submitting a task to a follower returns failure with the leader ID hint |

### State Machine Apply

| Test | Path | What It Verifies |
|---|---|---|
| `TestApplyAssignedEvent` | Happy | Committing `EventAssigned` creates a PENDING task record in the state machine |
| `TestApplyDoneAfterClaimed` | Happy | Committing `EventDone` after `EventClaimed` moves the task to COMPLETED |
| `TestApplyAssignedResetsStaleClaim` | Happy | Re-committing `EventAssigned` for a crashed worker's task clears the previous `AssignedTo` |
| `TestApplyWorkerUpDown` | Happy | `EventWorkerDown` and `EventWorkerUp` correctly toggle `ActiveWorkers` in the state machine |
| `TestApplyCallsApplier` | Happy | Every committed event is forwarded to the registered `TaskEventApplier` |
| `TestGetStateReflectsCommittedTask` | Happy | `GetState` RPC returns the task with its current status after each committed event |

### Membership and Claim Linearization

| Test | Path | What It Verifies |
|---|---|---|
| `TestMembershipClaimTaskOnlyOnce` | Happy | Two concurrent `ClaimTask` calls result in exactly one success; the second is rejected |
| `TestMembershipClaimTaskAfterDoneRejected` | Sad | `ClaimTask` is rejected if the task is already in COMPLETED state |
| `TestMembershipActiveMembersReflectsEvents` | Happy | `ActiveMembers()` returns the correct set after worker-up and worker-down events are committed |
| `TestFollowerForwardsTaskLifecycleToLeader` | Happy | A follower receiving `ClaimTask` or `ReportTaskSuccess` forwards it to the leader via RPC |

### Worker Heartbeat and Liveness

| Test | Path | What It Verifies |
|---|---|---|
| `TestWorkerHeartbeatMarksTrackedWorkerUp` | Happy | A `WorkerHeartbeat` RPC from a managed worker is accepted and records the timestamp |
| `TestHeartbeatTimeoutMarksWorkerDown` | Sad | A worker that stops heartbeating is marked down and `EventWorkerDown` is committed after the timeout |

### Task Recovery

| Test | Path | What It Verifies |
|---|---|---|
| `TestRecoverStaleClaimedTaskForDownWorker` | Happy | A task claimed by a now-dead worker is requeued as PENDING after the claim timeout |
| `TestDoesNotRecoverClaimedTaskForLiveWorker` | Sad | A task claimed by a still-alive worker is not requeued, even after the check interval |

### Applier Bridge

| Test | Path | What It Verifies |
|---|---|---|
| `TestApplyTaskEventToMaekawa` | Happy | Committed events are forwarded to the Maekawa worker's `ApplyTaskEvent` method |
| `TestApplyTaskEventToMaekawaNilNotifierSafe` | Sad | A nil applier does not panic when events are committed |
| `TestApplyTaskEventToMaekawaForwardsTaskEvents` | Happy | All task event types (assigned, claimed, done, failed) reach the applier |

### Node Invariants

| Test | Path | What It Verifies |
|---|---|---|
| `TestNodeDefaults` | Happy | Freshly created node starts with term 0, no votes, and an empty worker set |
| `TestMajority` | Happy | Majority calculation is correct for 1-, 3-, and 5-node clusters |
| `TestLastLogEmpty` | Happy | `lastLogIndex` and `lastLogTerm` return 0 on an empty log |
| `TestBecomeFollower` | Happy | `becomeFollower` resets role, term, and vote state correctly |

---

## Raft — Integration Tests (`internal/raft/runtime_integration_test.go`)

These tests start a full in-process 3-node Raft cluster with real gRPC, real Maekawa workers, and real task execution. They cover end-to-end scenarios including crashes.

| Test | Path | What It Verifies |
|---|---|---|
| `TestCombinedRuntimeSubmitTaskEndToEnd` | Happy | A task submitted over RPC completes on exactly one worker and is reflected as COMPLETED on all nodes |
| `TestCombinedRuntimePersistsStateAcrossRestart` | Happy | A completed task is still visible as COMPLETED after the node is stopped and restarted from persisted state |
| `TestCombinedRuntimeLeaderFailover` | Happy | After the leader crashes, a new leader is elected and a follow-up task completes successfully |
| `TestCombinedRuntimeLeaderFailoverDuringAssignmentWindow` | Mixed | If the leader crashes while workers are waiting to claim, the task either completes exactly once or returns to PENDING — no duplicate execution and no orphaned in-progress record |
| `TestCombinedRuntimeWorkerCrashBeforeClaim` | Sad | If a worker crashes before claiming, another worker picks up the task and completes it exactly once |
| `TestCombinedRuntimeLeaderCrashAfterClaimBeforeCommit` | Sad | If the leader crashes after receiving a claim but before committing it, the task resets to PENDING (unclaimed) and can be reclaimed after regrid |
| `TestCombinedRuntimeConflictingClaimsRace` | Sad | Two workers simultaneously calling `ClaimTask` results in exactly one success; the winner can then report completion |
| `TestCombinedRuntimeManyTaskFailoverRun` | Happy | Three pre-failover tasks and five post-failover tasks all complete exactly once with no duplicates |

---

## Maekawa — Unit Tests (`internal/maekawa/`)

### Quorum Math

| Test | Path | What It Verifies |
|---|---|---|
| `TestQuorumSize` | Happy | Grid quorum for N workers has the expected ⌈√N⌉ × 2 − 1 size |
| `TestQuorumSelfIncluded` | Happy | Every worker's quorum includes itself |
| `TestQuorumIntersection` | Happy | Any two quorums from the same active set share at least one member |
| `TestQuorumSorted` | Happy | Quorum membership list is returned in ascending ID order |
| `TestKnownQuorums` | Happy | Spot-checked quorums for 1, 3, 4, 9 workers match expected grid structure |
| `TestRegridAfterRemoval` | Happy | Removing a worker produces a valid quorum that still intersects with peers |
| `TestRegridAfterAdd` | Happy | Adding a worker produces a valid quorum covering the new member |

### Core Mutual Exclusion

| Test | Path | What It Verifies |
|---|---|---|
| `TestSingleWorkerNoContention` | Happy | A single worker acquires and releases the lock without any peers |
| `TestTwoWorkersSequential` | Happy | Two workers acquiring the lock sequentially each succeed without overlap |
| `TestRequesterWaitsUntilCurrentHolderReleases` | Happy | A second requester blocks until the first has released |
| `TestTwoWorkersMutualExclusion` | Happy | Two workers competing concurrently do not overlap in the critical section |
| `TestNineWorkersMutualExclusion` | Happy | Nine workers competing concurrently maintain mutual exclusion across all rounds |
| `TestNineWorkersMultipleRounds` | Happy | Nine workers each performing three lock rounds never violate mutual exclusion |
| `TestInquireIgnoredWhenInCS` | Sad | A worker already in the critical section ignores `Inquire` — it will release naturally |

### Lamport Clock

| Test | Path | What It Verifies |
|---|---|---|
| `TestLamportClockMonotonic` | Happy | Clock value strictly increases on every `tick()` call |
| `TestLamportClockSync` | Happy | `sync(received)` sets the clock to `max(local, received) + 1` |

### Message Safety

| Test | Path | What It Verifies |
|---|---|---|
| `TestDuplicateGrantIgnoredWhenInCS` | Sad | A duplicate `Grant` received while already in CS does not corrupt vote count |
| `TestDuplicateReleaseSafe` | Sad | A duplicate `ReleaseLock` from the same worker is idempotent |
| `TestStaleGrantIgnoredBadTimestamp` | Sad | A `Grant` with a timestamp that does not match the current lock round is discarded |
| `TestInquireYieldRoundtrip` | Happy | An `Inquire` sent to a waiting requester results in a `Yield` and then the voter re-grants the higher-priority requester |
| `TestRequestLockValidatesNodeID` | Sad | `RequestLock` with an invalid (zero) node ID is rejected |
| `TestConcurrentRequestsSafe` | Happy | Many concurrent `RequestLock` calls do not corrupt voter state |
| `TestReleaseLockUnknownNodeIgnored` | Sad | `ReleaseLock` from a node that was never granted a vote is silently ignored |

### Fault Tolerance

| Test | Path | What It Verifies |
|---|---|---|
| `TestMembershipDownUp` | Happy | A worker that goes down and comes back up causes no permanent lock state corruption |
| `TestRequestCSFailsWhenQuorumTooSmall` | Sad | Lock acquisition fails if too many quorum members are down to form a majority |
| `TestMutualExclusionWithOneWorkerDown` | Happy | Mutual exclusion holds with one worker down if quorum can still be formed |
| `TestRequestCSCancelledByWorkerDown` | Sad | An in-flight lock request is aborted when a quorum member goes down mid-request |
| `TestReleaseSafeAfterCancelledRequestCS` | Sad | Releasing a lock after a cancelled request does not deadlock or corrupt peers |
| `TestWorkerDownOutsideQuorumHasNoEffect` | Sad | A worker crashing that is not in the requester's quorum has no effect on the lock |
| `TestConcurrentMarkDown` | Sad | Multiple concurrent membership-down events are handled safely without race conditions |
| `TestRequestCSContextTimeout` | Sad | `RequestForGlobalLock` returns an error if the context deadline elapses before all grants arrive |
| `TestRequestCSContextCancelled` | Sad | `RequestForGlobalLock` returns immediately when the context is cancelled |
| `TestMembershipDownRacesWithReply` | Sad | A membership-down event racing with a grant reply from the same node is handled without panic |
| `TestWorkerRevived` | Happy | A worker that crashed and is revived can successfully acquire the lock again |
| `TestCSHolderDiesVotersEventuallyRelease` | Sad | If the CS holder crashes without releasing, voters time out and clear their votes so new requests can proceed |

### Membership Change

| Test | Path | What It Verifies |
|---|---|---|
| `TestMembershipChangeRegridsQuorum` | Happy | After a membership change, each worker's quorum reflects the new active set |
| `TestMembershipChangeSetsVotedForToNegOne` | Happy | A membership change clears the voter's current vote so no stale grant lingers |
| `TestOnMembershipChangeAbortsMidFlightRequest` | Sad | A lock acquisition in progress is aborted when membership changes during it |
| `TestMembershipChangeMutualExclusionPreserved` | Happy | Mutual exclusion still holds after a membership change |
| `TestQuorumIntersectionAfterChange` | Happy | Any two workers' quorums still intersect after a membership change |

### Fairness and No Starvation

| Test | Path | What It Verifies |
|---|---|---|
| `TestBarrierStart2Workers` | Happy | Two workers simultaneously requesting the lock both eventually acquire it |
| `TestBarrierStart9Workers` | Happy | Nine workers simultaneously requesting the lock all eventually acquire it |
| `TestChurnWhileContending` | Happy | Workers repeatedly entering and leaving the cluster while others contend for the lock — no worker starves and mutual exclusion holds throughout |
| `TestNoStarvation` | Happy | Under sustained contention, every requesting worker acquires the lock within a bounded number of rounds |

---

## Maekawa — Task Integration Tests

### Task Execution

| Test | Path | What It Verifies |
|---|---|---|
| `TestSharedTaskOnlyExecutedOnce` | Happy | A task visible to all workers in a cluster is executed by exactly one worker |
| `TestMultipleDistinctTasksAllExecuted` | Happy | Multiple distinct tasks are all executed and none are dropped |
| `TestCanceledSharedTaskNotExecuted` | Sad | A task canceled before execution is not run by any worker |
| `TestClusterMutualExclusionDuringTaskExecution` | Happy | Two workers executing different tasks do not overlap in the critical section |
| `TestTaskExecutedBySingleWorker` | Happy | A task enqueued on a single worker runs through the full lock-acquire-execute-release cycle |
| `TestCanceledTaskSkipped` | Sad | A canceled task dequeued by the task loop is skipped without acquiring the lock |
| `TestMultipleTasksExecutedSequentially` | Happy | Multiple tasks queued on a single worker are all executed in order |
| `TestApplyTaskEventDoneSkipsCanceled` | Sad | A `done` event for a task marks it canceled so it is not re-executed if re-enqueued |
| `TestContextCancellationStopsRunTaskLoop` | Sad | Cancelling the context cleanly stops the task loop without goroutine leaks |
| `TestMembershipImpactsTaskLoop` | Sad | A membership change that aborts a lock acquisition causes the task to be retried rather than dropped |
| `TestRunTaskLoopReportsDoneForAssignedTask` | Happy | The task loop acquires the lock, runs the executor, and calls `ReportTaskSuccess` |
| `TestWorkerSkipsCanceledTaskBeforeExecution` | Sad | A task canceled between dequeue and lock acquisition is not passed to the executor |
| `TestWorkerAppliesDoneEventSkipsTask` | Sad | A `done` event applied while the task is queued prevents a second execution |
| `TestRunTaskLoopProcessesMultipleTasksSequentially` | Happy | The task loop processes a burst of tasks one at a time without skipping any |
| `TestRunTaskLoopContextCancellationStops` | Sad | Context cancellation during task execution stops the loop cleanly |
| `TestApplyTaskEventCanceledEntriesMap` | Happy | `ApplyTaskEvent` for `EventCanceled` correctly updates the internal canceled map |
| `TestApplyTaskEventDoneEntriesMap` | Happy | `ApplyTaskEvent` for `EventDone` correctly updates the internal canceled map |
| `TestApplyTaskEventWorkerUpDownTriggersRegrid` | Happy | Worker-up and worker-down events applied via `ApplyTaskEvent` trigger a quorum regrid |
| `TestCanceledTaskIdempotency` | Sad | Canceling the same task twice is idempotent |
| `TestTaskNotExecutedWhenCanceledBeforeDequeue` | Sad | A task canceled before it is dequeued is silently discarded by the task loop |
| `TestClaimTaskBySingleWinner` | Happy | Among multiple workers calling `ClaimTask` for the same task, exactly one succeeds |

---

## Running All Tests

```bash
# Full suite
make test

# With race detector
make race

# Repeat a specific test 20 times (flakiness detection)
GOCACHE=/tmp/go-build go test ./internal/raft \
  -run TestCombinedRuntimeLeaderCrashAfterClaimBeforeCommit \
  -count=20 -timeout=300s

# Repeat both target tests 20 times
GOCACHE=/tmp/go-build go test ./internal/raft \
  -run 'TestCombinedRuntimeLeaderCrashAfterClaimBeforeCommit|TestCombinedRuntimeConflictingClaimsRace' \
  -count=20 -timeout=300s
```
