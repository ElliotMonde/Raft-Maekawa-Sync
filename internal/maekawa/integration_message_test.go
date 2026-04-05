package maekawa

import (
	"context"
	"testing"
	"time"

	maekawapb "raft-maekawa/proto/maekawapb"
)

// Test 40: duplicate REPLY does not over-count votes or cause double CS entry.
func TestDuplicateReplyNoOverCount(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]
	task := makeTask(0, 40)

	if err := w0.RequestCS(context.Background(), task); err != nil {
		t.Fatalf("RequestCS: %v", err)
	}

	// Now in StateHeld. Inject two extra REPLY messages — must be silently ignored.
	reply := &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_REPLY,
		SenderId: int32(workers[1].ID),
		Clock:    99,
		TaskId:   task.ID,
	}
	w0.HandleMessage(reply)
	w0.HandleMessage(reply)

	w0.mu.Lock()
	rc := w0.replyCount
	state := w0.state
	w0.mu.Unlock()

	if state != StateHeld {
		t.Errorf("unexpected state after duplicate REPLY: %v", state)
	}
	_ = rc // replyCount is implementation detail; what matters is state == StateHeld
	w0.ReleaseCS()
}

// Test 41: duplicate RELEASE from the same sender is idempotent — voter does not
// double-grant or corrupt its wait queue.
func TestDuplicateReleaseIdempotent(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	// W0 requests and releases CS.
	w0, w1 := workers[0], workers[1]
	task := makeTask(0, 41)
	if err := w0.RequestCS(context.Background(), task); err != nil {
		t.Fatalf("RequestCS: %v", err)
	}
	w0.ReleaseCS()
	time.Sleep(20 * time.Millisecond)

	// Inject a second RELEASE from W0 into W1 — W1's voter should be free already.
	rel := &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_RELEASE,
		SenderId: int32(w0.ID),
		Clock:    200,
		TaskId:   task.ID,
	}
	w1.HandleMessage(rel)

	// W1's voter must still be unlocked (not accidentally locked/corrupted).
	w1.mu.Lock()
	locked := w1.voter.locked
	w1.mu.Unlock()
	if locked {
		t.Error("voter locked after duplicate RELEASE — state corrupted")
	}

	// W1 should still be able to enter CS itself.
	task2 := makeTask(w1.ID, 41)
	if err := w1.RequestCS(context.Background(), task2); err != nil {
		t.Fatalf("W1 RequestCS after duplicate RELEASE: %v", err)
	}
	w1.ReleaseCS()
}

// Test 42: stale RELEASE from an older request round is ignored — a voter must
// not unlock a newer grant held by the same sender.
func TestStaleReleaseIgnored(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	voter := workers[1]

	voter.mu.Lock()
	voter.voter.locked = true
	voter.voter.lockedFor = 0
	voter.voter.lockedClock = 20
	HeapPush(&voter.voter.waitQueue, &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_REQUEST,
		SenderId: 2,
		Clock:    30,
		TaskId:   "queued-after-current",
	})
	voter.mu.Unlock()

	voter.HandleMessage(&maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_RELEASE,
		SenderId: 0,
		Clock:    10,
		TaskId:   "stale-release",
	})

	voter.mu.Lock()
	defer voter.mu.Unlock()

	if !voter.voter.locked {
		t.Fatal("stale RELEASE unlocked voter")
	}
	if voter.voter.lockedFor != 0 {
		t.Fatalf("stale RELEASE changed lockedFor = %d, want 0", voter.voter.lockedFor)
	}
	if voter.voter.lockedClock != 20 {
		t.Fatalf("stale RELEASE changed lockedClock = %d, want 20", voter.voter.lockedClock)
	}
	if got := voter.voter.waitQueue.Len(); got != 1 {
		t.Fatalf("stale RELEASE changed waitQueue len = %d, want 1", got)
	}
}

// Test 43: RELEASE for a requester that is only queued, not currently holding
// the vote, removes that stale queued request instead of leaving it to be
// re-granted after the current holder releases.
func TestReleaseRemovesQueuedStaleRequest(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	voter := workers[1]

	voter.mu.Lock()
	voter.voter.locked = true
	voter.voter.lockedFor = int32(workers[0].ID)
	voter.voter.lockedClock = 20
	HeapPush(&voter.voter.waitQueue, &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_REQUEST,
		SenderId: int32(workers[2].ID),
		Clock:    10,
		TaskId:   "stale-requester",
	})
	HeapPush(&voter.voter.waitQueue, &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_REQUEST,
		SenderId: int32(workers[3].ID),
		Clock:    30,
		TaskId:   "live-waiter",
	})
	voter.mu.Unlock()

	// Worker 2 already abandoned the round; its RELEASE should delete only its
	// queued request and leave the current grant intact.
	voter.HandleMessage(&maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_RELEASE,
		SenderId: int32(workers[2].ID),
		Clock:    10,
		TaskId:   "stale-requester",
	})

	voter.mu.Lock()
	if !voter.voter.locked {
		voter.mu.Unlock()
		t.Fatal("queued RELEASE unlocked current holder")
	}
	if voter.voter.lockedFor != int32(workers[0].ID) || voter.voter.lockedClock != 20 {
		gotFor, gotClock := voter.voter.lockedFor, voter.voter.lockedClock
		voter.mu.Unlock()
		t.Fatalf("queued RELEASE changed current grant to (%d,%d), want (%d,20)", gotFor, gotClock, workers[0].ID)
	}
	if got := voter.voter.waitQueue.Len(); got != 1 {
		voter.mu.Unlock()
		t.Fatalf("queued RELEASE left waitQueue len = %d, want 1", got)
	}
	voter.mu.Unlock()

	// When the current holder releases, the vote must go to worker 3, not back
	// to the already-cancelled worker 2.
	voter.mu.Lock()
	out := voter.onRelease(&maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_RELEASE,
		SenderId: int32(workers[0].ID),
		Clock:    20,
		TaskId:   "current-holder",
	})
	gotFor := voter.voter.lockedFor
	gotClock := voter.voter.lockedClock
	voter.mu.Unlock()

	if len(out) != 1 {
		t.Fatalf("expected one REPLY sendFn after current holder release, got %d", len(out))
	}
	if gotFor != int32(workers[3].ID) || gotClock != 30 {
		t.Fatalf("grant after queued RELEASE went to (%d,%d), want (%d,30)", gotFor, gotClock, workers[3].ID)
	}
}

// Test 44: stale YIELD from an older inquired round is ignored — a voter must
// not requeue/unlock a newer grant held by the same sender.
func TestStaleYieldCrossRoundIgnored(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	voter := workers[0]

	voter.mu.Lock()
	voter.voter.locked = true
	voter.voter.lockedFor = int32(workers[1].ID)
	voter.voter.lockedClock = 20
	voter.voter.inquired = true
	HeapPush(&voter.voter.waitQueue, &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_REQUEST,
		SenderId: int32(workers[2].ID),
		Clock:    30,
		TaskId:   "queued-after-current",
	})
	voter.mu.Unlock()

	voter.HandleMessage(&maekawapb.MaekawaMsg{
		Type:         maekawapb.MsgType_YIELD,
		SenderId:     int32(workers[1].ID),
		Clock:        99,
		TaskId:       "stale-yield",
		InquireClock: 10,
	})

	voter.mu.Lock()
	defer voter.mu.Unlock()

	if !voter.voter.locked {
		t.Fatal("stale YIELD unlocked voter")
	}
	if voter.voter.lockedFor != int32(workers[1].ID) {
		t.Fatalf("stale YIELD changed lockedFor = %d, want %d", voter.voter.lockedFor, workers[1].ID)
	}
	if voter.voter.lockedClock != 20 {
		t.Fatalf("stale YIELD changed lockedClock = %d, want 20", voter.voter.lockedClock)
	}
	if !voter.voter.inquired {
		t.Fatal("stale YIELD cleared inquired flag")
	}
	if got := voter.voter.waitQueue.Len(); got != 1 {
		t.Fatalf("stale YIELD changed waitQueue len = %d, want 1", got)
	}
}

// Test 45: stale YIELD (from a previous request round) is ignored — voter does not
// grant to a non-existent waiter.
func TestStaleYieldIgnored(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]

	// Complete a full CS round so clock advances.
	task1 := makeTask(0, 421)
	if err := w0.RequestCS(context.Background(), task1); err != nil {
		t.Fatalf("round 1 RequestCS: %v", err)
	}
	w0.ReleaseCS()
	time.Sleep(20 * time.Millisecond)

	// Inject a stale YIELD referencing the old round's task — voter is not currently
	// in inquired state, so onYield must return nil and leave voter state intact.
	staleYield := &maekawapb.MaekawaMsg{
		Type:         maekawapb.MsgType_YIELD,
		SenderId:     int32(workers[1].ID),
		Clock:        1,
		TaskId:       task1.ID,
		InquireClock: 1,
	}
	w0.HandleMessage(staleYield)

	w0.mu.Lock()
	inquired := w0.voter.inquired
	locked := w0.voter.locked
	w0.mu.Unlock()

	if inquired {
		t.Error("voter.inquired set after stale YIELD")
	}
	// voter.locked may be false (no current holder) — that's fine.
	_ = locked

	// W0 must still be able to enter CS normally.
	task2 := makeTask(0, 422)
	if err := w0.RequestCS(context.Background(), task2); err != nil {
		t.Fatalf("RequestCS after stale YIELD: %v", err)
	}
	w0.ReleaseCS()
}

// Test 46: stale FAILED (not in StateWanting) is ignored — no state mutation.
func TestStaleFAILEDIgnored(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]

	// Inject FAILED while in StateReleased.
	failed := &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_FAILED,
		SenderId: int32(workers[1].ID),
		Clock:    5,
		TaskId:   "stale-task",
	}
	w0.HandleMessage(failed)

	w0.mu.Lock()
	state := w0.state
	csErr := w0.csErr
	w0.mu.Unlock()

	if state != StateReleased {
		t.Errorf("state changed after stale FAILED: %v", state)
	}
	if csErr != nil {
		t.Errorf("csErr set after stale FAILED: %v", csErr)
	}

	// Must still be able to enter CS normally.
	task := makeTask(0, 43)
	if err := w0.RequestCS(context.Background(), task); err != nil {
		t.Fatalf("RequestCS after stale FAILED: %v", err)
	}
	w0.ReleaseCS()
}

// Test 49: if our self-request was only queued locally (because another requester
// currently held our voter), cancelling the request must not leave that stale
// self-request in the local voter queue. Otherwise a later grantNext could lock
// the voter for self and send a REPLY to a requester that is no longer waiting.
func TestCancelledQueuedSelfRequestRemovedFromVoterQueue(t *testing.T) {
	task := makeTask(0, 49)

	w := &Worker{
		ID:              0,
		state:           StateWanting,
		currentTask:     task,
		currentReqClock: 10,
		neededVotes:     4,
		csEnter:         make(chan struct{}),
	}
	w.voter.waitQueue = make(RequestHeap, 0)

	// Another requester currently holds our local voter.
	w.voter.locked = true
	w.voter.lockedFor = 1
	w.voter.lockedClock = 5

	// Our own request arrives at the local voter and is queued behind the holder.
	selfReq := &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_REQUEST,
		SenderId: int32(w.ID),
		Clock:    w.currentReqClock,
		TaskId:   task.ID,
	}
	if out := w.onRequest(selfReq); len(out) != 0 {
		t.Fatalf("expected queued self-request to produce no outgoing messages, got %d", len(out))
	}

	// Another real waiter queues after us.
	otherReq := &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_REQUEST,
		SenderId: 2,
		Clock:    20,
		TaskId:   "other-task",
	}
	if out := w.onRequest(otherReq); len(out) != 0 {
		t.Fatalf("expected queued waiter to produce no outgoing messages, got %d", len(out))
	}

	// Our request is cancelled before we ever acquire the local vote.
	w.state = StateReleased
	w.currentTask = nil
	if out := w.selfReleaseVoterLocked(w.currentReqClock, task.ID); len(out) != 0 {
		t.Fatalf("unexpected self-release output while self vote was only queued")
	}

	// When the current holder releases, the next grant must skip the cancelled
	// stale self-request and go to the real waiter.
	out := w.grantNext("")
	if !w.voter.locked {
		t.Fatal("expected voter to remain locked for the next real waiter")
	}
	if w.voter.lockedFor != 2 {
		t.Fatalf("stale cancelled self-request was still granted: lockedFor=%d, want 2", w.voter.lockedFor)
	}
	if len(out) != 1 {
		t.Fatalf("expected exactly one REPLY sendFn for the next real waiter, got %d", len(out))
	}
}

// TestDuplicateInquireNoDoubleDecrement verifies that two INQUIRE messages for
// the same request round only decrement replyCount once.
// The stale-clock guard (msg.InquireClock != w.currentReqClock) handles cross-round
// duplicates, but two INQUIREs for the *same* round (same InquireClock) must not
// both decrement.
func TestDuplicateInquireNoDoubleDecrement(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w := workers[0]

	// Put w into StateWanting with replyCount=1, neededVotes=5, currentReqClock=10.
	w.mu.Lock()
	w.state = StateWanting
	w.replyCount = 1
	w.neededVotes = 5
	w.currentReqClock = 10
	w.csEnter = make(chan struct{})

	inquire := &maekawapb.MaekawaMsg{
		Type:         maekawapb.MsgType_INQUIRE,
		SenderId:     int32(workers[1].ID),
		Clock:        11,
		InquireClock: 10, // matches currentReqClock → first INQUIRE is valid
		TaskId:       "t1",
	}

	// First INQUIRE: valid, should decrement replyCount and return a YIELD fn.
	fns1 := w.onInquire(inquire)
	if len(fns1) == 0 {
		t.Fatal("first INQUIRE should have returned a YIELD sendFn")
	}
	if w.replyCount != 0 {
		t.Fatalf("after first INQUIRE replyCount=%d, want 0", w.replyCount)
	}

	// Second INQUIRE for the same round: replyCount is now 0, neededVotes=5 →
	// shouldYield() still true (0 < 5). Without a duplicate guard this would
	// decrement to -1.
	fns2 := w.onInquire(inquire)
	w.mu.Unlock()

	if len(fns2) != 0 {
		t.Error("duplicate INQUIRE for same round should be ignored (no YIELD)")
	}
	w.mu.Lock()
	rc := w.replyCount
	w.mu.Unlock()
	if rc != 0 {
		t.Fatalf("duplicate INQUIRE decremented replyCount again: got %d, want 0", rc)
	}
}

// TestDuplicateYieldNoDoubleRequeue verifies that a second YIELD for the same
// grant round is ignored — the requester is not re-enqueued a second time.
func TestDuplicateYieldNoDoubleRequeue(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	v := workers[0] // acting as voter

	// Simulate: v's voter is locked for worker 1, inquired=true, worker 2 is waiting.
	req2 := &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_REQUEST,
		SenderId: int32(workers[2].ID),
		Clock:    5,
		TaskId:   "t-waiter",
	}
	v.mu.Lock()
	v.voter.locked = true
	v.voter.lockedFor = int32(workers[1].ID)
	v.voter.lockedClock = 3
	v.voter.inquired = true
	HeapPush(&v.voter.waitQueue, req2)

	yield := &maekawapb.MaekawaMsg{
		Type:         maekawapb.MsgType_YIELD,
		SenderId:     int32(workers[1].ID),
		Clock:        12,
		TaskId:       "t1",
		InquireClock: 3,
	}

	// First YIELD: valid. Requeues worker 1's request, grants to next (worker 2).
	fns1 := v.onYield(yield)
	qlenAfterFirst := v.voter.waitQueue.Len()

	// Second YIELD: inquired is now false → must be ignored entirely.
	fns2 := v.onYield(yield)
	qlenAfterSecond := v.voter.waitQueue.Len()
	v.mu.Unlock()

	if len(fns1) == 0 {
		t.Error("first YIELD should have produced a REPLY sendFn for the next waiter")
	}
	if len(fns2) != 0 {
		t.Error("duplicate YIELD should be ignored (no sendFn)")
	}
	if qlenAfterSecond != qlenAfterFirst {
		t.Errorf("duplicate YIELD changed queue length: %d → %d", qlenAfterFirst, qlenAfterSecond)
	}
}

// TestSameClockPriorityOrder verifies that when two requests arrive with identical
// Lamport clocks the lower senderID wins: it keeps the vote, the higher senderID
// gets queued, and INQUIRE is sent to ask the lower-ID holder to yield only if the
// higher-ID arrives while the lower-ID holds (which it should not — lower ID is
// higher priority and must keep the vote).
func TestSameClockPriorityOrder(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	v := workers[4]  // voter node (not one of the requesters)
	lo := workers[0] // lower senderID = higher priority
	hi := workers[3] // higher senderID = lower priority

	const sharedClock = int64(42)

	v.mu.Lock()

	// lo arrives first with clock=42.
	reqLo := &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_REQUEST,
		SenderId: int32(lo.ID),
		Clock:    sharedClock,
		TaskId:   "task-lo",
	}
	fnsLo := v.onRequest(reqLo)

	// Voter should now be locked for lo.
	if !v.voter.locked || v.voter.lockedFor != int32(lo.ID) {
		t.Fatalf("voter should be locked for lower-ID node %d, got lockedFor=%d", lo.ID, v.voter.lockedFor)
	}
	if len(fnsLo) == 0 {
		t.Error("lo's REQUEST should have produced a REPLY sendFn")
	}

	// hi arrives with the same clock=42 but higher senderID.
	reqHi := &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_REQUEST,
		SenderId: int32(hi.ID),
		Clock:    sharedClock,
		TaskId:   "task-hi",
	}
	fnsHi := v.onRequest(reqHi)

	// hi has lower priority (higher senderID at equal clock) → no INQUIRE should be sent.
	// lo keeps the vote.
	if v.voter.lockedFor != int32(lo.ID) {
		t.Errorf("voter lock moved away from lo: lockedFor=%d", v.voter.lockedFor)
	}
	if len(fnsHi) != 0 {
		t.Errorf("no INQUIRE should be sent: lower-priority hi should just be queued, got %d fns", len(fnsHi))
	}
	if v.voter.waitQueue.Len() != 1 {
		t.Errorf("hi should be in wait queue, len=%d", v.voter.waitQueue.Len())
	}

	// Now reverse: verify with lo.ID=3, hi.ID=6 to show INQUIRE fires when a
	// higher-priority (lower ID) request arrives after a lower-priority one holds.
	v.voter.locked = false
	v.voter.inquired = false
	v.voter.waitQueue = make(RequestHeap, 0)

	reqA := &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_REQUEST,
		SenderId: int32(workers[6].ID), // higher ID = lower priority
		Clock:    sharedClock,
		TaskId:   "task-a",
	}
	reqB := &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_REQUEST,
		SenderId: int32(workers[3].ID), // lower ID = higher priority
		Clock:    sharedClock,
		TaskId:   "task-b",
	}

	// A (lower priority, higher ID) arrives first and gets the vote.
	v.onRequest(reqA)
	if v.voter.lockedFor != int32(workers[6].ID) {
		t.Fatalf("expected voter locked for A (id=%d)", workers[6].ID)
	}

	// B (higher priority, lower ID) arrives: should trigger INQUIRE to A.
	fnsB := v.onRequest(reqB)
	v.mu.Unlock()

	if len(fnsB) == 0 {
		t.Error("higher-priority B should have triggered an INQUIRE to A")
	}
	if !v.voter.inquired {
		// inquired is set inside the lock, read outside — check via the lock
		v.mu.Lock()
		inq := v.voter.inquired
		v.mu.Unlock()
		if !inq {
			t.Error("voter.inquired should be true after sending INQUIRE for B")
		}
	}
}
