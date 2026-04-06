//go:build ignore
package maekawa

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"raft-maekawa-sync/api/maekawa"
)

// Test 9: NotifyWorkerDown marks a worker dead; NotifyWorkerUp revives it.
// Verifies liveWorkers map is updated correctly.
func TestNotifyWorkerDownUp(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w := workers[0]

	// initially all workers are live
	w.mu.Lock()
	lq := w.liveQuorum()
	w.mu.Unlock()
	if len(lq) != len(w.Quorum) {
		t.Fatalf("expected full quorum (%d), got %d", len(w.Quorum), len(lq))
	}

	// mark a quorum peer down (pick first non-self quorum member)
	var victim int
	for _, id := range w.Quorum {
		if id != w.ID {
			victim = id
			break
		}
	}
	w.NotifyWorkerDown(victim)

	w.mu.Lock()
	lqAfterDown := w.liveQuorum()
	w.mu.Unlock()
	if len(lqAfterDown) != len(w.Quorum)-1 {
		t.Errorf("expected quorum size %d after down, got %d", len(w.Quorum)-1, len(lqAfterDown))
	}
	for _, id := range lqAfterDown {
		if id == victim {
			t.Errorf("victim %d still in live quorum after NotifyWorkerDown", victim)
		}
	}

	// revive the worker
	w.NotifyWorkerUp(victim)

	w.mu.Lock()
	lqAfterUp := w.liveQuorum()
	w.mu.Unlock()
	if len(lqAfterUp) != len(w.Quorum) {
		t.Errorf("expected full quorum (%d) after up, got %d", len(w.Quorum), len(lqAfterUp))
	}
}

// Test 10: RequestCS returns an error immediately when live quorum is below minQuorumSize.
// Simulates enough quorum members going down to drop below threshold.
func TestRequestCSFailsWhenQuorumTooSmall(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w := workers[0]
	// W0's quorum = {0,1,2,3,6} (row0 ∪ col0). Kill 3 non-self peers → live = 2 < minQuorumSize(3).
	killed := 0
	for _, id := range w.Quorum {
		if id == w.ID {
			continue
		}
		w.NotifyWorkerDown(id)
		killed++
		if killed == 3 {
			break
		}
	}

	task := makeTask(0, 99)
	err := w.RequestCS(context.Background(), task)
	if err == nil {
		w.ReleaseCS()
		t.Fatal("expected RequestCS to fail with too-small quorum, but it succeeded")
	}
	t.Logf("got expected error: %v", err)
}

// Test 11: one worker down — workers whose live quorum drops below minQuorumSize
// correctly refuse CS. Workers with sufficient live quorum proceed safely.
//
// W8 (bottom-right corner) is stopped. Its quorum = {2,5,6,7,8}.
// Workers whose quorum does NOT include W8 are unaffected and must still
// satisfy mutual exclusion among themselves.
// Workers whose quorum DOES include W8 will have a smaller live quorum;
// if it drops below minQuorumSize they must return an error.
func TestMutualExclusionWithOneWorkerDown(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	// bring W8 (bottom-right) down — quorum {2,5,6,7,8}
	// W8 is only in quorums of: W2 (col2), W5 (col2), W6 (row2), W7 (row2), W8 (self)
	workers[8].Stop()
	for i := 0; i < 8; i++ {
		workers[i].NotifyWorkerDown(8)
	}

	var csCount int64
	var maxCS int64
	var wg sync.WaitGroup
	var errCount int64

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(w *Worker) {
			defer wg.Done()
			task := makeTask(w.ID, 0)
			if err := w.RequestCS(context.Background(), task); err != nil {
				// acceptable: live quorum dropped below minimum for this worker
				atomic.AddInt64(&errCount, 1)
				t.Logf("W%d refused CS (live quorum too small): %v", w.ID, err)
				return
			}
			cur := atomic.AddInt64(&csCount, 1)
			for {
				old := atomic.LoadInt64(&maxCS)
				if cur <= old || atomic.CompareAndSwapInt64(&maxCS, old, cur) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt64(&csCount, -1)
			w.ReleaseCS()
		}(workers[i])
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("workers did not complete within 30s after one node down")
	}

	if maxCS > 1 {
		t.Errorf("mutual exclusion violated: max concurrent CS = %d", maxCS)
	}
	t.Logf("completed: %d workers refused CS, %d entered CS safely", errCount, maxCS)
}

// Test 13: quorum member dies mid-flight — RequestCS returns an error, not deadlock.
// We stop the victim's gRPC server before RequestCS so the vote never arrives,
// guaranteeing W0 is blocked waiting when NotifyWorkerDown fires.
func TestRequestCSCancelledByWorkerDown(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]
	// W0 quorum = {0,1,2,3,6}; kill W1's server so its REPLY never arrives
	victim := 1
	workers[victim].Stop() // gRPC server down — W0's REQUEST to W1 will fail/hang

	task := makeTask(0, 0)
	result := make(chan error, 1)
	go func() { result <- w0.RequestCS(context.Background(), task) }()

	// W0 is now definitely blocked: W1 is dead, vote will never arrive
	time.Sleep(100 * time.Millisecond)

	// simulate Raft committing EventWorkerDown for victim
	w0.NotifyWorkerDown(victim)

	select {
	case err := <-result:
		if err == nil {
			w0.ReleaseCS()
			t.Fatal("expected RequestCS to be cancelled, but it succeeded")
		}
		t.Logf("correctly cancelled: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("RequestCS did not return within 5s after quorum member died (deadlock?)")
	}
}

// Test 14: mid-flight failure + ReleaseCS is safe — calling ReleaseCS after a
// cancelled RequestCS must not panic or send stale RELEASEs.
func TestReleaseSafeAfterCancelledRequestCS(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]
	victim := 1
	workers[victim].Stop()

	task := makeTask(0, 0)
	result := make(chan error, 1)
	go func() { result <- w0.RequestCS(context.Background(), task) }()

	time.Sleep(100 * time.Millisecond)
	w0.NotifyWorkerDown(victim)

	err := <-result
	if err == nil {
		w0.ReleaseCS()
		t.Fatal("expected cancelled RequestCS, got success")
	}

	// must not panic — ReleaseCS on a cancelled request is a no-op
	w0.ReleaseCS()
}

// Test 15: worker that was never in the quorum dying has no effect on RequestCS.
// W0's quorum = {0,1,2,3,6}. Killing W5 (not in W0's quorum) must not cancel W0's request.
func TestWorkerDownOutsideQuorumHasNoEffect(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]
	outsider := 5 // W5 is NOT in W0's quorum {0,1,2,3,6}

	task := makeTask(0, 0)
	result := make(chan error, 1)
	go func() { result <- w0.RequestCS(context.Background(), task) }()

	time.Sleep(50 * time.Millisecond)
	// killing an outsider must not affect W0
	w0.NotifyWorkerDown(outsider)

	select {
	case err := <-result:
		if err != nil {
			t.Errorf("RequestCS failed unexpectedly after outsider died: %v", err)
			return
		}
		w0.ReleaseCS()
	case <-time.After(10 * time.Second):
		t.Fatal("RequestCS timed out — outsider death should have had no effect")
	}
}

// Test 16: concurrent NotifyWorkerDown calls — multiple Raft nodes calling
// NotifyWorkerDown simultaneously must not cause a double-close panic or
// corrupt state. Run with -race to catch data races.
func TestConcurrentNotifyWorkerDown(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]

	// fire 8 concurrent NotifyWorkerDown calls for different workers
	var wg sync.WaitGroup
	for id := 1; id <= 8; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w0.NotifyWorkerDown(id)
		}(id)
	}
	wg.Wait()

	// all peers marked down — RequestCS must fail cleanly, not panic
	task := makeTask(0, 0)
	err := w0.RequestCS(context.Background(), task)
	if err == nil {
		w0.ReleaseCS()
		t.Fatal("expected error with all peers down, got success")
	}
	t.Logf("correctly refused: %v", err)
}

// Test 17: NotifyWorkerDown races with onReply — vote arrives at the exact same
// moment the voter is declared dead. Only one of the two outcomes is valid:
// either RequestCS succeeds (vote arrived first) or returns an error (death wins).
// Must never deadlock and must never violate mutual exclusion.
func TestNotifyWorkerDownRacesWithReply(t *testing.T) {
	const iterations = 20
	for i := 0; i < iterations; i++ {
		workers, _ := startWorkers(t, 9)

		w0 := workers[0]
		victim := 1 // in W0's quorum

		task := makeTask(0, i)
		result := make(chan error, 1)
		go func() { result <- w0.RequestCS(context.Background(), task) }()

		// fire NotifyWorkerDown with no sleep — maximum race with incoming REPLYs
		w0.NotifyWorkerDown(victim)

		select {
		case err := <-result:
			if err == nil {
				// succeeded — must not be in CS simultaneously with anyone else
				w0.ReleaseCS()
			}
			// error is also fine — victim died before/during vote collection
		case <-time.After(5 * time.Second):
			stopWorkers(workers)
			t.Fatalf("iteration %d: RequestCS deadlocked", i)
		}

		stopWorkers(workers)
	}
}

// Test 18: CS holder dies — voters that had granted their vote to the dead worker
// must force-release and grant to the next waiter. Without force-release, the
// next requester would block forever waiting for a REPLY that never comes.
//
// W0 holds CS (quorum {0,1,2,3,6}). W4 requests CS (quorum {1,3,4,5,7}).
// W0 and W4 share voters {1,3}. W4 is queued at those voters behind W0.
// W0 dies → voters W1,W3 force-release → W4 should eventually win CS.
// W0 is NOT in W4's quorum so NotifyWorkerDown(0) does not cancel W4's request.
func TestCSHolderDiesVotersForceRelease(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	// W0 enters CS
	task0 := makeTask(0, 0)
	if err := workers[0].RequestCS(context.Background(), task0); err != nil {
		t.Fatalf("W0 RequestCS: %v", err)
	}

	// W4 requests CS — queues at shared voters {1,3}
	task4 := makeTask(4, 0)
	w4result := make(chan error, 1)
	go func() { w4result <- workers[4].RequestCS(context.Background(), task4) }()

	// give W4 time to send REQUESTs and queue at voters
	time.Sleep(100 * time.Millisecond)

	// W0 dies while holding CS
	workers[0].Stop()
	for i := 1; i < 9; i++ {
		workers[i].NotifyWorkerDown(0)
	}

	// W4 must win CS — voters W1,W3 force-released their lock from W0
	select {
	case err := <-w4result:
		if err != nil {
			t.Fatalf("W4 RequestCS failed after W0 (CS holder) died: %v", err)
		}
		workers[4].ReleaseCS()
		t.Log("W4 correctly won CS after dead CS holder was force-released")
	case <-time.After(10 * time.Second):
		t.Fatal("W4 blocked forever — force-release did not unblock voters (deadlock)")
	}
}

// Test 19: CS holder dies with no waiters — force-release is a no-op (empty queue),
// next RequestCS from any worker proceeds normally.
func TestCSHolderDiesNoWaiters(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	// W0 enters CS alone (no contention)
	task0 := makeTask(0, 0)
	if err := workers[0].RequestCS(context.Background(), task0); err != nil {
		t.Fatalf("W0 RequestCS: %v", err)
	}

	// W0 dies while holding CS, no one is waiting
	workers[0].Stop()
	for i := 1; i < 9; i++ {
		workers[i].NotifyWorkerDown(0)
	}

	// W1 should be able to request CS normally afterwards
	// (W0 not in W1's quorum {0,1,2,4,7} — wait, W0 IS in W1's quorum)
	// W1's quorum = {0,1,2,4,7}: W0 is in it → W1 will refuse (live quorum < 5)
	// Use W4 instead: quorum = {1,4,7,3,6} — W0 not in W4's quorum
	task4 := makeTask(4, 0)
	if err := workers[4].RequestCS(context.Background(), task4); err != nil {
		t.Fatalf("W4 RequestCS failed after W0 died (W0 not in W4 quorum): %v", err)
	}
	workers[4].ReleaseCS()
	t.Log("W4 correctly won CS after dead CS holder with no waiters")
}

// Test 20: mutual exclusion holds after force-release — two workers compete
// after the CS holder dies; only one wins at a time.
func TestMutualExclusionAfterForceRelease(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	// W0 enters CS
	task0 := makeTask(0, 0)
	if err := workers[0].RequestCS(context.Background(), task0); err != nil {
		t.Fatalf("W0 RequestCS: %v", err)
	}

	// W3 and W6 both start requesting (they don't include W0 in quorum overlap with each other)
	// Actually use workers whose quorums don't include W0: W4={1,3,4,5,7}, W8={2,5,6,7,8}
	var csCount int64
	var maxCS int64
	var wg sync.WaitGroup

	for _, id := range []int{3, 4} {
		wg.Add(1)
		go func(w *Worker) {
			defer wg.Done()
			task := makeTask(w.ID, 0)
			if err := w.RequestCS(context.Background(), task); err != nil {
				t.Logf("W%d refused (expected if quorum affected): %v", w.ID, err)
				return
			}
			cur := atomic.AddInt64(&csCount, 1)
			for {
				old := atomic.LoadInt64(&maxCS)
				if cur <= old || atomic.CompareAndSwapInt64(&maxCS, old, cur) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt64(&csCount, -1)
			w.ReleaseCS()
		}(workers[id])
	}

	time.Sleep(100 * time.Millisecond)

	// W0 dies holding CS — trigger force-release
	workers[0].Stop()
	for i := 1; i < 9; i++ {
		workers[i].NotifyWorkerDown(0)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("workers did not complete after CS holder died")
	}

	if maxCS > 1 {
		t.Errorf("mutual exclusion violated after force-release: max concurrent CS = %d", maxCS)
	}
}

// Test 12: worker revived mid-run — after NotifyWorkerUp it participates again.
func TestWorkerRevived(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	// mark W1 down, then immediately revive it — net effect: still live
	workers[0].NotifyWorkerDown(1)
	workers[0].NotifyWorkerUp(1)

	// W0 should still be able to enter CS normally
	task := makeTask(0, 0)
	done := make(chan struct{})
	go func() {
		if err := workers[0].RequestCS(context.Background(), task); err != nil {
			t.Errorf("RequestCS after revive: %v", err)
		}
		workers[0].ReleaseCS()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("W0 did not enter CS within 10s after peer revived")
	}
}

// Test 21: stale INQUIRE from an old request round is ignored.
// Ensures old delayed messages cannot steal a vote from the current round.
func TestStaleInquireIgnored(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w := workers[0]

	w.mu.Lock()
	w.state = StateWanting
	w.replyCount = 1         // still waiting, so shouldYield() would be true
	w.currentReqClock = 1000 // current round

	msg := &maekawapb.MaekawaMsg{
		Type:         maekawapb.MsgType_INQUIRE,
		SenderId:     1,
		TaskId:       "task-stale-inquire",
		InquireClock: 999, // stale round
	}
	out := w.onInquire(msg)
	replies := w.replyCount
	w.mu.Unlock()

	if len(out) != 0 {
		t.Fatalf("expected stale INQUIRE to produce no outgoing messages, got %d", len(out))
	}
	if replies != 1 {
		t.Fatalf("expected replyCount to stay 1 for stale INQUIRE, got %d", replies)
	}
}

// Test 22: duplicate NotifyWorkerDown for the same worker is idempotent.
// Simulates Raft retries / duplicate apply notifications.
func TestDuplicateNotifyWorkerDownIdempotent(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]
	victim := 1 // in W0 quorum
	workers[victim].Stop()

	task := makeTask(0, 22)
	result := make(chan error, 1)
	go func() { result <- w0.RequestCS(context.Background(), task) }()

	time.Sleep(100 * time.Millisecond)

	// Duplicate down event for the same worker should be safe (no panic/double-close)
	w0.NotifyWorkerDown(victim)
	w0.NotifyWorkerDown(victim)

	select {
	case err := <-result:
		if err == nil {
			w0.ReleaseCS()
			t.Fatal("expected RequestCS cancellation after duplicate NotifyWorkerDown, got success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RequestCS did not return after duplicate NotifyWorkerDown")
	}
}

// Test 23: simulated Raft commit propagation for down/up reaches every worker.
// We broadcast NotifyWorkerDown/Up to all workers and verify local liveness state converges.
func TestBroadcastNotifyWorkerDownUpConsistency(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	victim := 8

	// Simulate committed EventWorkerDown applied on every node.
	for _, w := range workers {
		w.NotifyWorkerDown(victim)
	}

	for i, w := range workers {
		w.mu.Lock()
		alive := w.liveWorkers[victim]
		lq := w.liveQuorum()
		w.mu.Unlock()

		if alive {
			t.Fatalf("worker %d still marks victim %d alive after broadcast down", i, victim)
		}
		for _, id := range lq {
			if id == victim {
				t.Fatalf("worker %d still includes victim %d in live quorum after broadcast down", i, victim)
			}
		}
	}

	// Simulate committed EventWorkerUp applied on every node.
	for _, w := range workers {
		w.NotifyWorkerUp(victim)
	}

	for i, w := range workers {
		w.mu.Lock()
		alive := w.liveWorkers[victim]
		w.mu.Unlock()
		if !alive {
			t.Fatalf("worker %d still marks victim %d down after broadcast up", i, victim)
		}
	}
}

// Test 31: RequestCS context timeout — returns context.DeadlineExceeded, state resets.
func TestRequestCSContextTimeout(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]
	victim := 1
	// Stop victim so W0 can never collect all votes.
	workers[victim].Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	task := makeTask(0, 31)
	err := w0.RequestCS(ctx, task)
	if err == nil {
		w0.ReleaseCS()
		t.Fatal("expected timeout error, got success")
	}
	if err != context.DeadlineExceeded {
		t.Errorf("expected context.DeadlineExceeded, got: %v", err)
	}

	// State must be reset — a subsequent request (with victim back) should work.
	w0.mu.Lock()
	state := w0.state
	task0 := w0.currentTask
	w0.mu.Unlock()

	if state != StateReleased {
		t.Errorf("expected StateReleased after timeout, got %v", state)
	}
	if task0 != nil {
		t.Errorf("expected currentTask nil after timeout, got %v", task0)
	}
}

// Test 32: RequestCS context cancelled — returns context.Canceled, state resets.
func TestRequestCSContextCancelled(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]
	workers[1].Stop() // block votes so RequestCS hangs

	ctx, cancel := context.WithCancel(context.Background())
	task := makeTask(0, 32)
	result := make(chan error, 1)
	go func() { result <- w0.RequestCS(ctx, task) }()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-result:
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RequestCS did not return after context cancel")
	}

	w0.mu.Lock()
	state := w0.state
	w0.mu.Unlock()
	if state != StateReleased {
		t.Errorf("expected StateReleased after cancel, got %v", state)
	}
}

// Test 33: FAILED message cancels a pending RequestCS.
func TestFailedMessageCancelsRequestCS(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]
	workers[1].Stop() // prevent votes arriving so W0 stays in StateWanting

	task := makeTask(0, 33)
	result := make(chan error, 1)
	go func() { result <- w0.RequestCS(context.Background(), task) }()

	time.Sleep(100 * time.Millisecond)

	// inject a FAILED message directly as if a voter is going down
	w0.HandleMessage(&maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_FAILED,
		SenderId: 1,
		Clock:    999,
		TaskId:   task.ID,
	})

	select {
	case err := <-result:
		if err == nil {
			w0.ReleaseCS()
			t.Fatal("expected RequestCS to be cancelled by FAILED, got success")
		}
		t.Logf("correctly cancelled by FAILED: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("RequestCS did not return after FAILED message")
	}
}

// Test 34: FAILED ignored when not in StateWanting.
func TestFailedIgnoredWhenNotWanting(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]

	// inject FAILED while in StateReleased — must be a no-op
	w0.HandleMessage(&maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_FAILED,
		SenderId: 1,
		Clock:    1,
		TaskId:   "irrelevant",
	})

	w0.mu.Lock()
	state := w0.state
	w0.mu.Unlock()
	if state != StateReleased {
		t.Errorf("FAILED while Released should be no-op, state is now %v", state)
	}

	// also verify it doesn't break a subsequent normal RequestCS
	task := makeTask(0, 34)
	if err := w0.RequestCS(context.Background(), task); err != nil {
		t.Fatalf("RequestCS after ignored FAILED: %v", err)
	}
	w0.ReleaseCS()
}
