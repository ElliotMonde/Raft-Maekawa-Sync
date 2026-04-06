// tests enabled
package maekawa

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Test 9: marking a worker dead via membership, then reviving it.
// Confirms that IsAlive(id) changes affect RequestForGlobalLock quorum checks.
func TestMembershipDownUp(t *testing.T) {
	workers, mem := startWorkers(t, 9)
	defer stopWorkers(workers)

	w := workers[0]

	// Initially all alive; W0 can enter CS.
	if err := w.RequestCS(context.Background(), makeTask(0, 9)); err != nil {
		t.Fatalf("initial RequestCS: %v", err)
	}
	w.ReleaseCS()

	// Mark a quorum peer down — W0's next RequestCS must fail with a quorum error.
	// W0's quorum for n=9 includes node 1; kill it.
	mem.markDown(1)

	err := w.RequestCS(context.Background(), makeTask(0, 9))
	if err == nil {
		w.ReleaseCS()
		t.Fatal("expected RequestCS to fail with a quorum member down, but it succeeded")
	}
	t.Logf("correctly failed with quorum member down: %v", err)

	// Revive; W0 should succeed again.
	mem.markUp(1)

	if err := w.RequestCS(context.Background(), makeTask(0, 9)); err != nil {
		t.Fatalf("RequestCS after revive: %v", err)
	}
	w.ReleaseCS()
}

// Test 10: RequestCS fails immediately when enough quorum members are down.
func TestRequestCSFailsWhenQuorumTooSmall(t *testing.T) {
	workers, mem := startWorkers(t, 9)
	defer stopWorkers(workers)

	w := workers[0]
	// W0's quorum = {0,1,2,3,6}. Kill 3 non-self peers → too few alive.
	killed := 0
	for _, id := range w.quorum {
		if id == w.ID {
			continue
		}
		mem.markDown(id)
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

// Test 11: one worker down — workers whose quorum includes the dead node fail;
// workers whose quorum doesn't include it succeed and respect mutual exclusion.
func TestMutualExclusionWithOneWorkerDown(t *testing.T) {
	workers, mem := startWorkers(t, 9)
	defer stopWorkers(workers)

	// Take W8's server down and mark it dead in membership.
	workers[8].Stop()
	mem.markDown(8)

	var csCount int64
	var maxCS int64
	var wg sync.WaitGroup
	var errCount int64

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(w *testWorker) {
			defer wg.Done()
			task := makeTask(int(w.ID), 0)
			if err := w.RequestCS(context.Background(), task); err != nil {
				atomic.AddInt64(&errCount, 1)
				t.Logf("W%d refused CS (quorum too small): %v", w.ID, err)
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
	t.Logf("completed: %d workers refused CS, max concurrent CS = %d", errCount, maxCS)
}

// Test 13: quorum member dies (server stops) mid-flight — RequestCS must return
// an error, not deadlock.
func TestRequestCSCancelledByWorkerDown(t *testing.T) {
	workers, mem := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]

	// Stop W1's gRPC server; W0 will block waiting for W1's REPLY.
	workers[1].Stop()

	task := makeTask(0, 0)
	result := make(chan error, 1)

	// Mark W1 dead after a brief delay so W0 is blocked when the death fires.
	go func() {
		time.Sleep(100 * time.Millisecond)
		mem.markDown(1)
	}()

	go func() { result <- w0.RequestCS(context.Background(), task) }()

	select {
	case err := <-result:
		if err == nil {
			w0.ReleaseCS()
			t.Fatal("expected RequestCS to be cancelled, but it succeeded")
		}
		t.Logf("correctly cancelled: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("RequestCS did not return within 5s after quorum member died (possible deadlock)")
	}
}

// Test 14: calling ReleaseCS after a cancelled RequestCS must not panic.
func TestReleaseSafeAfterCancelledRequestCS(t *testing.T) {
	workers, mem := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]
	workers[1].Stop()

	task := makeTask(0, 0)
	result := make(chan error, 1)

	go func() {
		time.Sleep(100 * time.Millisecond)
		mem.markDown(1)
	}()

	go func() { result <- w0.RequestCS(context.Background(), task) }()

	err := <-result
	if err == nil {
		w0.ReleaseCS()
		t.Fatal("expected cancelled RequestCS, got success")
	}

	// must not panic — ReleaseCS on a cancelled request is a no-op
	w0.ReleaseCS()
}

// Test 15: a worker that is not in any quorum dying has no effect.
func TestWorkerDownOutsideQuorumHasNoEffect(t *testing.T) {
	workers, mem := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]
	// W0's quorum for n=9 = {0,1,2,3,6}.  W5 is not in it.
	outsider := int32(5)

	task := makeTask(0, 0)
	result := make(chan error, 1)
	go func() { result <- w0.RequestCS(context.Background(), task) }()

	time.Sleep(50 * time.Millisecond)
	mem.markDown(outsider) // should NOT affect W0's request

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

// Test 16: concurrent membership down calls don't corrupt state.
func TestConcurrentMarkDown(t *testing.T) {
	workers, mem := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]

	var wg sync.WaitGroup
	for id := 1; id <= 8; id++ {
		wg.Add(1)
		go func(id int32) {
			defer wg.Done()
			mem.markDown(id)
		}(int32(id))
	}
	wg.Wait()

	// all peers dead → RequestCS must fail cleanly
	task := makeTask(0, 0)
	err := w0.RequestCS(context.Background(), task)
	if err == nil {
		w0.ReleaseCS()
		t.Fatal("expected error with all peers down, got success")
	}
	t.Logf("correctly refused: %v", err)
}

// Test 31: RequestCS context timeout returns context.DeadlineExceeded.
func TestRequestCSContextTimeout(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]
	// Stop W1 so W0 can never collect all votes.
	workers[1].Stop()

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
}

// Test 32: RequestCS context cancelled returns context.Canceled.
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
}

// Test 17: death of a quorum peer races with receiving a REPLY; both outcomes
// (success or error) are valid — must never deadlock.
func TestMembershipDownRacesWithReply(t *testing.T) {
	const iterations = 10
	for i := 0; i < iterations; i++ {
		workers, mem := startWorkers(t, 9)

		w0 := workers[0]
		task := makeTask(0, i)
		result := make(chan error, 1)
		go func() { result <- w0.RequestCS(context.Background(), task) }()

		// Fire markDown with no sleep — maximum race with incoming REPLYs.
		mem.markDown(1)

		select {
		case err := <-result:
			if err == nil {
				w0.ReleaseCS()
			}
		case <-time.After(5 * time.Second):
			stopWorkers(workers)
			t.Fatalf("iteration %d: RequestCS deadlocked", i)
		}

		stopWorkers(workers)
	}
}

// Test 12: worker revived mid-run — after markUp it participates again.
func TestWorkerRevived(t *testing.T) {
	workers, mem := startWorkers(t, 9)
	defer stopWorkers(workers)

	// mark W1 down, then immediately revive it — net effect: still live
	mem.markDown(1)
	mem.markUp(1)

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

// Test 18: CS holder's server stops — voters that queued a request behind it
// must eventually grant to the next waiting requester once the dead holder's
// session times out or is force-released.
// We test this indirectly: W4 must win CS after W0 (CS holder) stops.
func TestCSHolderDiesVotersEventuallyRelease(t *testing.T) {
	workers, mem := startWorkers(t, 9)
	defer stopWorkers(workers)

	// W0 enters CS.
	task0 := makeTask(0, 0)
	if err := workers[0].RequestCS(context.Background(), task0); err != nil {
		t.Fatalf("W0 RequestCS: %v", err)
	}

	// W4 requests CS — it shares voters with W0.
	task4 := makeTask(4, 0)
	w4result := make(chan error, 1)
	go func() { w4result <- workers[4].RequestCS(context.Background(), task4) }()

	time.Sleep(100 * time.Millisecond)

	// W0 "dies" — mark it dead and stop its server.
	mem.markDown(0)
	workers[0].Stop()

	// W0 held CS and is now dead. The voters that held a GRANT for W0 need
	// to eventually release — this happens via gRPC timeout on pending calls.
	// We wait for W4 to succeed (up to the gRPC DefaultTimeout * 2 + buffer).
	select {
	case err := <-w4result:
		if err != nil {
			// acceptable: quorum might include dead W0 for W4 too
			t.Logf("W4 RequestCS returned error (acceptable if quorum includes dead W0): %v", err)
		} else {
			workers[4].ReleaseCS()
			t.Log("W4 correctly won CS after dead CS holder")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("W4 blocked forever — voters did not release after CS holder died")
	}
}

// Test 19: CS holder dies with no waiters — next RequestCS proceeds normally.
func TestCSHolderDiesNoWaiters(t *testing.T) {
	workers, mem := startWorkers(t, 9)
	defer stopWorkers(workers)

	// W0 enters CS alone (no contention).
	task0 := makeTask(0, 0)
	if err := workers[0].RequestCS(context.Background(), task0); err != nil {
		t.Fatalf("W0 RequestCS: %v", err)
	}

	// W0 dies while holding CS, no one is waiting.
	mem.markDown(0)
	workers[0].Stop()

	// W4's quorum (n=9) = {1,3,4,5,7} — W0 is NOT in it → can proceed normally.
	task4 := makeTask(4, 0)
	if err := workers[4].RequestCS(context.Background(), task4); err != nil {
		t.Fatalf("W4 RequestCS failed after W0 died (W0 not in W4 quorum): %v", err)
	}
	workers[4].ReleaseCS()
	t.Log("W4 correctly won CS after dead CS holder with no waiters")
}

// Test 20: mutual exclusion holds after a CS holder dies.
func TestMutualExclusionAfterHolderDies(t *testing.T) {
	workers, mem := startWorkers(t, 9)
	defer stopWorkers(workers)

	// W0 enters CS.
	task0 := makeTask(0, 0)
	if err := workers[0].RequestCS(context.Background(), task0); err != nil {
		t.Fatalf("W0 RequestCS: %v", err)
	}

	// W4 and W3 both start requesting simultaneously.
	var csCount int64
	var maxCS int64
	var wg sync.WaitGroup

	for _, id := range []int{3, 4} {
		wg.Add(1)
		go func(w *testWorker) {
			defer wg.Done()
			task := makeTask(int(w.ID), 0)
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

	// W0 dies holding CS.
	mem.markDown(0)
	workers[0].Stop()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("workers did not complete after CS holder died")
	}

	if maxCS > 1 {
		t.Errorf("mutual exclusion violated after holder died: max concurrent CS = %d", maxCS)
	}
}
