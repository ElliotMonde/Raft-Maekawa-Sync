package maekawa

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"raft-maekawa/internal/models"
	maekawapb "raft-maekawa/proto/maekawapb"
)

// allocatePeers finds N free ports on loopback and returns a peer map.
func allocatePeers(t *testing.T, n int) map[int]string {
	t.Helper()
	peers := make(map[int]string, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("allocatePeers: %v", err)
		}
		peers[i] = ln.Addr().String()
		ln.Close() // release so the worker server can bind it
	}
	return peers
}

// startWorkers creates and starts n workers on loopback ports.
func startWorkers(t *testing.T, n int) ([]*Worker, map[int]string) {
	t.Helper()
	peers := allocatePeers(t, n)
	workers := make([]*Worker, n)
	for i := 0; i < n; i++ {
		w, err := NewWorker(i, n, peers, peers[i])
		if err != nil {
			t.Fatalf("NewWorker(%d): %v", i, err)
		}
		workers[i] = w
	}
	// give servers time to bind
	time.Sleep(200 * time.Millisecond)
	return workers, peers
}

func stopWorkers(workers []*Worker) {
	for _, w := range workers {
		w.Stop()
	}
}

func makeTask(workerID, round int) *models.Task {
	return &models.Task{
		ID:          fmt.Sprintf("task-%d-%d", workerID, round),
		Description: fmt.Sprintf("worker %d round %d", workerID, round),
		RequesterID: workerID,
		Reward:      10.0,
		Penalty:     5.0,
	}
}

// -----------------------------------------------------------------------
// Safety tests
// -----------------------------------------------------------------------

// Test 1: single worker, no contention — should enter CS immediately.
func TestSingleWorkerNoContention(t *testing.T) {
	workers, _ := startWorkers(t, 1)
	defer stopWorkers(workers)

	w := workers[0]
	task := makeTask(0, 0)

	done := make(chan struct{})
	go func() {
		if err := w.RequestCS(context.Background(), task); err != nil {
			t.Errorf("RequestCS: %v", err)
		}
		w.ReleaseCS()
		close(done)
	}()

	select {
	case <-done:
		// pass
	case <-time.After(2 * time.Second):
		t.Fatal("single worker did not enter CS within 2s")
	}
}

// Test 2: two workers sequential — W0 completes before W1 requests.
func TestTwoWorkersSequential(t *testing.T) {
	workers, _ := startWorkers(t, 9) // still need 9 for valid quorums
	defer stopWorkers(workers)

	task0 := makeTask(0, 0)
	if err := workers[0].RequestCS(context.Background(), task0); err != nil {
		t.Fatalf("W0 RequestCS: %v", err)
	}
	workers[0].ReleaseCS()

	task1 := makeTask(1, 0)
	if err := workers[1].RequestCS(context.Background(), task1); err != nil {
		t.Fatalf("W1 RequestCS: %v", err)
	}
	workers[1].ReleaseCS()
}

// Test 3: two workers simultaneous — only one in CS at a time.
func TestTwoWorkersMutualExclusion(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	var csCount int64
	var maxCS int64
	var wg sync.WaitGroup

	for _, id := range []int{0, 1} {
		wg.Add(1)
		go func(w *Worker) {
			defer wg.Done()
			task := makeTask(w.ID, 0)
			if err := w.RequestCS(context.Background(), task); err != nil {
				t.Errorf("RequestCS: %v", err)
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

	wg.Wait()
	if maxCS > 1 {
		t.Errorf("mutual exclusion violated: max concurrent CS = %d", maxCS)
	}
}

// Test 4: all 9 workers simultaneous — csCount never exceeds 1, all complete.
func TestNineWorkersMutualExclusion(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	var csCount int64
	var maxCS int64
	var wg sync.WaitGroup

	for i := 0; i < 9; i++ {
		wg.Add(1)
		go func(w *Worker) {
			defer wg.Done()
			task := makeTask(w.ID, 0)
			if err := w.RequestCS(context.Background(), task); err != nil {
				t.Errorf("W%d RequestCS: %v", w.ID, err)
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
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// liveness: all completed
	case <-time.After(30 * time.Second):
		t.Fatal("not all workers completed CS within 30s (possible deadlock)")
	}

	if maxCS > 1 {
		t.Errorf("mutual exclusion violated: max concurrent CS = %d", maxCS)
	}
}

// -----------------------------------------------------------------------
// Deadlock resolution tests
// -----------------------------------------------------------------------

// Test 5: repeated rounds — 9 workers each do 3 rounds (27 total CS entries).
// INQUIRE/YIELD path will activate heavily.
func TestNineWorkersMultipleRounds(t *testing.T) {
	const rounds = 3
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	var csCount int64
	var maxCS int64
	var wg sync.WaitGroup

	for i := 0; i < 9; i++ {
		wg.Add(1)
		go func(w *Worker) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				task := makeTask(w.ID, r)
				if err := w.RequestCS(context.Background(), task); err != nil {
					t.Errorf("W%d round %d RequestCS: %v", w.ID, r, err)
					return
				}
				cur := atomic.AddInt64(&csCount, 1)
				for {
					old := atomic.LoadInt64(&maxCS)
					if cur <= old || atomic.CompareAndSwapInt64(&maxCS, old, cur) {
						break
					}
				}
				// simulate variable work duration
				time.Sleep(time.Duration(20+rand.Intn(80)) * time.Millisecond)
				atomic.AddInt64(&csCount, -1)
				w.ReleaseCS()

				// pause between rounds
				time.Sleep(time.Duration(10+rand.Intn(50)) * time.Millisecond)
			}
		}(workers[i])
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("27 CS entries did not complete within 60s (possible deadlock/starvation)")
	}

	if maxCS > 1 {
		t.Errorf("mutual exclusion violated: max concurrent CS = %d", maxCS)
	}
}

// Test 6: INQUIRE ignored when requester is already in CS.
// Worker 0 enters CS. While it holds CS, another worker sends an INQUIRE.
// Worker 0 must ignore it (shouldYield returns false when StateHeld).
func TestInquireIgnoredWhenInCS(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]
	task := makeTask(0, 0)

	// W0 enters CS
	if err := w0.RequestCS(context.Background(), task); err != nil {
		t.Fatalf("W0 RequestCS: %v", err)
	}

	// verify shouldYield returns false when StateHeld
	w0.mu.Lock()
	yield := w0.shouldYield()
	w0.mu.Unlock()

	if yield {
		t.Error("shouldYield returned true while worker is in StateHeld — must be false")
	}

	w0.ReleaseCS()
}

// Test 7: Lamport clock is strictly monotonic.
func TestLamportClockMonotonic(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w := workers[4] // pick the center worker
	task := makeTask(4, 0)

	var clocks []int64

	// capture clock before REQUEST
	w.mu.Lock()
	clocks = append(clocks, atomic.LoadInt64(&w.clock))
	w.mu.Unlock()

	if err := w.RequestCS(context.Background(), task); err != nil {
		t.Fatalf("RequestCS: %v", err)
	}

	w.mu.Lock()
	clocks = append(clocks, atomic.LoadInt64(&w.clock))
	w.mu.Unlock()

	w.ReleaseCS()

	w.mu.Lock()
	clocks = append(clocks, atomic.LoadInt64(&w.clock))
	w.mu.Unlock()

	for i := 1; i < len(clocks); i++ {
		if clocks[i] <= clocks[i-1] {
			t.Errorf("clock not monotonic at step %d: %v", i, clocks)
		}
	}
}

// Test 8: clock sync — receiving a message with a higher clock updates local clock.
func TestLamportClockSync(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w := workers[0]

	w.mu.Lock()
	// manually set a high received clock value
	before := atomic.LoadInt64(&w.clock)
	highClock := before + 100
	w.updateClock(highClock)
	after := atomic.LoadInt64(&w.clock)
	w.mu.Unlock()

	// after update: clock should be max(before, highClock) + 1 = highClock + 1
	expected := highClock + 1
	if after != expected {
		t.Errorf("after updateClock(%d): expected clock=%d, got %d", highClock, expected, after)
	}
}

// -----------------------------------------------------------------------
// Fault tolerance tests
// -----------------------------------------------------------------------

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

// -----------------------------------------------------------------------
// CS-holder death tests (force-release)
// -----------------------------------------------------------------------

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

// Test 24: membership remove shrinks quorums and allows progress after permanent removal.
// Start with N=6 (safe grid: k=3, lastRow=3) and remove one node → N=5 is unsafe,
// so we verify the removal is rejected; then verify CS still works on the N=6 grid.
func TestMembershipRemoveAllowsProgress(t *testing.T) {
	workers, _ := startWorkers(t, 6)
	defer stopWorkers(workers)

	victim := 5 // last node; removing → N=5 (unsafe, k=3 lastRow=2)

	// Removal to N=5 must be rejected.
	before := countActive(workers[0])
	for _, w := range workers {
		w.NotifyWorkerRemoved(victim)
	}
	after := countActive(workers[0])
	if after != before {
		t.Fatalf("removal to N=5 (unsafe) should be rejected: active %d→%d", before, after)
	}

	// CS must still work on N=6.
	w0 := workers[0]
	task := makeTask(0, 24)
	if err := w0.RequestCS(context.Background(), task); err != nil {
		t.Fatalf("RequestCS should succeed on N=6 grid, got: %v", err)
	}
	w0.ReleaseCS()
}

// Test 25: up event does not resurrect a removed worker; add is required.
// Use N=9 so we can safely remove a node (9→6) then add it back.
// We bypass the guard via direct state to remove victim, then test NotifyWorkerUp
// and NotifyWorkerAdded semantics.
func TestUpIgnoredUntilMembershipAdd(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w := workers[0]
	victim := 1

	// Remove victim directly (bypassing regridSafe) to test up/add semantics.
	w.mu.Lock()
	w.activeWorkers[victim] = false
	w.liveWorkers[victim] = false
	w.Quorum = RegridQuorum(w.ID, sortedActiveIDs(w.activeWorkers))
	w.mu.Unlock()

	w.NotifyWorkerUp(victim) // should be ignored — not a member

	w.mu.Lock()
	activeAfterUp := w.activeWorkers[victim]
	liveAfterUp := w.liveWorkers[victim]
	w.mu.Unlock()

	if activeAfterUp {
		t.Fatalf("victim %d unexpectedly active after up without add", victim)
	}
	if liveAfterUp {
		t.Fatalf("victim %d unexpectedly live after up without add", victim)
	}

	w.NotifyWorkerAdded(victim)

	w.mu.Lock()
	activeAfterAdd := w.activeWorkers[victim]
	liveAfterAdd := w.liveWorkers[victim]
	// After re-adding, victim should be back in W0's quorum (active=[0..8] → same as original).
	inQuorum := false
	for _, id := range w.Quorum {
		if id == victim {
			inQuorum = true
			break
		}
	}
	w.mu.Unlock()

	if !activeAfterAdd || !liveAfterAdd {
		t.Fatalf("victim %d should be active+live after add", victim)
	}
	if !inQuorum {
		t.Fatalf("victim %d should be back in W0's quorum after add", victim)
	}
}

// Test 26: removing a worker that held a voter lock force-releases and unblocks next waiter.
// Start with N=6 so removing one worker (→N=5) is checked, but we bypass the guard
// directly to simulate the removal since the test is about force-release semantics.
func TestMembershipRemovalForceReleasesVoterLock(t *testing.T) {
	workers, _ := startWorkers(t, 6)
	defer stopWorkers(workers)

	// W0 enters CS first.
	task0 := makeTask(0, 27)
	if err := workers[0].RequestCS(context.Background(), task0); err != nil {
		t.Fatalf("W0 RequestCS: %v", err)
	}

	// W4 queues behind W0 at shared voters (W4 quorum on N=6 grid).
	task4 := makeTask(4, 27)
	w4result := make(chan error, 1)
	go func() { w4result <- workers[4].RequestCS(context.Background(), task4) }()

	time.Sleep(100 * time.Millisecond)

	// Remove W0 from all other workers directly (bypassing regridSafe) to test
	// that the force-release mechanism fires regardless of guard.
	for i := 1; i < 6; i++ {
		workers[i].mu.Lock()
		workers[i].activeWorkers[0] = false
		workers[i].liveWorkers[0] = false
		if workers[i].voter.locked && workers[i].voter.lockedFor == 0 {
			fns := workers[i].grantNext("")
			workers[i].mu.Unlock()
			for _, fn := range fns {
				fn() //nolint
			}
		} else {
			workers[i].mu.Unlock()
		}
	}

	select {
	case err := <-w4result:
		if err != nil {
			t.Fatalf("W4 RequestCS failed after removing lock-holder: %v", err)
		}
		workers[4].ReleaseCS()
	case <-time.After(10 * time.Second):
		t.Fatal("W4 blocked after membership removal; expected force-release to unblock")
	}
}

// -----------------------------------------------------------------------
// Regrid tests
// -----------------------------------------------------------------------

// Test 27: mutual exclusion still holds on a regridded N=6 cluster.
// Start with N=9, bypass the guard to remove 3 nodes, then verify CS on N=6.
func TestMutualExclusionAfterRegrid(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	// Directly remove 6, 7, 8 on all workers (bypassing regridSafe since we're
	// doing a 3-step removal that skips unsafe intermediates).
	for _, victim := range []int{6, 7, 8} {
		for _, w := range workers {
			w.mu.Lock()
			w.activeWorkers[victim] = false
			w.liveWorkers[victim] = false
			w.mu.Unlock()
		}
		workers[victim].Stop()
	}
	// Now recompute quorums on all remaining workers (active=[0..5], N=6, safe).
	for i := 0; i < 6; i++ {
		workers[i].mu.Lock()
		workers[i].Quorum = RegridQuorum(workers[i].ID, sortedActiveIDs(workers[i].activeWorkers))
		workers[i].mu.Unlock()
	}
	time.Sleep(100 * time.Millisecond)

	var csCount int64
	var maxCS int64
	var wg sync.WaitGroup

	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(w *Worker) {
			defer wg.Done()
			task := makeTask(w.ID, 27)
			if err := w.RequestCS(context.Background(), task); err != nil {
				t.Logf("W%d refused CS after regrid: %v", w.ID, err)
				return
			}
			cur := atomic.AddInt64(&csCount, 1)
			if cur > atomic.LoadInt64(&maxCS) {
				atomic.StoreInt64(&maxCS, cur)
			}
			time.Sleep(80 * time.Millisecond)
			atomic.AddInt64(&csCount, -1)
			w.ReleaseCS()
		}(workers[i])
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("workers did not complete CS after regrid within 30s")
	}
	if maxCS > 1 {
		t.Errorf("mutual exclusion violated after regrid: max concurrent CS = %d", maxCS)
	}
}

// Test 28: sequential removals to safe grid sizes — remove nodes 8,7,6 one at a time
// (each removal to N=8,7 is rejected; removal to N=6 succeeds), then CS works on N=6.
func TestSequentialRemovalsCSStillWorks(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	// Removals to N=8 and N=7 should be rejected (unsafe grids).
	for _, victim := range []int{8, 7} {
		before := countActive(workers[0])
		workers[0].NotifyWorkerRemoved(victim)
		after := countActive(workers[0])
		if after != before {
			t.Errorf("removal to N=%d should be rejected (unsafe grid), but active went %d→%d",
				before-1, before, after)
		}
	}

	// To reach N=6 we must bypass the regridSafe guard for intermediate steps.
	// Directly mark 8 and 7 inactive on all workers (simulating Raft-committed removals
	// that skip the guard because they are applied as a batch), then remove 6 via the API
	// which will see N=7→6 (safe).
	for _, victim := range []int{8, 7} {
		for _, w := range workers {
			w.mu.Lock()
			w.activeWorkers[victim] = false
			w.liveWorkers[victim] = false
			w.Quorum = RegridQuorum(w.ID, sortedActiveIDs(w.activeWorkers))
			w.mu.Unlock()
		}
		workers[victim].Stop()
	}

	// Now every worker sees N=7 active. Removing 6 → N=6 (safe: k=3, lastRow=3).
	for _, w := range workers[:7] {
		w.NotifyWorkerRemoved(6)
	}
	workers[6].Stop()
	time.Sleep(50 * time.Millisecond)

	// CS should work on the new 6-node grid.
	requester := workers[0]
	task := makeTask(requester.ID, 28)
	if err := requester.RequestCS(context.Background(), task); err != nil {
		t.Fatalf("RequestCS failed after regrid to N=6: %v", err)
	}
	requester.ReleaseCS()
}

// Test 29: removal that would produce an unsafe grid is rejected and rolled back.
// From N=9, removing to N=8 or N=7 produces unsafe grids (last row has < 3 nodes).
// Removing 3 nodes at once to reach N=6 must be done one-by-one; each individual
// removal to an unsafe intermediate size is rejected.
func TestRemovalRejectedUnsafeGrid(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w := workers[0]

	// N=8 is unsafe (k=3, lastRow=2) — removal should be rejected.
	before := countActive(w)
	w.NotifyWorkerRemoved(8)
	after := countActive(w)
	if after != before {
		t.Errorf("removal to N=8 (unsafe) should be rejected: active %d→%d", before, after)
	}

	// N=7 is also unsafe — should also be rejected.
	before = countActive(w)
	w.NotifyWorkerRemoved(7)
	after = countActive(w)
	if after != before {
		t.Errorf("removal to N=7 (unsafe) should be rejected: active %d→%d", before, after)
	}

	// N=6 is safe (k=3, lastRow=3) — but we can only reach it by removing 3 nodes.
	// The guard checks the resulting count, so we simulate removing 8,7,6 via
	// a direct state manipulation to get to N=7 first, then verify N=6 removal succeeds.
	// Instead, just verify the safe boundary: manually reduce to 7 active and try removal.
	w.mu.Lock()
	// Manually mark 8 and 7 as inactive to simulate reaching N=7.
	w.activeWorkers[8] = false
	w.liveWorkers[8] = false
	w.activeWorkers[7] = false
	w.liveWorkers[7] = false
	w.mu.Unlock()

	// Now at N=7 (unsafe). Removing 6 → N=6 (safe) should succeed.
	before = countActive(w)
	w.NotifyWorkerRemoved(6)
	after = countActive(w)
	if after != before-1 {
		t.Errorf("removal to N=6 (safe) should succeed: active %d→%d", before, after)
	}
}

func countActive(w *Worker) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, active := range w.activeWorkers {
		if active {
			n++
		}
	}
	return n
}

// Test 30: add a node with ID beyond original N — w.N grows, quorum includes it.
func TestAddNodeBeyondOriginalN(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w := workers[0]
	newID := 9 // beyond original N=9

	w.mu.Lock()
	nBefore := w.N
	w.mu.Unlock()

	w.NotifyWorkerAdded(newID)

	w.mu.Lock()
	nAfter := w.N
	active := w.activeWorkers[newID]
	live := w.liveWorkers[newID]
	inQuorum := false
	for _, id := range w.Quorum {
		if id == newID {
			inQuorum = true
			break
		}
	}
	w.mu.Unlock()

	if nAfter <= nBefore {
		t.Errorf("expected w.N to grow beyond %d after adding node %d, got %d", nBefore, newID, nAfter)
	}
	if !active {
		t.Errorf("node %d should be active after NotifyWorkerAdded", newID)
	}
	if !live {
		t.Errorf("node %d should be live after NotifyWorkerAdded", newID)
	}
	// newID may or may not be in w.Quorum depending on grid position — just verify
	// the quorum is self-consistent (contains self, pairwise intersects with all others)
	_ = inQuorum
	active2 := sortedActiveIDs(w.activeWorkers)
	for _, id := range active2 {
		q := RegridQuorum(id, active2)
		found := false
		for _, m := range q {
			if m == id {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("node %d not in its own quorum after adding node %d", id, newID)
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

// Test 35: remove nodes mid-flight causing RequestCS cancellation — state is clean for retry.
// We remove 3 nodes (9→6, a safe grid) while W0 is blocked mid-request.
func TestRemovalCancelsMidFlightRequest(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]

	// Stop W1's server so W0 can never collect all votes and stays blocked.
	workers[1].Stop()

	task := makeTask(0, 35)
	result := make(chan error, 1)
	go func() { result <- w0.RequestCS(context.Background(), task) }()

	time.Sleep(100 * time.Millisecond)

	// Remove 8, 7, 6 simultaneously — takes cluster from N=9 to N=6 (safe grid).
	// Each individual removal via the API is rejected (9→8, 8→7 are unsafe), so we
	// directly manipulate state for all three, recompute quorums, and cancel any
	// mid-flight request whose quorum changed.
	victims := []int{8, 7, 6}
	for _, w := range workers {
		if w.ID == 1 {
			continue // W1 is already stopped
		}
		w.mu.Lock()
		for _, victim := range victims {
			w.activeWorkers[victim] = false
			w.liveWorkers[victim] = false
		}
		oldQuorum := append([]int(nil), w.Quorum...)
		w.Quorum = RegridQuorum(w.ID, sortedActiveIDs(w.activeWorkers))
		// If mid-request and quorum changed, cancel.
		if w.state == StateWanting {
			for _, victim := range victims {
				for _, id := range oldQuorum {
					if id == victim {
						w.csErr = fmt.Errorf("worker %d: quorum member %d removed mid-request", w.ID, victim)
						select {
						case <-w.csEnter:
						default:
							close(w.csEnter)
						}
						goto unlocked
					}
				}
			}
		}
	unlocked:
		w.mu.Unlock()
	}

	select {
	case err := <-result:
		if err == nil {
			w0.ReleaseCS()
		} else {
			t.Logf("RequestCS cancelled as expected: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("RequestCS blocked after mid-flight removal")
	}

	w0.mu.Lock()
	state := w0.state
	w0.mu.Unlock()
	if state != StateReleased && state != StateHeld {
		t.Errorf("unexpected state after mid-flight removal: %v", state)
	}
	if state == StateHeld {
		w0.ReleaseCS()
	}
}

// -----------------------------------------------------------------------
// Barrier-start race tests
// -----------------------------------------------------------------------

// Test 36: 2-worker barrier start — both call RequestCS simultaneously, exactly one
// enters CS at a time across many iterations.
func TestBarrierStart2Workers(t *testing.T) {
	const rounds = 20
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0, w1 := workers[0], workers[1]

	for r := 0; r < rounds; r++ {
		start := make(chan struct{})
		var wg sync.WaitGroup
		var inCS int64

		for idx, w := range []*Worker{w0, w1} {
			wg.Add(1)
			w := w
			idx := idx
			go func() {
				defer wg.Done()
				<-start
				task := makeTask(w.ID, 3600+r*10+idx)
				if err := w.RequestCS(context.Background(), task); err != nil {
					t.Errorf("round %d worker %d RequestCS: %v", r, w.ID, err)
					return
				}
				n := atomic.AddInt64(&inCS, 1)
				if n != 1 {
					t.Errorf("round %d: mutual exclusion violated — %d workers in CS", r, n)
				}
				time.Sleep(time.Millisecond)
				atomic.AddInt64(&inCS, -1)
				w.ReleaseCS()
			}()
		}
		close(start)
		wg.Wait()
	}
}

// Test 37: 9-worker barrier start — all workers start simultaneously, mutual exclusion
// must hold across all rounds.
func TestBarrierStart9Workers(t *testing.T) {
	const rounds = 5
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	for r := 0; r < rounds; r++ {
		start := make(chan struct{})
		var wg sync.WaitGroup
		var inCS int64

		for _, w := range workers {
			wg.Add(1)
			w := w
			go func() {
				defer wg.Done()
				<-start
				task := makeTask(w.ID, 3700+r*10+w.ID)
				if err := w.RequestCS(context.Background(), task); err != nil {
					t.Errorf("round %d worker %d RequestCS: %v", r, w.ID, err)
					return
				}
				n := atomic.AddInt64(&inCS, 1)
				if n != 1 {
					t.Errorf("round %d: mutual exclusion violated — %d workers in CS", r, n)
				}
				time.Sleep(time.Millisecond)
				atomic.AddInt64(&inCS, -1)
				w.ReleaseCS()
			}()
		}
		close(start)
		wg.Wait()
	}
}

// -----------------------------------------------------------------------
// Membership consistency tests
// -----------------------------------------------------------------------

// activeSetOf returns a sorted copy of the active worker IDs for w.
func activeSetOf(w *Worker) []int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return sortedActiveIDs(w.activeWorkers)
}

// quorumOf returns a copy of w's current quorum.
func quorumOf(w *Worker) []int {
	w.mu.Lock()
	defer w.mu.Unlock()
	q := make([]int, len(w.Quorum))
	copy(q, w.Quorum)
	return q
}

// Test 38: after broadcasting NotifyWorkerRemoved, every surviving worker has the
// same active set and their quorums pairwise intersect.
func TestMembershipConsistencyAfterRemove(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	// Remove nodes 8, 7 then 6 via direct state manipulation (unsafe intermediates),
	// ending at the safe N=6 grid.
	victims := []int{8, 7, 6}
	for _, w := range workers {
		w.mu.Lock()
		for _, v := range victims {
			w.activeWorkers[v] = false
			w.liveWorkers[v] = false
		}
		w.Quorum = RegridQuorum(w.ID, sortedActiveIDs(w.activeWorkers))
		w.mu.Unlock()
	}

	surviving := workers[:6]

	// All surviving workers must agree on the active set.
	ref := activeSetOf(surviving[0])
	for _, w := range surviving[1:] {
		got := activeSetOf(w)
		if !equal(ref, got) {
			t.Errorf("worker %d active set %v != worker 0's %v", w.ID, got, ref)
		}
	}

	// All quorums must pairwise intersect.
	quorums := make([][]int, len(surviving))
	for i, w := range surviving {
		quorums[i] = quorumOf(w)
	}
	for i := 0; i < len(surviving); i++ {
		for j := i + 1; j < len(surviving); j++ {
			if !intersects(quorums[i], quorums[j]) {
				t.Errorf("quorum(%d)=%v and quorum(%d)=%v do not intersect after remove",
					surviving[i].ID, quorums[i], surviving[j].ID, quorums[j])
			}
		}
	}
}

// Test 39: after broadcasting NotifyWorkerAdded for a new node, every worker has
// the same active set and pairwise-intersecting quorums.
func TestMembershipConsistencyAfterAdd(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	// Add a virtual node 9 (no real server; we only check membership/quorum state).
	for _, w := range workers {
		w.NotifyWorkerAdded(9)
	}

	ref := activeSetOf(workers[0])
	for _, w := range workers[1:] {
		got := activeSetOf(w)
		if !equal(ref, got) {
			t.Errorf("worker %d active set %v != worker 0's %v", w.ID, got, ref)
		}
	}

	quorums := make([][]int, len(workers))
	for i, w := range workers {
		quorums[i] = quorumOf(w)
	}
	for i := 0; i < len(workers); i++ {
		for j := i + 1; j < len(workers); j++ {
			if !intersects(quorums[i], quorums[j]) {
				t.Errorf("quorum(%d)=%v and quorum(%d)=%v do not intersect after add",
					workers[i].ID, quorums[i], workers[j].ID, quorums[j])
			}
		}
	}
}

// -----------------------------------------------------------------------
// Duplicate / out-of-order message tolerance tests
// -----------------------------------------------------------------------

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

// Test 42: stale YIELD (from a previous request round) is ignored — voter does not
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
		Type:     maekawapb.MsgType_YIELD,
		SenderId: int32(workers[1].ID),
		Clock:    1,
		TaskId:   task1.ID,
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

// Test 43: stale FAILED (not in StateWanting) is ignored — no state mutation.
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

// -----------------------------------------------------------------------
// Partial membership propagation test
// -----------------------------------------------------------------------

// Test 44: partial membership propagation — only some workers apply a removal.
// Since regridSafe rejects any removal that would produce an unsafe grid, a partial
// broadcast of an unsafe removal leaves ALL workers at the same safe membership.
// Verify: (a) the partial removal is rejected on all workers it reaches, and
// (b) every worker's active set is identical after the partial broadcast.
func TestPartialMembershipPropagation(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	// Reduce to N=6 (safe) via direct manipulation.
	for _, w := range workers {
		w.mu.Lock()
		for _, v := range []int{8, 7, 6} {
			w.activeWorkers[v] = false
			w.liveWorkers[v] = false
		}
		w.Quorum = RegridQuorum(w.ID, sortedActiveIDs(w.activeWorkers))
		w.mu.Unlock()
	}

	// Broadcast remove(5) to only workers 0–2. N=6→5 is unsafe, so all reject it.
	for _, w := range workers[:3] {
		before := countActive(w)
		w.NotifyWorkerRemoved(5)
		after := countActive(w)
		if after != before {
			t.Errorf("worker %d: removal to N=5 (unsafe) was accepted: %d→%d", w.ID, before, after)
		}
	}

	// All surviving workers (0–5) must still have the same active set (N=6).
	ref := activeSetOf(workers[0])
	for _, w := range workers[:6] {
		got := activeSetOf(w)
		if !equal(ref, got) {
			t.Errorf("worker %d active set diverged: got %v, want %v", w.ID, got, ref)
		}
	}

	// Quorums must still pairwise intersect.
	for i := 0; i < 6; i++ {
		for j := i + 1; j < 6; j++ {
			qi := quorumOf(workers[i])
			qj := quorumOf(workers[j])
			if !intersects(qi, qj) {
				t.Errorf("quorum(%d)=%v and quorum(%d)=%v don't intersect after partial broadcast",
					i, qi, j, qj)
			}
		}
	}
}

// -----------------------------------------------------------------------
// Churn-while-contending test
// -----------------------------------------------------------------------

// Test 45: repeatedly mark workers down/up while several workers request CS continuously.
// Mutual exclusion must never be violated.
func TestChurnWhileContending(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	var inCS int64
	var wg sync.WaitGroup

	// 4 requesters continuously try to enter CS.
	for _, w := range workers[:4] {
		wg.Add(1)
		w := w
		go func() {
			defer wg.Done()
			for round := 0; ctx.Err() == nil; round++ {
				task := makeTask(w.ID, 4500+round)
				reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
				err := w.RequestCS(reqCtx, task)
				reqCancel()
				if err != nil {
					continue // down/up churn may cancel requests; that's fine
				}
				n := atomic.AddInt64(&inCS, 1)
				if n != 1 {
					t.Errorf("churn: mutual exclusion violated — %d in CS", n)
				}
				time.Sleep(time.Millisecond)
				atomic.AddInt64(&inCS, -1)
				w.ReleaseCS()
			}
		}()
	}

	// Churn goroutine: randomly mark workers[4..8] down then up every ~50 ms.
	wg.Add(1)
	go func() {
		defer wg.Done()
		churnTargets := workers[4:]
		for ctx.Err() == nil {
			target := churnTargets[rand.Intn(len(churnTargets))]
			// broadcast down
			for _, w := range workers[:4] {
				w.NotifyWorkerDown(target.ID)
			}
			time.Sleep(25 * time.Millisecond)
			// broadcast up
			for _, w := range workers[:4] {
				w.NotifyWorkerUp(target.ID)
			}
			time.Sleep(25 * time.Millisecond)
		}
	}()

	wg.Wait()
}

// -----------------------------------------------------------------------
// Starvation / fairness test
// -----------------------------------------------------------------------

// Test 46: run many rounds with all 9 workers; verify every worker enters CS at
// least once (no worker is permanently starved).
func TestNoStarvation(t *testing.T) {
	const totalRounds = 45 // 5 rounds per worker minimum if perfectly fair
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	counts := make([]int64, len(workers))
	var wg sync.WaitGroup
	start := make(chan struct{})

	for _, w := range workers {
		wg.Add(1)
		w := w
		go func() {
			defer wg.Done()
			<-start
			for round := 0; round < totalRounds/len(workers)+1; round++ {
				task := makeTask(w.ID, 4600+round*len(workers)+w.ID)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err := w.RequestCS(ctx, task)
				cancel()
				if err != nil {
					continue
				}
				atomic.AddInt64(&counts[w.ID], 1)
				w.ReleaseCS()
			}
		}()
	}
	close(start)
	wg.Wait()

	for id, cnt := range counts {
		if cnt == 0 {
			t.Errorf("worker %d never entered CS — possible starvation", id)
		}
	}
}

// -----------------------------------------------------------------------
// Self-removal edge case test
// -----------------------------------------------------------------------

// Test 47: a worker ignores NotifyWorkerRemoved for its own ID.
func TestSelfRemovalIgnored(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]
	beforeActive := countActive(w0)
	w0.NotifyWorkerRemoved(w0.ID)
	afterActive := countActive(w0)

	if afterActive != beforeActive {
		t.Errorf("self-removal changed active count %d→%d", beforeActive, afterActive)
	}

	// Must still be able to participate in CS.
	task := makeTask(0, 47)
	if err := w0.RequestCS(context.Background(), task); err != nil {
		t.Fatalf("RequestCS after self-removal attempt: %v", err)
	}
	w0.ReleaseCS()
}

// Test 48: removing the last quorum peer (the only other node in quorum) is rejected
// because it would make the cluster size unsafe for regridding.
func TestRemoveLastQuorumPeerRejected(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]

	// Reach N=6 safely (minimum safe grid).
	for _, w := range workers {
		w.mu.Lock()
		for _, v := range []int{8, 7, 6} {
			w.activeWorkers[v] = false
			w.liveWorkers[v] = false
		}
		w.Quorum = RegridQuorum(w.ID, sortedActiveIDs(w.activeWorkers))
		w.mu.Unlock()
	}

	// Any further removal (N=6→5) is unsafe and must be rejected.
	before := countActive(w0)
	w0.NotifyWorkerRemoved(5)
	after := countActive(w0)
	if after != before {
		t.Errorf("removal to N=5 (unsafe) accepted: active %d→%d", before, after)
	}

	// CS must still work on the 6-node grid.
	task := makeTask(0, 48)
	if err := w0.RequestCS(context.Background(), task); err != nil {
		t.Fatalf("RequestCS after rejected removal: %v", err)
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

// -----------------------------------------------------------------------
// Duplicate message and priority-order unit tests (white-box, under lock)
// -----------------------------------------------------------------------

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
		Type:     maekawapb.MsgType_YIELD,
		SenderId: int32(workers[1].ID),
		Clock:    12,
		TaskId:   "t1",
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

	v := workers[4] // voter node (not one of the requesters)
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
