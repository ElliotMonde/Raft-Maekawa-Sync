//go:build ignore

package maekawa

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

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

// Test 2b: while one worker holds CS, another requester must wait until the
// current holder releases before it can enter.
func TestRequesterWaitsUntilCurrentHolderReleases(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	holder := workers[0]
	waiter := workers[1]

	if err := holder.RequestCS(context.Background(), makeTask(holder.ID, 200)); err != nil {
		t.Fatalf("holder RequestCS: %v", err)
	}

	waiterEntered := make(chan struct{}, 1)
	waiterErr := make(chan error, 1)
	go func() {
		if err := waiter.RequestCS(context.Background(), makeTask(waiter.ID, 200)); err != nil {
			waiterErr <- err
			return
		}
		waiterEntered <- struct{}{}
	}()

	select {
	case err := <-waiterErr:
		t.Fatalf("waiter RequestCS returned early with error: %v", err)
	case <-waiterEntered:
		t.Fatal("waiter entered CS before holder released")
	case <-time.After(200 * time.Millisecond):
	}

	holder.ReleaseCS()

	select {
	case err := <-waiterErr:
		t.Fatalf("waiter RequestCS failed after holder release: %v", err)
	case <-waiterEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("waiter did not enter CS after holder released")
	}

	waiter.ReleaseCS()
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
