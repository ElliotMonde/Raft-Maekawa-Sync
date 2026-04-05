package maekawa

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

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
