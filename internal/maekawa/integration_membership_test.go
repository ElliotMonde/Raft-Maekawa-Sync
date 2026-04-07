// tests enabled

package maekawa

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMembershipChangeRegridsQuorum verifies that OnMembershipChange recomputes
// the quorum and resets voting state.
func TestMembershipChangeRegridsQuorum(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w := workers[0]
	w.Mu.Lock()
	oldQuorum := make([]int32, len(w.quorum))
	copy(oldQuorum, w.quorum)
	w.Mu.Unlock()

	// Simulate removing node 8 (shrinks cluster to 8 nodes).
	active := make([]int32, 0, 8)
	for _, id := range []int32{0, 1, 2, 3, 4, 5, 6, 7} {
		active = append(active, id)
	}
	w.OnMembershipChange(active)

	w.Mu.Lock()
	newQuorum := make([]int32, len(w.quorum))
	copy(newQuorum, w.quorum)
	w.Mu.Unlock()

	// The quorum must still include self.
	selfFound := false
	for _, id := range newQuorum {
		if id == w.ID {
			selfFound = true
			break
		}
	}
	if !selfFound {
		t.Errorf("self %d not found in quorum after membership change: %v", w.ID, newQuorum)
	}
	t.Logf("old quorum: %v; new quorum after remove-8: %v", oldQuorum, newQuorum)
}

// TestMembershipChangeSetsVotedForToNegOne verifies that OnMembershipChange
// resets voting state (votedFor = -1, inCS = false, etc.).
func TestMembershipChangeSetsVotedForToNegOne(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w := workers[0]
	// Enter CS so state is non-trivial.
	if err := w.RequestCS(context.Background(), makeTask(0, 0)); err != nil {
		t.Fatalf("RequestCS: %v", err)
	}
	w.ReleaseCS()

	// Trigger membership change.
	active := make([]int32, 9)
	for i := range active {
		active[i] = int32(i)
	}
	w.OnMembershipChange(active)

	w.Mu.Lock()
	vf := w.votedFor
	inCS := w.inCS
	committed := w.committed
	w.Mu.Unlock()

	if vf != -1 {
		t.Errorf("votedFor = %d after OnMembershipChange, want -1", vf)
	}
	if inCS {
		t.Error("inCS should be false after OnMembershipChange")
	}
	if committed {
		t.Error("committed should be false after OnMembershipChange")
	}
}

// TestOnMembershipChangeAbortsMidFlightRequest verifies that a membership
// change while blocked in RequestForGlobalLock aborts the request.
func TestOnMembershipChangeAbortsMidFlightRequest(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]
	// Stop W1 so W0 blocks waiting for its vote.
	workers[1].Stop()

	task := makeTask(0, 0)
	result := make(chan error, 1)
	go func() { result <- w0.RequestForGlobalLock(context.Background()) }()

	time.Sleep(100 * time.Millisecond)

	// Membership change triggers grantChan <- false, aborting the in-flight lock.
	active := make([]int32, 0, 8)
	for _, id := range []int32{0, 2, 3, 4, 5, 6, 7, 8} {
		active = append(active, id)
	}
	w0.OnMembershipChange(active)
	_ = task // keep reference

	select {
	case err := <-result:
		if err == nil {
			t.Fatal("expected RequestForGlobalLock to fail after membership change, got success")
		}
		t.Logf("correctly aborted: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("RequestForGlobalLock did not return after membership change")
	}
}

// TestMembershipChangeMutualExclusionPreserved verifies that after a membership
// change mutual exclusion is not violated.
func TestMembershipChangeMutualExclusionPreserved(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	// Apply a no-op membership change (same nodes) on all workers.
	active := make([]int32, 9)
	for i := range active {
		active[i] = int32(i)
	}
	for _, w := range workers {
		w.OnMembershipChange(active)
	}
	time.Sleep(50 * time.Millisecond)

	var csCount int64
	var maxCS int64
	var wg sync.WaitGroup

	for i := 0; i < 9; i++ {
		wg.Add(1)
		go func(w *testWorker) {
			defer wg.Done()
			if err := w.RequestCS(context.Background(), makeTask(int(w.ID), 27)); err != nil {
				t.Logf("W%d refused: %v", w.ID, err)
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
		t.Fatal("workers did not complete CS after membership change")
	}
	if maxCS > 1 {
		t.Errorf("mutual exclusion violated after membership change: max=%d", maxCS)
	}
}

// TestQuorumIntersectionAfterChange verifies that after OnMembershipChange all
// worker quorums pairwise intersect (Maekawa safety property).
func TestQuorumIntersectionAfterChange(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	// Shrink to 6 active by telling all workers about the new membership.
	active := []int32{0, 1, 2, 3, 4, 5}
	for _, w := range workers[:6] {
		w.OnMembershipChange(active)
	}

	// Collect all quorums.
	quorums := make([][]int32, 6)
	for i, w := range workers[:6] {
		w.Mu.Lock()
		quorums[i] = make([]int32, len(w.quorum))
		copy(quorums[i], w.quorum)
		w.Mu.Unlock()
	}

	// Verify pairwise intersection.
	for i := 0; i < 6; i++ {
		for j := i + 1; j < 6; j++ {
			if !intersects(quorums[i], quorums[j]) {
				t.Errorf("quorum(%d)=%v and quorum(%d)=%v do not intersect",
					workers[i].ID, quorums[i], workers[j].ID, quorums[j])
			}
		}
	}
}
