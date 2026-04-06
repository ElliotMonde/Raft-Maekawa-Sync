// tests enabled
package maekawa

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	maekawapb "raft-maekawa-sync/api/maekawa"
)

// TestDuplicateGrantIgnoredWhenInCS verifies that a stale Grant received while
// already in the CS (committed flag set) is silently ignored and does not
// corrupt the vote counter.
func TestDuplicateGrantIgnoredWhenInCS(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]

	if err := w0.RequestCS(context.Background(), makeTask(0, 40)); err != nil {
		t.Fatalf("RequestCS: %v", err)
	}

	// Now in CS (inCS=true, committed=true). Inject a duplicate Grant via gRPC.
	w0.Mu.Lock()
	ts := w0.ownReqTimestamp
	w0.Mu.Unlock()

	// Pretend voter 1 sends a duplicate Grant.
	_, err := w0.Grant(context.Background(), &maekawapb.GrantRequest{
		SenderId:  workers[1].ID,
		Timestamp: ts,
	})
	if err != nil {
		t.Fatalf("duplicate Grant RPC returned unexpected error: %v", err)
	}

	// votesReceived must not exceed quorum size.
	w0.Mu.Lock()
	vr := w0.votesReceived
	inCS := w0.inCS
	w0.Mu.Unlock()

	if !inCS {
		t.Error("inCS should still be true after duplicate Grant")
	}
	if vr > len(w0.quorum) {
		t.Errorf("votesReceived (%d) exceeded quorum size (%d) after duplicate Grant", vr, len(w0.quorum))
	}

	w0.ReleaseCS()
}

// TestDuplicateReleaseSafe verifies that calling exitGlobalCS twice does not
// panic and the second call is a no-op.
func TestDuplicateReleaseSafe(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]
	if err := w0.RequestCS(context.Background(), makeTask(0, 41)); err != nil {
		t.Fatalf("RequestCS: %v", err)
	}
	w0.ReleaseCS()
	// Second release must not panic.
	w0.ReleaseCS()
}

// TestStaleGrantIgnoredBadTimestamp verifies that a Grant with a wrong
// timestamp is ignored (timestamp mismatch guard in Grant handler).
func TestStaleGrantIgnoredBadTimestamp(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w0 := workers[0]

	// Send a Grant with a timestamp that cannot be the current round.
	w0.Mu.Lock()
	staleTS := w0.ownReqTimestamp - 100
	votesBefore := w0.votesReceived
	w0.Mu.Unlock()

	_, err := w0.Grant(context.Background(), &maekawapb.GrantRequest{
		SenderId:  1,
		Timestamp: staleTS,
	})
	if err != nil {
		t.Fatalf("Grant returned error: %v", err)
	}

	w0.Mu.Lock()
	votesAfter := w0.votesReceived
	w0.Mu.Unlock()

	if votesAfter != votesBefore {
		t.Errorf("stale Grant changed votesReceived: %d → %d (want no change)", votesBefore, votesAfter)
	}
}

// TestInquireYieldRoundtrip verifies that the Inquire → Yield round-trip works
// without deadlock during contention: two workers race; the higher-priority one
// (lower timestamp or lower ID) should eventually win.
func TestInquireYieldRoundtrip(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	var csCount int64
	var maxCS int64
	var wg sync.WaitGroup

	// Start workers 0 and 1 simultaneously — they share quorum nodes and
	// will trigger INQUIRE/YIELD.
	for _, w := range workers[:2] {
		wg.Add(1)
		w := w
		go func() {
			defer wg.Done()
			if err := w.RequestCS(context.Background(), makeTask(int(w.ID), 100)); err != nil {
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
			time.Sleep(20 * time.Millisecond)
			atomic.AddInt64(&csCount, -1)
			w.ReleaseCS()
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("Inquire/Yield deadlock: two workers did not complete within 15s")
	}

	if maxCS > 1 {
		t.Errorf("mutual exclusion violated during INQUIRE/YIELD test: maxCS=%d", maxCS)
	}
}

// TestRequestLockValidatesNodeID verifies that RequestLock rejects negative NodeIDs.
func TestRequestLockValidatesNodeID(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w := workers[0]
	_, err := w.RequestLock(context.Background(), &maekawapb.LockRequest{
		NodeId:    -1,
		Timestamp: 42,
	})
	if err == nil {
		t.Fatal("expected error for negative NodeID, got nil")
	}
}

// TestReleaseLockUnknownNodeIgnored verifies that ReleaseLock for an ID that
// never held the vote is safe (no panic, no state corruption).
func TestReleaseLockUnknownNodeIgnored(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	w := workers[0]
	_, err := w.ReleaseLock(context.Background(), &maekawapb.ReleaseRequest{
		NodeId: 99, // never held the vote
	})
	if err != nil {
		t.Fatalf("ReleaseLock for unknown node returned error: %v", err)
	}

	w.Mu.Lock()
	vf := w.votedFor
	w.Mu.Unlock()
	if vf == 99 {
		t.Errorf("votedFor became 99 after release from unknown node")
	}
}

// TestConcurrentRequestsSafe verifies that concurrent RequestLock and
// ReleaseLock calls on the same worker don't corrupt the heap or voter state.
func TestConcurrentRequestsSafe(t *testing.T) {
	workers, _ := startWorkers(t, 9)
	defer stopWorkers(workers)

	voter := workers[4] // a node in many quorums

	const goroutines = 8
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(senderID int32) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := voter.RequestLock(ctx, &maekawapb.LockRequest{
				NodeId:    senderID,
				Timestamp: int64(senderID) * 10,
			})
			if err != nil {
				return
			}
			// Always release — if granted, clears the vote; if queued, removes from queue.
			voter.ReleaseLock(ctx, &maekawapb.ReleaseRequest{NodeId: senderID, Timestamp: int64(senderID) * 10})
		}(int32(i))
	}
	wg.Wait()

	// After all releases, votedFor must be -1 (or the heap is empty).
	voter.Mu.Lock()
	qLen := voter.requestQueue.Len()
	vf := voter.votedFor
	voter.Mu.Unlock()

	if qLen != 0 {
		t.Errorf("request queue not empty after all releases: len=%d, votedFor=%d", qLen, vf)
	}
}
