// tests enabled

package maekawa

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	maekawapb "raft-maekawa-sync/api/maekawa"
	"raft-maekawa-sync/internal/models"
	"raft-maekawa-sync/internal/rpc"
)

// ---------------------------------------------------------------------------
// fakeMembership — in-memory ClusterMembership for integration tests.
// ---------------------------------------------------------------------------

type fakeMembership struct {
	mu     sync.Mutex
	alive  map[int32]bool
	active []int32
	wins   map[string]int32
}

func newFakeMembership(ids []int32) *fakeMembership {
	alive := make(map[int32]bool, len(ids))
	for _, id := range ids {
		alive[id] = true
	}
	active := make([]int32, len(ids))
	copy(active, ids)
	return &fakeMembership{alive: alive, active: active, wins: make(map[string]int32)}
}

func (m *fakeMembership) IsAlive(id int32) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.alive[id]
}

func (m *fakeMembership) ActiveMembers() []int32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]int32, len(m.active))
	copy(out, m.active)
	return out
}

func (m *fakeMembership) ClaimTask(taskID string, workerID int32) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.wins[taskID]; ok {
		return false, nil
	}
	if !m.alive[workerID] {
		return false, fmt.Errorf("worker %d is not alive", workerID)
	}
	m.wins[taskID] = workerID
	return true, nil
}

func (m *fakeMembership) ReportTaskSuccess(taskID string, workerID int32, result string) error {
	return nil
}

func (m *fakeMembership) ReportTaskFailure(taskID string, workerID int32, reason string) error {
	return nil
}

func (m *fakeMembership) markDown(id int32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alive[id] = false
}

func (m *fakeMembership) markUp(id int32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alive[id] = true
}

// ---------------------------------------------------------------------------
// testWorker — a Worker bundled with its gRPC server.
// ---------------------------------------------------------------------------

type testWorker struct {
	*Worker
	srv  *rpc.Server
	addr string
}

func (tw *testWorker) Stop() {
	tw.srv.Stop()
}

// RequestCS wraps RequestForGlobalLock so safety/fairness tests read naturally.
func (tw *testWorker) RequestCS(ctx context.Context, _ *models.Task) error {
	return tw.RequestForGlobalLock(ctx)
}

// ReleaseCS wraps exitGlobalCS.
func (tw *testWorker) ReleaseCS() {
	tw.exitGlobalCS()
}

// ---------------------------------------------------------------------------
// startWorkers / stopWorkers — cluster lifecycle helpers.
// ---------------------------------------------------------------------------

func startWorkers(t *testing.T, n int) ([]*testWorker, *fakeMembership) {
	t.Helper()

	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("allocate port %d: %v", i, err)
		}
		addrs[i] = ln.Addr().String()
		ln.Close()
	}

	ids := make([]int32, n)
	for i := range ids {
		ids[i] = int32(i)
	}
	membership := newFakeMembership(ids)

	workers := make([]*testWorker, n)
	for i := 0; i < n; i++ {
		quorum := QuorumFor(int32(i), int32(n))
		w := NewWorker(int32(i), quorum, membership)

		srv := rpc.NewServer()
		maekawapb.RegisterMaekawaServer(srv.GRPC(), w)
		if err := srv.Start(addrs[i]); err != nil {
			t.Fatalf("start gRPC server %d: %v", i, err)
		}
		workers[i] = &testWorker{Worker: w, srv: srv, addr: addrs[i]}
	}

	// Wire each worker's client manager to all peers.
	peers := make(map[int32]string, n)
	for i, tw := range workers {
		peers[int32(i)] = tw.addr
	}
	for _, tw := range workers {
		if err := tw.clientMgr.InitClients(peers, tw.ID); err != nil {
			t.Fatalf("init clients worker %d: %v", tw.ID, err)
		}
	}

	time.Sleep(60 * time.Millisecond) // allow gRPC servers to accept
	return workers, membership
}

func stopWorkers(workers []*testWorker) {
	for _, tw := range workers {
		tw.Stop()
	}
}

// makeTask builds a minimal Task.
func makeTask(workerID, round int) *models.Task {
	return &models.Task{
		ID:   fmt.Sprintf("task-%d-%d", workerID, round),
		Data: fmt.Sprintf("worker %d round %d", workerID, round),
	}
}

// ---------------------------------------------------------------------------
// fakeTaskReporter — simple event sink used by task-level local tests.
// ---------------------------------------------------------------------------

type fakeTaskReporter struct {
	mu     sync.Mutex
	events []models.TaskEvent
	ch     chan models.TaskEvent
	submit func(context.Context, models.TaskEvent) error
}

func newFakeTaskReporter() *fakeTaskReporter {
	return &fakeTaskReporter{ch: make(chan models.TaskEvent, 8)}
}

func (r *fakeTaskReporter) SubmitTaskEvent(ctx context.Context, event models.TaskEvent) error {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
	select {
	case r.ch <- event:
	default:
	}
	if r.submit != nil {
		return r.submit(ctx, event)
	}
	return nil
}
