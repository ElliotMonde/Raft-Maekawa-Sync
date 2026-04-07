// tests enabled

package maekawa

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	maekawapb "raft-maekawa-sync/api/maekawa"
	"raft-maekawa-sync/internal/models"
	"raft-maekawa-sync/internal/rpc"
)

// ---------------------------------------------------------------------------
// startWorkersWithMembership: like startWorkers but uses a caller-provided
// ClusterMembership so tests can inspect/intercept task lifecycle events.
// ---------------------------------------------------------------------------

func startWorkersWithMembership(t *testing.T, n int, mem ClusterMembership) []*testWorker {
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

	workers := make([]*testWorker, n)
	for i := 0; i < n; i++ {
		quorum := QuorumFor(int32(i), int32(n))
		w := NewWorker(int32(i), quorum, mem)

		srv := rpc.NewServer()
		maekawapb.RegisterMaekawaServer(srv.GRPC(), w)
		if err := srv.Start(addrs[i]); err != nil {
			t.Fatalf("start server %d: %v", i, err)
		}
		workers[i] = &testWorker{Worker: w, srv: srv, addr: addrs[i]}
	}

	peers := make(map[int32]string, n)
	for i, tw := range workers {
		peers[int32(i)] = tw.addr
	}
	for _, tw := range workers {
		if err := tw.clientMgr.InitClients(peers, tw.ID); err != nil {
			t.Fatalf("init clients worker %d: %v", tw.ID, err)
		}
	}

	time.Sleep(60 * time.Millisecond)
	return workers
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestSharedTaskOnlyExecutedOnce: all 9 workers enqueue the same task; it must
// be executed exactly once (ClaimTask enforces the global exclusivity).
func TestSharedTaskOnlyExecutedOnce(t *testing.T) {
	n := 9
	ids := make([]int32, n)
	for i := range ids {
		ids[i] = int32(i)
	}
	mem := newTaskMembership(ids)
	workers := startWorkersWithMembership(t, n, mem)
	defer stopWorkers(workers)

	var execCount int32
	for _, w := range workers {
		w := w
		w.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
			atomic.AddInt32(&execCount, 1)
			time.Sleep(30 * time.Millisecond)
			return fmt.Sprintf("done-by-%d", w.ID), nil
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, w := range workers {
		w := w
		go func() { w.RunTaskLoop(ctx) }()
	}

	task := &models.Task{ID: "shared-once", Data: ""}
	for _, w := range workers {
		w.taskQueue <- task
	}

	mem.waitForDone(t, task.ID, 5*time.Second)
	time.Sleep(300 * time.Millisecond)

	if got := atomic.LoadInt32(&execCount); got != 1 {
		t.Errorf("task executed %d times, want exactly 1", got)
	}
}

// TestMultipleDistinctTasksAllExecuted: each worker enqueues a unique task;
// all must complete.
func TestMultipleDistinctTasksAllExecuted(t *testing.T) {
	n := 9
	ids := make([]int32, n)
	for i := range ids {
		ids[i] = int32(i)
	}
	mem := newTaskMembership(ids)
	workers := startWorkersWithMembership(t, n, mem)
	defer stopWorkers(workers)

	for _, w := range workers {
		w.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
			return "ok:" + task.ID, nil
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, w := range workers {
		w := w
		go func() { w.RunTaskLoop(ctx) }()
	}

	const taskCount = 9
	for i := 0; i < taskCount; i++ {
		task := &models.Task{ID: fmt.Sprintf("distinct-task-%d", i), Data: ""}
		workers[i].taskQueue <- task
	}

	deadline := time.After(10 * time.Second)
	done := 0
	for done < taskCount {
		select {
		case <-mem.done:
			done++
		case <-deadline:
			t.Fatalf("only %d of %d tasks completed within 10s", done, taskCount)
		}
	}
}

// TestCanceledSharedTaskNotExecuted: all workers cancel a task then enqueue it;
// no worker should execute it.
func TestCanceledSharedTaskNotExecuted(t *testing.T) {
	n := 9
	ids := make([]int32, n)
	for i := range ids {
		ids[i] = int32(i)
	}
	mem := newTaskMembership(ids)
	workers := startWorkersWithMembership(t, n, mem)
	defer stopWorkers(workers)

	executed := make(chan int32, 4)
	for _, w := range workers {
		wid := w.ID
		w.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
			select {
			case executed <- wid:
			default:
			}
			return "ok", nil
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, w := range workers {
		w := w
		go func() { w.RunTaskLoop(ctx) }()
	}

	task := &models.Task{ID: "shared-cancel", Data: ""}
	for _, w := range workers {
		w.ApplyTaskEvent(models.TaskEvent{Type: models.EventCanceled, TaskID: task.ID})
	}
	for _, w := range workers {
		w.taskQueue <- task
	}

	select {
	case wid := <-executed:
		t.Fatalf("canceled task was executed by worker %d", wid)
	case <-time.After(500 * time.Millisecond):
	}
}

// TestClusterMutualExclusionDuringTaskExecution: n workers compete for the CS
// while executing tasks — at most one executor at a time.
func TestClusterMutualExclusionDuringTaskExecution(t *testing.T) {
	n := 9
	ids := make([]int32, n)
	for i := range ids {
		ids[i] = int32(i)
	}
	mem := newTaskMembership(ids)
	workers := startWorkersWithMembership(t, n, mem)
	defer stopWorkers(workers)

	var inCS int64
	var maxCS int64

	for _, w := range workers {
		w.SetTaskExecutor(func(_ context.Context, task *models.Task) (string, error) {
			cur := atomic.AddInt64(&inCS, 1)
			for {
				old := atomic.LoadInt64(&maxCS)
				if cur <= old || atomic.CompareAndSwapInt64(&maxCS, old, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			atomic.AddInt64(&inCS, -1)
			return "ok", nil
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, w := range workers {
		w := w
		go func() { w.RunTaskLoop(ctx) }()
	}

	const taskCount = 9
	for i := 0; i < taskCount; i++ {
		task := &models.Task{ID: fmt.Sprintf("me-task-%d", i), Data: ""}
		workers[i].taskQueue <- task
	}

	deadline := time.After(15 * time.Second)
	done := 0
	for done < taskCount {
		select {
		case <-mem.done:
			done++
		case <-deadline:
			t.Fatalf("only %d of %d tasks completed within 15s", done, taskCount)
		}
	}

	if maxCS > 1 {
		t.Errorf("mutual exclusion violated: max concurrent executors = %d", maxCS)
	}
}
