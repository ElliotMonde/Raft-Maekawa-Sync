package maekawa

import (
	"fmt"
	"net"
	"testing"
	"time"

	"raft-maekawa/internal/models"
)

func allocatePeers(t *testing.T, n int) map[int]string {
	t.Helper()
	peers := make(map[int]string, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("allocatePeers: %v", err)
		}
		peers[i] = ln.Addr().String()
		ln.Close()
	}
	return peers
}

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

func activeSetOf(w *Worker) []int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return sortedActiveIDs(w.activeWorkers)
}

func quorumOf(w *Worker) []int {
	w.mu.Lock()
	defer w.mu.Unlock()
	q := make([]int, len(w.Quorum))
	copy(q, w.Quorum)
	return q
}
