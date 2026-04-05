package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"raft-maekawa/internal/maekawa"
	"raft-maekawa/internal/models"
)

func main() {
	// -- 1. Read config from environment --
	workerID, err := strconv.Atoi(os.Getenv("WORKER_ID"))
	if err != nil {
		slog.Error("WORKER_ID not set or invalid", "err", err)
		os.Exit(1)
	}

	peers, err := parsePeers(os.Getenv("WORKER_PEERS"))
	if err != nil {
		slog.Error("WORKER_PEERS invalid", "err", err)
		os.Exit(1)
	}

	listenAddr, ok := peers[workerID]
	if !ok {
		slog.Error("WORKER_ID not found in WORKER_PEERS", "id", workerID)
		os.Exit(1)
	}

	slog.Info("starting worker", "id", workerID, "addr", listenAddr)

	if demoModeEnabled() {
		if workerID != 0 {
			slog.Info("fake raft demo mode only runs on WORKER_ID=0; exiting", "id", workerID)
			return
		}
		if err := runFakeRaftDemo(peers); err != nil {
			slog.Error("fake raft demo failed", "err", err)
			os.Exit(1)
		}
		return
	}

	// -- 2. Start only this worker's server --
	w, err := maekawa.NewWorker(workerID, len(peers), peers, listenAddr)
	if err != nil {
		slog.Error("failed to start worker", "err", err)
		os.Exit(1)
	}
	defer w.Stop()

	// -- 3. Wait for peer servers to bind --
	slog.Info("waiting for peers to bind", "id", workerID)
	time.Sleep(500 * time.Millisecond)

	// -- 4. Wire up the Raft bridge and task executor --
	//
	// In production the raftBridge is replaced by a real Raft client that:
	//   - proposes the event to the leader
	//   - blocks until the leader commits it
	//   - the committed log entry is then applied on every node via ApplyTaskEvent
	//
	// The executor is the actual work payload supplied by the application layer.
	bridge := &raftBridge{}
	w.SetTaskEventReporter(bridge)
	w.SetTaskExecutor(func(ctx context.Context, task *models.Task) (string, error) {
		dur := time.Duration(200+rand.Intn(500)) * time.Millisecond
		slog.Info("executing task", "worker", workerID, "task", task.ID, "duration", dur)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(dur):
			return fmt.Sprintf("completed-by-%d", workerID), nil
		}
	})

	// -- 5. Run the task loop --
	// Blocks until ctx is cancelled; tasks arrive via w.ApplyTaskEvent called by Raft.
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := w.RunTaskLoop(runCtx); err != nil && err != context.Canceled {
			slog.Error("task loop stopped", "err", err)
		}
	}()

	slog.Info("worker ready", "id", workerID)

	// -- 6. Graceful shutdown --
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	cancel()
	slog.Info("shutting down", "id", workerID)
}

// parsePeers parses "0=host:port,1=host:port,..." into map[int]string.
func parsePeers(raw string) (map[int]string, error) {
	peers := make(map[int]string)
	if raw == "" {
		return nil, fmt.Errorf("WORKER_PEERS is empty")
	}
	for _, entry := range strings.Split(raw, ",") {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid peer entry %q", entry)
		}
		id, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, fmt.Errorf("invalid peer id %q", parts[0])
		}
		peers[id] = strings.TrimSpace(parts[1])
	}
	return peers, nil
}

func demoModeEnabled() bool {
	raw := strings.TrimSpace(os.Getenv("FAKE_RAFT_DEMO"))
	return raw == "1" || strings.EqualFold(raw, "true") || strings.EqualFold(raw, "yes")
}

func demoTaskCount() int {
	raw := strings.TrimSpace(os.Getenv("FAKE_RAFT_TASKS"))
	if raw == "" {
		return 3
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 3
	}
	return n
}

func runFakeRaftDemo(peers map[int]string) error {
	slog.Info("starting fake raft demo", "workers", len(peers), "tasks", demoTaskCount())

	workers := make([]*maekawa.Worker, len(peers))
	for id, addr := range peers {
		w, err := maekawa.NewWorker(id, len(peers), peers, addr)
		if err != nil {
			for _, started := range workers {
				if started != nil {
					started.Stop()
				}
			}
			return fmt.Errorf("start demo worker %d: %w", id, err)
		}
		workers[id] = w
	}
	defer func() {
		for _, w := range workers {
			if w != nil {
				w.Stop()
			}
		}
	}()

	time.Sleep(500 * time.Millisecond)

	bridge := &fakeRaftBridge{
		workers:  workers,
		winners:  make(map[string]int),
		terminal: make(map[string]models.EventType),
		events:   make(chan models.TaskEvent, demoTaskCount()*2),
	}

	for _, w := range workers {
		wid := w.ID
		w.SetTaskEventReporter(bridge)
		w.SetTaskExecutor(func(ctx context.Context, task *models.Task) (string, error) {
			dur := time.Duration(200+rand.Intn(500)) * time.Millisecond
			slog.Info("demo executor started", "worker", wid, "task", task.ID, "duration", dur)
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(dur):
				return fmt.Sprintf("completed-by-%d", wid), nil
			}
		})
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for _, w := range workers {
		wg.Add(1)
		w := w
		go func() {
			defer wg.Done()
			if err := w.RunTaskLoop(runCtx); err != nil && err != context.Canceled {
				slog.Error("demo task loop stopped", "worker", w.ID, "err", err)
			}
		}()
	}

	go func() {
		for i := 0; i < demoTaskCount(); i++ {
			task := &models.Task{
				ID:          fmt.Sprintf("demo-task-%d", i),
				Description: fmt.Sprintf("shared demo task %d", i),
				RequesterID: 0,
				Reward:      10.0,
				Penalty:     5.0,
			}
			bridge.broadcastCommitted(models.TaskEvent{
				Type:   models.EventAssigned,
				TaskID: task.ID,
				Task:   task,
			})
			time.Sleep(1 * time.Second)
		}
	}()

	slog.Info("fake raft demo ready")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

	completed := 0
	target := demoTaskCount()
	for completed < target {
		select {
		case <-quit:
			cancel()
			wg.Wait()
			slog.Info("fake raft demo stopped")
			return nil
		case event := <-bridge.events:
			if event.Type != models.EventDone && event.Type != models.EventFailed && event.Type != models.EventCanceled {
				continue
			}
			completed++
			slog.Info("fake raft demo task finished",
				"completed", completed,
				"total", target,
				"task", event.TaskID,
				"type", event.Type,
				"worker", event.WorkerID)
		}
	}

	cancel()
	wg.Wait()
	slog.Info("fake raft demo completed", "tasks", target)
	return nil
}

// raftBridge is the stub TaskEventReporter that will be replaced by a real Raft
// client. SubmitTaskEvent proposes the event to the Raft leader and blocks until
// the leader commits it and broadcasts it to all nodes via ApplyTaskEvent.
type raftBridge struct {
	// TODO(raft): add Raft client connection fields here.
}

func (b *raftBridge) SubmitTaskEvent(_ context.Context, event models.TaskEvent) error {
	// TODO(raft): propose event to leader, block until committed.
	// The leader replicates to a quorum, commits, then each node's apply loop
	// calls worker.ApplyTaskEvent(event) — that is the broadcast path.
	//
	// This stub must fail fast so the worker does not proceed as if Raft had
	// accepted the event. Execution starts only after the committed event comes
	// back through ApplyTaskEvent, which requires a real Raft implementation.
	slog.Warn("raft-bridge: rejecting event because no Raft client is wired",
		"type", event.Type, "task", event.TaskID, "worker", event.WorkerID)
	return errors.New("raft bridge not implemented")
}

type fakeRaftBridge struct {
	mu       sync.Mutex
	workers  []*maekawa.Worker
	winners  map[string]int
	terminal map[string]models.EventType
	events   chan models.TaskEvent
}

func (b *fakeRaftBridge) SubmitTaskEvent(_ context.Context, event models.TaskEvent) error {
	b.mu.Lock()

	switch event.Type {
	case models.EventWon:
		if winner, ok := b.winners[event.TaskID]; ok {
			if winner != event.WorkerID {
				b.mu.Unlock()
				return fmt.Errorf("task %s already won by worker %d", event.TaskID, winner)
			}
		} else {
			b.winners[event.TaskID] = event.WorkerID
		}
	case models.EventDone, models.EventFailed:
		winner, ok := b.winners[event.TaskID]
		if !ok {
			b.mu.Unlock()
			return fmt.Errorf("task %s has no committed winner", event.TaskID)
		}
		if winner != event.WorkerID {
			b.mu.Unlock()
			return fmt.Errorf("task %s terminal event from worker %d, want %d", event.TaskID, event.WorkerID, winner)
		}
		if committed, ok := b.terminal[event.TaskID]; ok && committed != event.Type {
			b.mu.Unlock()
			return fmt.Errorf("task %s already terminal as %v", event.TaskID, committed)
		}
		b.terminal[event.TaskID] = event.Type
	}
	b.mu.Unlock()

	b.broadcastCommitted(event)
	if event.Type == models.EventDone || event.Type == models.EventFailed {
		select {
		case b.events <- event:
		default:
		}
	}
	return nil
}

func (b *fakeRaftBridge) broadcastCommitted(event models.TaskEvent) {
	slog.Info("fake raft committed event", "type", event.Type, "task", event.TaskID, "worker", event.WorkerID)
	for _, w := range b.workers {
		if w != nil {
			w.ApplyTaskEvent(event)
		}
	}
}
