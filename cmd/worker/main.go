package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
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

	// -- 2. Create and start worker --
	w, err := maekawa.NewWorker(workerID, len(peers), peers, listenAddr)
	if err != nil {
		slog.Error("failed to start worker", "err", err)
		os.Exit(1)
	}
	defer w.Stop()

	// -- 3. Wait for peers to bind their ports --
	slog.Info("waiting for peers", "id", workerID)
	time.Sleep(500 * time.Millisecond)

	// -- 4. Simulate 3 rounds of task execution --
	for i := 0; i < 3; i++ {
		task := &models.Task{
			ID:          fmt.Sprintf("task-%d-%d", workerID, i),
			Description: fmt.Sprintf("worker %d task %d", workerID, i),
			RequesterID: workerID,
			Reward:      10.0,
			Penalty:     5.0,
		}

		slog.Info("requesting CS", "task", task.ID)
		if err := w.RequestCS(context.Background(), task); err != nil {
			slog.Error("RequestCS failed", "err", err)
			continue
		}

		// simulate work — random duration between 200ms and 700ms
		workDuration := time.Duration(200+rand.Intn(500)) * time.Millisecond
		slog.Info("executing task", "task", task.ID, "duration", workDuration)
		time.Sleep(workDuration)

		w.ReleaseCS()

		// random pause before next request — spreads out contention
		time.Sleep(time.Duration(100+rand.Intn(400)) * time.Millisecond)
	}

	slog.Info("all tasks done, waiting for shutdown signal", "id", workerID)

	// -- 5. Graceful shutdown on SIGTERM / SIGINT --
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

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
