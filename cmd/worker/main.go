package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	maekawapb "raft-maekawa-sync/api/maekawa"
	raftpb "raft-maekawa-sync/api/raft"
	"raft-maekawa-sync/internal/maekawa"
	"raft-maekawa-sync/internal/models"
	"raft-maekawa-sync/internal/raft"
	"raft-maekawa-sync/internal/rpc"
)

func main() {
	id := flag.Int("id", 0, "worker/raft node ID")
	addr := flag.String("addr", "", "listen address, e.g. 127.0.0.1:5001")
	peersRaw := flag.String("peers", "", "comma-separated id=addr entries")
	dataDir := flag.String("data-dir", ".raft-state", "directory for persisted raft state")
	flag.Parse()

	if *id <= 0 || *addr == "" {
		log.Fatal("--id and --addr are required")
	}

	peers, err := parsePeers(*peersRaw)
	if err != nil {
		log.Fatalf("parse peers: %v", err)
	}

	activeIDs := make([]int32, 0, len(peers)+1)
	activeIDs = append(activeIDs, int32(*id))
	for pid := range peers {
		activeIDs = append(activeIDs, pid)
	}
	sort.Slice(activeIDs, func(i, j int) bool { return activeIDs[i] < activeIDs[j] })
	quorum := maekawa.RegridQuorum(int32(*id), activeIDs)
	if len(quorum) == 0 {
		quorum = []int32{int32(*id)}
	}

	node := raft.NewNode(int32(*id), *addr, peers, nil)
	if err := node.SetStoragePath(filepath.Join(*dataDir, fmt.Sprintf("node-%d.json", *id))); err != nil {
		log.Fatalf("load raft state: %v", err)
	}
	worker := maekawa.NewWorker(int32(*id), quorum, node)
	node.SetApplier(worker)
	worker.SetTaskExecutor(func(ctx context.Context, task *models.Task) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
		return fmt.Sprintf("done:%s", task.ID), nil
	})

	peerEndpoints := make(map[int32]string, len(peers)+1)
	peerEndpoints[int32(*id)] = *addr
	for pid, paddr := range peers {
		peerEndpoints[pid] = paddr
	}
	if err := worker.InitClients(peerEndpoints, int32(*id)); err != nil {
		log.Fatalf("init maekawa clients: %v", err)
	}

	s := rpc.NewServer()
	raftpb.RegisterRaftServer(s.GRPC(), node)
	maekawapb.RegisterMaekawaServer(s.GRPC(), worker)
	if err := s.Start(*addr); err != nil {
		log.Fatalf("start worker server: %v", err)
	}
	log.Printf("worker node %d listening on %s", *id, *addr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer s.Stop()

	go node.Run(ctx)
	go worker.RunTaskLoop(ctx)
	<-ctx.Done()
}

func parsePeers(raw string) (map[int32]string, error) {
	peers := make(map[int32]string)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return peers, nil
	}
	for _, part := range strings.Split(raw, ",") {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		kv := strings.SplitN(item, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid peer entry %q", item)
		}
		peerID, err := strconv.Atoi(strings.TrimSpace(kv[0]))
		if err != nil {
			return nil, fmt.Errorf("invalid peer id %q: %w", kv[0], err)
		}
		peerAddr := strings.TrimSpace(kv[1])
		if peerAddr == "" {
			return nil, fmt.Errorf("empty address for peer id %d", peerID)
		}
		peers[int32(peerID)] = peerAddr
	}
	return peers, nil
}
