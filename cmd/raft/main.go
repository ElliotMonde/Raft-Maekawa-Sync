package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	raftpb "raft-maekawa-sync/api/raft"
	"raft-maekawa-sync/internal/raft"
	"raft-maekawa-sync/internal/rpc"
)

func main() {
	id := flag.Int("id", 0, "this node's raft id")
	addr := flag.String("addr", "", "listen address, e.g. 127.0.0.1:5001")
	peersRaw := flag.String("peers", "", "comma-separated id=addr entries")
	workersRaw := flag.String("workers", "", "comma-separated worker IDs managed by this raft cluster")
	dataDir := flag.String("data-dir", ".raft-state", "directory for persisted raft state")
	flag.Parse()

	if *id <= 0 || *addr == "" {
		log.Fatal("--id and --addr are required")
	}

	peers, err := parsePeers(*peersRaw)
	if err != nil {
		log.Fatalf("parse peers: %v", err)
	}

	node := raft.NewNode(int32(*id), *addr, peers, nil)
	if err := node.SetStoragePath(filepath.Join(*dataDir, fmt.Sprintf("node-%d.json", *id))); err != nil {
		log.Fatalf("load raft state: %v", err)
	}
	if workerIDs, err := parseIDs(*workersRaw); err != nil {
		log.Fatalf("parse workers: %v", err)
	} else if len(workerIDs) > 0 {
		node.SetManagedWorkers(workerIDs)
	}
	s := rpc.NewServer()
	raftpb.RegisterRaftServer(s.GRPC(), node)
	if err := s.Start(*addr); err != nil {
		log.Fatalf("start raft server: %v", err)
	}

	log.Printf("raft node %d listening on %s", *id, *addr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer s.Stop()

	node.Run(ctx)
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

func parseIDs(raw string) ([]int32, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	ids := make([]int32, 0, len(strings.Split(raw, ",")))
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid id %q: %w", part, err)
		}
		ids = append(ids, int32(id))
	}
	return ids, nil
}
