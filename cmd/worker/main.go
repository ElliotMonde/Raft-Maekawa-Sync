package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	maekawapb "raft-maekawa-sync/api/maekawa"
	"raft-maekawa-sync/internal/maekawa"
	"raft-maekawa-sync/internal/models"
	"raft-maekawa-sync/internal/raft"
	"raft-maekawa-sync/internal/rpc"
)

func main() {
	id := flag.Int("id", 0, "worker node ID")
	addr := flag.String("addr", "", "listen address, e.g. 127.0.0.1:6001")
	peersRaw := flag.String("peers", "", "comma-separated worker id=addr entries")
	raftRaw := flag.String("raft", "", "comma-separated raft id=addr entries")
	flag.String("data-dir", ".raft-state", "retained for compatibility; unused by pure worker nodes")
	dashboardAddr := flag.String("dashboard", "", "optional dashboard HTTP addr for Maekawa event reporting, e.g. dashboard:8080")
	flag.Parse()

	if *id <= 0 || *addr == "" {
		log.Fatal("--id and --addr are required")
	}

	peers, err := parsePeers(*peersRaw)
	if err != nil {
		log.Fatalf("parse peers: %v", err)
	}
	raftPeers, err := parsePeers(*raftRaw)
	if err != nil {
		log.Fatalf("parse raft peers: %v", err)
	}
	if len(raftPeers) == 0 {
		log.Fatal("--raft is required")
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

	membership := raft.NewRemoteMembership(int32(*id), raftPeers)
	defer membership.Close()

	worker := maekawa.NewWorker(int32(*id), quorum, membership)
	if *dashboardAddr != "" {
		endpoint := "http://" + *dashboardAddr + "/api/maekawa-event"
		worker.EventHook = func(evtType string, from, to int32, clock int64) {
			type payload struct {
				Type      string `json:"type"`
				From      int32  `json:"from"`
				To        int32  `json:"to"`
				NodeID    int32  `json:"node_id"`
				Timestamp int64  `json:"timestamp"`
			}
			b, _ := json.Marshal(payload{Type: evtType, From: from, To: to, NodeID: from, Timestamp: clock})
			resp, err := http.Post(endpoint, "application/json", bytes.NewReader(b))
			if err == nil {
				resp.Body.Close()
			}
		}
	}
	worker.SetTaskExecutor(func(ctx context.Context, task *models.Task) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(2000 * time.Millisecond):
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
	maekawapb.RegisterMaekawaServer(s.GRPC(), worker)
	if err := s.Start(*addr); err != nil {
		log.Fatalf("start worker server: %v", err)
	}
	log.Printf("worker node %d listening on %s", *id, *addr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer s.Stop()

	go membership.RunSync(ctx, worker)
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
