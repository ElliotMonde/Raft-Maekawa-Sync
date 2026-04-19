package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"raft-maekawa-sync/internal/dashboard"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP/WebSocket listen address")
	nodes := flag.String("nodes", "", "comma-separated id=grpcAddr pairs (can be empty if using --worker-bin)")
	webDir := flag.String("web-dir", "./web/dist", "path to built frontend files")
	workerBin := flag.String("worker-bin", "", "path to worker binary; enables Start/Stop worker from UI")
	dockerComposeFile := flag.String("docker-compose-file", "", "path to docker-compose.yml; enables Docker-backed Start/Stop worker from UI")
	dockerProjectName := flag.String("docker-project-name", "", "optional docker compose project name")
	dataDir := flag.String("data-dir", ".raft-state", "data directory passed to spawned worker processes")
	flag.Parse()

	nodeCfgs, err := parseNodes(*nodes)
	if err != nil {
		log.Fatalf("parse nodes: %v", err)
	}

	hub := dashboard.NewHub()
	collector := dashboard.NewCollector(nodeCfgs, hub)
	if *workerBin != "" {
		collector.Manager = dashboard.NewProcessManager(*workerBin, *dataDir)
	}
	if *dockerComposeFile != "" {
		if *workerBin != "" {
			log.Fatal("use either --worker-bin or --docker-compose-file, not both")
		}
		collector.Manager = dashboard.NewDockerComposeManager(*dockerComposeFile, *dockerProjectName, nodeCfgs)
	}
	collector.Dial()
	defer collector.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go collector.Run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", hub.ServeWS(collector))
	mux.HandleFunc("/api/action", dashboard.ServeAction(collector))
	mux.HandleFunc("/api/nodes", dashboard.ServeNodes(collector))
	mux.HandleFunc("/api/maekawa-event", hub.ServeMaekawaEvent)
	mux.Handle("/", http.FileServer(http.Dir(*webDir)))

	srv := &http.Server{Addr: *addr, Handler: mux}
	log.Printf("dashboard: listening on %s, serving frontend from %s", *addr, *webDir)

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("dashboard: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	cancel()
	srv.Close()
}

func parseNodes(s string) ([]dashboard.NodeConfig, error) {
	var cfgs []dashboard.NodeConfig
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("expected id=addr, got %q", part)
		}
		id, err := strconv.Atoi(kv[0])
		if err != nil {
			return nil, fmt.Errorf("invalid node id %q: %v", kv[0], err)
		}
		cfgs = append(cfgs, dashboard.NodeConfig{ID: int32(id), Addr: kv[1]})
	}
	return cfgs, nil
}
