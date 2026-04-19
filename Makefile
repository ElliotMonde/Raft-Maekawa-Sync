GO ?= $(shell command -v go 2>/dev/null || echo /usr/local/go/bin/go)
PROTOC ?= $(shell command -v protoc 2>/dev/null || echo protoc)
BIN_DIR := bin

.PHONY: help fmt test test-maekawa test-raft race race-maekawa race-raft build \
	build-dashboard build-raft build-requester build-worker docker-up docker-down \
	docker-logs docker-request proto proto-clean clean

help:
	@echo "Targets:"
	@echo "  make fmt              - run go fmt on all packages"
	@echo "  make test             - run tests for internal/maekawa and internal/raft"
	@echo "  make test-maekawa     - run tests for internal/maekawa"
	@echo "  make test-raft        - run tests for internal/raft"
	@echo "  make race             - run race tests for internal/maekawa and internal/raft"
	@echo "  make race-maekawa     - run race tests for internal/maekawa"
	@echo "  make race-raft        - run race tests for internal/raft"
	@echo "  make build            - build all cmd binaries into bin/"
	@echo "  make build-dashboard  - build cmd/dashboard"
	@echo "  make build-raft       - build cmd/raft"
	@echo "  make build-requester  - build cmd/requester"
	@echo "  make build-worker     - build cmd/worker"
	@echo "  make docker-up        - start the 3-node worker cluster with Docker Compose"
	@echo "  make docker-down      - stop the Docker Compose cluster"
	@echo "  make docker-logs      - tail Docker Compose logs"
	@echo "  make docker-request   - submit a task inside Docker (use DATA=... and optional RAFT=workerX:port)"
	@echo "  make proto            - regenerate protobuf Go files"
	@echo "  make proto-clean      - remove generated protobuf Go files"
	@echo "  make clean            - remove built binaries"

fmt:
	$(GO) fmt ./...

test: test-maekawa test-raft

test-maekawa:
	$(GO) test ./internal/maekawa

test-raft:
	$(GO) test ./internal/raft

race: race-maekawa race-raft

race-maekawa:
	$(GO) test -race ./internal/maekawa

race-raft:
	$(GO) test -race ./internal/raft

build: build-dashboard build-raft build-requester build-worker

build-dashboard:
	mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/dashboard ./cmd/dashboard

build-raft:
	mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/raft ./cmd/raft

build-requester:
	mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/requester ./cmd/requester

build-worker:
	mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/worker ./cmd/worker

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

RAFT ?= worker1:5001
DATA ?= demo-task

docker-request:
	docker compose run --rm requester --raft $(RAFT) --data "$(DATA)"

proto:
	$(PROTOC) \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_out=. \
		--go-grpc_opt=paths=source_relative \
		api/*/*.proto

proto-clean:
	find api/ -name "*.pb.go" -delete

clean:
	rm -rf $(BIN_DIR)
