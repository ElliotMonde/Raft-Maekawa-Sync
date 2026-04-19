<img width="3470" height="1696" alt="design_architecture" src="https://github.com/user-attachments/assets/441ed63c-e8e9-48b2-9b96-1640334a09df" />


## Project Structure
```/
├── docker-compose.yml
├── go.mod                  # Project dependencies and module name
├── go.sum
├── Makefile                # make build, make up, make test
├── .gitignore              # Updated for Go and Docker
├── cmd/                    # Entry points for your binaries
│   ├── raft/               # Main for Raft nodes
│   │   └── main.go
│   ├── worker/             # Main for Maekawa workers
│   │   └── main.go
│   ├── requester/          # Main for the client CLI
│   │   └── main.go
│   └── dashboard/          # Main for the visualizer
│       └── main.go
├── internal/               # Private logic (not importable by other projects)
│   ├── raft/               # Raft consensus logic (State machine, Election)
│   ├── maekawa/            # Maekawa's algorithm implementation 
│   ├── network/            # Native TCP/UDP handlers and RPC logic 
│   ├── models/             # Shared Go structs for messages (RequestVote, etc.)
│   └── dashboard/          # Dashboard backend logic
├── pkg/                    # Shared code (if you want it to be public/reusable)
├── web/                    # Frontend assets (HTML/JS for the dashboard)
├── tests/                  # Integration and unit tests 
└── docs/                   # Final report and diagrams
```

## Docker Cluster

Start the 3-node Raft + 3-node worker cluster:

```bash
make docker-up
# open http://localhost:8080
```

Watch logs:

```bash
make docker-logs
```

Submit a task from the host:

```bash
go run ./cmd/requester --raft 127.0.0.1:5001 --data "demo-task"
```

If node `5001` is not the leader, the requester will print the leader ID. You can retry against `127.0.0.1:5002` or `127.0.0.1:5003`.

Submit a task from inside Docker:

```bash
make docker-request DATA="demo-task"
```

You can also target a specific Raft container address:

```bash
make docker-request RAFT=raft-node2:5002 DATA="demo-task"
```

Check worker health:

```bash
docker compose -p raft-maekawa-sync ps
```

The dashboard frontend is served from the `dashboard` container at `http://localhost:8080`. That dashboard also has access to the Docker socket, so the frontend can stop/start worker containers directly.

Stop the cluster:

```bash
make docker-down
```

Each Raft node stores its state in its own Docker volume so restarts preserve state.
