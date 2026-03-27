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