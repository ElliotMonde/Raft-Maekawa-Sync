FROM golang:1.26-bookworm AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN mkdir -p /out \
	&& CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/worker ./cmd/worker \
	&& CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/raft ./cmd/raft \
	&& CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/requester ./cmd/requester \
	&& CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/dashboard ./cmd/dashboard

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates netcat-openbsd \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /out/worker /app/bin/worker
COPY --from=builder /out/raft /app/bin/raft
COPY --from=builder /out/requester /app/bin/requester
COPY --from=builder /out/dashboard /app/bin/dashboard

ENTRYPOINT ["/app/bin/worker"]
