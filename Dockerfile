# ── Stage 1: build frontend ──────────────────────────────────────────────────
FROM node:22-bookworm-slim AS frontend

WORKDIR /web
COPY web/package.json web/package-lock.json* ./
RUN npm ci
COPY web/ .
RUN npm run build

# ── Stage 2: build Go binaries ───────────────────────────────────────────────
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

# ── Stage 3: base runtime image ──────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime-base

RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates netcat-openbsd docker.io docker-compose \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY docker-compose.yml /app/docker-compose.yml

# ── Stage 4: Raft-only target ───────────────────────────────────────────────
FROM runtime-base AS raft

COPY --from=builder /out/raft /app/bin/raft

ENTRYPOINT ["/app/bin/raft"]

# ── Stage 5: Worker (Raft + Maekawa) target ─────────────────────────────────
FROM runtime-base AS worker

COPY --from=builder /out/worker /app/bin/worker
COPY --from=builder /out/requester /app/bin/requester
COPY --from=builder /out/dashboard /app/bin/dashboard
COPY --from=frontend /web/dist /app/web/dist

ENTRYPOINT ["/app/bin/worker"]

# ── Stage 6: Dashboard target ────────────────────────────────────────────────
FROM runtime-base AS dashboard

COPY --from=builder /out/dashboard /app/bin/dashboard
COPY --from=frontend /web/dist /app/web/dist

ENTRYPOINT ["/app/bin/dashboard"]
