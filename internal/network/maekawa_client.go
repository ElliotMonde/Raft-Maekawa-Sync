package network

import (
	"context"
	"fmt"
	"sync"
	"time"

	maekawapb "raft-maekawa/proto/maekawapb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// MaekawaClient holds one persistent gRPC connection per peer worker.
// Connections are created lazily on first Send and reused after that.
type MaekawaClient struct {
	mu    sync.Mutex
	conns map[int]*grpc.ClientConn          // nodeID -> open connection
	stubs map[int]maekawapb.MaekawaServiceClient // nodeID -> RPC stub
	peers map[int]string                    // nodeID -> "host:port"
}

// NewMaekawaClient creates a client for the given peer map.
// peers maps nodeID -> "host:port" for every worker in the cluster.
func NewMaekawaClient(peers map[int]string) *MaekawaClient {
	return &MaekawaClient{
		conns: make(map[int]*grpc.ClientConn),
		stubs: make(map[int]maekawapb.MaekawaServiceClient),
		peers: peers,
	}
}

// Send delivers msg to the worker identified by targetID.
// It dials a new connection if one does not exist yet, with retries.
func (c *MaekawaClient) Send(targetID int, msg *maekawapb.MaekawaMsg) error {
	stub, err := c.getStub(targetID)
	if err != nil {
		return fmt.Errorf("maekawa client: get stub for node %d: %w", targetID, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = stub.Send(ctx, msg)
	if err != nil {
		// connection may be stale — remove it so next Send re-dials
		c.mu.Lock()
		delete(c.conns, targetID)
		delete(c.stubs, targetID)
		c.mu.Unlock()
		return fmt.Errorf("maekawa client: send to node %d: %w", targetID, err)
	}
	return nil
}

// Close closes all open connections.
func (c *MaekawaClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, conn := range c.conns {
		conn.Close()
		delete(c.conns, id)
		delete(c.stubs, id)
	}
}

// getStub returns a cached stub or dials a new connection with backoff.
func (c *MaekawaClient) getStub(targetID int) (maekawapb.MaekawaServiceClient, error) {
	c.mu.Lock()
	if stub, ok := c.stubs[targetID]; ok {
		c.mu.Unlock()
		return stub, nil
	}
	c.mu.Unlock()

	// dial with exponential backoff: 100ms, 200ms, 400ms
	addr, ok := c.peers[targetID]
	if !ok {
		return nil, fmt.Errorf("unknown peer node %d", targetID)
	}

	var conn *grpc.ClientConn
	var err error
	backoff := 100 * time.Millisecond
	for attempt := 0; attempt < 3; attempt++ {
		conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			break
		}
		time.Sleep(backoff)
		backoff *= 2
	}
	if err != nil {
		return nil, fmt.Errorf("dial node %d at %s: %w", targetID, addr, err)
	}

	stub := maekawapb.NewMaekawaServiceClient(conn)

	c.mu.Lock()
	c.conns[targetID] = conn
	c.stubs[targetID] = stub
	c.mu.Unlock()

	return stub, nil
}
