package network

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	maekawapb "raft-maekawa/proto/maekawapb"
)

// MaekawaClient caches gRPC connections and stubs per peer.
type MaekawaClient struct {
	mu    sync.Mutex
	conns map[int]*grpc.ClientConn
	stubs map[int]maekawapb.MaekawaServiceClient
	peers map[int]string
}

// NewMaekawaClient creates a client for the given peer map.
func NewMaekawaClient(peers map[int]string) *MaekawaClient {
	return &MaekawaClient{
		conns: make(map[int]*grpc.ClientConn),
		stubs: make(map[int]maekawapb.MaekawaServiceClient),
		peers: peers,
	}
}

// Send delivers msg to targetID.
func (c *MaekawaClient) Send(targetID int, msg *maekawapb.MaekawaMsg) error {
	stub, err := c.getStub(targetID)
	if err != nil {
		return fmt.Errorf("maekawa client: get stub for node %d: %w", targetID, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = stub.Send(ctx, msg)
	if err != nil {
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

// getStub returns a cached stub or dials a new connection.
func (c *MaekawaClient) getStub(targetID int) (maekawapb.MaekawaServiceClient, error) {
	c.mu.Lock()
	if stub, ok := c.stubs[targetID]; ok {
		c.mu.Unlock()
		return stub, nil
	}
	c.mu.Unlock()

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
