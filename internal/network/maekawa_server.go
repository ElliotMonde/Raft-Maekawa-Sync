package network

import (
	"context"
	"fmt"
	"net"

	maekawapb "raft-maekawa/proto/maekawapb"
	"google.golang.org/grpc"
)

// MaekawaHandler is the function the Worker provides to handle incoming messages.
// The network layer calls this whenever a MaekawaMsg arrives.
type MaekawaHandler func(msg *maekawapb.MaekawaMsg) error

// maekawaServer implements the generated MaekawaServiceServer interface.
// It is thin — it just receives the gRPC call and forwards to the handler.
type maekawaServer struct {
	maekawapb.UnimplementedMaekawaServiceServer
	handler MaekawaHandler
}

// Send is called by gRPC when another worker sends a MaekawaMsg to this node.
func (s *maekawaServer) Send(_ context.Context, msg *maekawapb.MaekawaMsg) (*maekawapb.Ack, error) {
	err := s.handler(msg)
	return &maekawapb.Ack{Ok: err == nil}, err
}

// MaekawaServer wraps a gRPC server and exposes Stop().
type MaekawaServer struct {
	grpc *grpc.Server
}

// StartMaekawaServer binds to addr and starts serving Maekawa RPCs.
// handler is called for every incoming MaekawaMsg — this will be Worker.HandleMessage.
func StartMaekawaServer(addr string, handler MaekawaHandler) (*MaekawaServer, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("maekawa server: listen %s: %w", addr, err)
	}

	grpcSrv := grpc.NewServer()
	maekawapb.RegisterMaekawaServiceServer(grpcSrv, &maekawaServer{handler: handler})

	// serve in background — caller blocks on other work or signal
	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			// server stopped — either via Stop() or a fatal error
		}
	}()

	return &MaekawaServer{grpc: grpcSrv}, nil
}

// Stop gracefully shuts down the gRPC server.
func (s *MaekawaServer) Stop() {
	s.grpc.GracefulStop()
}
