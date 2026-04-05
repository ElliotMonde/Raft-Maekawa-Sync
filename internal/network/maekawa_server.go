package network

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"
	maekawapb "raft-maekawa/proto/maekawapb"
)

// MaekawaHandler handles an incoming Maekawa message.
type MaekawaHandler func(msg *maekawapb.MaekawaMsg) error

type maekawaServer struct {
	maekawapb.UnimplementedMaekawaServiceServer
	handler MaekawaHandler
}

func (s *maekawaServer) Send(_ context.Context, msg *maekawapb.MaekawaMsg) (*maekawapb.Ack, error) {
	err := s.handler(msg)
	return &maekawapb.Ack{Ok: err == nil}, err
}

// MaekawaServer wraps the gRPC server.
type MaekawaServer struct {
	grpc *grpc.Server
}

// StartMaekawaServer binds to addr and starts serving Maekawa RPCs.
func StartMaekawaServer(addr string, handler MaekawaHandler) (*MaekawaServer, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("maekawa server: listen %s: %w", addr, err)
	}

	grpcSrv := grpc.NewServer()
	maekawapb.RegisterMaekawaServiceServer(grpcSrv, &maekawaServer{handler: handler})

	go func() {
		_ = grpcSrv.Serve(lis)
	}()

	return &MaekawaServer{grpc: grpcSrv}, nil
}

// Stop gracefully shuts down the gRPC server.
func (s *MaekawaServer) Stop() {
	s.grpc.GracefulStop()
}
