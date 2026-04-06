package rpc

import (
    "fmt"
    "net"

    "google.golang.org/grpc"
)

// Server wraps a gRPC server with start/stop lifecycle.
type Server struct {
    grpc *grpc.Server
}

func NewServer() *Server {
    return &Server{grpc: grpc.NewServer()}
}

// GRPC returns the underlying server so callers can register services.
// e.g. maekawa.RegisterMaekawaServer(s.GRPC(), worker)
func (s *Server) GRPC() *grpc.Server {
    return s.grpc
}

func (s *Server) Start(addr string) error {
    lis, err := net.Listen("tcp", addr)
    if err != nil {
        return fmt.Errorf("rpc server: listen %s: %w", addr, err)
    }
    go func() { _ = s.grpc.Serve(lis) }()
    return nil
}

func (s *Server) Stop() {
    s.grpc.GracefulStop()
}