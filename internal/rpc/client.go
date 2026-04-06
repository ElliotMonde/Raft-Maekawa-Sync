package rpc

import (
    "fmt"
    "time"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

// Dial opens a gRPC connection to addr with exponential backoff.
func Dial(addr string) (*grpc.ClientConn, error) {
    backoff := 100 * time.Millisecond
    var (
        conn *grpc.ClientConn
        err  error
    )
    for range 3 {
        conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
        if err == nil {
            return conn, nil
        }
        time.Sleep(backoff)
        backoff *= 2
    }
    return nil, fmt.Errorf("dial %s: %w", addr, err)
}