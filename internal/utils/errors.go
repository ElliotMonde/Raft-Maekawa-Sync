package utils

import (
	"context"
	"math/rand"
	"time"

	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	DefaultBackoff time.Duration = 50 * time.Millisecond
	NumRetires     int32         = 3
)

func IsRetriable(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		// Not gRPC error
		return false
	}
	switch st.Code() {
	case codes.Unavailable, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

// retry with exponential backoff
func ExecuteWithRetry(ctx context.Context, operation func() error) error {
	backoff := DefaultBackoff
	for range NumRetires {
		err := operation()
		if err == nil || !IsRetriable(err) {
			return err
		}

		jitter := time.Duration(rand.Intn(10)) * time.Millisecond

		select {
		case <-time.After(backoff + jitter):
			backoff *= 2
		case <-ctx.Done(): // exit if global timeout or request cancelled
			return ctx.Err()
		}
	}
	return fmt.Errorf("operation failed after retries")
}
