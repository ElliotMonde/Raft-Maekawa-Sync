// implement Yield and Inquire, circular-wait prevention logic

package maekawa

import (
	"container/heap"
	"context"
	"raft-maekawa-sync/api/maekawa"
	"raft-maekawa-sync/internal/utils"
	"time"
)

const (
	DefaultTimeout = 200 * time.Millisecond
)

func (w *Worker) sendInquire(targetID int32) {
	client := w.clientMgr.GetClient(targetID)
	if client == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	_ = utils.ExecuteWithRetry(ctx, func() error {
		_, err := client.Inquire(ctx, &maekawa.InquireRequest{SenderId: w.ID})
		return err
	})
}

func (w *Worker) handleYield(ctx context.Context, req *maekawa.YieldRequest) (*maekawa.Empty, error) {
	w.Mu.Lock()

	// Ensure the yielding node is actually the node this worker voted for
	if req.SenderId != w.votedFor {
		return &maekawa.Empty{}, nil
	}

	w.votedFor = -1
	w.currentReq = nil
	w.isPinned = false

	var next *maekawa.LockRequest

	if w.requestQueue.Len() > 0 {
		next = heap.Pop(w.requestQueue).(*maekawa.LockRequest)
		w.votedFor = next.NodeId
		w.currentReq = next
	}

	w.Mu.Unlock()

	if next != nil {
		go w.sendGrant(next.NodeId)
	}

	return &maekawa.Empty{}, nil
}

func (w *Worker) sendGrant(targetId int32)
