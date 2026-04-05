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

func (w *Worker) Inquire(ctx context.Context, req *maekawa.InquireRequest) (*maekawa.Empty, error) {
	w.Mu.Lock()
	defer w.Mu.Unlock()

	if !w.inCS {
		if w.votesReceived > 0 {
			w.votesReceived--
		}
		w.Mu.Unlock()
		go w.sendYield(req.SenderId)
		w.Mu.Lock()
	}

	return &maekawa.Empty{}, nil
}

func (w *Worker) sendYield(senderID int32) {
	client := w.clientMgr.GetClient(senderID)
	if client == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	_ = utils.ExecuteWithRetry(ctx, func() error {
		_, err := client.Yield(ctx, &maekawa.YieldRequest{
			SenderId: w.ID,
		})
		return err
	})
}

func (w *Worker) Yield(ctx context.Context, req *maekawa.YieldRequest) (*maekawa.Empty, error) {
	w.Mu.Lock()
	defer w.Mu.Unlock()

	if req.SenderId != w.votedFor {
		// not the correct node that has this node's vote
		return &maekawa.Empty{}, nil
	}

	w.votedFor = -1
	w.isPinned = false

	if next := w.popNextFromHeap(); next != nil {
		go w.sendGrant(next.NodeId)
	}

	return &maekawa.Empty{}, nil
}

func (w *Worker) popNextFromHeap() *maekawa.LockRequest {

	if w.requestQueue.Len() == 0 {
		return nil
	}

	next := heap.Pop(w.requestQueue).(*maekawa.LockRequest)
	w.votedFor = next.NodeId
	w.currentReq = next
	return next
}
