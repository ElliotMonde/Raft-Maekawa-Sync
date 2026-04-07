// implement Yield and Inquire, circular-wait prevention logic

package maekawa

import (
	"container/heap"
	"context"
	maekawapb "raft-maekawa-sync/api/maekawa"
	"raft-maekawa-sync/internal/utils"
	"time"
)

const (
	DefaultTimeout = 3 * time.Second
)

func (w *Worker) sendInquire(targetID int32, timestamp int64) {
	if targetID == w.ID {
		w.Inquire(context.Background(), &maekawapb.InquireRequest{SenderId: w.ID, Timestamp: timestamp})
		return
	}

	client := w.clientMgr.GetClient(targetID)
	if client == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	_ = utils.ExecuteWithRetry(ctx, func() error {
		_, err := client.Inquire(ctx, &maekawapb.InquireRequest{
			SenderId:  w.ID,
			Timestamp: timestamp,
		})
		return err
	})
}

func (w *Worker) Inquire(ctx context.Context, req *maekawapb.InquireRequest) (*maekawapb.Empty, error) {
	w.Mu.Lock()
	defer w.Mu.Unlock()

	// Ignore if we're no longer waiting for a lock or this is for a different round
	if w.ownReqTimestamp < 0 || req.Timestamp != w.ownReqTimestamp {
		return &maekawapb.Empty{}, nil
	}
	// Already yielded to this sender this round
	if prev, ok := w.yieldedTo[req.SenderId]; ok && prev == req.Timestamp {
		return &maekawapb.Empty{}, nil
	}

	if w.inCS || w.committed {
		return &maekawapb.Empty{}, nil
	}

	if w.grantsReceived[req.SenderId] {
		// We hold this voter's grant yield it back now
		w.yieldedTo[req.SenderId] = req.Timestamp
		delete(w.grantsReceived, req.SenderId)
		w.votesReceived--
		go w.sendYield(req.SenderId, req.Timestamp)
	} else {
		// Grant hasn't arrived yet; remember to yield when it does.
		w.pendingInquiries[req.SenderId] = req.Timestamp
	}

	return &maekawapb.Empty{}, nil
}

func (w *Worker) sendYield(senderID int32, timestamp int64) {
	if senderID == w.ID {
		w.Yield(context.Background(), &maekawapb.YieldRequest{SenderId: w.ID, Timestamp: timestamp})
		return
	}

	client := w.clientMgr.GetClient(senderID)
	if client == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	_ = utils.ExecuteWithRetry(ctx, func() error {
		_, err := client.Yield(ctx, &maekawapb.YieldRequest{
			SenderId:  w.ID,
			Timestamp: timestamp,
		})
		return err
	})
}

func (w *Worker) Yield(ctx context.Context, req *maekawapb.YieldRequest) (*maekawapb.Empty, error) {
	w.Mu.Lock()
	defer w.Mu.Unlock()

	if req.SenderId != w.votedFor {
		return &maekawapb.Empty{}, nil
	}

	// Push the yielding request back so it can compete again later.
	if w.currentReq != nil {
		heap.Push(w.requestQueue, w.currentReq)
	}

	w.votedFor = -1
	w.currentReq = nil
	w.isPinned = false

	if next := w.popNextFromHeap(); next != nil {
		go w.sendGrant(next.NodeId, next.Timestamp)
	}

	return &maekawapb.Empty{}, nil
}

func (w *Worker) popNextFromHeap() *maekawapb.LockRequest {
	if w.requestQueue.Len() == 0 {
		return nil
	}
	next := heap.Pop(w.requestQueue).(*maekawapb.LockRequest)
	w.votedFor = next.NodeId
	w.currentReq = next
	return next
}
