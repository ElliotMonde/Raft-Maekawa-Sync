// handle priority/voting logic

package maekawa

import (
	"context"
	"raft-maekawa-sync/api/maekawa"
	"sync"
	"time"

	"container/heap"
	"raft-maekawa-sync/internal/utils"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Worker struct {
	maekawa.UnimplementedMaekawaServer

	ID        int32   // Worker's ID
	quorum    []int32 // IDs of nodes in quorum set
	clientMgr *ClientManager

	Mu sync.Mutex // State for Voting (as a Voter), local mutex

	votedFor     int32
	currentReq   *maekawa.LockRequest
	isPinned     bool // is inquiring already; prevent multiple inquiry requests simultaneously
	requestQueue *utils.GenericMinHeap[*maekawa.LockRequest]

	votesReceived int
	inCS          bool      // whether self is in global CS
	committed     bool      // true once all votes received, before CS entry
	grantChan     chan bool // signal to enter CS

}

func NewWorker(id int32, quorum []int32) *Worker {
	h := utils.NewGenericMinHeap[*maekawa.LockRequest](
		func(a, b *maekawa.LockRequest) bool {
			return a.Timestamp < b.Timestamp
		},
	)

	heap.Init(h)
	return &Worker{
		ID:            id,
		quorum:        quorum,
		votedFor:      -1,
		requestQueue:  h,
		votesReceived: 0,
		committed:     false,
		grantChan:     make(chan bool, 1),
		clientMgr: new(ClientManager), // TODO: check init client mgr
	}
}

func (w *Worker) RequestForGlobalLock(ctx context.Context) error {
	w.Mu.Lock()
	w.votesReceived = 0
	w.inCS = false
	currTimestamp := time.Now().UnixNano()
	w.Mu.Unlock()

	for _, peerID := range w.quorum {
		go w.sendLockRequest(peerID, currTimestamp)
	}

	select {
	case <-w.grantChan: // if have all quorum votes, granted
		w.Mu.Lock()
		w.inCS = true
		w.Mu.Unlock()
		// TODO: do stuff in CS
		w.exitGlobalCS()
	case <-ctx.Done(): // context expired or cancelled, clean up pending votes
		w.Mu.Lock()
		w.votesReceived = 0
		w.committed = false
		w.Mu.Unlock()
		for _, peerID := range w.quorum {
			go w.sendRelease(peerID)
		}
		return ctx.Err()
	}
	return nil
}

func (w *Worker) sendLockRequest(targetID int32, timestamp int64) {
	client := w.clientMgr.GetClient(targetID)
	if client == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	_ = utils.ExecuteWithRetry(ctx, func() error {
		resp, err := client.RequestLock(ctx, &maekawa.LockRequest{
			NodeId:    w.ID,
			Timestamp: timestamp,
		})
		if err == nil && resp.Granted {
			w.Grant(ctx, &maekawa.GrantRequest{SenderId: targetID})
		}
		return err
	})
}

func (w *Worker) RequestLock(ctx context.Context, req *maekawa.LockRequest) (*maekawa.LockResponse, error) {
	if req.NodeId < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "NodeID cannot be negative")
	}

	w.Mu.Lock()
	defer w.Mu.Unlock() // Unlock even if Panic

	// if hasn't voted any node, vote for request's node
	if w.votedFor == -1 {
		w.votedFor = req.NodeId
		w.currentReq = req
		return &maekawa.LockResponse{NodeId: w.ID, Granted: true}, nil
	}

	// if voted for another request, compare timestamps
	if req.Timestamp < w.currentReq.Timestamp && !w.isPinned {
		// inquire the voted node, ask to yield
		w.isPinned = true
		go w.sendInquire(w.votedFor)
	}
	// append current request to request queue; only release when grant request in HandleYield
	heap.Push(w.requestQueue, req)
	return &maekawa.LockResponse{NodeId: w.ID, Granted: false}, nil

}

func (w *Worker) sendGrant(targetID int32) {
	client := w.clientMgr.GetClient(targetID)
	if client == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	_ = utils.ExecuteWithRetry(ctx, func() error {
		_, err := client.Grant(ctx, &maekawa.GrantRequest{SenderId: w.ID})
		return err
	})
}

func (w *Worker) Grant(ctx context.Context, req *maekawa.GrantRequest) (*maekawa.Empty, error) {
	w.Mu.Lock()
	defer w.Mu.Unlock()

	w.votesReceived++

	if w.votesReceived == len(w.quorum) {
		w.committed = true
		select {
		case w.grantChan <- true: // push signal into channel
		default: // Grant handler job done regardless
		}
	}

	return &maekawa.Empty{}, nil
}

func (w *Worker) exitGlobalCS() {
	w.Mu.Lock()
	w.inCS = false
	w.committed = false
	w.votesReceived = 0
	w.Mu.Unlock()

	for _, peerID := range w.quorum {
		go w.sendRelease(peerID)
	}
}
func (w *Worker) sendRelease(targetID int32) {
	client := w.clientMgr.GetClient(targetID)
	if client == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	_ = utils.ExecuteWithRetry(ctx, func() error {
		_, err := client.ReleaseLock(ctx, &maekawa.ReleaseRequest{NodeId: w.ID})
		return err
	})

}

func (w *Worker) ReleaseLock(ctx context.Context, req *maekawa.ReleaseRequest) (*maekawa.Empty, error) {
	w.Mu.Lock()
	defer w.Mu.Unlock()

	if req.NodeId == w.votedFor {
		w.votedFor = -1
		w.currentReq = nil

		if next := w.popNextFromHeap(); next != nil {
			go w.sendGrant(next.NodeId)
		}
	}

	return &maekawa.Empty{}, nil
}
