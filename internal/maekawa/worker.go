// handle priority/voting logic

package maekawa

import (
	"context"
	"raft-maekawa-sync/api/maekawa"
	"sync"

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
	}
}

func (w *Worker) HandleRequestLock(ctx context.Context, req *maekawa.LockRequest) (*maekawa.LockResponse, error) {
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
		// if yield response true, vote for current req node
		w.isPinned = true
		go w.sendInquire(w.votedFor)
	}
	// append current request to request queue; only release when grant request in HandleYield
	heap.Push(w.requestQueue, req)
	return &maekawa.LockResponse{NodeId: w.ID, Granted: false}, nil

}

func (w *Worker) sendGrant(targetID int32) {
	
}

func (w *Worker) Grant(ctx context.Context, req *maekawa.GrantRequest) (*maekawa.Empty, error) {
	w.Mu.Lock()
	defer w.Mu.Unlock()

	w.votesReceived++

	if w.votesReceived == len(w.quorum) {
		// TODO: trigger logic to enter global CS, e.g. sending signal to channel that RequestForGlobalLock is waiting on
	}

	return &maekawa.Empty{}, nil
}

func (w *Worker) RequestForGlobalLock()
