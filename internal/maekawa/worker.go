// handle priority/voting logic

package maekawa

import (
	"sync"
	"context"
	"raft-maekawa-sync/api/maekawa"
)

type Worker struct {
	maekawa.UnimplementedMaekawaServer

	ID int32 // Worker's ID
	Quorum []int32 // IDs of nodes in quorum set
	Mu sync.Mutex // State for Voting (as a Voter)
	votedFor int32
	CurrentReq *maekawa.LockRequest
	RequestQueue [] *maekawa.LockRequest
}

func NewWorker(id int32, quorum []int32) *Worker {
	return &Worker{
		ID: id,
		Quorum: quorum,
		votedFor: -1,
		RequestQueue: make([]*maekawa.LockRequest, 0),
	}
}

func (w *Worker) RequestLock(ctx context.Context, req *maekawa.LockRequest) (*maekawa.LockResponse, error) {
	w.Mu.Lock()
	defer w.Mu.Unlock()

	// if hasn't voted any node, vote for request's node
	if w.votedFor == -1 {
		w.votedFor = req.NodeId
		w.CurrentReq = req
		return &maekawa.LockResponse{NodeId: w.ID, Granted: true}, nil
	}
	
	// if voted for another request, append current request to queue
	w.RequestQueue = append(w.RequestQueue, req)
	return &maekawa.LockResponse{NodeId: w.ID, Granted: false}, nil

}
