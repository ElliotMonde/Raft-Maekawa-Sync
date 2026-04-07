// handle priority/voting logic

package maekawa

import (
	"context"
	"fmt"
	maekawapb "raft-maekawa-sync/api/maekawa"
	"sync"
	"time"

	"container/heap"
	"raft-maekawa-sync/internal/models"
	"raft-maekawa-sync/internal/utils"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Worker struct {
	maekawapb.UnimplementedMaekawaServer

	ID         int32   // Worker's ID
	quorum     []int32 // IDs of nodes in quorum set
	membership ClusterMembership
	clientMgr  *ClientManager

	Mu sync.Mutex // State for Voting (as a Voter), local mutex

	votedFor     int32
	currentReq   *maekawapb.LockRequest
	isPinned     bool // is inquiring already; prevent multiple inquiry requests simultaneously
	requestQueue *utils.GenericMinHeap[*maekawapb.LockRequest]

	taskQueue     chan *models.Task
	canceledTasks map[string]bool
	executor      TaskExecutor

	votesReceived    int
	grantsReceived   map[int32]bool // which quorum members have granted us this round
	inCS             bool           // whether self is in global CS
	committed        bool           // true once all votes received, before CS entry
	grantChan        chan bool      // signal to enter CS
	clock            int64
	ownReqTimestamp  int64
	yieldedTo        map[int32]int64 // voters we yielded to this round to avoid double-yield
	pendingInquiries map[int32]int64 // inquiries received before the sender's grant arrived
}

func NewWorker(id int32, quorum []int32, membership ClusterMembership) *Worker {
	h := utils.NewGenericMinHeap[*maekawapb.LockRequest](
		func(a, b *maekawapb.LockRequest) bool {
			if a.Timestamp != b.Timestamp {
				return a.Timestamp < b.Timestamp
			}
			return a.NodeId < b.NodeId
		},
	)
	heap.Init(h)
	return &Worker{
		ID:               id,
		quorum:           quorum,
		membership:       membership,
		votedFor:         -1,
		ownReqTimestamp:  -1,
		requestQueue:     h,
		grantChan:        make(chan bool, 1),
		taskQueue:        make(chan *models.Task, 64),
		canceledTasks:    make(map[string]bool),
		clientMgr:        NewClientManager(),
		grantsReceived:   make(map[int32]bool),
		yieldedTo:        make(map[int32]int64),
		pendingInquiries: make(map[int32]int64),
	}
}

func (w *Worker) SetTaskExecutor(executor TaskExecutor) {
	w.Mu.Lock()
	defer w.Mu.Unlock()
	w.executor = executor
}

func (w *Worker) RequestForGlobalLock(ctx context.Context) error {
	w.Mu.Lock()
	w.votesReceived = 0
	w.grantsReceived = make(map[int32]bool)
	w.inCS = false
	w.committed = false
	w.isPinned = false
	w.pendingInquiries = make(map[int32]int64)
	currTimestamp := w.tick()
	w.ownReqTimestamp = currTimestamp
	w.yieldedTo = make(map[int32]int64)

	select { // drain stale signal from previous round
	case <-w.grantChan:
	default:
	}

	for _, peerID := range w.quorum {
		if !w.membership.IsAlive(peerID) {
			w.Mu.Unlock()
			return fmt.Errorf("quorum member %d is unreachable", peerID)
		}
	}
	quorum := append([]int32(nil), w.quorum...) // snapshot before releasing lock
	w.Mu.Unlock()

	for _, peerID := range quorum {
		go w.sendLockRequest(peerID, currTimestamp)
	}

	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case success := <-w.grantChan:
			if !success {
				return fmt.Errorf("membership changed during lock acquisition")
			}
			w.Mu.Lock()
			w.inCS = true
			w.Mu.Unlock()
			return nil
		case <-ctx.Done():
			w.Mu.Lock()
			reqTimestamp := w.ownReqTimestamp
			quorum = append([]int32(nil), w.quorum...)
			w.votesReceived = 0
			w.grantsReceived = make(map[int32]bool)
			w.committed = false
			w.ownReqTimestamp = -1
			w.Mu.Unlock()
			for _, peerID := range quorum {
				go w.sendRelease(peerID, reqTimestamp)
			}
			return ctx.Err()
		case <-ticker.C:
			w.Mu.Lock()
			quorum = append([]int32(nil), w.quorum...)
			w.Mu.Unlock()
			for _, peerID := range quorum {
				if !w.membership.IsAlive(peerID) {
					w.Mu.Lock()
					reqTimestamp := w.ownReqTimestamp
					quorum = append([]int32(nil), w.quorum...)
					w.votesReceived = 0
					w.grantsReceived = make(map[int32]bool)
					w.committed = false
					w.ownReqTimestamp = -1
					w.Mu.Unlock()
					for _, p := range quorum {
						go w.sendRelease(p, reqTimestamp)
					}
					return fmt.Errorf("quorum member %d is unreachable", peerID)
				}
			}
		}
	}
}

func (w *Worker) sendLockRequest(targetID int32, timestamp int64) {
	if targetID == w.ID {
		resp, _ := w.RequestLock(context.Background(), &maekawapb.LockRequest{NodeId: w.ID, Timestamp: timestamp})
		if resp.Granted {
			w.Grant(context.Background(), &maekawapb.GrantRequest{SenderId: w.ID, Timestamp: timestamp})
		}
		return
	}

	client := w.clientMgr.GetClient(targetID)
	if client == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	_ = utils.ExecuteWithRetry(ctx, func() error {
		resp, err := client.RequestLock(ctx, &maekawapb.LockRequest{
			NodeId:    w.ID,
			Timestamp: timestamp,
		})
		if err == nil && resp.Granted {
			w.Grant(ctx, &maekawapb.GrantRequest{SenderId: targetID, Timestamp: timestamp})
		}
		return err
	})
}

func (w *Worker) RequestLock(ctx context.Context, req *maekawapb.LockRequest) (*maekawapb.LockResponse, error) {
	if req.NodeId < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "NodeID cannot be negative")
	}
	w.updateClock(req.Timestamp)

	w.Mu.Lock()
	defer w.Mu.Unlock()

	if w.votedFor == -1 {
		w.votedFor = req.NodeId
		w.currentReq = req
		return &maekawapb.LockResponse{NodeId: w.ID, Granted: true}, nil
	}

	if !w.isPinned {
		if req.Timestamp < w.currentReq.Timestamp || (req.Timestamp == w.currentReq.Timestamp && req.NodeId < w.currentReq.NodeId) {
			w.isPinned = true
			go w.sendInquire(w.votedFor, w.currentReq.Timestamp)
		}
	}
	heap.Push(w.requestQueue, req)
	return &maekawapb.LockResponse{NodeId: w.ID, Granted: false}, nil
}

func (w *Worker) sendGrant(targetID int32, timestamp int64) {
	if targetID == w.ID {
		w.Grant(context.Background(), &maekawapb.GrantRequest{SenderId: w.ID, Timestamp: timestamp})
		return
	}

	client := w.clientMgr.GetClient(targetID)
	if client == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	_ = utils.ExecuteWithRetry(ctx, func() error {
		_, err := client.Grant(ctx, &maekawapb.GrantRequest{SenderId: w.ID, Timestamp: timestamp})
		return err
	})
}

func (w *Worker) Grant(ctx context.Context, req *maekawapb.GrantRequest) (*maekawapb.Empty, error) {
	w.Mu.Lock()
	defer w.Mu.Unlock()

	if w.ownReqTimestamp < 0 || w.inCS || req.Timestamp != w.ownReqTimestamp {
		return &maekawapb.Empty{}, nil
	}

	// Re-grant after we yielded this sender's vote back to them.
	if prev, ok := w.yieldedTo[req.SenderId]; ok && prev == req.Timestamp {
		delete(w.yieldedTo, req.SenderId)
		w.grantsReceived[req.SenderId] = true
		w.votesReceived++
		if w.votesReceived == len(w.quorum) {
			w.committed = true
			select {
			case w.grantChan <- true:
			default:
			}
		}
		return &maekawapb.Empty{}, nil
	}

	if w.grantsReceived[req.SenderId] {
		return &maekawapb.Empty{}, nil // duplicate
	}

	w.grantsReceived[req.SenderId] = true
	w.votesReceived++

	// If this sender inquired before their grant arrived, yield back now.
	if pendingTs, ok := w.pendingInquiries[req.SenderId]; ok && pendingTs == req.Timestamp {
		delete(w.pendingInquiries, req.SenderId)
		w.yieldedTo[req.SenderId] = pendingTs
		delete(w.grantsReceived, req.SenderId)
		w.votesReceived--
		go w.sendYield(req.SenderId, pendingTs)
		return &maekawapb.Empty{}, nil
	}

	if w.votesReceived == len(w.quorum) {
		w.committed = true
		select {
		case w.grantChan <- true:
		default:
		}
	}
	return &maekawapb.Empty{}, nil
}

func (w *Worker) exitGlobalCS() {
	w.tick()
	w.Mu.Lock()
	reqTimestamp := w.ownReqTimestamp
	quorum := append([]int32(nil), w.quorum...)
	w.inCS = false
	w.committed = false
	w.votesReceived = 0
	w.grantsReceived = make(map[int32]bool)
	w.ownReqTimestamp = -1
	w.Mu.Unlock()

	for _, peerID := range quorum {
		go w.sendRelease(peerID, reqTimestamp)
	}
}

func (w *Worker) sendRelease(targetID int32, timestamp int64) {
	if targetID == w.ID {
		w.ReleaseLock(context.Background(), &maekawapb.ReleaseRequest{NodeId: w.ID, Timestamp: timestamp})
		return
	}

	client := w.clientMgr.GetClient(targetID)
	if client == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	_ = utils.ExecuteWithRetry(ctx, func() error {
		_, err := client.ReleaseLock(ctx, &maekawapb.ReleaseRequest{NodeId: w.ID, Timestamp: timestamp})
		return err
	})
}

func (w *Worker) ReleaseLock(ctx context.Context, req *maekawapb.ReleaseRequest) (*maekawapb.Empty, error) {
	w.Mu.Lock()
	defer w.Mu.Unlock()

	if req.NodeId == w.votedFor && w.currentReq != nil && req.Timestamp == w.currentReq.Timestamp {
		w.votedFor = -1
		w.currentReq = nil
		w.isPinned = false

		if next := w.popNextFromHeap(); next != nil {
			go w.sendGrant(next.NodeId, next.Timestamp)
		}
		return &maekawapb.Empty{}, nil
	}

	// Release a queued (not yet granted) request.
	w.requestQueue.RemoveIf(func(item *maekawapb.LockRequest) bool {
		return item.NodeId == req.NodeId && item.Timestamp == req.Timestamp
	})

	return &maekawapb.Empty{}, nil
}
