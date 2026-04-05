package maekawa

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"raft-maekawa/internal/models"
	"raft-maekawa/internal/network"
	maekawapb "raft-maekawa/proto/maekawapb"
)

type WorkerState uint8

const (
	StateReleased WorkerState = iota
	StateWanting
	StateHeld
)

type voterState struct {
	locked      bool
	lockedFor   int32
	lockedClock int64
	inquired    bool
	waitQueue   RequestHeap
}

type Worker struct {
	mu  sync.Mutex
	log *slog.Logger

	ID     int
	N      int
	Quorum []int
	peers  map[int]string

	liveWorkers   map[int]bool
	activeWorkers map[int]bool

	state           WorkerState
	replyCount      int
	currentReqClock int64
	neededVotes     int
	csEnter         chan struct{}
	csErr           error
	currentTask     *models.Task
	execCancel      context.CancelFunc
	taskQueue       chan *models.Task
	taskStatus      map[string]models.TaskStatus
	queuedTasks     map[string]bool
	taskCommits     map[string]*taskCommitState
	yieldedTo       map[int32]int64
	reporter        TaskEventReporter
	executor        TaskExecutor

	voter voterState

	clock int64

	rpc    *network.MaekawaRPC
	server *network.MaekawaServer
}

func NewWorker(id, n int, peers map[int]string, listenAddr string) (*Worker, error) {
	live := make(map[int]bool, n)
	active := make(map[int]bool, n)
	for i := 0; i < n; i++ {
		live[i] = true
		active[i] = true
	}

	w := &Worker{
		ID:            id,
		N:             n,
		Quorum:        QuorumFor(id, n),
		peers:         peers,
		liveWorkers:   live,
		activeWorkers: active,
		state:         StateReleased,
		csEnter:       make(chan struct{}),
		taskQueue:     make(chan *models.Task, 64),
		taskStatus:    make(map[string]models.TaskStatus),
		queuedTasks:   make(map[string]bool),
		taskCommits:   make(map[string]*taskCommitState),
		log:           slog.Default().With("worker", id),
	}
	w.voter.waitQueue = make(RequestHeap, 0)
	w.yieldedTo = make(map[int32]int64)

	w.rpc = network.NewMaekawaRPC(id, peers)

	srv, err := network.StartMaekawaServer(listenAddr, w.HandleMessage)
	if err != nil {
		return nil, fmt.Errorf("worker %d: start server: %w", id, err)
	}
	w.server = srv

	return w, nil
}

func (w *Worker) Stop() {
	w.server.Stop()
	w.rpc.Close()
}

func (w *Worker) RequestCS(ctx context.Context, task *models.Task) error {
	w.mu.Lock()
	lq := w.liveQuorum()
	required := len(w.Quorum)
	if len(lq) < required {
		w.mu.Unlock()
		return fmt.Errorf("worker %d: live quorum too small (%d < %d), refusing CS for task %s",
			w.ID, len(lq), required, task.ID)
	}
	w.state = StateWanting
	w.currentTask = task
	w.replyCount = 0
	w.neededVotes = len(lq)
	w.yieldedTo = make(map[int32]int64)
	w.csEnter = make(chan struct{})
	clk := w.tick()
	w.currentReqClock = clk

	selfReqMsg := &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_REQUEST,
		SenderId: int32(w.ID),
		Clock:    clk,
		TaskId:   task.ID,
	}
	selfFns := w.onRequest(selfReqMsg)
	selfGranted := w.voter.locked && w.voter.lockedFor == int32(w.ID) && w.voter.lockedClock == clk
	if selfGranted {
		selfReplyMsg := &maekawapb.MaekawaMsg{
			Type:     maekawapb.MsgType_REPLY,
			SenderId: int32(w.ID),
			Clock:    clk,
			TaskId:   task.ID,
		}
		w.onReply(selfReplyMsg)
	}
	var selfOut []sendFn
	if !selfGranted {
		selfOut = selfFns
	}
	w.mu.Unlock()

	for _, fn := range selfOut {
		if err := fn(); err != nil {
			w.log.Warn("failed to send self-voter INQUIRE", "err", err)
		}
	}

	for _, peerID := range lq {
		if peerID == w.ID {
			continue
		}
		if err := w.rpc.SendRequest(peerID, task.ID, clk); err != nil {
			w.log.Warn("failed to send REQUEST", "to", peerID, "err", err)
		}
	}

	select {
	case <-w.csEnter:
	case <-ctx.Done():
		w.mu.Lock()
		w.state = StateReleased
		w.currentTask = nil
		quorum := append([]int(nil), w.Quorum...)
		out := w.selfReleaseVoterLocked(clk, task.ID)
		w.mu.Unlock()
		for _, fn := range out {
			if err := fn(); err != nil {
				w.log.Warn("ctx-cancel self-release: failed to send REPLY", "err", err)
			}
		}
		for _, peerID := range quorum {
			if peerID == w.ID {
				continue
			}
			if err := w.rpc.SendRelease(peerID, task.ID, clk); err != nil {
				w.log.Warn("ctx-cancel: failed to send RELEASE", "to", peerID, "err", err)
			}
		}
		return ctx.Err()
	}

	w.mu.Lock()
	err := w.csErr
	w.csErr = nil
	if err != nil {
		w.state = StateReleased
		w.currentTask = nil
		quorum := append([]int(nil), w.Quorum...)
		out := w.selfReleaseVoterLocked(clk, task.ID)
		w.mu.Unlock()
		for _, fn := range out {
			if fn2err := fn(); fn2err != nil {
				w.log.Warn("csErr self-release: failed to send REPLY", "err", fn2err)
			}
		}
		for _, peerID := range quorum {
			if peerID == w.ID {
				continue
			}
			if sendErr := w.rpc.SendRelease(peerID, task.ID, clk); sendErr != nil {
				w.log.Warn("csErr: failed to send RELEASE", "to", peerID, "err", sendErr)
			}
		}
		return err
	}
	w.state = StateHeld
	w.mu.Unlock()

	w.log.Info("entered CS", "task", task.ID)
	return nil
}

func (w *Worker) selfReleaseVoterLocked(reqClock int64, taskID string) []sendFn {
	if w.voter.locked && w.voter.lockedFor == int32(w.ID) && w.voter.lockedClock == reqClock {
		return w.grantNext(taskID)
	}
	HeapRemove(&w.voter.waitQueue, int32(w.ID), reqClock)
	return nil
}

func (w *Worker) ReleaseCS() {
	w.mu.Lock()
	w.state = StateReleased
	task := w.currentTask
	w.currentTask = nil
	quorum := w.Quorum
	reqClock := w.currentReqClock
	w.tick()
	var selfOut []sendFn
	if task != nil {
		selfOut = w.selfReleaseVoterLocked(reqClock, task.ID)
	}
	w.mu.Unlock()

	if task == nil {
		return
	}

	for _, fn := range selfOut {
		if err := fn(); err != nil {
			w.log.Warn("self-release: failed to send REPLY to next waiter", "err", err)
		}
	}

	for _, peerID := range quorum {
		if peerID == w.ID {
			continue
		}
		if err := w.rpc.SendRelease(peerID, task.ID, reqClock); err != nil {
			w.log.Warn("failed to send RELEASE", "to", peerID, "err", err)
		}
	}

	w.log.Info("released CS", "task", task.ID)
}

func (w *Worker) HandleMessage(msg *maekawapb.MaekawaMsg) error {
	var out []sendFn

	w.mu.Lock()
	w.updateClock(msg.Clock)

	switch msg.Type {
	case maekawapb.MsgType_REQUEST:
		out = w.onRequest(msg)

	case maekawapb.MsgType_REPLY:
		out = w.onReply(msg)

	case maekawapb.MsgType_RELEASE:
		out = w.onRelease(msg)

	case maekawapb.MsgType_INQUIRE:
		out = w.onInquire(msg)

	case maekawapb.MsgType_YIELD:
		out = w.onYield(msg)

	case maekawapb.MsgType_FAILED:
		out = w.onFailed(msg)
	}
	w.mu.Unlock()

	for _, fn := range out {
		if err := fn(); err != nil {
			w.log.Warn("failed to send message", "err", err)
		}
	}
	return nil
}

type sendFn = func() error

func (w *Worker) onFailed(_ *maekawapb.MaekawaMsg) []sendFn {
	if w.state != StateWanting {
		return nil
	}
	w.csErr = fmt.Errorf("worker %d: received FAILED from quorum member", w.ID)
	select {
	case <-w.csEnter:
	default:
		close(w.csEnter)
	}
	return nil
}

func (w *Worker) onReply(_ *maekawapb.MaekawaMsg) []sendFn {
	if w.state != StateWanting {
		return nil
	}
	w.replyCount++
	if w.replyCount == w.neededVotes {
		select {
		case <-w.csEnter:
		default:
			close(w.csEnter)
		}
	}
	return nil
}

func (w *Worker) onRelease(msg *maekawapb.MaekawaMsg) []sendFn {
	if !w.voter.locked {
		HeapRemove(&w.voter.waitQueue, msg.SenderId, msg.Clock)
		return nil
	}
	if w.voter.lockedFor == msg.SenderId && w.voter.lockedClock == msg.Clock {
		return w.grantNext(msg.TaskId)
	}

	HeapRemove(&w.voter.waitQueue, msg.SenderId, msg.Clock)
	return nil
}
