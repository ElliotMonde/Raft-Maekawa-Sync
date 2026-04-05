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

// requesting states
type WorkerState uint8

const (
	StateReleased WorkerState = iota // not requesting CS
	StateWanting                     // sent REQUESTs, waiting for votes
	StateHeld                        // inside CS, executing task
)

// voter states
type voterState struct {
	locked      bool        // granted a vote?
	lockedFor   int32       // senderID granted to
	lockedClock int64       // clock of the request granted
	inquired    bool        // sent INQUIRE to lockedFor?
	waitQueue   RequestHeap // pending REQUESTs ordered by (clock, senderID)
}

type Worker struct {
	mu  sync.Mutex
	log *slog.Logger

	// identity
	ID     int
	N      int            // total workers
	Quorum []int          // precomputed quorum set (includes self)
	peers  map[int]string // nodeID -> "host:port"

	// fault tolerance: set of workers known to be alive
	// initialised to all workers; updated by Raft via NotifyWorkerDown/Up
	liveWorkers map[int]bool
	// active membership set (cluster config)
	// membership in cluster
	activeWorkers map[int]bool

	state           WorkerState
	replyCount      int           // votes collected; starts at 1 (self-vote)
	currentReqClock int64         // Lamport clock when we sent REQUEST
	neededVotes     int           // len(liveQuorum) at RequestCS time
	csEnter         chan struct{} // closed when replyCount == neededVotes OR quorum fails
	csErr           error         // set before closing csEnter on failure; read by RequestCS
	currentTask     *models.Task
	// yieldedTo tracks the (voterID, inquireClock) pairs we already yielded to in
	// this round, so duplicate INQUIREs with the same clock are ignored.
	yieldedTo map[int32]int64

	// voter state
	voter voterState

	// Lamport clock
	clock int64

	// network
	rpc    *network.MaekawaRPC
	server *network.MaekawaServer
}

func NewWorker(id, n int, peers map[int]string, listenAddr string) (*Worker, error) {
	// n = number of workers in cluster
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

// Stop shuts down the worker's gRPC server and closes connections
func (w *Worker) Stop() {
	w.server.Stop()
	w.rpc.Close()
}

// -- Requester API --

// RequestCS sends REQUEST to all live quorum peers and blocks until CS is acquired.
// The provided context can be used to set a deadline or timeout; if it expires
// before all votes are collected, RequestCS returns ctx.Err().
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
	w.replyCount = 0 // no votes yet — self-vote is processed below under the lock
	w.neededVotes = len(lq)
	w.yieldedTo = make(map[int32]int64)
	w.csEnter = make(chan struct{})
	clk := w.tick()
	w.currentReqClock = clk

	// Process the self-REQUEST under the same lock so voter state is committed
	// atomically before any remote REQUEST from a concurrent requester can arrive.
	// This is the fix for the quorum-intersection safety bug:
	//   Without this, a concurrent higher-priority requester could receive our voter
	//   grant (voter free) before we locked it for ourselves, allowing both workers
	//   to simultaneously collect all votes and enter CS.
	// We also count the self-REPLY inline (no loopback RPC) so that replyCount
	// is correct before any INQUIRE can decrement it below zero.
	selfReqMsg := &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_REQUEST,
		SenderId: int32(w.ID),
		Clock:    clk,
		TaskId:   task.ID,
	}
	selfFns := w.onRequest(selfReqMsg)
	selfGranted := w.voter.locked && w.voter.lockedFor == int32(w.ID) && w.voter.lockedClock == clk
	if selfGranted {
		// Voter was free and is now locked for us. Count the self-vote inline;
		// discard the REPLY sendFn — no loopback RPC needed.
		selfReplyMsg := &maekawapb.MaekawaMsg{
			Type:     maekawapb.MsgType_REPLY,
			SenderId: int32(w.ID),
			Clock:    clk,
			TaskId:   task.ID,
		}
		w.onReply(selfReplyMsg)
	}
	// selfFns may be non-empty for two reasons:
	//   1. Voter was free → REPLY fn (handled inline above, fn discarded)
	//   2. Voter was busy and we have higher priority than the current holder → INQUIRE fn
	// In case 2 the INQUIRE must be sent; in case 1 the fn is dropped (no loopback).
	var selfOut []sendFn
	if !selfGranted {
		selfOut = selfFns // contains INQUIRE fn if needed, or nil
	}
	w.mu.Unlock()

	for _, fn := range selfOut {
		if err := fn(); err != nil {
			w.log.Warn("failed to send self-voter INQUIRE", "err", err)
		}
	}

	// Send REQUEST to all live quorum peers (excluding self — handled above).
	for _, peerID := range lq {
		if peerID == w.ID {
			continue
		}
		if err := w.rpc.SendRequest(peerID, task.ID, clk); err != nil {
			w.log.Warn("failed to send REQUEST", "to", peerID, "err", err)
		}
	}

	// Block until all votes collected, quorum failure, or context cancellation.
	select {
	case <-w.csEnter:
	case <-ctx.Done():
		w.mu.Lock()
		w.state = StateReleased
		w.currentTask = nil
		// If we locked our own voter for this request, release it now so
		// any queued waiter can proceed.
		out := w.selfReleaseVoterLocked(clk, task.ID)
		w.mu.Unlock()
		for _, fn := range out {
			if err := fn(); err != nil {
				w.log.Warn("ctx-cancel self-release: failed to send REPLY", "err", err)
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
		// Same voter unwind on csErr cancellation.
		out := w.selfReleaseVoterLocked(clk, task.ID)
		w.mu.Unlock()
		for _, fn := range out {
			if fn2err := fn(); fn2err != nil {
				w.log.Warn("csErr self-release: failed to send REPLY", "err", fn2err)
			}
		}
		return err
	}
	w.state = StateHeld
	w.mu.Unlock()

	w.log.Info("entered CS", "task", task.ID)
	return nil
}

// selfReleaseVoterLocked cleans up our local voter state when a RequestCS is
// cancelled or fails. Must be called under w.mu.
// Handles two cases:
//  1. Voter is locked for our current request → release it via grantNext so the
//     next waiter in the queue is not permanently blocked.
//  2. Our self-REQUEST is still sitting in the voter wait queue (voter was busy
//     when we sent it) → remove it so grantNext never delivers a stale grant to
//     a worker that has already given up.
//
// Returns sendFns to deliver REPLY to the next waiter (case 1 only), to be called outside lock.
func (w *Worker) selfReleaseVoterLocked(reqClock int64, taskID string) []sendFn {
	if w.voter.locked && w.voter.lockedFor == int32(w.ID) && w.voter.lockedClock == reqClock {
		// Case 1: voter is locked for us — release it.
		return w.grantNext(taskID)
	}
	// Case 2: self-REQUEST may be queued — scan and remove it.
	HeapRemove(&w.voter.waitQueue, int32(w.ID), reqClock)
	return nil
}

// ReleaseCS sends RELEASE to all quorum peers and resets requester state.
// No-op if no task is currently held (e.g. called after a failed RequestCS).
func (w *Worker) ReleaseCS() {
	w.mu.Lock()
	w.state = StateReleased
	task := w.currentTask
	w.currentTask = nil
	clk := w.tick()
	quorum := w.Quorum
	reqClock := w.currentReqClock
	// Release our own voter lock inline (no loopback RPC) and collect any
	// sendFn to deliver REPLY to the next waiter in our voter queue.
	var selfOut []sendFn
	if task != nil {
		selfOut = w.selfReleaseVoterLocked(reqClock, task.ID)
	}
	w.mu.Unlock()

	if task == nil {
		return
	}

	// Deliver REPLY to next waiter (if our voter had one queued).
	for _, fn := range selfOut {
		if err := fn(); err != nil {
			w.log.Warn("self-release: failed to send REPLY to next waiter", "err", err)
		}
	}

	for _, peerID := range quorum {
		if peerID == w.ID {
			continue
		}
		if err := w.rpc.SendRelease(peerID, task.ID, clk); err != nil {
			w.log.Warn("failed to send RELEASE", "to", peerID, "err", err)
		}
	}

	w.log.Info("released CS", "task", task.ID)
}

// update the Lamport clock, then dispatches to the correct handler (gRPC)
func (w *Worker) HandleMessage(msg *maekawapb.MaekawaMsg) error {
	// collect outgoing messages to send AFTER releasing the lock
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

	// send outside lock — never hold mutex during I/O
	for _, fn := range out {
		if err := fn(); err != nil {
			w.log.Warn("failed to send message", "err", err)
		}
	}
	return nil
}

// onRequest, onInquire, onYield, grantNext live in deadlock.go. (all in .mu)

type sendFn = func() error

// onFailed handles an explicit rejection from a voter (e.g. voter is going down).
// Treated the same as a quorum failure — cancels the pending RequestCS.
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
		case <-w.csEnter: // already closed by NotifyWorkerDown or another path
		default:
			close(w.csEnter) // unblock RequestCS
		}
	}
	return nil
}

func (w *Worker) onRelease(msg *maekawapb.MaekawaMsg) []sendFn {
	if !w.voter.locked || w.voter.lockedFor != msg.SenderId {
		return nil
	}
	return w.grantNext(msg.TaskId)
}
