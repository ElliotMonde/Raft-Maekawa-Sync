package maekawa

// Deadlock resolution — Sanders (1987) extension to Maekawa's algorithm.
// rescinding vote when V already grants its vote to a node of lower priority when another request of higher priority arrives later

// Resolution:
//   1. V sends INQUIRE to A: "can I take back my vote?"
//   2a. A is already in CS (StateHeld) → A ignores INQUIRE. V keeps its vote.
//   2b. A has not yet collected all votes (StateWanting, replyCount < quorum)
//       → A sends YIELD to V: "yes take it back"
//   3. V requeues A's request, grants vote to B instead.
//   4. A will eventually get V's vote again once B releases.

import maekawapb "raft-maekawa/proto/maekawapb"

// shouldYield reports whether this worker (as a requester) should yield
// back a vote when it receives an INQUIRE from a voter.
// Returns true only if still StateWanting AND haven't collected all votes yet.
// Must be called under w.mu.
func (w *Worker) shouldYield() bool {
	return w.state == StateWanting && w.replyCount < w.neededVotes
}

// onRequest handles an incoming REQUEST from another worker acting as a voter.
// If free, grants vote immediately. If already voted, enqueues and optionally
// sends INQUIRE to the current holder if the incoming request has higher priority.
// Must be called under w.mu.
func (w *Worker) onRequest(msg *maekawapb.MaekawaMsg) []sendFn {
	if !w.voter.locked {
		// free to grant — send REPLY immediately
		w.voter.locked = true
		w.voter.lockedFor = msg.SenderId
		w.voter.lockedClock = msg.Clock
		clk := w.tick()
		return []sendFn{func() error {
			return w.rpc.SendReply(int(msg.SenderId), msg.TaskId, clk)
		}}
	}

	// already voted — compare priorities: lower (clock, senderID) = higher priority
	currentHigher := w.voter.lockedClock < msg.Clock ||
		(w.voter.lockedClock == msg.Clock && w.voter.lockedFor < msg.SenderId)

	HeapPush(&w.voter.waitQueue, msg)

	if !currentHigher && !w.voter.inquired {
		// incoming has higher priority than current holder → ask holder to yield
		w.voter.inquired = true
		lockedFor := w.voter.lockedFor
		lockedClock := w.voter.lockedClock
		clk := w.tick()
		return []sendFn{func() error {
			return w.rpc.SendInquire(int(lockedFor), msg.TaskId, clk, lockedClock)
		}}
	}

	return nil
}

// onInquire handles an incoming INQUIRE from a voter asking us to yield our vote.
// Yields only if we are still StateWanting and haven't collected all votes yet.
// Ignores stale INQUIREs from previous request rounds.
// Must be called under w.mu.
func (w *Worker) onInquire(msg *maekawapb.MaekawaMsg) []sendFn {
	if !w.shouldYield() {
		return nil // already in CS or have all votes — ignore
	}
	// ignore stale INQUIRE (from a previous request round)
	if msg.InquireClock != w.currentReqClock {
		return nil
	}
	// ignore duplicate INQUIRE from the same voter with the same clock
	if prev, ok := w.yieldedTo[msg.SenderId]; ok && prev == msg.InquireClock {
		return nil
	}
	w.yieldedTo[msg.SenderId] = msg.InquireClock
	w.replyCount--
	clk := w.tick()
	senderID := msg.SenderId
	taskID := msg.TaskId
	inquiredClock := msg.InquireClock
	return []sendFn{func() error {
		return w.rpc.SendYield(int(senderID), taskID, clk, inquiredClock)
	}}
}

// onYield handles an incoming YIELD — the requester we sent INQUIRE to is
// giving back their vote. Requeue their request and grant to the next highest
// priority requester in the wait queue.
// Must be called under w.mu.
func (w *Worker) onYield(msg *maekawapb.MaekawaMsg) []sendFn {
	if !w.voter.inquired || w.voter.lockedFor != msg.SenderId || w.voter.lockedClock != msg.InquireClock {
		return nil
	}
	// requeue the yielder's original request
	HeapPush(&w.voter.waitQueue, &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_REQUEST,
		SenderId: w.voter.lockedFor,
		Clock:    w.voter.lockedClock,
		TaskId:   msg.TaskId,
	})
	w.voter.locked = false
	w.voter.inquired = false
	return w.grantNext(msg.TaskId)
}

// grantNext unlocks the voter and grants the vote to the highest priority
// request in the waitQueue (if any). Must be called under w.mu.
// taskID is ignored — the REPLY uses the next requester's own taskID from the queue.
func (w *Worker) grantNext(_ string) []sendFn {
	w.voter.locked = false
	w.voter.inquired = false

	if w.voter.waitQueue.Len() == 0 {
		return nil
	}

	next := HeapPop(&w.voter.waitQueue)
	w.voter.locked = true
	w.voter.lockedFor = next.SenderId
	w.voter.lockedClock = next.Clock
	clk := w.tick()

	return []sendFn{func() error {
		return w.rpc.SendReply(int(next.SenderId), next.TaskId, clk)
	}}
}
