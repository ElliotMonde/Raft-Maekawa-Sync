package maekawa

import maekawapb "raft-maekawa/proto/maekawapb"

func (w *Worker) shouldYield() bool {
	return w.state == StateWanting && w.replyCount < w.neededVotes
}

func (w *Worker) onRequest(msg *maekawapb.MaekawaMsg) []sendFn {
	if !w.voter.locked {
		w.voter.locked = true
		w.voter.lockedFor = msg.SenderId
		w.voter.lockedClock = msg.Clock
		clk := w.tick()
		return []sendFn{func() error {
			return w.rpc.SendReply(int(msg.SenderId), msg.TaskId, clk)
		}}
	}

	currentHigher := w.voter.lockedClock < msg.Clock ||
		(w.voter.lockedClock == msg.Clock && w.voter.lockedFor < msg.SenderId)

	HeapPush(&w.voter.waitQueue, msg)

	if !currentHigher && !w.voter.inquired {
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

func (w *Worker) onInquire(msg *maekawapb.MaekawaMsg) []sendFn {
	if !w.shouldYield() {
		return nil
	}
	if msg.InquireClock != w.currentReqClock {
		return nil
	}
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

func (w *Worker) onYield(msg *maekawapb.MaekawaMsg) []sendFn {
	if !w.voter.inquired || w.voter.lockedFor != msg.SenderId || w.voter.lockedClock != msg.InquireClock {
		return nil
	}
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
