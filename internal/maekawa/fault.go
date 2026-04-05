package maekawa

import "fmt"

const MinClusterSize = 3

func regridSafe(n int) bool {
	if n < MinClusterSize {
		return false
	}
	k := gridSizeFor(n)
	rows := (n + k - 1) / k // ceil(n/k)
	lastRowCount := n - (rows-1)*k
	return k >= 3 && lastRowCount >= 3
}

func (w *Worker) liveQuorum() []int {
	live := make([]int, 0, len(w.Quorum))
	for _, id := range w.Quorum {
		if w.liveWorkers[id] {
			live = append(live, id)
		}
	}
	return live
}

func (w *Worker) NotifyWorkerDown(workerID int) {
	w.mu.Lock()
	if !w.activeWorkers[workerID] {
		w.mu.Unlock()
		w.log.Info("ignoring down for non-member worker", "id", workerID)
		return
	}
	w.liveWorkers[workerID] = false

	var out []sendFn
	if w.voter.locked && w.voter.lockedFor == int32(workerID) {
		w.log.Info("force-releasing voter lock held by dead worker", "dead", workerID)
		out = w.grantNext("")
	}

	if workerID == w.ID && w.state == StateHeld && w.execCancel != nil {
		w.execCancel()
	}

	if w.state == StateWanting && workerID != w.ID {
		inQuorum := false
		for _, id := range w.Quorum {
			if id == workerID {
				inQuorum = true
				break
			}
		}
		if inQuorum {
			w.csErr = fmt.Errorf("worker %d: quorum member %d died mid-request, cancelling CS", w.ID, workerID)
			select {
			case <-w.csEnter:
			default:
				close(w.csEnter)
			}
		}
	}
	w.mu.Unlock()

	for _, fn := range out {
		if err := fn(); err != nil {
			w.log.Warn("force-release: failed to send REPLY", "err", err)
		}
	}

	w.log.Info("worker marked down", "id", workerID)
}

func (w *Worker) NotifyWorkerUp(workerID int) {
	w.mu.Lock()
	if !w.activeWorkers[workerID] {
		w.mu.Unlock()
		w.log.Info("ignoring up for non-member worker", "id", workerID)
		return
	}
	w.liveWorkers[workerID] = true
	w.mu.Unlock()
	w.log.Info("worker marked up", "id", workerID)
}

func (w *Worker) NotifyWorkerRemoved(workerID int) {
	w.mu.Lock()
	oldQuorum := append([]int(nil), w.Quorum...)
	if workerID == w.ID {
		w.mu.Unlock()
		w.log.Warn("ignoring remove for self", "id", workerID)
		return
	}
	if !w.activeWorkers[workerID] {
		w.mu.Unlock()
		w.log.Info("worker already removed", "id", workerID)
		return
	}

	w.activeWorkers[workerID] = false
	w.liveWorkers[workerID] = false

	activeCount := 0
	for _, active := range w.activeWorkers {
		if active {
			activeCount++
		}
	}
	if !regridSafe(activeCount) {
		w.activeWorkers[workerID] = true
		w.liveWorkers[workerID] = true
		w.mu.Unlock()
		w.log.Warn("rejecting removal: resulting grid would be unsafe",
			"id", workerID, "activeAfter", activeCount, "min", MinClusterSize)
		return
	}

	var out []sendFn
	if w.voter.locked && w.voter.lockedFor == int32(workerID) {
		w.log.Info("force-releasing voter lock held by removed worker", "removed", workerID)
		out = w.grantNext("")
	}

	w.Quorum = RegridQuorum(w.ID, sortedActiveIDs(w.activeWorkers))

	if w.state == StateWanting {
		inQuorum := false
		for _, id := range oldQuorum {
			if id == workerID {
				inQuorum = true
				break
			}
		}
		if inQuorum {
			w.csErr = fmt.Errorf("worker %d: quorum member %d removed, cancelling CS", w.ID, workerID)
			select {
			case <-w.csEnter:
			default:
				close(w.csEnter)
			}
		}
	}

	w.mu.Unlock()

	for _, fn := range out {
		if err := fn(); err != nil {
			w.log.Warn("force-release after removal: failed to send REPLY", "err", err)
		}
	}

	w.log.Info("worker removed from active membership", "id", workerID)
}

func (w *Worker) NotifyWorkerAdded(workerID int) {
	w.mu.Lock()
	if workerID < 0 {
		w.mu.Unlock()
		w.log.Warn("ignoring add for invalid worker id", "id", workerID)
		return
	}
	if w.activeWorkers[workerID] {
		w.liveWorkers[workerID] = true
		w.mu.Unlock()
		w.log.Info("worker already active, marked up", "id", workerID)
		return
	}

	w.activeWorkers[workerID] = true
	w.liveWorkers[workerID] = true
	if workerID >= w.N {
		w.N = workerID + 1
	}

	w.Quorum = RegridQuorum(w.ID, sortedActiveIDs(w.activeWorkers))
	w.mu.Unlock()
	w.log.Info("worker added to active membership", "id", workerID)
}
