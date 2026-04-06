package maekawa

import (
	maekawapb "raft-maekawa-sync/api/maekawa"
	"raft-maekawa-sync/internal/utils"
)

type ClusterMembership interface {
	ActiveMembers() []int32
	IsAlive(id int32) bool
	ClaimTask(taskID string, workerID int32) (bool, error)
	ReportTaskSuccess(taskID string, workerID int32, result string) error
	ReportTaskFailure(taskID string, workerID int32, reason string) error
}

func (w *Worker) OnMembershipChange(newActiveNodes []int32) {
	w.Mu.Lock()
	defer w.Mu.Unlock()

	alive := make(map[int32]bool, len(newActiveNodes))
	for _, id := range newActiveNodes {
		alive[id] = true
	}

	// If the current vote holder died, evict and grant the next waiter
	if w.votedFor != -1 && !alive[w.votedFor] {
		w.votedFor = -1
		w.currentReq = nil
		w.isPinned = false
		if next := w.popNextFromHeap(); next != nil {
			go w.sendGrant(next.NodeId, next.Timestamp)
		}
	}

	newQuorum := RegridQuorum(w.ID, newActiveNodes)
	if newQuorum == nil {
		return
	}

	w.quorum = newQuorum
	w.votesReceived = 0
	w.grantsReceived = make(map[int32]bool)
	w.yieldedTo = make(map[int32]int64)
	w.pendingInquiries = make(map[int32]int64)
	w.votedFor = -1
	w.currentReq = nil
	w.isPinned = false
	w.committed = false
	w.inCS = false
	w.ownReqTimestamp = -1

	// Signal any in-flight RequestForGlobalLock to abort
	select {
	case w.grantChan <- false:
	default:
		select {
		case <-w.grantChan:
		default:
		}
		w.grantChan <- false
	}

	w.requestQueue = utils.NewGenericMinHeap[*maekawapb.LockRequest](
		func(a, b *maekawapb.LockRequest) bool {
			if a.Timestamp != b.Timestamp {
				return a.Timestamp < b.Timestamp
			}
			return a.NodeId < b.NodeId
		},
	)
}
