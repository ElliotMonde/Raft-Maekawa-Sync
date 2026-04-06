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

    newQuorum := RegridQuorum(w.ID, newActiveNodes)
    if newQuorum == nil {
        return
    }

    w.quorum = newQuorum
    w.votesReceived = 0
    w.votedFor = -1
    w.isPinned = false
    w.committed = false
    w.inCS = false

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
            return a.Timestamp < b.Timestamp
        },
    )
}