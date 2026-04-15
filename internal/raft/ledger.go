package raft

import (
	raftpb "raft-maekawa-sync/api/raft"
	"raft-maekawa-sync/internal/models"
)

// appendEntry appends a log entry using the node's current term.
// Caller must hold n.mu.
func (n *Node) appendEntry(command string) raftpb.LogEntry {
	entry := raftpb.LogEntry{
		Term:    n.currentTerm,
		Index:   int32(len(n.log)) + 1,
		Command: command,
	}
	n.log = append(n.log, entry)
	return entry
}

// applyCommittedEntries applies entries in (lastApplied, commitIndex].
// Caller must hold n.mu.
func (n *Node) applyCommittedEntries() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied-1]
		event, err := models.DecodeTaskEvent(entry.Command)
		if err != nil {
			continue
		}
		n.applyEvent(*event)
		applyTaskEventToMaekawa(*event, n.applier)
	}
}

// applyEvent mutates the in-memory state machine for one committed event.
// Caller must hold n.mu.
func (n *Node) applyEvent(event models.TaskEvent) {
	switch event.Type {
	case models.EventAssigned:
		record := &TaskRecord{
			ID:     event.TaskID,
			Status: models.EventAssigned,
		}
		if event.Task != nil {
			record.Data = event.Task.Data
		}
		n.state.Tasks[event.TaskID] = record

	case models.EventClaimed:
		record, ok := n.state.Tasks[event.TaskID]
		if !ok {
			record = &TaskRecord{ID: event.TaskID}
			n.state.Tasks[event.TaskID] = record
		}
		record.AssignedTo = event.WorkerID
		record.Status = models.EventClaimed

	case models.EventDone:
		record, ok := n.state.Tasks[event.TaskID]
		if !ok {
			record = &TaskRecord{ID: event.TaskID}
			n.state.Tasks[event.TaskID] = record
		}
		record.Status = models.EventDone
		record.Result = event.Result

	case models.EventFailed:
		record, ok := n.state.Tasks[event.TaskID]
		if !ok {
			record = &TaskRecord{ID: event.TaskID}
			n.state.Tasks[event.TaskID] = record
		}
		record.Status = models.EventFailed
		record.Reason = event.Reason

	case models.EventCanceled:
		record, ok := n.state.Tasks[event.TaskID]
		if !ok {
			record = &TaskRecord{ID: event.TaskID}
			n.state.Tasks[event.TaskID] = record
		}
		record.Status = models.EventCanceled
		record.Reason = event.Reason

	case models.EventWorkerUp, models.EventWorkerAdded:
		n.state.ActiveWorkers[event.WorkerID] = true

	case models.EventWorkerDown, models.EventWorkerRemoved:
		n.state.ActiveWorkers[event.WorkerID] = false
	}
}

// buildGetStateResponse snapshots the current state machine into a proto response.
// Caller must hold n.mu.
func (n *Node) buildGetStateResponse() *raftpb.GetStateResponse {
	resp := &raftpb.GetStateResponse{
		ActiveNodes: make([]int32, 0, len(n.state.ActiveWorkers)),
		Tasks:       make([]*raftpb.Task, 0, len(n.state.Tasks)),
	}

	for workerID, alive := range n.state.ActiveWorkers {
		if alive {
			resp.ActiveNodes = append(resp.ActiveNodes, workerID)
		}
	}

	for _, task := range n.state.Tasks {
		status := raftpb.TaskStatus_PENDING
		switch task.Status {
		case models.EventAssigned:
			status = raftpb.TaskStatus_PENDING
		case models.EventClaimed:
			status = raftpb.TaskStatus_IN_PROGRESS
		case models.EventDone:
			status = raftpb.TaskStatus_COMPLETED
		case models.EventFailed, models.EventCanceled:
			status = raftpb.TaskStatus_FAILED
		}

		resp.Tasks = append(resp.Tasks, &raftpb.Task{
			TaskId:     task.ID,
			Data:       task.Data,
			Status:     status,
			AssignedTo: task.AssignedTo,
		})
	}

	return resp
}
