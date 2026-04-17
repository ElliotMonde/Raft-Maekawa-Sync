package raft

import (
	"context"
	"fmt"
	"time"

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

func (n *Node) AppendEntries(_ context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return &raftpb.AppendEntriesResponse{Term: n.currentTerm, Success: false}, nil
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = -1
	}
	n.role = Follower
	n.leaderID = req.LeaderId
	n.electionReset = time.Now()

	if req.PrevLogIndex > 0 {
		if int(req.PrevLogIndex) > len(n.log) {
			return &raftpb.AppendEntriesResponse{Term: n.currentTerm, Success: false}, nil
		}
		if n.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			n.log = n.log[:req.PrevLogIndex-1]
			return &raftpb.AppendEntriesResponse{Term: n.currentTerm, Success: false}, nil
		}
	}

	if len(req.Entries) > 0 {
		insertAt := req.PrevLogIndex + 1
		for i, incoming := range req.Entries {
			localIndex := insertAt + int32(i)
			if localIndex <= int32(len(n.log)) {
				if n.log[localIndex-1].Term != incoming.Term {
					n.log = n.log[:localIndex-1]
					for _, e := range req.Entries[i:] {
						n.log = append(n.log, *e)
					}
					break
				}
				continue
			}

			for _, e := range req.Entries[i:] {
				n.log = append(n.log, *e)
			}
			break
		}
	}

	if req.LeaderCommit > n.commitIndex {
		if req.LeaderCommit < int32(len(n.log)) {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = int32(len(n.log))
		}
		n.applyCommittedEntries()
	}

	return &raftpb.AppendEntriesResponse{Term: n.currentTerm, Success: true}, nil
}

func (n *Node) replicateToPeer(ctx context.Context, peerID int32) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}

	term := n.currentTerm
	nextIdx, ok := n.nextIndex[peerID]
	if !ok || nextIdx < 1 {
		nextIdx = 1
		n.nextIndex[peerID] = nextIdx
	}

	prevLogIndex := nextIdx - 1
	prevLogTerm := int32(0)
	if prevLogIndex > 0 && int(prevLogIndex) <= len(n.log) {
		prevLogTerm = n.log[prevLogIndex-1].Term
	}

	entries := make([]*raftpb.LogEntry, 0)
	if int(nextIdx) <= len(n.log) {
		for i := nextIdx - 1; i < int32(len(n.log)); i++ {
			e := n.log[i]
			entries = append(entries, &raftpb.LogEntry{Term: e.Term, Index: e.Index, Command: e.Command})
		}
	}
	leaderCommit := n.commitIndex
	leaderID := n.id
	n.mu.Unlock()

	client, err := n.getPeerClient(peerID)
	if err != nil {
		return
	}

	resp, err := client.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term:         term,
		LeaderId:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	})
	if err != nil || resp == nil {
		return
	}

	if resp.Term > term {
		n.becomeFollower(resp.Term, -1)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader || n.currentTerm != term {
		return
	}

	if resp.Success {
		match := prevLogIndex + int32(len(entries))
		if match > n.matchIndex[peerID] {
			n.matchIndex[peerID] = match
		}
		n.nextIndex[peerID] = n.matchIndex[peerID] + 1
		n.tryAdvanceCommitIndex()
		return
	}

	if n.nextIndex[peerID] > 1 {
		n.nextIndex[peerID]--
	}
}

// tryAdvanceCommitIndex advances commit index for entries replicated on a majority.
// Caller must hold n.mu.
func (n *Node) tryAdvanceCommitIndex() {
	majority := (len(n.peers)+1)/2 + 1
	for idx := int32(len(n.log)); idx > n.commitIndex; idx-- {
		if n.log[idx-1].Term != n.currentTerm {
			continue
		}

		count := 1 // leader itself
		for peerID := range n.peers {
			if n.matchIndex[peerID] >= idx {
				count++
			}
		}

		if count >= majority {
			n.commitIndex = idx
			n.applyCommittedEntries()
			return
		}
	}
}

func (n *Node) SubmitTask(ctx context.Context, req *raftpb.SubmitTaskRequest) (*raftpb.SubmitTaskResponse, error) {
	n.mu.Lock()
	if n.role != Leader {
		leaderID := n.leaderID
		n.mu.Unlock()
		return &raftpb.SubmitTaskResponse{Success: false, LeaderId: leaderID}, nil
	}
	leaderID := n.id
	n.mu.Unlock()

	taskID := fmt.Sprintf("task-%d-%d", leaderID, time.Now().UnixNano())
	event := models.TaskEvent{
		Type:   models.EventAssigned,
		TaskID: taskID,
		Task:   &models.Task{ID: taskID, Data: req.Data},
	}
	cmd, err := event.Encode()
	if err != nil {
		return nil, err
	}

	n.mu.Lock()
	n.appendEntry(cmd)
	newIndex := int32(len(n.log))
	peerIDs := make([]int32, 0, len(n.peers))
	for peerID := range n.peers {
		peerIDs = append(peerIDs, peerID)
	}
	if len(n.peers) == 0 {
		n.commitIndex = newIndex
		n.applyCommittedEntries()
	}
	n.mu.Unlock()

	for _, peerID := range peerIDs {
		go n.replicateToPeer(ctx, peerID)
	}

	if !n.waitForCommit(ctx, newIndex, 5*time.Second) {
		n.mu.Lock()
		leaderHint := n.leaderID
		n.mu.Unlock()
		return &raftpb.SubmitTaskResponse{Success: false, LeaderId: leaderHint}, nil
	}

	return &raftpb.SubmitTaskResponse{Success: true, LeaderId: leaderID, TaskId: taskID}, nil
}

func (n *Node) waitForCommit(ctx context.Context, index int32, timeout time.Duration) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		n.mu.Lock()
		committed := n.commitIndex >= index
		n.mu.Unlock()
		if committed {
			return true
		}

		select {
		case <-ctx.Done():
			return false
		case <-deadline.C:
			return false
		case <-ticker.C:
		}
	}
}

func (n *Node) GetState(_ context.Context, _ *raftpb.GetStateRequest) (*raftpb.GetStateResponse, error) {
	n.mu.Lock()
	resp := n.buildGetStateResponse()
	n.mu.Unlock()
	return resp, nil
}
