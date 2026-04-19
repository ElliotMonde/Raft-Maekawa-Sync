package raft

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	raftpb "raft-maekawa-sync/api/raft"
	"raft-maekawa-sync/internal/models"
)

const internalSubmitPrefix = "__raft_internal_event__:"

func eventUnixNano(event models.TaskEvent) int64 {
	if event.EventUnixNano > 0 {
		return event.EventUnixNano
	}
	return time.Now().UnixNano()
}

// appendEntry appends a log entry using the node's current term.
// Caller must hold n.mu.
func (n *Node) appendEntry(command string) *raftpb.LogEntry {
	entry := &raftpb.LogEntry{
		Term:    n.currentTerm,
		Index:   int32(len(n.log)) + 1,
		Command: command,
	}
	n.log = append(n.log, entry)
	return entry
}

func encodeInternalSubmit(event models.TaskEvent) (string, error) {
	cmd, err := event.Encode()
	if err != nil {
		return "", err
	}
	return internalSubmitPrefix + cmd, nil
}

func decodeInternalSubmit(data string) (models.TaskEvent, bool, error) {
	if !strings.HasPrefix(data, internalSubmitPrefix) {
		return models.TaskEvent{}, false, nil
	}
	event, err := models.DecodeTaskEvent(strings.TrimPrefix(data, internalSubmitPrefix))
	if err != nil {
		return models.TaskEvent{}, true, err
	}
	return *event, true, nil
}

// applyCommittedEntries applies entries in (lastApplied, commitIndex].
// It temporarily releases n.mu before notifying the Maekawa applier to avoid
// lock inversion when the applier consults Raft-backed membership state.
func (n *Node) applyCommittedEntries() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied-1]
		event, err := models.DecodeTaskEvent(entry.Command)
		if err != nil {
			log.Printf("raft node %d: failed to decode log index %d: %v", n.id, n.lastApplied, err)
			continue
		}
		n.applyEvent(*event)
		log.Printf("raft node %d: applied %s %s", n.id, event.Type, event.TaskID)
		applier := n.applier
		n.mu.Unlock()
		applyTaskEventToMaekawa(*event, applier)
		n.mu.Lock()
	}
}

// applyEvent mutates the in-memory state machine for one committed event.
// Caller must hold n.mu.
func (n *Node) applyEvent(event models.TaskEvent) {
	updatedAt := eventUnixNano(event)

	switch event.Type {
	case models.EventAssigned:
		record := &TaskRecord{
			ID:     event.TaskID,
			Status: models.EventAssigned,
			UpdatedAtUnixNano: updatedAt,
		}
		if event.Task != nil {
			record.Data = event.Task.Data
		}
		n.state.Tasks[event.TaskID] = record
		delete(n.pendingTaskRecovery, event.TaskID)

	case models.EventClaimed:
		record, ok := n.state.Tasks[event.TaskID]
		if !ok {
			record = &TaskRecord{ID: event.TaskID}
			n.state.Tasks[event.TaskID] = record
		}
		record.AssignedTo = event.WorkerID
		record.Status = models.EventClaimed
		record.Result = ""
		record.Reason = ""
		record.UpdatedAtUnixNano = updatedAt

	case models.EventDone:
		record, ok := n.state.Tasks[event.TaskID]
		if !ok {
			record = &TaskRecord{ID: event.TaskID}
			n.state.Tasks[event.TaskID] = record
		}
		record.Status = models.EventDone
		record.Result = event.Result
		record.UpdatedAtUnixNano = updatedAt
		delete(n.pendingTaskRecovery, event.TaskID)

	case models.EventFailed:
		record, ok := n.state.Tasks[event.TaskID]
		if !ok {
			record = &TaskRecord{ID: event.TaskID}
			n.state.Tasks[event.TaskID] = record
		}
		record.Status = models.EventFailed
		record.Reason = event.Reason
		record.UpdatedAtUnixNano = updatedAt
		delete(n.pendingTaskRecovery, event.TaskID)

	case models.EventCanceled:
		record, ok := n.state.Tasks[event.TaskID]
		if !ok {
			record = &TaskRecord{ID: event.TaskID}
			n.state.Tasks[event.TaskID] = record
		}
		record.Status = models.EventCanceled
		record.Reason = event.Reason
		record.UpdatedAtUnixNano = updatedAt
		delete(n.pendingTaskRecovery, event.TaskID)

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

	roleStr := "follower"
	switch n.role {
	case Leader:
		roleStr = "leader"
	case Candidate:
		roleStr = "candidate"
	}
	resp.NodeInfo = &raftpb.NodeInfo{
		NodeId:      n.id,
		Role:        roleStr,
		Term:        n.currentTerm,
		CommitIndex: n.commitIndex,
		LogLength:   int32(len(n.log)),
		LeaderId:    n.leaderID,
	}

	return resp
}

func (n *Node) AppendEntries(_ context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	dirty := false

	if req.Term < n.currentTerm {
		return &raftpb.AppendEntriesResponse{Term: n.currentTerm, Success: false}, nil
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = -1
		dirty = true
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
			dirty = true
			_ = n.persistLocked()
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
						n.log = append(n.log, e)
					}
					dirty = true
					break
				}
				continue
			}

			for _, e := range req.Entries[i:] {
				n.log = append(n.log, e)
			}
			dirty = true
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
		dirty = true
	}
	if dirty {
		_ = n.persistLocked()
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
			entries = append(entries, n.log[i])
		}
	}
	leaderCommit := n.commitIndex
	leaderID := n.id
	n.mu.Unlock()

	client, err := n.getPeerClient(peerID)
	if err != nil {
		go n.notePeerReachability(peerID, false)
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
		go n.notePeerReachability(peerID, false)
		return
	}
	go n.notePeerReachability(peerID, true)

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
			_ = n.persistLocked()
			return
		}
	}
}

func (n *Node) shouldCommitEventLocked(event models.TaskEvent) bool {
	switch event.Type {
	case models.EventAssigned:
		if event.Task == nil || event.TaskID == "" {
			return false
		}
		task, ok := n.state.Tasks[event.TaskID]
		if !ok {
			return true
		}
		return n.canRecoverClaimedTaskLocked(task)
	case models.EventClaimed:
		task, ok := n.state.Tasks[event.TaskID]
		return ok && task.Status == models.EventAssigned
	case models.EventDone, models.EventFailed:
		task, ok := n.state.Tasks[event.TaskID]
		return ok && task.Status == models.EventClaimed && task.AssignedTo == event.WorkerID
	case models.EventCanceled:
		task, ok := n.state.Tasks[event.TaskID]
		return ok && task.Status != models.EventDone && task.Status != models.EventFailed
	case models.EventWorkerUp, models.EventWorkerAdded, models.EventWorkerDown, models.EventWorkerRemoved:
		return true
	default:
		return false
	}
}

func (n *Node) canRecoverClaimedTaskLocked(task *TaskRecord) bool {
	if task == nil || task.Status != models.EventClaimed || task.AssignedTo == 0 {
		return false
	}
	if n.state.ActiveWorkers[task.AssignedTo] {
		return false
	}
	if task.UpdatedAtUnixNano == 0 {
		return false
	}
	return time.Since(time.Unix(0, task.UpdatedAtUnixNano)) >= n.taskClaimTimeout
}

func (n *Node) commitTaskEventAsLeader(ctx context.Context, event models.TaskEvent) (bool, int32, error) {
	cmd, err := event.Encode()
	if err != nil {
		return false, 0, err
	}

	n.mu.Lock()
	if n.role != Leader {
		leaderID := n.leaderID
		n.mu.Unlock()
		return false, leaderID, nil
	}
	if !n.shouldCommitEventLocked(event) {
		leaderID := n.id
		n.mu.Unlock()
		return false, leaderID, nil
	}

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
	leaderID := n.id
	beforeReplicate := n.beforeReplicate
	_ = n.persistLocked()
	n.mu.Unlock()

	if beforeReplicate != nil && !beforeReplicate(event) {
		return false, leaderID, nil
	}

	for _, peerID := range peerIDs {
		go n.replicateToPeer(ctx, peerID)
	}

	if !n.waitForCommit(ctx, newIndex, 5*time.Second) {
		n.mu.Lock()
		leaderHint := n.leaderID
		n.mu.Unlock()
		return false, leaderHint, nil
	}

	return true, leaderID, nil
}

func (n *Node) SubmitTask(ctx context.Context, req *raftpb.SubmitTaskRequest) (*raftpb.SubmitTaskResponse, error) {
	event, internal, err := decodeInternalSubmit(req.Data)
	if err != nil {
		return nil, err
	}

	if !internal {
		n.mu.Lock()
		taskID := fmt.Sprintf("task-%d-%d", n.id, time.Now().UnixNano())
		n.mu.Unlock()
		event = models.TaskEvent{
			Type:          models.EventAssigned,
			TaskID:        taskID,
			Task:          &models.Task{ID: taskID, Data: req.Data},
			EventUnixNano: time.Now().UnixNano(),
		}
	} else if event.EventUnixNano == 0 {
		event.EventUnixNano = time.Now().UnixNano()
	}

	accepted, leaderID, err := n.commitTaskEventAsLeader(ctx, event)
	if err != nil {
		return nil, err
	}
	taskID := event.TaskID
	if !accepted {
		taskID = ""
	}
	return &raftpb.SubmitTaskResponse{
		Success:  accepted,
		LeaderId: leaderID,
		TaskId:   taskID,
	}, nil
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
