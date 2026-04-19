package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	raftpb "raft-maekawa-sync/api/raft"
)

type persistedState struct {
	CurrentTerm int32              `json:"current_term"`
	VotedFor    int32              `json:"voted_for"`
	Log         []*raftpb.LogEntry `json:"log"`
	CommitIndex int32              `json:"commit_index"`
}

func (n *Node) SetStoragePath(path string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.storagePath = path
	if path == "" {
		return nil
	}
	return n.loadLocked()
}

func (n *Node) initStateMachineLocked() {
	activeWorkers := make(map[int32]bool, len(n.peers)+1)
	activeWorkers[n.id] = true
	for peerID := range n.peers {
		activeWorkers[peerID] = true
	}
	n.state = &StateMachine{
		ActiveWorkers: activeWorkers,
		Tasks:         make(map[string]*TaskRecord),
	}
}

func (n *Node) loadLocked() error {
	if n.storagePath == "" {
		return nil
	}

	data, err := os.ReadFile(n.storagePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read raft state %s: %w", n.storagePath, err)
	}

	var persisted persistedState
	if err := json.Unmarshal(data, &persisted); err != nil {
		return fmt.Errorf("decode raft state %s: %w", n.storagePath, err)
	}

	n.currentTerm = persisted.CurrentTerm
	n.votedFor = persisted.VotedFor
	n.log = append([]*raftpb.LogEntry(nil), persisted.Log...)
	n.commitIndex = persisted.CommitIndex
	if n.commitIndex > int32(len(n.log)) {
		n.commitIndex = int32(len(n.log))
	}
	n.lastApplied = 0
	n.initStateMachineLocked()
	n.applyCommittedEntries()

	return nil
}

func (n *Node) persistLocked() error {
	if n.storagePath == "" {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(n.storagePath), 0o755); err != nil {
		return fmt.Errorf("create raft state directory: %w", err)
	}

	payload := persistedState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         append([]*raftpb.LogEntry(nil), n.log...),
		CommitIndex: n.commitIndex,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode raft state: %w", err)
	}

	tmpPath := n.storagePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("write raft state: %w", err)
	}
	if err := os.Rename(tmpPath, n.storagePath); err != nil {
		return fmt.Errorf("replace raft state: %w", err)
	}
	return nil
}
