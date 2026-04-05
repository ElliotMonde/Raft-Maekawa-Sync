package network

import (
	maekawapb "raft-maekawa/proto/maekawapb"
)

// MaekawaRPC wraps MaekawaClient with typed send methods.
// Worker code calls these instead of constructing MaekawaMsg directly.
type MaekawaRPC struct {
	SelfID int
	client *MaekawaClient
}

// NewMaekawaRPC creates a MaekawaRPC for the given worker.
func NewMaekawaRPC(selfID int, peers map[int]string) *MaekawaRPC {
	return &MaekawaRPC{
		SelfID: selfID,
		client: NewMaekawaClient(peers),
	}
}

// SendRequest sends a REQUEST to targetID — "I want to enter CS for taskID".
func (r *MaekawaRPC) SendRequest(targetID int, taskID string, clock int64) error {
	return r.client.Send(targetID, &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_REQUEST,
		SenderId: int32(r.SelfID),
		Clock:    clock,
		TaskId:   taskID,
	})
}

// SendReply sends a REPLY to targetID — "you have my vote".
func (r *MaekawaRPC) SendReply(targetID int, taskID string, clock int64) error {
	return r.client.Send(targetID, &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_REPLY,
		SenderId: int32(r.SelfID),
		Clock:    clock,
		TaskId:   taskID,
	})
}

// SendRelease sends a RELEASE to targetID — "I am done, free your vote".
func (r *MaekawaRPC) SendRelease(targetID int, taskID string, clock int64) error {
	return r.client.Send(targetID, &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_RELEASE,
		SenderId: int32(r.SelfID),
		Clock:    clock,
		TaskId:   taskID,
	})
}

// SendInquire sends an INQUIRE to targetID — "can I rescind my vote?" (deadlock resolution).
// inquiredClock is the clock of the REQUEST the voter originally granted.
func (r *MaekawaRPC) SendInquire(targetID int, taskID string, clock int64, inquiredClock int64) error {
	return r.client.Send(targetID, &maekawapb.MaekawaMsg{
		Type:         maekawapb.MsgType_INQUIRE,
		SenderId:     int32(r.SelfID),
		Clock:        clock,
		TaskId:       taskID,
		InquireClock: inquiredClock,
	})
}

// SendYield sends a YIELD to targetID — "yes, take back your vote".
func (r *MaekawaRPC) SendYield(targetID int, taskID string, clock int64) error {
	return r.client.Send(targetID, &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_YIELD,
		SenderId: int32(r.SelfID),
		Clock:    clock,
		TaskId:   taskID,
	})
}

// SendFailed sends a FAILED to targetID — "I cannot grant your request".
func (r *MaekawaRPC) SendFailed(targetID int, taskID string, clock int64) error {
	return r.client.Send(targetID, &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_FAILED,
		SenderId: int32(r.SelfID),
		Clock:    clock,
		TaskId:   taskID,
	})
}

// Close closes all underlying connections.
func (r *MaekawaRPC) Close() {
	r.client.Close()
}
