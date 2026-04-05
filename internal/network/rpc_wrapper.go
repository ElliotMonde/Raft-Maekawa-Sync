package network

import (
	maekawapb "raft-maekawa/proto/maekawapb"
)

// MaekawaRPC wraps MaekawaClient with typed send methods.
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

// SendRequest sends a REQUEST.
func (r *MaekawaRPC) SendRequest(targetID int, taskID string, clock int64) error {
	return r.client.Send(targetID, &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_REQUEST,
		SenderId: int32(r.SelfID),
		Clock:    clock,
		TaskId:   taskID,
	})
}

// SendReply sends a REPLY.
func (r *MaekawaRPC) SendReply(targetID int, taskID string, clock int64) error {
	return r.client.Send(targetID, &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_REPLY,
		SenderId: int32(r.SelfID),
		Clock:    clock,
		TaskId:   taskID,
	})
}

// SendRelease sends a RELEASE.
func (r *MaekawaRPC) SendRelease(targetID int, taskID string, clock int64) error {
	return r.client.Send(targetID, &maekawapb.MaekawaMsg{
		Type:     maekawapb.MsgType_RELEASE,
		SenderId: int32(r.SelfID),
		Clock:    clock,
		TaskId:   taskID,
	})
}

// SendInquire sends an INQUIRE for the granted request identified by inquiredClock.
func (r *MaekawaRPC) SendInquire(targetID int, taskID string, clock int64, inquiredClock int64) error {
	return r.client.Send(targetID, &maekawapb.MaekawaMsg{
		Type:         maekawapb.MsgType_INQUIRE,
		SenderId:     int32(r.SelfID),
		Clock:        clock,
		TaskId:       taskID,
		InquireClock: inquiredClock,
	})
}

// SendYield sends a YIELD for the inquiry identified by inquiredClock.
func (r *MaekawaRPC) SendYield(targetID int, taskID string, clock int64, inquiredClock int64) error {
	return r.client.Send(targetID, &maekawapb.MaekawaMsg{
		Type:         maekawapb.MsgType_YIELD,
		SenderId:     int32(r.SelfID),
		Clock:        clock,
		TaskId:       taskID,
		InquireClock: inquiredClock,
	})
}

// SendFailed sends a FAILED.
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
