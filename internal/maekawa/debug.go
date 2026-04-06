package maekawa

import (
	"log"
	"os"

	maekawapb "raft-maekawa-sync/api/maekawa"
)

var maekawaDebugEnabled = os.Getenv("MAEKAWA_DEBUG") == "1"

func (w *Worker) debugf(format string, args ...any) {
	if !maekawaDebugEnabled {
		return
	}
	log.Printf("[maekawa w=%d] "+format, append([]any{w.ID}, args...)...)
}

func (w *Worker) debugStateLocked(prefix string) {
	if !maekawaDebugEnabled {
		return
	}
	var current string
	if w.currentReq != nil {
		current = requestLabel(w.currentReq)
	} else {
		current = "-"
	}
	log.Printf(
		"[maekawa w=%d] %s state own_ts=%d votes=%d in_cs=%t committed=%t voted_for=%d current_req=%s pinned=%t queue_len=%d yielded_to=%d",
		w.ID,
		prefix,
		w.ownReqTimestamp,
		w.votesReceived,
		w.inCS,
		w.committed,
		w.votedFor,
		current,
		w.isPinned,
		w.requestQueue.Len(),
		len(w.yieldedTo),
	)
}

func requestLabel(req *maekawapb.LockRequest) string {
	if req == nil {
		return "-"
	}
	return "node=" + itoa32(req.NodeId) + ",ts=" + itoa64(req.Timestamp)
}

func itoa32(v int32) string {
	return itoa64(int64(v))
}

func itoa64(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + (v % 10))
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
