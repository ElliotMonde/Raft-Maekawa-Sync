package maekawa

import (
	//go's heap interface
	"container/heap"
	"math"
	"sort"

	maekawapb "raft-maekawa/proto/maekawapb"
)

// gridSizeFor returns ceil(sqrt(n)), the grid dimension for n nodes.
func gridSizeFor(n int) int {
	return int(math.Ceil(math.Sqrt(float64(n))))
}

// row of Node
func rowOf(nodeID, n int) int { return nodeID / gridSizeFor(n) }

// col of node
func colOf(nodeID, n int) int { return nodeID % gridSizeFor(n) }

func QuorumFor(nodeID, n int) []int {
	seen := make(map[int]struct{})

	k := gridSizeFor(n) // k×k virtual grid
	row := rowOf(nodeID, n)
	col := colOf(nodeID, n)

	// add all real nodes in the same row
	for i := 0; i < n; i++ {
		if rowOf(i, n) == row {
			seen[i] = struct{}{}
		}
	}

	// add all real nodes in the same column across all k rows
	for r := 0; r < k; r++ {
		id := r*k + col
		if id < n {
			seen[id] = struct{}{}
		}
	}

	result := make([]int, 0, len(seen))
	for id := range seen {
		result = append(result, id)
	}
	sort.Ints(result)
	return result
}

// sortedActiveIDs returns the keys of activeWorkers that are true, in ascending order.
func sortedActiveIDs(activeWorkers map[int]bool) []int {
	ids := make([]int, 0, len(activeWorkers))
	for id, active := range activeWorkers {
		if active {
			ids = append(ids, id)
		}
	}
	sort.Ints(ids)
	return ids
}

// RegridQuorum computes a fresh quorum for selfID after membership changes.
// It maps the active set onto positions 0..len(active)-1, runs QuorumFor on
// the position of selfID, then maps positions back to original IDs.
func RegridQuorum(selfID int, active []int) []int {
	pos := -1
	for i, id := range active {
		if id == selfID {
			pos = i
			break
		}
	}
	if pos < 0 {
		return nil
	}
	positions := QuorumFor(pos, len(active))
	result := make([]int, len(positions))
	for i, p := range positions {
		result[i] = active[p]
	}
	sort.Ints(result)
	return result
}

// priority queue using min-heap by lamport's clock and sender ID
type RequestHeap []*maekawapb.MaekawaMsg

func (h RequestHeap) Len() int { return len(h) }

func (h RequestHeap) Less(i, j int) bool {
	if h[i].Clock != h[j].Clock {
		return h[i].Clock < h[j].Clock
	}
	return h[i].SenderId < h[j].SenderId
}

func (h RequestHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *RequestHeap) Push(x any) {
	*h = append(*h, x.(*maekawapb.MaekawaMsg))
}

func (h *RequestHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return x
}

func HeapPush(h *RequestHeap, msg *maekawapb.MaekawaMsg) {
	heap.Push(h, msg)
}

// pop highest priority request -> lowest lock/ sender ID if clocks the same
func HeapPop(h *RequestHeap) *maekawapb.MaekawaMsg {
	return heap.Pop(h).(*maekawapb.MaekawaMsg)
}

// HeapRemove removes the first entry matching (senderID, clock) from the heap.
func HeapRemove(h *RequestHeap, senderID int32, clock int64) {
	for i, msg := range *h {
		if msg.SenderId == senderID && msg.Clock == clock {
			heap.Remove(h, i)
			return
		}
	}
}
