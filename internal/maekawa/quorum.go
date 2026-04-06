// handle quorum logic
package maekawa

import (
	"math"
	"sort"
)

func gridSizeFor(n int32) int32 {
	return int32(math.Ceil(math.Sqrt(float64(n))))
}

func rowOf(nodeID, n int32) int32 { return nodeID / gridSizeFor(n) }

func colOf(nodeID, n int32) int32 { return nodeID % gridSizeFor(n) }

func QuorumFor(nodeID, n int32) []int32 {
	seen := make(map[int32]struct{})

	k := gridSizeFor(n)
	row := rowOf(nodeID, n)
	col := colOf(nodeID, n)

	for i := range n {
		if rowOf(i, n) == row {
			seen[i] = struct{}{}
		}
	}

	for r := range k {
		id := r*k + col
		if id < n {
			seen[id] = struct{}{}
		}
	}

	result := make([]int32, 0, len(seen))
	for id := range seen {
		result = append(result, id)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})
	return result
}

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
func RegridQuorum(selfID int32, active []int32) []int32 {
	sort.Slice(active, func(i, j int) bool {
		return active[i] < active[j]
	})

	pos:= -1
	for i, id := range active {
		if id == selfID {
			pos = i
			break
		}
	}

	if pos < 0 {
		return nil
	}

	positions := QuorumFor(int32(pos), int32(len(active)))
	result := make([]int32, len(positions))
	for i, p := range positions {
		result[i] = active[p]
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})

	return result
}

//TODO: rewire to GenericMinHeap

// type RequestHeap []*utils.GenericMinHeap[*maekawapb.Worker]

// func (h RequestHeap) Len() int { return len(h) }

// func (h RequestHeap) Less(i, j int) bool {
// 	if h[i].Clock != h[j].Clock {
// 		return h[i].Clock < h[j].Clock
// 	}
// 	return h[i].SenderId < h[j].SenderId
// }

// func (h RequestHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// func (h *RequestHeap) Push(x any) {
// 	*h = append(*h, x.(*maekawapb.MaekawaMsg))
// }

// func (h *RequestHeap) Pop() any {
// 	old := *h
// 	n := len(old)
// 	x := old[n-1]
// 	old[n-1] = nil
// 	*h = old[:n-1]
// 	return x
// }

// func HeapPush(h *RequestHeap, msg *maekawapb.MaekawaMsg) {
// 	heap.Push(h, msg)
// }

// func HeapPop(h *RequestHeap) *maekawapb.MaekawaMsg {
// 	return heap.Pop(h).(*maekawapb.MaekawaMsg)
// }

// // HeapRemove removes the first entry matching (senderID, clock) from the heap.
// func HeapRemove(h *RequestHeap, senderID int32, clock int64) {
// 	for i, msg := range *h {
// 		if msg.SenderId == senderID && msg.Clock == clock {
// 			heap.Remove(h, i)
// 			return
// 		}
// 	}
// }
