package maekawa

import (
	"testing"

	maekawapb "raft-maekawa/proto/maekawapb"
)

const n = 9 // total workers

// TestQuorumSize checks the expected quorum size for a 3x3 grid.
func TestQuorumSize(t *testing.T) {
	for id := 0; id < n; id++ {
		q := QuorumFor(id, n)
		if len(q) != 5 {
			t.Errorf("node %d: expected quorum size 5, got %d: %v", id, len(q), q)
		}
	}
}

// TestQuorumSelfIncluded checks that each node is in its own quorum.
func TestQuorumSelfIncluded(t *testing.T) {
	for id := 0; id < n; id++ {
		q := QuorumFor(id, n)
		found := false
		for _, member := range q {
			if member == id {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("node %d not found in its own quorum: %v", id, q)
		}
	}
}

// TestQuorumIntersection checks that every pair of quorums intersects.
func TestQuorumIntersection(t *testing.T) {
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			qi := QuorumFor(i, n)
			qj := QuorumFor(j, n)
			if !intersects(qi, qj) {
				t.Errorf("quorum(%d)=%v and quorum(%d)=%v do not intersect", i, qi, j, qj)
			}
		}
	}
}

// TestQuorumSorted checks that QuorumFor returns sorted members.
func TestQuorumSorted(t *testing.T) {
	for id := 0; id < n; id++ {
		q := QuorumFor(id, n)
		for k := 1; k < len(q); k++ {
			if q[k] < q[k-1] {
				t.Errorf("node %d: quorum not sorted: %v", id, q)
			}
		}
	}
}

// TestKnownQuorums spot-checks RegridQuorum against known full-grid results.
func TestKnownQuorums(t *testing.T) {
	active := []int{0, 1, 2, 3, 4, 5, 6, 7, 8}
	cases := map[int][]int{
		0: {0, 1, 2, 3, 6},
		4: {1, 3, 4, 5, 7},
		8: {2, 5, 6, 7, 8},
	}
	for id, expected := range cases {
		got := RegridQuorum(id, active)
		if !equal(got, expected) {
			t.Errorf("node %d: expected %v, got %v", id, expected, got)
		}
	}
}

// TestRegridAfterRemoval checks self-inclusion and intersection after a removal.
func TestRegridAfterRemoval(t *testing.T) {
	removed := 4
	active := make([]int, 0, n-1)
	for id := 0; id < n; id++ {
		if id != removed {
			active = append(active, id)
		}
	}

	quorums := make(map[int][]int, len(active))
	for _, id := range active {
		q := RegridQuorum(id, active)
		found := false
		for _, m := range q {
			if m == id {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("node %d not in its own regrid quorum %v", id, q)
		}
		quorums[id] = q
	}

	for i := 0; i < len(active); i++ {
		for j := i + 1; j < len(active); j++ {
			a, b := active[i], active[j]
			if !intersects(quorums[a], quorums[b]) {
				t.Errorf("quorum(%d)=%v and quorum(%d)=%v do not intersect after removal of %d",
					a, quorums[a], b, quorums[b], removed)
			}
		}
	}
}

// TestRegridAfterAdd checks self-inclusion and intersection after an add.
func TestRegridAfterAdd(t *testing.T) {
	newID := 9
	active := make([]int, 0, n+1)
	for id := 0; id < n; id++ {
		active = append(active, id)
	}
	active = append(active, newID)

	quorums := make(map[int][]int, len(active))
	for _, id := range active {
		q := RegridQuorum(id, active)
		found := false
		for _, m := range q {
			if m == id {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("node %d not in its own regrid quorum %v after add", id, q)
		}
		quorums[id] = q
	}

	for i := 0; i < len(active); i++ {
		for j := i + 1; j < len(active); j++ {
			a, b := active[i], active[j]
			if !intersects(quorums[a], quorums[b]) {
				t.Errorf("quorum(%d)=%v and quorum(%d)=%v do not intersect after add of %d",
					a, quorums[a], b, quorums[b], newID)
			}
		}
	}
}

// TestRequestHeapOrder checks heap order by clock then sender ID.
func TestRequestHeapOrder(t *testing.T) {
	h := &RequestHeap{}

	msgs := []*maekawapb.MaekawaMsg{
		{SenderId: 3, Clock: 10},
		{SenderId: 1, Clock: 5},
		{SenderId: 7, Clock: 5},
		{SenderId: 2, Clock: 8},
		{SenderId: 6, Clock: 1},
	}

	for _, m := range msgs {
		HeapPush(h, m)
	}

	expected := []struct {
		clock    int64
		senderID int32
	}{
		{1, 6},
		{5, 1},
		{5, 7},
		{8, 2},
		{10, 3},
	}

	for i, exp := range expected {
		msg := HeapPop(h)
		if msg.Clock != exp.clock || msg.SenderId != exp.senderID {
			t.Errorf("pop %d: expected (clock=%d, sender=%d), got (clock=%d, sender=%d)",
				i, exp.clock, exp.senderID, msg.Clock, msg.SenderId)
		}
	}
}

func intersects(a, b []int) bool {
	set := make(map[int]bool, len(a))
	for _, v := range a {
		set[v] = true
	}
	for _, v := range b {
		if set[v] {
			return true
		}
	}
	return false
}

func equal(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
