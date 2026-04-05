package utils

import (
	"fmt"
)

type GenericMinHeap[T any] struct {
	data     []T
	less func(a, b T) bool
}

func NewGenericMinHeap[T any](less func(a, b T) bool) *GenericMinHeap[T] {
	return &GenericMinHeap[T] {
		data: make([]T, 0),
		less: less,
	}
}

func (h *GenericMinHeap[T]) Len() int {
	return len(h.data)
}
func (h *GenericMinHeap[T]) Less(i, j int) bool {
	return h.less(h.data[i], h.data[j])
}
func (h *GenericMinHeap[T]) Swap(i, j int) {
	h.data[i], h.data[j] = h.data[j], h.data[i]
}
func (h *GenericMinHeap[T]) Push(x any) {
	typedVal, ok := x.(T)
	if !ok {
		fmt.Printf("INTERNAL ERROR: Heap received wrong type. Got %T, wants %T\n", x, *new(T))
		return
	}
	h.data = append(h.data, typedVal)
}
func (h *GenericMinHeap[T]) Pop() any {
	old := h.data
	n := h.Len()
	if n == 0 {
		return nil
	}
	item := old[n-1]
	var zero T
	old[n-1] = zero // Nil out the entry to prevent mem leaks
	h.data = old[0 : n-1]
	return item
}
