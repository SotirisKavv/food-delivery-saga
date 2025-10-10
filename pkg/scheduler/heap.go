package scheduler

import (
	"time"
)

type Entry[T any] struct {
	ID      string
	Index   int // Heap i
	Value   T
	ReadyAt time.Time
}

type Heap[T any] struct {
	Entries []*Entry[T]
}

// get first item
func (h Heap[T]) Peek() *Entry[T] {
	if h.Len() == 0 {
		return nil
	}
	return h.Entries[0]
}

func (h Heap[T]) Len() int { return len(h.Entries) }

func (h Heap[T]) Less(i, j int) bool {
	return h.Entries[i].ReadyAt.Before(h.Entries[j].ReadyAt)
}

func (h Heap[T]) Swap(i, j int) {
	h.Entries[i], h.Entries[j] = h.Entries[j], h.Entries[i]
	h.Entries[i].Index = i
	h.Entries[j].Index = j
}

// set item to last position
func (h *Heap[T]) Push(item *Entry[T]) {
	item.Index = len(h.Entries)
	h.Entries = append(h.Entries, item)
	h.ShiftUp(item.Index)
}

// get the last item
func (h *Heap[T]) Pop() *Entry[T] {
	n := h.Len()
	if n == 0 {
		return nil
	}

	h.Swap(0, n-1)
	min := h.Entries[n-1]
	h.Entries[n-1] = nil
	h.Entries = h.Entries[:n-1]
	min.Index = -1
	if len(h.Entries) > 0 {
		h.ShiftDown(0)
	}

	return min
}

func (h *Heap[T]) Remove(index int) *Entry[T] {
	n := h.Len()
	if index < 0 || index >= n {
		return nil
	}

	h.Swap(index, n-1)
	removed := h.Entries[n-1]
	h.Entries[n-1] = nil
	h.Entries = h.Entries[:n-1]
	removed.Index = -1
	if len(h.Entries) > 0 {
		h.Fix(index)
	}
	return removed
}

func (h *Heap[T]) Fix(index int) {
	if index < 0 || h.Len() <= index {
		return
	}

	if !h.ShiftDown(index) {
		h.ShiftUp(index)
	}
}

func (h *Heap[T]) ShiftUp(index int) {
	for {
		if index == 0 {
			return
		}
		p := (index - 1) / 2
		if !h.Less(index, p) {
			return
		}
		h.Swap(index, p)
		index = p
	}
}

func (h *Heap[T]) ShiftDown(index int) bool {
	moved := false
	n := h.Len()

	for {
		left := 2*index + 1
		if left >= n {
			break
		}
		smallest := left
		right := left + 1
		if right < n && h.Less(right, left) {
			smallest = right
		}
		if !h.Less(smallest, index) {
			break
		}
		h.Swap(index, smallest)
		index = smallest
		moved = true
	}

	return moved
}
