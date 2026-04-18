/*
 * merge_sorted.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import "container/heap"

// MergeSorted merges multiple pre-sorted sources into a single sorted stream
// using a min-heap. The less function defines the ordering (return true when
// a should appear before b). All input sources must already be sorted
// according to the same ordering.
func MergeSorted[T any](less func(T, T) bool, sources ...Source[T, NotUsed]) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			iters := make([]iterator[T], len(sources))
			for i, s := range sources {
				iters[i], _ = s.factory()
			}
			return &mergeSortedIterator[T]{
				iters: iters,
				h:     &mergeSortedHeap[T]{less: less},
			}, NotUsed{}
		},
	}
}

type mergeSortedEntry[T any] struct {
	value     T
	sourceIdx int
}

type mergeSortedHeap[T any] struct {
	entries []mergeSortedEntry[T]
	less    func(T, T) bool
}

func (h *mergeSortedHeap[T]) Len() int            { return len(h.entries) }
func (h *mergeSortedHeap[T]) Less(i, j int) bool   { return h.less(h.entries[i].value, h.entries[j].value) }
func (h *mergeSortedHeap[T]) Swap(i, j int)        { h.entries[i], h.entries[j] = h.entries[j], h.entries[i] }
func (h *mergeSortedHeap[T]) Push(x any)            { h.entries = append(h.entries, x.(mergeSortedEntry[T])) }
func (h *mergeSortedHeap[T]) Pop() any {
	old := h.entries
	n := len(old)
	e := old[n-1]
	h.entries = old[:n-1]
	return e
}

type mergeSortedIterator[T any] struct {
	iters  []iterator[T]
	h      *mergeSortedHeap[T]
	inited bool
}

func (m *mergeSortedIterator[T]) next() (T, bool, error) {
	var zero T

	if !m.inited {
		m.inited = true
		for i, it := range m.iters {
			v, ok, err := it.next()
			if err != nil {
				return zero, false, err
			}
			if ok {
				heap.Push(m.h, mergeSortedEntry[T]{value: v, sourceIdx: i})
			}
		}
	}

	if m.h.Len() == 0 {
		return zero, false, nil
	}

	entry := heap.Pop(m.h).(mergeSortedEntry[T])

	// Advance the source we just consumed from.
	v, ok, err := m.iters[entry.sourceIdx].next()
	if err != nil {
		return zero, false, err
	}
	if ok {
		heap.Push(m.h, mergeSortedEntry[T]{value: v, sourceIdx: entry.sourceIdx})
	}

	return entry.value, true, nil
}
