/*
 * map_async_partitioned.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import "sync"

// MapAsyncPartitioned processes elements in parallel while preserving per-key ordering.
//
// Elements are grouped by keyFn. Within a partition (same key), elements are
// dispatched one at a time: the next element for that key is only launched once
// the previous one completes. Different partitions may overlap up to the
// parallelism limit (total concurrent goroutines across all keys).
//
// This matches Pekko Streams' mapAsyncPartitioned operator introduced in 1.1.
func MapAsyncPartitioned[In any, Key comparable, Out any](
	src Source[In, NotUsed],
	parallelism int,
	keyFn func(In) Key,
	fn func(In) <-chan Out,
) Source[Out, NotUsed] {
	return Source[Out, NotUsed]{
		factory: func() (iterator[Out], NotUsed) {
			upstream, _ := src.factory()
			return &mapAsyncPartitionedIter[In, Key, Out]{
				upstream:   upstream,
				keyFn:      keyFn,
				fn:         fn,
				partitions: make(map[Key]*partitionQueue[Out]),
				sem:        make(chan struct{}, parallelism),
			}, NotUsed{}
		},
	}
}

// partitionQueue tracks pending result channels for a single partition key.
// Elements are queued in arrival order; the head is the oldest in-flight result.
// Invariant: len(pending) <= 1 — the dispatch loop serialises per-key launches.
type partitionQueue[Out any] struct {
	pending []<-chan Out // ordered: oldest first
}

type mapAsyncPartitionedIter[In any, Key comparable, Out any] struct {
	upstream iterator[In]
	keyFn    func(In) Key
	fn       func(In) <-chan Out

	mu         sync.Mutex
	partitions map[Key]*partitionQueue[Out]
	sem        chan struct{}
	outBuf     []Out
	// upstreamDone is accessed only from next(); not goroutine-safe.
	upstreamDone bool
}

func (m *mapAsyncPartitionedIter[In, Key, Out]) next() (Out, bool, error) {
	var zero Out
	for {
		// Emit buffered results first.
		if len(m.outBuf) > 0 {
			v := m.outBuf[0]
			m.outBuf = m.outBuf[1:]
			return v, true, nil
		}

		// If upstream is done, drain all remaining in-flight results.
		if m.upstreamDone {
			return m.drainAll()
		}

		// Try to return a completed head result from any partition (non-blocking).
		if v, ok := m.tryDequeue(); ok {
			return v, true, nil
		}

		// Pull next upstream element.
		elem, ok, err := m.upstream.next()
		if err != nil {
			return zero, false, err
		}
		if !ok {
			m.upstreamDone = true
			// Drain all remaining in-flight results.
			return m.drainAll()
		}

		key := m.keyFn(elem)

		// Within a partition, only one element may be in-flight at a time.
		// If the partition already has an in-flight element, we must wait for it
		// to complete before launching the next one for that key.
		m.mu.Lock()
		q := m.partitions[key]
		if q != nil && len(q.pending) > 0 {
			// Partition is busy: wait for its head to complete before dispatching.
			headCh := q.pending[0]
			m.mu.Unlock()

			// Block until the head result arrives.
			val, received := <-headCh

			m.mu.Lock()
			// Remove the head from the queue.
			q.pending = q.pending[1:]
			if len(q.pending) == 0 {
				delete(m.partitions, key)
			}
			m.mu.Unlock()

			// Now launch the new element for this partition.
			m.sem <- struct{}{}
			resCh := make(chan Out, 1)
			go func(e In, out chan<- Out) {
				userCh := m.fn(e)
				out <- <-userCh
				<-m.sem
			}(elem, resCh)

			m.mu.Lock()
			pq := m.partitions[key]
			if pq == nil {
				pq = &partitionQueue[Out]{}
				m.partitions[key] = pq
			}
			pq.pending = append(pq.pending, resCh)
			m.mu.Unlock()

			// Emit the head result we already received.
			if received {
				return val, true, nil
			}
			// Channel closed without value (drop), continue loop.
			continue
		}
		m.mu.Unlock()

		// Partition is idle: acquire semaphore and launch goroutine.
		m.sem <- struct{}{}
		resCh := make(chan Out, 1)
		go func(e In, out chan<- Out) {
			userCh := m.fn(e)
			out <- <-userCh
			<-m.sem
		}(elem, resCh)

		m.mu.Lock()
		pq := m.partitions[key]
		if pq == nil {
			pq = &partitionQueue[Out]{}
			m.partitions[key] = pq
		}
		pq.pending = append(pq.pending, resCh)
		m.mu.Unlock()
	}
}

// tryDequeue checks the head channel of each partition for a ready result.
// Returns (value, true) if one is ready without blocking; (zero, false) otherwise.
func (m *mapAsyncPartitionedIter[In, Key, Out]) tryDequeue() (Out, bool) {
	var zero Out
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, q := range m.partitions {
		if len(q.pending) == 0 {
			continue
		}
		select {
		case v, ok := <-q.pending[0]:
			q.pending = q.pending[1:]
			if len(q.pending) == 0 {
				delete(m.partitions, key)
			}
			if ok {
				return v, true
			}
			// Channel closed without value: drop and try next partition.
		default:
		}
	}
	return zero, false
}

// drainAll blocks until all in-flight results are collected, respecting
// per-partition ordering. Returns the first result and buffers the rest.
func (m *mapAsyncPartitionedIter[In, Key, Out]) drainAll() (Out, bool, error) {
	var zero Out

	m.mu.Lock()
	// Collect all pending channels in per-partition order.
	// We gather them as (key, channel) pairs grouped by partition.
	type keyedCh struct {
		key Key
		ch  <-chan Out
	}
	// allPending collects all in-flight channels. Cross-partition order is
	// non-deterministic (map iteration); intra-partition order is preserved
	// because q.pending is an arrival-ordered slice.
	var allPending []keyedCh
	for key, q := range m.partitions {
		for _, ch := range q.pending {
			allPending = append(allPending, keyedCh{key: key, ch: ch})
		}
	}
	m.partitions = make(map[Key]*partitionQueue[Out])
	m.mu.Unlock()

	if len(allPending) == 0 {
		return zero, false, nil
	}

	// Receive from each channel in order (blocking). Collect non-dropped results.
	var results []Out
	for _, kc := range allPending {
		val, ok := <-kc.ch
		if ok {
			results = append(results, val)
		}
	}

	if len(results) == 0 {
		return zero, false, nil
	}
	if len(results) > 1 {
		m.outBuf = append(m.outBuf, results[1:]...)
	}
	return results[0], true, nil
}
