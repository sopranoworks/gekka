/*
 * flow_ops.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

// ─── Intersperse ─────────────────────────────────────────────────────────

// Intersperse injects sep between each pair of elements emitted by src.
func Intersperse[T any](src Source[T, NotUsed], sep T) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			up, _ := src.factory()
			return &intersperseIterator[T]{upstream: up, sep: sep}, NotUsed{}
		},
	}
}

// IntersperseAll injects start before the first element, sep between each
// pair of elements, and end after the last element.
func IntersperseAll[T any](src Source[T, NotUsed], start, sep, end T) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			up, _ := src.factory()
			return &intersperseAllIterator[T]{upstream: up, start: start, sep: sep, end: end}, NotUsed{}
		},
	}
}

type intersperseIterator[T any] struct {
	upstream  iterator[T]
	sep       T
	emittedFirst bool
	pending   *T
}

func (it *intersperseIterator[T]) next() (T, bool, error) {
	// Emit pending element after separator
	if it.pending != nil {
		v := *it.pending
		it.pending = nil
		return v, true, nil
	}

	elem, ok, err := it.upstream.next()
	if !ok || err != nil {
		return elem, ok, err
	}

	if it.emittedFirst {
		// Queue the actual element and emit separator first
		it.pending = &elem
		return it.sep, true, nil
	}

	it.emittedFirst = true
	return elem, true, nil
}

type intersperseAllIterator[T any] struct {
	upstream  iterator[T]
	start     T
	sep       T
	end       T
	phase     int // 0=start, 1=elements, 2=end, 3=done
	emittedEl bool
	pending   *T
}

func (it *intersperseAllIterator[T]) next() (T, bool, error) {
	switch it.phase {
	case 0:
		it.phase = 1
		return it.start, true, nil
	case 1:
		// Emit pending element from separator split
		if it.pending != nil {
			v := *it.pending
			it.pending = nil
			return v, true, nil
		}
		elem, ok, err := it.upstream.next()
		if err != nil {
			return elem, false, err
		}
		if !ok {
			it.phase = 2
			return it.end, true, nil
		}
		if it.emittedEl {
			it.pending = &elem
			return it.sep, true, nil
		}
		it.emittedEl = true
		return elem, true, nil
	case 2:
		it.phase = 3
		var zero T
		return zero, false, nil
	default:
		var zero T
		return zero, false, nil
	}
}

// ─── RecoverWithRetries ──────────────────────────────────────────────────

// RecoverWithRetries switches to a recovery source on failure, up to
// attempts times. If attempts is exhausted, the final error propagates.
// Use attempts < 0 for unlimited retries.
func RecoverWithRetries[T any](src Source[T, NotUsed], attempts int, fn func(error) Source[T, NotUsed]) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			up, _ := src.factory()
			return &recoverWithRetriesIterator[T]{
				current:  up,
				fn:       fn,
				attempts: attempts,
			}, NotUsed{}
		},
	}
}

type recoverWithRetriesIterator[T any] struct {
	current  iterator[T]
	fn       func(error) Source[T, NotUsed]
	attempts int
	retries  int
}

func (r *recoverWithRetriesIterator[T]) next() (T, bool, error) {
	for {
		elem, ok, err := r.current.next()
		if err != nil {
			if r.attempts >= 0 && r.retries >= r.attempts {
				return elem, false, err
			}
			r.retries++
			backup := r.fn(err)
			newUp, _ := backup.factory()
			r.current = newUp
			continue
		}
		return elem, ok, nil
	}
}

// ─── MapError ────────────────────────────────────────────────────────────

// MapError transforms errors produced by src using fn. Normal elements
// pass through unchanged.
func MapError[T any](src Source[T, NotUsed], fn func(error) error) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			up, _ := src.factory()
			return &mapErrorIterator[T]{upstream: up, fn: fn}, NotUsed{}
		},
	}
}

type mapErrorIterator[T any] struct {
	upstream iterator[T]
	fn       func(error) error
}

func (m *mapErrorIterator[T]) next() (T, bool, error) {
	elem, ok, err := m.upstream.next()
	if err != nil {
		return elem, ok, m.fn(err)
	}
	return elem, ok, nil
}

// ─── StatefulMap ─────────────────────────────────────────────────────────

// StatefulMap applies a stateful 1:1 transformation to each element.
// create is called once per materialization and returns the mapping function
// that is applied to each element.
func StatefulMap[In, Out any](src Source[In, NotUsed], create func() func(In) Out) Source[Out, NotUsed] {
	return Source[Out, NotUsed]{
		factory: func() (iterator[Out], NotUsed) {
			up, _ := src.factory()
			fn := create()
			return &mapIterator[In, Out]{upstream: up, fn: fn}, NotUsed{}
		},
	}
}

// ─── BatchWeighted ───────────────────────────────────────────────────────

// BatchWeighted is like Batch but uses a cost function to determine when a
// batch is full. Elements are aggregated until the cumulative cost exceeds
// maxWeight.
func BatchWeighted[In, Out any](src Source[In, NotUsed], maxWeight int64, costFn func(In) int64, seed func(In) Out, aggregate func(Out, In) Out) Source[Out, NotUsed] {
	return Source[Out, NotUsed]{
		factory: func() (iterator[Out], NotUsed) {
			up, _ := src.factory()
			return newBatchWeightedIterator(up, maxWeight, costFn, seed, aggregate), NotUsed{}
		},
	}
}

type batchWeightedIterator[In, Out any] struct {
	upstream  iterator[In]
	maxWeight int64
	costFn    func(In) int64
	seed      func(In) Out
	aggregate func(Out, In) Out
	done      bool
}

func newBatchWeightedIterator[In, Out any](up iterator[In], maxWeight int64, costFn func(In) int64, seed func(In) Out, aggregate func(Out, In) Out) *batchWeightedIterator[In, Out] {
	return &batchWeightedIterator[In, Out]{
		upstream:  up,
		maxWeight: maxWeight,
		costFn:    costFn,
		seed:      seed,
		aggregate: aggregate,
	}
}

func (b *batchWeightedIterator[In, Out]) next() (Out, bool, error) {
	if b.done {
		var zero Out
		return zero, false, nil
	}

	// Pull first element to start batch
	elem, ok, err := b.upstream.next()
	if err != nil {
		var zero Out
		return zero, false, err
	}
	if !ok {
		b.done = true
		var zero Out
		return zero, false, nil
	}

	batch := b.seed(elem)
	weight := b.costFn(elem)

	// Aggregate until weight exceeds maxWeight or upstream exhausted
	for weight < b.maxWeight {
		elem, ok, err = b.upstream.next()
		if err != nil {
			var zero Out
			return zero, false, err
		}
		if !ok {
			b.done = true
			return batch, true, nil
		}
		cost := b.costFn(elem)
		if weight+cost > b.maxWeight {
			// This element would exceed the limit; buffer it for the next batch.
			b.upstream = &prependIterator[In]{first: elem, hasFirst: true, rest: b.upstream}
			return batch, true, nil
		}
		weight += cost
		batch = b.aggregate(batch, elem)
	}

	return batch, true, nil
}
