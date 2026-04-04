/*
 * ops.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"sync"
)

// ─── TakeWhile ────────────────────────────────────────────────────────────────

// TakeWhile creates a Flow that forwards elements while pred is true.
// The first element for which pred returns false terminates the stream
// (that element is never emitted).
//
// Mirroring Pekko's Flow.takeWhile.
func TakeWhile[T any](pred func(T) bool) Flow[T, T, NotUsed] {
	return Flow[T, T, NotUsed]{
		attach: func(up iterator[T]) (iterator[T], NotUsed) {
			return &takeWhileIterator[T]{upstream: up, pred: pred}, NotUsed{}
		},
	}
}

// TakeWhile appends a TakeWhile stage after this Flow's output port.
func (f Flow[In, Out, Mat]) TakeWhile(pred func(Out) bool) Flow[In, Out, Mat] {
	return Flow[In, Out, Mat]{
		attach: func(up iterator[In]) (iterator[Out], Mat) {
			inner, mat := f.attach(up)
			return &takeWhileIterator[Out]{upstream: inner, pred: pred}, mat
		},
	}
}

// TakeWhile appends a TakeWhile stage after this Source.
func (s Source[T, Mat]) TakeWhile(pred func(T) bool) Source[T, Mat] {
	return Source[T, Mat]{
		factory: func() (iterator[T], Mat) {
			up, mat := s.factory()
			return &takeWhileIterator[T]{upstream: up, pred: pred}, mat
		},
	}
}

// ─── DropWhile ────────────────────────────────────────────────────────────────

// DropWhile creates a Flow that discards elements while pred is true.
// Once pred returns false for an element, that element and all subsequent
// elements are forwarded downstream.
//
// Mirroring Pekko's Flow.dropWhile.
func DropWhile[T any](pred func(T) bool) Flow[T, T, NotUsed] {
	return Flow[T, T, NotUsed]{
		attach: func(up iterator[T]) (iterator[T], NotUsed) {
			return newDropWhileIterator(up, pred), NotUsed{}
		},
	}
}

// DropWhile appends a DropWhile stage after this Flow's output port.
func (f Flow[In, Out, Mat]) DropWhile(pred func(Out) bool) Flow[In, Out, Mat] {
	return Flow[In, Out, Mat]{
		attach: func(up iterator[In]) (iterator[Out], Mat) {
			inner, mat := f.attach(up)
			return newDropWhileIterator(inner, pred), mat
		},
	}
}

// DropWhile appends a DropWhile stage after this Source.
func (s Source[T, Mat]) DropWhile(pred func(T) bool) Source[T, Mat] {
	return Source[T, Mat]{
		factory: func() (iterator[T], Mat) {
			up, mat := s.factory()
			return newDropWhileIterator(up, pred), mat
		},
	}
}

// ─── FlatMapConcat ────────────────────────────────────────────────────────────

// FlatMapConcat applies fn to each element of src, producing a sub-source per
// element, and concatenates the sub-sources sequentially — the first
// sub-source runs to completion before the next is started.
//
// This is equivalent to FlattenMerge with breadth=1 and preserves element
// order across sub-sources.
//
// Mirroring Pekko's Source/Flow.flatMapConcat.
func FlatMapConcat[In, Out any](src Source[In, NotUsed], fn func(In) Source[Out, NotUsed]) Source[Out, NotUsed] {
	return Source[Out, NotUsed]{
		factory: func() (iterator[Out], NotUsed) {
			upstream, _ := src.factory()
			// Map each element to a Source, then flatten sequentially (breadth=1).
			mapped := &mapIterator[In, Source[Out, NotUsed]]{upstream: upstream, fn: fn}
			return newFlattenMergeIterator[Out](mapped, 1), NotUsed{}
		},
	}
}

// ─── MapAsyncUnordered ────────────────────────────────────────────────────────

// MapAsyncUnordered applies fn to each element of src concurrently (up to
// parallelism goroutines) and emits results as they complete, without
// preserving input order.
//
// This trades ordering guarantees for reduced head-of-line blocking: a slow
// element does not delay faster ones that were submitted later.
//
// Like [MapAsync], fn must return a channel that delivers exactly one value.
//
// Mirroring Pekko's Source.mapAsyncUnordered.
func MapAsyncUnordered[In, Out any](src Source[In, NotUsed], parallelism int, fn func(In) <-chan Out) Source[Out, NotUsed] {
	return Source[Out, NotUsed]{
		factory: func() (iterator[Out], NotUsed) {
			upstream, _ := src.factory()
			return newMapAsyncUnorderedIterator(upstream, parallelism, fn), NotUsed{}
		},
	}
}

// ─── Scan ─────────────────────────────────────────────────────────────────────

// Scan emits a running accumulated value, starting with zero and updating it
// with fn for each element.  The first emission is zero itself (before any
// element is processed), matching Pekko's scan semantics.
//
// Example — running sum over [1,2,3]:
//
//	Scan(src, 0, func(acc, n int) int { return acc + n })
//	// emits 0, 1, 3, 6
//
// Mirroring Pekko's Source/Flow.scan.
func Scan[In, S any](src Source[In, NotUsed], zero S, fn func(S, In) S) Source[S, NotUsed] {
	return Source[S, NotUsed]{
		factory: func() (iterator[S], NotUsed) {
			upstream, _ := src.factory()
			return &scanIterator[In, S]{upstream: upstream, acc: zero, emitSeed: true, fn: fn}, NotUsed{}
		},
	}
}

// ─── mapAsyncUnorderedIterator ────────────────────────────────────────────────

type mapAsyncUnorderedIterator[In, Out any] struct {
	ch    chan Out
	errCh chan error
}

func newMapAsyncUnorderedIterator[In, Out any](
	upstream iterator[In],
	parallelism int,
	fn func(In) <-chan Out,
) *mapAsyncUnorderedIterator[In, Out] {
	it := &mapAsyncUnorderedIterator[In, Out]{
		ch:    make(chan Out, parallelism),
		errCh: make(chan error, 1),
	}

	go func() {
		defer close(it.ch)

		sem := make(chan struct{}, parallelism)
		var wg sync.WaitGroup

		for {
			elem, ok, err := upstream.next()
			if err != nil {
				select {
				case it.errCh <- err:
				default:
				}
				wg.Wait()
				return
			}
			if !ok {
				break
			}

			sem <- struct{}{}
			wg.Add(1)
			go func(e In) {
				defer wg.Done()
				defer func() { <-sem }()
				resultCh := fn(e)
				if v, ok2 := <-resultCh; ok2 {
					it.ch <- v
				}
			}(elem)
		}
		wg.Wait()
	}()

	return it
}

func (m *mapAsyncUnorderedIterator[In, Out]) next() (Out, bool, error) {
	select {
	case err := <-m.errCh:
		var zero Out
		return zero, false, err
	default:
	}
	select {
	case err := <-m.errCh:
		var zero Out
		return zero, false, err
	case v, ok := <-m.ch:
		if !ok {
			select {
			case err := <-m.errCh:
				var zero Out
				return zero, false, err
			default:
				var zero Out
				return zero, false, nil
			}
		}
		return v, true, nil
	}
}

// ─── takeWhileIterator ────────────────────────────────────────────────────────

type takeWhileIterator[T any] struct {
	upstream iterator[T]
	pred     func(T) bool
	done     bool
}

func (t *takeWhileIterator[T]) next() (T, bool, error) {
	if t.done {
		var zero T
		return zero, false, nil
	}
	elem, ok, err := t.upstream.next()
	if !ok || err != nil {
		return elem, ok, err
	}
	if !t.pred(elem) {
		t.done = true
		var zero T
		return zero, false, nil
	}
	return elem, true, nil
}

// ─── dropWhileIterator ────────────────────────────────────────────────────────

type dropWhileIterator[T any] struct {
	upstream iterator[T]
	pred     func(T) bool
	dropping bool
}

func newDropWhileIterator[T any](upstream iterator[T], pred func(T) bool) *dropWhileIterator[T] {
	return &dropWhileIterator[T]{upstream: upstream, pred: pred, dropping: true}
}

func (d *dropWhileIterator[T]) next() (T, bool, error) {
	for {
		elem, ok, err := d.upstream.next()
		if !ok || err != nil {
			return elem, ok, err
		}
		if d.dropping {
			if d.pred(elem) {
				continue // drop
			}
			d.dropping = false
		}
		return elem, true, nil
	}
}

// ─── scanIterator ─────────────────────────────────────────────────────────────

type scanIterator[In, S any] struct {
	upstream iterator[In]
	acc      S
	fn       func(S, In) S
	emitSeed bool // true until the seed value has been emitted
	done     bool
}

func (s *scanIterator[In, S]) next() (S, bool, error) {
	if s.done {
		var zero S
		return zero, false, nil
	}
	// Emit the seed value before pulling the first upstream element.
	if s.emitSeed {
		s.emitSeed = false
		return s.acc, true, nil
	}
	elem, ok, err := s.upstream.next()
	if !ok || err != nil {
		s.done = true
		return s.acc, ok, err
	}
	s.acc = s.fn(s.acc, elem)
	return s.acc, true, nil
}
