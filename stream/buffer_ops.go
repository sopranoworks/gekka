/*
 * buffer_ops.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"fmt"
	"sync"
)

// ─── Batch ────────────────────────────────────────────────────────────────

// Batch creates a Flow that collapses elements from a fast producer.
// When downstream pulls:
//   - If elements have been received, emit the aggregated batch.
//   - The batch accumulates using seed (first element) and aggregate
//     (subsequent elements), up to max weighted cost.
func Batch[In, Out any](max int64, costFn func(In) int64, seed func(In) Out, aggregate func(Out, In) Out) Flow[In, Out, NotUsed] {
	return Flow[In, Out, NotUsed]{
		attach: func(up iterator[In]) (iterator[Out], NotUsed) {
			return newBatchIterator(up, max, costFn, seed, aggregate), NotUsed{}
		},
	}
}

type batchIterator[In, Out any] struct {
	upstream  iterator[In]
	max       int64
	costFn    func(In) int64
	seed      func(In) Out
	aggregate func(Out, In) Out
	pending   *Out
	weight    int64
	done      bool
}

func newBatchIterator[In, Out any](up iterator[In], max int64, costFn func(In) int64, seed func(In) Out, aggregate func(Out, In) Out) *batchIterator[In, Out] {
	return &batchIterator[In, Out]{
		upstream:  up,
		max:       max,
		costFn:    costFn,
		seed:      seed,
		aggregate: aggregate,
	}
}

func (b *batchIterator[In, Out]) next() (Out, bool, error) {
	if b.done {
		var zero Out
		return zero, false, nil
	}

	// If we have a pending batch from a previous overflow, emit it first
	if b.pending != nil {
		result := *b.pending
		b.pending = nil
		b.weight = 0
		return result, true, nil
	}

	// Pull first element to start a new batch
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
	b.weight = b.costFn(elem)

	// Try to accumulate more elements while within weight limit
	for b.weight < b.max {
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
		if b.weight+cost > b.max {
			// Start a new batch with this element for next pull
			next := b.seed(elem)
			b.pending = &next
			b.weight = cost
			return batch, true, nil
		}

		batch = b.aggregate(batch, elem)
		b.weight += cost
	}

	return batch, true, nil
}

// ─── Expand ───────────────────────────────────────────────────────────────

// Expand creates a Flow that generates extra elements when the consumer is
// faster than the producer. The expander function is called with each upstream
// element and returns an iterator function: each call yields (value, hasMore).
// When upstream completes, remaining generated elements are drained.
func Expand[T any](expander func(T) func() (T, bool)) Flow[T, T, NotUsed] {
	return Flow[T, T, NotUsed]{
		attach: func(up iterator[T]) (iterator[T], NotUsed) {
			return newExpandIterator(up, expander), NotUsed{}
		},
	}
}

// Expand appends an Expand stage after this Source.
func (s Source[T, Mat]) Expand(expander func(T) func() (T, bool)) Source[T, Mat] {
	return Source[T, Mat]{
		factory: func() (iterator[T], Mat) {
			up, mat := s.factory()
			return newExpandIterator(up, expander), mat
		},
	}
}

// Extrapolate creates a Flow similar to Expand but optionally provides an
// initial element whose expander is used before the first upstream element.
func Extrapolate[T any](expander func(T) func() (T, bool), initial ...T) Flow[T, T, NotUsed] {
	return Flow[T, T, NotUsed]{
		attach: func(up iterator[T]) (iterator[T], NotUsed) {
			it := newExpandIterator(up, expander)
			if len(initial) > 0 {
				gen := expander(initial[0])
				it.gen = gen
				it.hasGen = true
			}
			return it, NotUsed{}
		},
	}
}

type expandIterator[T any] struct {
	upstream     iterator[T]
	expander     func(T) func() (T, bool)
	gen          func() (T, bool)
	hasGen       bool
	seenUpstream bool
	done         bool
}

func newExpandIterator[T any](up iterator[T], expander func(T) func() (T, bool)) *expandIterator[T] {
	return &expandIterator[T]{upstream: up, expander: expander}
}

func (e *expandIterator[T]) next() (T, bool, error) {
	if e.done {
		var zero T
		return zero, false, nil
	}

	// Emit initial generated element (from Extrapolate) before first upstream pull
	if e.hasGen && !e.seenUpstream {
		e.seenUpstream = true
		v, _ := e.gen()
		return v, true, nil
	}

	// Pull from upstream
	elem, ok, err := e.upstream.next()
	if err != nil {
		var zero T
		return zero, false, err
	}
	if ok {
		e.seenUpstream = true
		e.gen = e.expander(elem)
		e.hasGen = true
		return elem, true, nil
	}

	// Upstream completed — stream completes
	e.done = true
	var zero T
	return zero, false, nil
}

// ─── Sliding ──────────────────────────────────────────────────────────────

// Sliding creates a Flow that emits overlapping windows of size n, advancing
// by step elements. Each emitted element is a slice of length n (except
// possibly the last window if fewer than n elements remain).
func Sliding[T any](n, step int) Flow[T, []T, NotUsed] {
	return Flow[T, []T, NotUsed]{
		attach: func(up iterator[T]) (iterator[[]T], NotUsed) {
			return newSlidingIterator[T](up, n, step), NotUsed{}
		},
	}
}

type slidingIterator[T any] struct {
	upstream iterator[T]
	n        int
	step     int
	window   []T
	filled   bool
	done     bool
}

func newSlidingIterator[T any](up iterator[T], n, step int) *slidingIterator[T] {
	if n <= 0 {
		n = 1
	}
	if step <= 0 {
		step = 1
	}
	return &slidingIterator[T]{upstream: up, n: n, step: step}
}

func (s *slidingIterator[T]) next() ([]T, bool, error) {
	if s.done {
		return nil, false, nil
	}

	if !s.filled {
		// Fill initial window
		for len(s.window) < s.n {
			elem, ok, err := s.upstream.next()
			if err != nil {
				return nil, false, err
			}
			if !ok {
				s.done = true
				if len(s.window) > 0 {
					cp := make([]T, len(s.window))
					copy(cp, s.window)
					s.window = nil
					return cp, true, nil
				}
				return nil, false, nil
			}
			s.window = append(s.window, elem)
		}
		s.filled = true
		cp := make([]T, len(s.window))
		copy(cp, s.window)
		return cp, true, nil
	}

	// Advance by step
	for i := 0; i < s.step; i++ {
		elem, ok, err := s.upstream.next()
		if err != nil {
			return nil, false, err
		}
		if !ok {
			s.done = true
			return nil, false, nil
		}
		// Shift window left and append new element
		newWindow := make([]T, s.n)
		copy(newWindow, s.window[1:])
		newWindow[s.n-1] = elem
		s.window = newWindow
	}

	cp := make([]T, len(s.window))
	copy(cp, s.window)
	return cp, true, nil
}

// ─── SplitWhen / SplitAfter ─────────────────────────────────────────────

// SplitWhen creates a SubFlow that starts a new sub-stream each time the
// predicate returns true. The element that triggers the split becomes the
// first element of the new sub-stream.
func SplitWhen[T any](src Source[T, NotUsed], maxSubstreams int, pred func(T) bool) SubFlow[T] {
	return SubFlow[T]{
		src: Source[SubStream[T], NotUsed]{
			factory: func() (iterator[SubStream[T]], NotUsed) {
				up, _ := src.factory()
				return newSplitIterator[T](up, maxSubstreams, pred, false), NotUsed{}
			},
		},
		maxSubstreams: maxSubstreams,
	}
}

// SplitAfter creates a SubFlow that starts a new sub-stream after each
// element for which the predicate returns true. The triggering element is
// the last element of the current sub-stream.
func SplitAfter[T any](src Source[T, NotUsed], maxSubstreams int, pred func(T) bool) SubFlow[T] {
	return SubFlow[T]{
		src: Source[SubStream[T], NotUsed]{
			factory: func() (iterator[SubStream[T]], NotUsed) {
				up, _ := src.factory()
				return newSplitIterator[T](up, maxSubstreams, pred, true), NotUsed{}
			},
		},
		maxSubstreams: maxSubstreams,
	}
}

type splitIterator[T any] struct {
	upstream      iterator[T]
	maxSubstreams int
	pred          func(T) bool
	after         bool // true = splitAfter, false = splitWhen
	done          bool
	subIdx        int
	pending       *T // element that triggered split in SplitWhen mode
}

func newSplitIterator[T any](up iterator[T], maxSubstreams int, pred func(T) bool, after bool) *splitIterator[T] {
	return &splitIterator[T]{upstream: up, maxSubstreams: maxSubstreams, pred: pred, after: after}
}

func (s *splitIterator[T]) next() (SubStream[T], bool, error) {
	if s.done {
		return SubStream[T]{}, false, nil
	}

	var elems []T

	// If we have a pending element from a previous SplitWhen trigger, start with it
	if s.pending != nil {
		elems = append(elems, *s.pending)
		s.pending = nil
	}

	for {
		elem, ok, err := s.upstream.next()
		if err != nil {
			return SubStream[T]{}, false, err
		}
		if !ok {
			s.done = true
			if len(elems) > 0 {
				key := fmt.Sprintf("sub-%d", s.subIdx)
				s.subIdx++
				return SubStream[T]{Key: key, Source: FromSlice(elems)}, true, nil
			}
			return SubStream[T]{}, false, nil
		}

		if s.after {
			// SplitAfter: element goes into current sub-stream, split after if pred matches
			elems = append(elems, elem)
			if s.pred(elem) {
				key := fmt.Sprintf("sub-%d", s.subIdx)
				s.subIdx++
				return SubStream[T]{Key: key, Source: FromSlice(elems)}, true, nil
			}
		} else {
			// SplitWhen: split before the element if pred matches
			if s.pred(elem) && len(elems) > 0 {
				s.pending = &elem
				key := fmt.Sprintf("sub-%d", s.subIdx)
				s.subIdx++
				return SubStream[T]{Key: key, Source: FromSlice(elems)}, true, nil
			}
			elems = append(elems, elem)
		}
	}
}

// ─── ScanAsync ────────────────────────────────────────────────────────────

// ScanAsync creates a Flow that is the asynchronous variant of Scan. The
// accumulator function returns a channel; the operator waits for the result
// before processing the next element. The seed value is emitted first.
func ScanAsync[In, S any](zero S, fn func(S, In) <-chan S) Flow[In, S, NotUsed] {
	return Flow[In, S, NotUsed]{
		attach: func(up iterator[In]) (iterator[S], NotUsed) {
			return &scanAsyncIterator[In, S]{upstream: up, acc: zero, fn: fn, emitSeed: true}, NotUsed{}
		},
	}
}

type scanAsyncIterator[In, S any] struct {
	upstream iterator[In]
	acc      S
	fn       func(S, In) <-chan S
	emitSeed bool
	done     bool
}

func (s *scanAsyncIterator[In, S]) next() (S, bool, error) {
	if s.done {
		var zero S
		return zero, false, nil
	}
	if s.emitSeed {
		s.emitSeed = false
		return s.acc, true, nil
	}
	elem, ok, err := s.upstream.next()
	if !ok || err != nil {
		s.done = true
		return s.acc, ok, err
	}
	result := <-s.fn(s.acc, elem)
	s.acc = result
	return s.acc, true, nil
}

// ─── Reactive Streams Interop ─────────────────────────────────────────────

// Publisher is a Go equivalent of the Reactive Streams Publisher interface.
// A Publisher produces elements to one or more Subscribers.
type Publisher[T any] interface {
	Subscribe(sub Subscriber[T])
}

// Subscriber is a Go equivalent of the Reactive Streams Subscriber interface.
type Subscriber[T any] interface {
	OnSubscribe(sub Subscription)
	OnNext(elem T)
	OnError(err error)
	OnComplete()
}

// Subscription is a Go equivalent of the Reactive Streams Subscription interface.
type Subscription interface {
	Request(n int64)
	Cancel()
}

// Processor is a Go equivalent of the Reactive Streams Processor interface.
// It is both a Subscriber and a Publisher.
type Processor[In, Out any] interface {
	Publisher[Out]
	Subscriber[In]
}

// FromPublisher creates a Source from a Reactive Streams Publisher.
// Elements are pulled from the publisher on demand.
func FromPublisher[T any](pub Publisher[T]) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			ch := make(chan T, 1)
			errCh := make(chan error, 1)
			done := make(chan struct{})

			sub := &channelSubscriber[T]{
				ch:    ch,
				errCh: errCh,
				done:  done,
			}
			pub.Subscribe(sub)

			return &publisherIterator[T]{ch: ch, errCh: errCh, done: done}, NotUsed{}
		},
	}
}

type channelSubscriber[T any] struct {
	ch           chan T
	errCh        chan error
	done         chan struct{}
	closeOnce    sync.Once
	subscription Subscription
}

func (s *channelSubscriber[T]) OnSubscribe(sub Subscription) {
	s.subscription = sub
	sub.Request(1)
}

func (s *channelSubscriber[T]) OnNext(elem T) {
	s.ch <- elem
	// Request next element asynchronously to avoid re-entrant lock
	if s.subscription != nil {
		go s.subscription.Request(1)
	}
}

func (s *channelSubscriber[T]) OnError(err error) {
	select {
	case s.errCh <- err:
	default:
	}
	s.closeOnce.Do(func() { close(s.done) })
}

func (s *channelSubscriber[T]) OnComplete() {
	s.closeOnce.Do(func() { close(s.done) })
}

type publisherIterator[T any] struct {
	ch    chan T
	errCh chan error
	done  chan struct{}
}

func (p *publisherIterator[T]) next() (T, bool, error) {
	select {
	case err := <-p.errCh:
		var zero T
		return zero, false, err
	default:
	}
	select {
	case err := <-p.errCh:
		var zero T
		return zero, false, err
	case v, ok := <-p.ch:
		if ok {
			return v, true, nil
		}
		select {
		case err := <-p.errCh:
			var zero T
			return zero, false, err
		default:
			var zero T
			return zero, false, nil
		}
	case <-p.done:
		select {
		case v, ok := <-p.ch:
			if ok {
				return v, true, nil
			}
		default:
		}
		select {
		case err := <-p.errCh:
			var zero T
			return zero, false, err
		default:
			var zero T
			return zero, false, nil
		}
	}
}

// AsPublisher exposes a Source as a Reactive Streams Publisher.
// Each subscriber receives elements from its own independent materialization.
func AsPublisher[T any](src Source[T, NotUsed]) Publisher[T] {
	return &sourcePublisher[T]{src: src}
}

type sourcePublisher[T any] struct {
	src Source[T, NotUsed]
}

func (p *sourcePublisher[T]) Subscribe(sub Subscriber[T]) {
	it, _ := p.src.factory()
	s := &iteratorSubscription[T]{it: it, sub: sub}
	sub.OnSubscribe(s)
}

type iteratorSubscription[T any] struct {
	it        iterator[T]
	sub       Subscriber[T]
	cancelled bool
	mu        sync.Mutex
}

func (s *iteratorSubscription[T]) Request(n int64) {
	go func() {
		for i := int64(0); i < n; i++ {
			s.mu.Lock()
			if s.cancelled {
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()

			elem, ok, err := s.it.next()
			if err != nil {
				s.sub.OnError(err)
				return
			}
			if !ok {
				s.sub.OnComplete()
				return
			}
			s.sub.OnNext(elem)
		}
	}()
}

func (s *iteratorSubscription[T]) Cancel() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancelled = true
}
