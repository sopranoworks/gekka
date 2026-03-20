/*
 * iterators.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"log"
	"time"
)

// ─── funcIterator ─────────────────────────────────────────────────────────

// funcIterator wraps a closure as an [iterator].
type funcIterator[T any] struct {
	pullFn func() (T, bool, error)
}

func (f *funcIterator[T]) next() (T, bool, error) { return f.pullFn() }

// sliceIterator emits elements from a slice, one per pull.
type sliceIterator[T any] struct {
	elems []T
	pos   int
}

func (s *sliceIterator[T]) next() (T, bool, error) {
	if s.pos >= len(s.elems) {
		var zero T
		return zero, false, nil
	}
	v := s.elems[s.pos]
	s.pos++
	return v, true, nil
}

// ─── mapIterator ──────────────────────────────────────────────────────────

type mapIterator[In, Out any] struct {
	upstream iterator[In]
	fn       func(In) Out
}

func (m *mapIterator[In, Out]) next() (Out, bool, error) {
	in, ok, err := m.upstream.next()
	if !ok || err != nil {
		var zero Out
		return zero, ok, err
	}
	return m.fn(in), true, nil
}

// ─── filterIterator ───────────────────────────────────────────────────────

type filterIterator[T any] struct {
	upstream iterator[T]
	pred     func(T) bool
}

func (f *filterIterator[T]) next() (T, bool, error) {
	for {
		elem, ok, err := f.upstream.next()
		if !ok || err != nil {
			return elem, ok, err
		}
		if f.pred(elem) {
			return elem, true, nil
		}
	}
}

// ─── takeIterator ─────────────────────────────────────────────────────────

type takeIterator[T any] struct {
	upstream iterator[T]
	n        int
	emitted  int
}

func (t *takeIterator[T]) next() (T, bool, error) {
	if t.emitted >= t.n {
		var zero T
		return zero, false, nil
	}
	elem, ok, err := t.upstream.next()
	if ok {
		t.emitted++
	}
	return elem, ok, err
}

// ─── mapEIterator ─────────────────────────────────────────────────────────

// mapEIterator applies an error-returning function to each upstream element.
// If fn returns an error the iterator propagates it so that an enclosing
// [supervisedIterator] can apply the configured [Decider].
type mapEIterator[In, Out any] struct {
	upstream iterator[In]
	fn       func(In) (Out, error)
}

func (m *mapEIterator[In, Out]) next() (Out, bool, error) {
	in, ok, err := m.upstream.next()
	if !ok || err != nil {
		var zero Out
		return zero, ok, err
	}
	out, err := m.fn(in)
	if err != nil {
		var zero Out
		return zero, false, err
	}
	return out, true, nil
}

// ─── bufferIterator ───────────────────────────────────────────────────────

// bufferIterator runs the upstream in a dedicated goroutine and stores
// elements in a bounded channel.  The overflow strategy determines what
// happens when the channel is full.
type bufferIterator[T any] struct {
	ch     chan T
	errCh  chan error
	stopCh chan struct{}
}

func newBufferIterator[T any](upstream iterator[T], size int, strategy OverflowStrategy) *bufferIterator[T] {
	b := &bufferIterator[T]{
		ch:     make(chan T, size),
		errCh:  make(chan error, 1),
		stopCh: make(chan struct{}),
	}
	go func() {
		defer close(b.ch)
		for {
			elem, ok, err := upstream.next()
			if err != nil {
				select {
				case b.errCh <- err:
				default:
				}
				return
			}
			if !ok {
				return
			}
			switch strategy {
			case OverflowBackpressure:
				// Block until the consumer makes room or the stream is stopped.
				select {
				case b.ch <- elem:
				case <-b.stopCh:
					return
				}
			case OverflowDropTail:
				// Non-blocking send: drop the incoming element if full.
				select {
				case b.ch <- elem:
				default:
				}
			case OverflowDropHead:
				// Non-blocking send; on overflow drain the oldest element first.
				// Single-producer invariant: no mutex needed.
				select {
				case b.ch <- elem:
				default:
					select {
					case <-b.ch: // discard oldest
					default:
					}
					b.ch <- elem // guaranteed safe: we just freed a slot
				}
			case OverflowFail:
				select {
				case b.ch <- elem:
				default:
					select {
					case b.errCh <- ErrBufferFull:
					default:
					}
					return
				}
			}
		}
	}()
	return b
}

func (b *bufferIterator[T]) next() (T, bool, error) {
	// Fast path: surface a pending error without blocking.
	select {
	case err := <-b.errCh:
		var zero T
		return zero, false, err
	default:
	}
	select {
	case err := <-b.errCh:
		var zero T
		return zero, false, err
	case v, ok := <-b.ch:
		if !ok {
			// Channel closed — check for a final error.
			select {
			case err := <-b.errCh:
				var zero T
				return zero, false, err
			default:
				var zero T
				return zero, false, nil
			}
		}
		return v, true, nil
	}
}

// ─── throttleIterator ─────────────────────────────────────────────────────

// throttleIterator limits throughput to at most `elements` per `per` duration
// using a synchronous token-bucket algorithm.  Up to `burst` tokens may
// accumulate, allowing short bursts above the steady-state rate.
type throttleIterator[T any] struct {
	upstream  iterator[T]
	interval  time.Duration // per / elements
	last      time.Time     // wall-clock time of the last token bucket update
	burst     int           // maximum token accumulation
	available int           // tokens currently available
}

func newThrottleIterator[T any](upstream iterator[T], elements int, per time.Duration, burst int) *throttleIterator[T] {
	if burst <= 0 {
		burst = elements
	}
	return &throttleIterator[T]{
		upstream:  upstream,
		interval:  per / time.Duration(elements),
		last:      time.Now(),
		burst:     burst,
		available: burst, // start with a full bucket
	}
}

func (t *throttleIterator[T]) next() (T, bool, error) {
	// Pull from upstream first: if the source is exhausted we must not consume
	// a token (which would cause an unnecessary sleep on the terminal pull).
	elem, ok, err := t.upstream.next()
	if !ok || err != nil {
		return elem, ok, err
	}

	// We have a real element — replenish tokens and wait if the bucket is empty.
	elapsed := time.Since(t.last)
	if newTokens := int(elapsed / t.interval); newTokens > 0 {
		t.available = min(t.available+newTokens, t.burst)
		t.last = t.last.Add(time.Duration(newTokens) * t.interval)
	}

	if t.available > 0 {
		t.available--
	} else {
		sleepUntil := t.last.Add(t.interval)
		if d := time.Until(sleepUntil); d > 0 {
			time.Sleep(d)
		}
		t.last = sleepUntil
	}
	return elem, true, nil
}

// ─── delayIterator ────────────────────────────────────────────────────────

// delayIterator introduces a fixed pause after each element before returning
// it to the downstream stage, creating an inter-element delay.
type delayIterator[T any] struct {
	upstream iterator[T]
	delay    time.Duration
}

func (d *delayIterator[T]) next() (T, bool, error) {
	elem, ok, err := d.upstream.next()
	if ok && err == nil {
		time.Sleep(d.delay)
	}
	return elem, ok, err
}

// ─── errorIterator ────────────────────────────────────────────────────────

// errorIterator immediately fails with an error on the first pull.
type errorIterator[T any] struct {
	err error
}

func (e *errorIterator[T]) next() (T, bool, error) {
	var zero T
	return zero, false, e.err
}

// ─── zipIterator ──────────────────────────────────────────────────────────

// zipIterator pairs elements from two upstream iterators element-by-element.
// It completes when either upstream is exhausted and propagates errors from
// either side immediately.
type zipIterator[T1, T2, Out any] struct {
	left    iterator[T1]
	right   iterator[T2]
	combine func(T1, T2) Out
}

func (z *zipIterator[T1, T2, Out]) next() (Out, bool, error) {
	l, ok, err := z.left.next()
	if !ok || err != nil {
		var zero Out
		return zero, ok, err
	}
	r, ok, err := z.right.next()
	if !ok || err != nil {
		var zero Out
		return zero, ok, err
	}
	return z.combine(l, r), true, nil
}

// ─── logIterator ──────────────────────────────────────────────────────────

// logIterator logs each element that passes through using the standard logger.
type logIterator[T any] struct {
	upstream iterator[T]
	name     string
}

func (l *logIterator[T]) next() (T, bool, error) {
	elem, ok, err := l.upstream.next()
	if ok && err == nil {
		log.Printf("[stream] %s: %v", l.name, elem)
	}
	return elem, ok, err
}

// ─── groupBy helpers ──────────────────────────────────────────────────────

// newGroupByIterator pre-collects all elements from upstream, groups them by
// key, and returns an iterator that emits one SubStream per distinct key in
// insertion order.  If upstream fails or maxSubstreams is exceeded the
// returned iterator immediately propagates the error.
func newGroupByIterator[T any](upstream iterator[T], maxSubstreams int, key func(T) string) iterator[SubStream[T]] {
	groups := make(map[string][]T)
	var order []string

	for {
		elem, ok, err := upstream.next()
		if err != nil {
			return &errorIterator[SubStream[T]]{err: err}
		}
		if !ok {
			break
		}
		k := key(elem)
		if _, exists := groups[k]; !exists {
			if len(groups) >= maxSubstreams {
				return &errorIterator[SubStream[T]]{err: ErrTooManySubstreams}
			}
			order = append(order, k)
		}
		groups[k] = append(groups[k], elem)
	}

	substreams := make([]SubStream[T], len(order))
	for i, k := range order {
		substreams[i] = SubStream[T]{Key: k, Source: FromSlice(groups[k])}
	}
	return &sliceIterator[SubStream[T]]{elems: substreams}
}
