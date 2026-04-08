/*
 * flow_extra.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"errors"
	"sync"
	"time"
)

// ─── MapConcat ───────────────────────────────────────────────────────────

// MapConcat applies f to each element of src, producing a slice of output
// elements per input element, and emits each output element individually.
func MapConcat[In, Out any](src Source[In, NotUsed], f func(In) []Out) Source[Out, NotUsed] {
	return Source[Out, NotUsed]{
		factory: func() (iterator[Out], NotUsed) {
			up, _ := src.factory()
			return &mapConcatIterator[In, Out]{upstream: up, f: f}, NotUsed{}
		},
	}
}

type mapConcatIterator[In, Out any] struct {
	upstream iterator[In]
	f        func(In) []Out
	buf      []Out
	pos      int
}

func (m *mapConcatIterator[In, Out]) next() (Out, bool, error) {
	for {
		if m.pos < len(m.buf) {
			v := m.buf[m.pos]
			m.pos++
			return v, true, nil
		}
		elem, ok, err := m.upstream.next()
		if !ok || err != nil {
			var zero Out
			return zero, ok, err
		}
		m.buf = m.f(elem)
		m.pos = 0
	}
}

// ─── FlatMapMerge ────────────────────────────────────────────────────────

// FlatMapMerge applies f to each element of src, producing a sub-source per
// element, and merges up to breadth sub-sources concurrently.
func FlatMapMerge[In, Out any](src Source[In, NotUsed], breadth int, f func(In) Source[Out, NotUsed]) Source[Out, NotUsed] {
	return Source[Out, NotUsed]{
		factory: func() (iterator[Out], NotUsed) {
			up, _ := src.factory()
			ch := make(chan Out, breadth*DefaultAsyncBufSize)
			sharedErr := newSharedError()
			sem := make(chan struct{}, breadth)

			var wg sync.WaitGroup

			// Dispatcher goroutine: pull from upstream, spawn sub-source workers.
			go func() {
				defer func() {
					wg.Wait()
					close(ch)
				}()
				for {
					select {
					case <-sharedErr.sig:
						return
					default:
					}
					elem, ok, err := up.next()
					if err != nil {
						sharedErr.fail(err)
						return
					}
					if !ok {
						return
					}
					sem <- struct{}{} // limit concurrency
					wg.Add(1)
					subSrc := f(elem)
					subIter, _ := subSrc.factory()
					go func(it iterator[Out]) {
						defer func() {
							<-sem
							wg.Done()
						}()
						for {
							select {
							case <-sharedErr.sig:
								return
							default:
							}
							e, ok2, err2 := it.next()
							if err2 != nil {
								sharedErr.fail(err2)
								return
							}
							if !ok2 {
								return
							}
							select {
							case ch <- e:
							case <-sharedErr.sig:
								return
							}
						}
					}(subIter)
				}
			}()

			return &channelIterator[Out]{ch: ch, err: sharedErr}, NotUsed{}
		},
	}
}

// ─── TakeWithin / DropWithin ─────────────────────────────────────────────

// TakeWithin emits elements from src for duration d, then completes.
func TakeWithin[T any](src Source[T, NotUsed], d time.Duration) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			up, _ := src.factory()
			return &takeWithinIterator[T]{upstream: up, deadline: time.Now().Add(d)}, NotUsed{}
		},
	}
}

type takeWithinIterator[T any] struct {
	upstream iterator[T]
	deadline time.Time
	done     bool
}

func (tw *takeWithinIterator[T]) next() (T, bool, error) {
	if tw.done || time.Now().After(tw.deadline) {
		tw.done = true
		var zero T
		return zero, false, nil
	}
	elem, ok, err := tw.upstream.next()
	if !ok || err != nil {
		return elem, ok, err
	}
	if time.Now().After(tw.deadline) {
		tw.done = true
		var zero T
		return zero, false, nil
	}
	return elem, true, nil
}

// DropWithin drops elements from src for duration d, then passes all remaining
// elements through.
func DropWithin[T any](src Source[T, NotUsed], d time.Duration) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			up, _ := src.factory()
			return &dropWithinIterator[T]{upstream: up, deadline: time.Now().Add(d)}, NotUsed{}
		},
	}
}

type dropWithinIterator[T any] struct {
	upstream iterator[T]
	deadline time.Time
}

func (dw *dropWithinIterator[T]) next() (T, bool, error) {
	for {
		elem, ok, err := dw.upstream.next()
		if !ok || err != nil {
			return elem, ok, err
		}
		if time.Now().After(dw.deadline) {
			return elem, true, nil
		}
		// drop element (still within duration)
	}
}

// ─── DivertTo / AlsoTo ───────────────────────────────────────────────────

// DivertTo routes elements matching predicate to the side sink, passing the
// rest through to the main output.
func DivertTo[T any](src Source[T, NotUsed], side Sink[T, NotUsed], predicate func(T) bool) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			up, _ := src.factory()
			sideCh := make(chan T, DefaultAsyncBufSize)
			done := make(chan struct{})

			// Side sink consumer goroutine
			go func() {
				defer close(done)
				sideIter := &channelDrainIterator[T]{ch: sideCh}
				side.runWith(sideIter)
			}()

			return &divertToIterator[T]{
				upstream:  up,
				predicate: predicate,
				sideCh:    sideCh,
				done:      done,
			}, NotUsed{}
		},
	}
}

type channelDrainIterator[T any] struct {
	ch chan T
}

func (c *channelDrainIterator[T]) next() (T, bool, error) {
	elem, ok := <-c.ch
	if !ok {
		var zero T
		return zero, false, nil
	}
	return elem, true, nil
}

type divertToIterator[T any] struct {
	upstream  iterator[T]
	predicate func(T) bool
	sideCh    chan T
	done      chan struct{}
	finished  bool
}

func (d *divertToIterator[T]) next() (T, bool, error) {
	for {
		elem, ok, err := d.upstream.next()
		if !ok || err != nil {
			if !d.finished {
				d.finished = true
				close(d.sideCh)
			}
			return elem, ok, err
		}
		if d.predicate(elem) {
			d.sideCh <- elem
			continue
		}
		return elem, true, nil
	}
}

// AlsoTo sends a copy of every element to the side sink while also passing
// all elements through to the main output.
func AlsoTo[T any](src Source[T, NotUsed], side Sink[T, NotUsed]) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			up, _ := src.factory()
			sideCh := make(chan T, DefaultAsyncBufSize)
			done := make(chan struct{})

			go func() {
				defer close(done)
				sideIter := &channelDrainIterator[T]{ch: sideCh}
				side.runWith(sideIter)
			}()

			return &alsoToIterator[T]{
				upstream: up,
				sideCh:   sideCh,
				done:     done,
			}, NotUsed{}
		},
	}
}

type alsoToIterator[T any] struct {
	upstream iterator[T]
	sideCh   chan T
	done     chan struct{}
	finished bool
}

func (a *alsoToIterator[T]) next() (T, bool, error) {
	elem, ok, err := a.upstream.next()
	if !ok || err != nil {
		if !a.finished {
			a.finished = true
			close(a.sideCh)
		}
		return elem, ok, err
	}
	a.sideCh <- elem
	return elem, true, nil
}

// ─── Timeout operators ──────────────────────────────────────────────────

// ErrInitialTimeout is returned when no first element arrives within the deadline.
var ErrInitialTimeout = errors.New("initial timeout")

// ErrCompletionTimeout is returned when the stream does not complete within the deadline.
var ErrCompletionTimeout = errors.New("completion timeout")

// ErrIdleTimeout is returned when no element arrives for the specified duration.
var ErrIdleTimeout = errors.New("idle timeout")

// InitialTimeout fails the stream if no first element arrives within d.
func InitialTimeout[T any](src Source[T, NotUsed], d time.Duration) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			up, _ := src.factory()
			return &initialTimeoutIterator[T]{upstream: up, timeout: d}, NotUsed{}
		},
	}
}

type initialTimeoutIterator[T any] struct {
	upstream iterator[T]
	timeout  time.Duration
	first    bool
}

func (it *initialTimeoutIterator[T]) next() (T, bool, error) {
	if !it.first {
		it.first = true
		type result struct {
			elem T
			ok   bool
			err  error
		}
		ch := make(chan result, 1)
		go func() {
			e, ok, err := it.upstream.next()
			ch <- result{e, ok, err}
		}()
		select {
		case r := <-ch:
			return r.elem, r.ok, r.err
		case <-time.After(it.timeout):
			var zero T
			return zero, false, ErrInitialTimeout
		}
	}
	return it.upstream.next()
}

// CompletionTimeout fails the stream if it does not complete within d from the
// start of materialization.
func CompletionTimeout[T any](src Source[T, NotUsed], d time.Duration) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			up, _ := src.factory()
			return &completionTimeoutIterator[T]{upstream: up, deadline: time.Now().Add(d)}, NotUsed{}
		},
	}
}

type completionTimeoutIterator[T any] struct {
	upstream iterator[T]
	deadline time.Time
}

func (ct *completionTimeoutIterator[T]) next() (T, bool, error) {
	type result struct {
		elem T
		ok   bool
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		e, ok, err := ct.upstream.next()
		ch <- result{e, ok, err}
	}()
	remaining := time.Until(ct.deadline)
	if remaining <= 0 {
		var zero T
		return zero, false, ErrCompletionTimeout
	}
	select {
	case r := <-ch:
		return r.elem, r.ok, r.err
	case <-time.After(remaining):
		var zero T
		return zero, false, ErrCompletionTimeout
	}
}

// IdleTimeout fails the stream if no element arrives for duration d.
func IdleTimeout[T any](src Source[T, NotUsed], d time.Duration) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			up, _ := src.factory()
			return &idleTimeoutIterator[T]{upstream: up, timeout: d}, NotUsed{}
		},
	}
}

type idleTimeoutIterator[T any] struct {
	upstream iterator[T]
	timeout  time.Duration
}

func (it *idleTimeoutIterator[T]) next() (T, bool, error) {
	type result struct {
		elem T
		ok   bool
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		e, ok, err := it.upstream.next()
		ch <- result{e, ok, err}
	}()
	select {
	case r := <-ch:
		return r.elem, r.ok, r.err
	case <-time.After(it.timeout):
		var zero T
		return zero, false, ErrIdleTimeout
	}
}

// KeepAlive injects inject when no element arrives from src for duration d.
func KeepAlive[T any](src Source[T, NotUsed], d time.Duration, inject T) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			up, _ := src.factory()
			return &keepAliveIterator[T]{upstream: up, timeout: d, inject: inject}, NotUsed{}
		},
	}
}

type keepAliveIterator[T any] struct {
	upstream iterator[T]
	timeout  time.Duration
	inject   T
	done     bool
	pending  chan keepAliveResult[T]
}

type keepAliveResult[T any] struct {
	elem T
	ok   bool
	err  error
}

func (k *keepAliveIterator[T]) next() (T, bool, error) {
	if k.done {
		var zero T
		return zero, false, nil
	}

	// If there's a pending result from a previous timeout, use it.
	if k.pending != nil {
		select {
		case r := <-k.pending:
			k.pending = nil
			if !r.ok || r.err != nil {
				k.done = true
			}
			return r.elem, r.ok, r.err
		case <-time.After(k.timeout):
			return k.inject, true, nil
		}
	}

	ch := make(chan keepAliveResult[T], 1)
	go func() {
		e, ok, err := k.upstream.next()
		ch <- keepAliveResult[T]{e, ok, err}
	}()
	select {
	case r := <-ch:
		if !r.ok || r.err != nil {
			k.done = true
		}
		return r.elem, r.ok, r.err
	case <-time.After(k.timeout):
		k.pending = ch
		return k.inject, true, nil
	}
}

// ─── DelayWith / OrElse / Prepend ────────────────────────────────────────

// DelayWith delays each element by a per-element duration computed by f.
func DelayWith[T any](src Source[T, NotUsed], f func(T) time.Duration) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			up, _ := src.factory()
			return &delayWithIterator[T]{upstream: up, f: f}, NotUsed{}
		},
	}
}

type delayWithIterator[T any] struct {
	upstream iterator[T]
	f        func(T) time.Duration
}

func (dw *delayWithIterator[T]) next() (T, bool, error) {
	elem, ok, err := dw.upstream.next()
	if !ok || err != nil {
		return elem, ok, err
	}
	d := dw.f(elem)
	if d > 0 {
		time.Sleep(d)
	}
	return elem, true, nil
}

// OrElse uses fallback when src completes without emitting any elements.
func OrElse[T any](src Source[T, NotUsed], fallback Source[T, NotUsed]) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			up, _ := src.factory()
			fb, _ := fallback.factory()
			return &orElseIterator[T]{upstream: up, fallback: fb}, NotUsed{}
		},
	}
}

type orElseIterator[T any] struct {
	upstream iterator[T]
	fallback iterator[T]
	switched bool
	emitted  bool
}

func (o *orElseIterator[T]) next() (T, bool, error) {
	if o.switched {
		return o.fallback.next()
	}
	elem, ok, err := o.upstream.next()
	if err != nil {
		return elem, ok, err
	}
	if ok {
		o.emitted = true
		return elem, true, nil
	}
	// upstream exhausted
	if !o.emitted {
		o.switched = true
		return o.fallback.next()
	}
	var zero T
	return zero, false, nil
}

// Prepend emits all elements from prefix before elements from main.
func Prepend[T any](prefix, main Source[T, NotUsed]) Source[T, NotUsed] {
	return Concat(prefix, main)
}

// ─── WatchTermination / Monitor ──────────────────────────────────────────

// WatchTermination calls callback when the stream completes (nil error) or
// fails (non-nil error).  All elements pass through unchanged.
func WatchTermination[T any](src Source[T, NotUsed], callback func(error)) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			up, _ := src.factory()
			return &watchTerminationIterator[T]{upstream: up, callback: callback}, NotUsed{}
		},
	}
}

type watchTerminationIterator[T any] struct {
	upstream iterator[T]
	callback func(error)
	fired    bool
}

func (w *watchTerminationIterator[T]) next() (T, bool, error) {
	elem, ok, err := w.upstream.next()
	if (!ok || err != nil) && !w.fired {
		w.fired = true
		w.callback(err)
	}
	return elem, ok, err
}

// Monitor calls observer for every element passing through, without modifying
// the stream.
func Monitor[T any](src Source[T, NotUsed], observer func(T)) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			up, _ := src.factory()
			return &monitorIterator[T]{upstream: up, observer: observer}, NotUsed{}
		},
	}
}

type monitorIterator[T any] struct {
	upstream iterator[T]
	observer func(T)
}

func (m *monitorIterator[T]) next() (T, bool, error) {
	elem, ok, err := m.upstream.next()
	if ok && err == nil {
		m.observer(elem)
	}
	return elem, ok, err
}
