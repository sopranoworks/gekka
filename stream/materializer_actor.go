/*
 * materializer_actor.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import "sync"

// DefaultAsyncBufSize is the capacity of the bounded channel buffer inserted
// at each Async() boundary.  Upstream is blocked when full, enforcing
// demand-driven back-pressure on the producer.
const DefaultAsyncBufSize = 16

// asyncBoundary[T] splits a stream into two concurrently-executing "islands"
// separated by a bounded channel.
//
// The upstream island runs in a dedicated goroutine that pulls elements and
// sends them into the channel.  The downstream island calls next() which reads
// from the channel and blocks when it is empty.
//
// Back-pressure is applied automatically: once the channel is full the
// upstream goroutine blocks until the downstream consumes an element,
// preventing unbounded memory growth even when the source is faster than the
// sink.  This models the Reactive Streams Request(n)/OnNext(data) handshake in
// a Go-idiomatic way using channel flow control.
type asyncBoundary[T any] struct {
	ch     chan T
	errCh  chan error
	stopCh chan struct{}
	once   sync.Once
}

func newAsyncBoundary[T any](upstream iterator[T], bufSize int) *asyncBoundary[T] {
	ab := &asyncBoundary[T]{
		ch:     make(chan T, bufSize),
		errCh:  make(chan error, 1),
		stopCh: make(chan struct{}),
	}
	go func() {
		defer close(ab.ch)
		for {
			elem, ok, err := upstream.next()
			if err != nil {
				select {
				case ab.errCh <- err:
				default:
				}
				return
			}
			if !ok {
				return
			}
			select {
			case ab.ch <- elem:
			case <-ab.stopCh:
				return
			}
		}
	}()
	return ab
}

// next implements iterator[T].  It blocks until an element arrives, the
// upstream goroutine terminates normally, or cancel() has been called.
func (a *asyncBoundary[T]) next() (T, bool, error) {
	elem, ok := <-a.ch
	if !ok {
		// upstream goroutine exited — collect any pending error.
		select {
		case err := <-a.errCh:
			var zero T
			return zero, false, err
		default:
			var zero T
			return zero, false, nil
		}
	}
	return elem, true, nil
}

// cancel signals the upstream goroutine to stop early, for example when the
// downstream has signalled completion or an error.  Safe to call multiple times.
func (a *asyncBoundary[T]) cancel() {
	a.once.Do(func() { close(a.stopCh) })
}

// ─── GraphInterpreter ─────────────────────────────────────────────────────

// GraphInterpreter is the core engine that manages the goroutines ("actor
// islands") spawned by async boundaries within a single graph run.
//
// Each call to Async() on a Source, Flow, or Sink creates one upstream island
// that runs concurrently with its downstream neighbor.  The interpreter tracks
// all such islands so callers can wait for full quiescence after the sink
// finishes draining.
//
// In normal use the interpreter is transparent — async boundaries start their
// goroutines internally and the graph is driven by the sink as usual.
// Advanced users can obtain a shared interpreter for observability or
// coordinated shutdown.
type GraphInterpreter struct {
	wg sync.WaitGroup
}

// NewGraphInterpreter allocates an empty GraphInterpreter.
func NewGraphInterpreter() *GraphInterpreter { return &GraphInterpreter{} }

// Go starts fn in a new goroutine whose lifetime is tracked by this
// interpreter.  [Wait] will block until fn returns.
func (g *GraphInterpreter) Go(fn func()) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		fn()
	}()
}

// Wait blocks until every goroutine registered via Go has returned.
func (g *GraphInterpreter) Wait() { g.wg.Wait() }
