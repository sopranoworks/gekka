/*
 * graph.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"sync"
	"sync/atomic"
)

// ─── sharedError ──────────────────────────────────────────────────────────

// sharedError lets one goroutine broadcast a failure to any number of
// concurrent consumers.  The first call to fail() stores the error and closes
// sig; subsequent calls are no-ops.  All goroutines blocking on sig see the
// close simultaneously, so every consumer receives the error.
type sharedError struct {
	once sync.Once
	val  atomic.Value // stores a non-nil error once set
	sig  chan struct{} // closed exactly once when an error is stored
}

func newSharedError() *sharedError {
	return &sharedError{sig: make(chan struct{})}
}

// fail records err and unblocks all consumers.  Safe to call from multiple
// goroutines; only the first call is effective.
func (s *sharedError) fail(err error) {
	s.once.Do(func() {
		s.val.Store(err)
		close(s.sig)
	})
}

// load returns the stored error, or nil if fail has never been called.
func (s *sharedError) load() error {
	v := s.val.Load()
	if v == nil {
		return nil
	}
	return v.(error)
}

// ─── channelIterator ──────────────────────────────────────────────────────

// channelIterator wraps a pre-existing channel as an [iterator][T].
// It is used by [Merge], [Broadcast], and [Balance] to adapt the push-based
// channel model back into the pull-based stream model.
type channelIterator[T any] struct {
	ch  <-chan T
	err *sharedError
}

func (c *channelIterator[T]) next() (T, bool, error) {
	// Fast path: surface a pending error without blocking.
	select {
	case <-c.err.sig:
		var zero T
		return zero, false, c.err.load()
	default:
	}
	// Block on the next element, an error signal, or normal completion.
	select {
	case <-c.err.sig:
		var zero T
		return zero, false, c.err.load()
	case v, ok := <-c.ch:
		if !ok {
			// Driver closed the channel — check for a final error.
			if err := c.err.load(); err != nil {
				var zero T
				return zero, false, err
			}
			var zero T
			return zero, false, nil
		}
		return v, true, nil
	}
}

// ─── Merge ────────────────────────────────────────────────────────────────

// Merge pulls from all provided sources concurrently and emits elements into
// a single merged [Source] as they arrive.  Emission order is non-deterministic.
//
// The merged source completes when every upstream has completed.  If any
// upstream fails, the error is forwarded immediately and all remaining
// upstreams are signalled to stop.
//
// Example — combine two actor-backed sources into one pipeline:
//
//	merged := stream.Merge(src1, src2, src3)
//	result, err := stream.RunWith(merged, stream.Collect[int](), stream.ActorMaterializer{})
func Merge[T any](sources ...Source[T, NotUsed]) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			n := len(sources)
			// Buffer proportional to source count for pipelining headroom.
			ch := make(chan T, n*DefaultAsyncBufSize)
			sharedErr := newSharedError()

			var wg sync.WaitGroup
			for _, src := range sources {
				wg.Add(1)
				iter, _ := src.factory()
				go func(it iterator[T]) {
					defer wg.Done()
					for {
						// Honour cancellation before pulling.
						select {
						case <-sharedErr.sig:
							return
						default:
						}

						elem, ok, err := it.next()
						if err != nil {
							sharedErr.fail(err)
							return
						}
						if !ok {
							return
						}

						// Forward element or exit on error/cancellation.
						select {
						case ch <- elem:
						case <-sharedErr.sig:
							return
						}
					}
				}(iter)
			}

			// Close the output channel once every producer goroutine exits.
			go func() {
				wg.Wait()
				close(ch)
			}()

			return &channelIterator[T]{ch: ch, err: sharedErr}, NotUsed{}
		},
	}
}

// ─── Broadcast ────────────────────────────────────────────────────────────

// Broadcast fans out one source to fanOut independent output sources.
// Every output source receives a complete, identical copy of every element
// emitted by the original source.
//
// Back-pressure: the driver goroutine distributes each element to all output
// channels sequentially; the slowest consumer controls the overall throughput.
// Increase bufferSize to decouple fast and slow consumers at the cost of
// additional memory per output branch.
//
// The driver goroutine is started lazily on the first factory call (i.e. when
// the first output source is materialized by a sink).  All output sources
// share a single driver, so they must all be run concurrently — typically each
// in its own goroutine.
//
// Example — fan out to three parallel sinks:
//
//	branches := stream.Broadcast(src, 3, 16)
//	var wg sync.WaitGroup
//	for _, b := range branches {
//	    wg.Add(1)
//	    go func(s stream.Source[int, stream.NotUsed]) {
//	        defer wg.Done()
//	        stream.RunWith(s, mySink, stream.ActorMaterializer{})
//	    }(b)
//	}
//	wg.Wait()
func Broadcast[T any](src Source[T, NotUsed], fanOut int, bufferSize int) []Source[T, NotUsed] {
	channels := make([]chan T, fanOut)
	for i := range channels {
		channels[i] = make(chan T, bufferSize)
	}
	sharedErr := newSharedError()

	var once sync.Once
	start := func() {
		go func() {
			defer func() {
				for _, ch := range channels {
					close(ch)
				}
			}()
			iter, _ := src.factory()
			for {
				elem, ok, err := iter.next()
				if err != nil {
					sharedErr.fail(err)
					return
				}
				if !ok {
					return
				}
				// Send to every output channel.  Blocks on the slowest consumer
				// when its buffer is full — this is the desired back-pressure.
				for _, ch := range channels {
					ch <- elem
				}
			}
		}()
	}

	sources := make([]Source[T, NotUsed], fanOut)
	for i, ch := range channels {
		sources[i] = Source[T, NotUsed]{
			factory: func() (iterator[T], NotUsed) {
				once.Do(start) // idempotent: driver starts exactly once
				return &channelIterator[T]{ch: ch, err: sharedErr}, NotUsed{}
			},
		}
	}
	return sources
}

// ─── Balance ──────────────────────────────────────────────────────────────

// Balance fans out one source to fanOut independent output sources using
// work-stealing distribution: each element is delivered to exactly one output
// source — whichever calls next() first.
//
// Unlike [Broadcast], elements are NOT duplicated.  The total number of
// elements received across all output sources equals the source's total.
// Distribution is demand-driven, so faster consumers naturally receive more
// elements.
//
// bufferSize is the capacity of the shared internal channel.
//
// Example — distribute work across four parallel workers:
//
//	branches := stream.Balance(src, 4, 32)
//	var wg sync.WaitGroup
//	for _, b := range branches {
//	    wg.Add(1)
//	    go func(s stream.Source[int, stream.NotUsed]) {
//	        defer wg.Done()
//	        stream.RunWith(s, myWorkerSink, stream.ActorMaterializer{})
//	    }(b)
//	}
//	wg.Wait()
func Balance[T any](src Source[T, NotUsed], fanOut int, bufferSize int) []Source[T, NotUsed] {
	// All output sources share a single channel; Go's channel receive semantics
	// guarantee each element is delivered to exactly one consumer.
	shared := make(chan T, bufferSize)
	sharedErr := newSharedError()

	var once sync.Once
	start := func() {
		go func() {
			defer close(shared)
			iter, _ := src.factory()
			for {
				elem, ok, err := iter.next()
				if err != nil {
					sharedErr.fail(err)
					return
				}
				if !ok {
					return
				}
				shared <- elem
			}
		}()
	}

	sources := make([]Source[T, NotUsed], fanOut)
	for i := range sources {
		sources[i] = Source[T, NotUsed]{
			factory: func() (iterator[T], NotUsed) {
				once.Do(start) // driver starts exactly once
				return &channelIterator[T]{ch: shared, err: sharedErr}, NotUsed{}
			},
		}
	}
	return sources
}
