/*
 * probes.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package testkit provides reactive test probes for Gekka Streams.
//
// Probes allow a test goroutine to manually drive demand and element emission
// so that backpressure propagation through a pipeline can be verified step by
// step.
//
// # Typical usage
//
//	srcProbe  := testkit.NewTestSourceProbe[int]()
//	sinkProbe := testkit.NewTestSinkProbe[int]()
//
//	graph := stream.Via(srcProbe.AsSource(),
//	             stream.Map(func(x int) int { return x * 2 })).
//	         To(sinkProbe.AsSink())
//
//	go graph.Run(stream.SyncMaterializer{}) // must run in a separate goroutine
//
//	sinkProbe.Request(1)          // downstream demands one element
//	srcProbe.ExpectRequest()      // verify source received the demand signal
//	srcProbe.SendNext(21)         // push element through the pipeline
//	sinkProbe.ExpectNext(t, 42)   // assert Map doubled it
//
//	sinkProbe.Request(1)
//	srcProbe.ExpectRequest()
//	srcProbe.SendComplete()       // signal end-of-stream
//	sinkProbe.ExpectComplete(t)
package testkit

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/stream"
)

const defaultTimeout = 3 * time.Second

// ─── TestSourceProbe ──────────────────────────────────────────────────────────

// TestSourceProbe[T] controls the upstream end of a test pipeline.
//
// Create one with [NewTestSourceProbe], wire it into a graph via [AsSource],
// run the graph in a background goroutine, then use [ExpectRequest],
// [SendNext], and [SendComplete] to drive the pipeline from the test.
type TestSourceProbe[T any] struct {
	demandCh  chan struct{}
	elemCh    chan T
	doneCh    chan struct{}
	errCh     chan error
	requested atomic.Int64
	closeOnce sync.Once
}

// NewTestSourceProbe creates a TestSourceProbe ready to use.
func NewTestSourceProbe[T any]() *TestSourceProbe[T] {
	return &TestSourceProbe[T]{
		demandCh: make(chan struct{}, 256),
		elemCh:   make(chan T, 1),
		doneCh:   make(chan struct{}),
		errCh:    make(chan error, 1),
	}
}

// AsSource returns a [stream.Source] backed by this probe.  The returned
// Source can be used in any pipeline; the probe retains full control over
// what elements are produced.
func (p *TestSourceProbe[T]) AsSource() stream.Source[T, stream.NotUsed] {
	return stream.FromIteratorFunc(func() (T, bool, error) {
		var zero T
		// Fast-path: already complete.
		select {
		case <-p.doneCh:
			return zero, false, nil
		default:
		}
		// Signal demand to the test goroutine.
		select {
		case p.demandCh <- struct{}{}:
		case <-p.doneCh:
			return zero, false, nil
		}
		// Wait for element, completion, or error.
		select {
		case elem := <-p.elemCh:
			return elem, true, nil
		case <-p.doneCh:
			return zero, false, nil
		case err := <-p.errCh:
			return zero, false, err
		}
	})
}

// ExpectRequest blocks until the downstream issues one demand signal and
// returns the new cumulative total of requested elements.
//
// Panics if no demand arrives within the default timeout (3 s).
func (p *TestSourceProbe[T]) ExpectRequest() int64 {
	select {
	case <-p.demandCh:
		return p.requested.Add(1)
	case <-time.After(defaultTimeout):
		panic(fmt.Sprintf("testkit: TestSourceProbe.ExpectRequest: no demand signal within %s", defaultTimeout))
	}
}

// SendNext pushes elem into the pipeline.  Must be called after
// [ExpectRequest] has consumed the corresponding demand signal.
//
// Panics if the element cannot be delivered within the default timeout.
func (p *TestSourceProbe[T]) SendNext(elem T) {
	select {
	case p.elemCh <- elem:
	case <-time.After(defaultTimeout):
		panic(fmt.Sprintf("testkit: TestSourceProbe.SendNext: delivery timed out after %s", defaultTimeout))
	}
}

// SendComplete signals normal end-of-stream.  Safe to call multiple times.
func (p *TestSourceProbe[T]) SendComplete() {
	p.closeOnce.Do(func() { close(p.doneCh) })
}

// SendError fails the stream with err.
func (p *TestSourceProbe[T]) SendError(err error) {
	select {
	case p.errCh <- err:
	default:
	}
}

// ─── TestSinkProbe ────────────────────────────────────────────────────────────

// TestSinkProbe[T] controls the downstream end of a test pipeline.
//
// Create one with [NewTestSinkProbe], wire it into a graph via [AsSink], run
// the graph in a background goroutine, then use [Request], [ExpectNext], and
// [ExpectComplete] to observe what the pipeline produces.
type TestSinkProbe[T any] struct {
	requestCh  chan int64
	receivedCh chan T
	doneCh     chan struct{}
	errCh      chan error
	closeOnce  sync.Once
}

// NewTestSinkProbe creates a TestSinkProbe ready to use.
func NewTestSinkProbe[T any]() *TestSinkProbe[T] {
	return &TestSinkProbe[T]{
		requestCh:  make(chan int64, 256),
		receivedCh: make(chan T, 256),
		doneCh:     make(chan struct{}),
		errCh:      make(chan error, 1),
	}
}

// AsSink returns a [stream.Sink] backed by this probe.  Elements are only
// pulled from upstream after [Request] has been called.
func (p *TestSinkProbe[T]) AsSink() stream.Sink[T, stream.NotUsed] {
	return stream.SinkFromFunc(func(pull func() (T, bool, error)) error {
		var allowed int64
		for {
			// Drain as many elements as the current budget allows.
			for allowed > 0 {
				elem, ok, err := pull()
				if err != nil {
					select {
					case p.errCh <- err:
					default:
					}
					return err
				}
				if !ok {
					p.closeOnce.Do(func() { close(p.doneCh) })
					return nil
				}
				p.receivedCh <- elem
				allowed--
			}
			// Wait for more demand from the test goroutine.
			n := <-p.requestCh
			allowed += n
		}
	})
}

// Request asks the sink to pull n more elements from upstream.
func (p *TestSinkProbe[T]) Request(n int64) {
	p.requestCh <- n
}

// ExpectNext asserts that the next element received equals expected.
// Fails the test if no element arrives within the default timeout (3 s).
func (p *TestSinkProbe[T]) ExpectNext(t testing.TB, expected T) {
	t.Helper()
	select {
	case got := <-p.receivedCh:
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("testkit: ExpectNext: got %v, want %v", got, expected)
		}
	case <-time.After(defaultTimeout):
		t.Fatalf("testkit: ExpectNext: no element within %s (want %v)", defaultTimeout, expected)
	}
}

// ExpectComplete asserts that the stream completed normally.
// Fails the test if completion is not signalled within the default timeout.
func (p *TestSinkProbe[T]) ExpectComplete(t testing.TB) {
	t.Helper()
	select {
	case <-p.doneCh:
		// stream completed normally
	case err := <-p.errCh:
		t.Fatalf("testkit: ExpectComplete: stream failed with error: %v", err)
	case <-time.After(defaultTimeout):
		t.Fatalf("testkit: ExpectComplete: completion not signalled within %s", defaultTimeout)
	}
}
