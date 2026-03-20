/*
 * flow.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import "time"

// Flow[In, Out, Mat] is a stream stage with exactly one input port and one
// output port.  It transforms elements of type In into elements of type Out
// and materializes to Mat.
//
// Attach a Flow to a Source using the package-level [Via] function.
// Chain two Flows using the package-level [ViaFlow] function.
type Flow[In, Out, Mat any] struct {
	// attach creates a downstream iterator by wrapping an upstream iterator,
	// and returns the Flow's materialized value.
	attach func(upstream iterator[In]) (iterator[Out], Mat)
}

// ─── Constructors ─────────────────────────────────────────────────────────

// Map creates a Flow that transforms each element using fn.
// The materialized value is NotUsed.
func Map[In, Out any](fn func(In) Out) Flow[In, Out, NotUsed] {
	return Flow[In, Out, NotUsed]{
		attach: func(up iterator[In]) (iterator[Out], NotUsed) {
			return &mapIterator[In, Out]{upstream: up, fn: fn}, NotUsed{}
		},
	}
}

// MapE creates a Flow that transforms each element using fn, where fn may
// return an error.  On error the default behaviour is to fail the stream
// ([Stop]).  Attach a supervision strategy with
// [Flow.WithSupervisionStrategy] to change this:
//
//	flow := stream.MapE(fn).WithSupervisionStrategy(stream.ResumeDecider)
func MapE[In, Out any](fn func(In) (Out, error)) Flow[In, Out, NotUsed] {
	return Flow[In, Out, NotUsed]{
		attach: func(up iterator[In]) (iterator[Out], NotUsed) {
			return &mapEIterator[In, Out]{upstream: up, fn: fn}, NotUsed{}
		},
	}
}

// Filter creates a Flow that only passes elements satisfying pred.
// The element type is unchanged and the materialized value is NotUsed.
func Filter[T any](pred func(T) bool) Flow[T, T, NotUsed] {
	return Flow[T, T, NotUsed]{
		attach: func(up iterator[T]) (iterator[T], NotUsed) {
			return &filterIterator[T]{upstream: up, pred: pred}, NotUsed{}
		},
	}
}

// Take creates a Flow that passes at most n elements, then signals completion.
func Take[T any](n int) Flow[T, T, NotUsed] {
	return Flow[T, T, NotUsed]{
		attach: func(up iterator[T]) (iterator[T], NotUsed) {
			return &takeIterator[T]{upstream: up, n: n}, NotUsed{}
		},
	}
}

// ─── Supervision ──────────────────────────────────────────────────────────

// WithSupervisionStrategy wraps this Flow with a [supervisedIterator] that
// applies decider to every error emitted by the Flow's output iterator.
//
//   - [Stop]    — propagate the error (default, equivalent to no strategy).
//   - [Resume]  — swallow the error and continue with the next upstream element.
//   - [Restart] — recreate the Flow's inner stage from scratch and continue.
//
// WithSupervisionStrategy composes with [Async]:
//
//	stream.MapE(fn).WithSupervisionStrategy(stream.ResumeDecider).Async()
func (f Flow[In, Out, Mat]) WithSupervisionStrategy(decider Decider) Flow[In, Out, Mat] {
	return Flow[In, Out, Mat]{
		attach: func(upstream iterator[In]) (iterator[Out], Mat) {
			inner, mat := f.attach(upstream)
			remake := func() iterator[Out] {
				newInner, _ := f.attach(upstream)
				return newInner
			}
			return &supervisedIterator[Out]{
				inner:   inner,
				decider: decider,
				remake:  remake,
			}, mat
		},
	}
}

// ─── Async boundary ───────────────────────────────────────────────────────

// Async inserts an asynchronous boundary after this Flow's output port.
// The output iterator runs in a dedicated goroutine backed by a bounded channel
// of capacity [DefaultAsyncBufSize], decoupling the upstream and downstream
// execution islands.  When the channel is full the upstream goroutine blocks,
// enforcing demand-driven back-pressure across the boundary.
func (f Flow[In, Out, Mat]) Async() Flow[In, Out, Mat] {
	return Flow[In, Out, Mat]{
		attach: func(up iterator[In]) (iterator[Out], Mat) {
			out, mat := f.attach(up)
			return newAsyncBoundary[Out](out, DefaultAsyncBufSize), mat
		},
	}
}

// ─── Flow chaining ────────────────────────────────────────────────────────

// ViaFlow connects two flows, producing a composite Flow.
// The first Flow's materialized value is kept; the second's is discarded.
//
// Like [Via] for sources, this is a package-level function because Go does
// not allow methods with additional type parameters.
func ViaFlow[In, Mid, Out, Mat1, Mat2 any](f1 Flow[In, Mid, Mat1], f2 Flow[Mid, Out, Mat2]) Flow[In, Out, Mat1] {
	return Flow[In, Out, Mat1]{
		attach: func(up iterator[In]) (iterator[Out], Mat1) {
			mid, mat1 := f1.attach(up)
			out, _ := f2.attach(mid)
			return out, mat1
		},
	}
}

// ─── Buffer ───────────────────────────────────────────────────────────────

// Buffer creates a [Flow] that asynchronously buffers up to size elements
// using the given overflow strategy.
//
// The upstream runs in a dedicated goroutine that fills the buffer ahead of
// the downstream consumer:
//
//   - [OverflowBackpressure]: the goroutine blocks when the buffer is full,
//     applying demand-driven back-pressure to the upstream.
//   - [OverflowDropTail]: incoming elements are silently dropped when full.
//   - [OverflowDropHead]: the oldest buffered element is evicted to make room.
//   - [OverflowFail]: the stream fails with [ErrBufferFull] on overflow.
//
// Use [OverflowBackpressure] (or the equivalent [Source.Async] / [Flow.Async])
// when correctness requires that no elements are lost.
func Buffer[T any](size int, strategy OverflowStrategy) Flow[T, T, NotUsed] {
	return Flow[T, T, NotUsed]{
		attach: func(up iterator[T]) (iterator[T], NotUsed) {
			return newBufferIterator(up, size, strategy), NotUsed{}
		},
	}
}

// Buffer wraps this Flow with a [Buffer] stage appended after its output port.
// See the package-level [Buffer] function for parameter semantics.
func (f Flow[In, Out, Mat]) Buffer(size int, strategy OverflowStrategy) Flow[In, Out, Mat] {
	return Flow[In, Out, Mat]{
		attach: func(up iterator[In]) (iterator[Out], Mat) {
			inner, mat := f.attach(up)
			return newBufferIterator(inner, size, strategy), mat
		},
	}
}

// ─── Throttle ─────────────────────────────────────────────────────────────

// Throttle creates a [Flow] that limits throughput to at most elements per per
// duration, using a token-bucket algorithm.
//
//   - elements — number of elements to emit within each per interval.
//   - per      — the measurement window (e.g. time.Second).
//   - burst    — maximum number of tokens that can accumulate, allowing short
//     bursts above the steady-state rate.  Pass 0 to use elements as the
//     burst size (i.e. allow one full interval's worth of burst).
//
// Example — limit to 100 elements/second with a burst of 10:
//
//	stream.Via(src, stream.Throttle[int](100, time.Second, 10))
func Throttle[T any](elements int, per time.Duration, burst int) Flow[T, T, NotUsed] {
	return Flow[T, T, NotUsed]{
		attach: func(up iterator[T]) (iterator[T], NotUsed) {
			return newThrottleIterator(up, elements, per, burst), NotUsed{}
		},
	}
}

// Throttle wraps this Flow with a [Throttle] stage appended after its output
// port.  See the package-level [Throttle] function for parameter semantics.
func (f Flow[In, Out, Mat]) Throttle(elements int, per time.Duration, burst int) Flow[In, Out, Mat] {
	return Flow[In, Out, Mat]{
		attach: func(up iterator[In]) (iterator[Out], Mat) {
			inner, mat := f.attach(up)
			return newThrottleIterator(inner, elements, per, burst), mat
		},
	}
}

// ─── Delay ────────────────────────────────────────────────────────────────

// Delay creates a [Flow] that introduces a fixed pause of d after receiving
// each element before forwarding it downstream.
//
// This is distinct from back-pressure: the delay is applied regardless of
// downstream demand and increases end-to-end latency by d per element.
func Delay[T any](d time.Duration) Flow[T, T, NotUsed] {
	return Flow[T, T, NotUsed]{
		attach: func(up iterator[T]) (iterator[T], NotUsed) {
			return &delayIterator[T]{upstream: up, delay: d}, NotUsed{}
		},
	}
}

// Delay wraps this Flow with a [Delay] stage appended after its output port.
// See the package-level [Delay] function for parameter semantics.
func (f Flow[In, Out, Mat]) Delay(d time.Duration) Flow[In, Out, Mat] {
	return Flow[In, Out, Mat]{
		attach: func(up iterator[In]) (iterator[Out], Mat) {
			inner, mat := f.attach(up)
			return &delayIterator[Out]{upstream: inner, delay: d}, mat
		},
	}
}

// ─── GroupBy ──────────────────────────────────────────────────────────────

// GroupBy distributes elements from src to keyed sub-streams.  The key
// function is called for each element; elements that share the same key are
// collected into the same [SubStream].  The returned Source emits one
// SubStream per distinct key in the order the key is first observed.
//
// maxSubstreams caps the number of distinct keys; the stream fails with
// [ErrTooManySubstreams] if more keys are observed.
//
// Note: GroupBy pre-collects all elements from src before emitting any
// SubStream.  For large inputs consider bounding the source first (e.g.
// with Take or a sliding window).
//
// GroupBy is a package-level function rather than a method on [Flow] because
// Go's generics system does not allow a method to return a type that
// instantiates the receiver's own generic with a different type argument.
//
// Example — group integers by odd/even:
//
//	src := stream.FromSlice([]int{1, 2, 3, 4, 5})
//	grouped, _ := stream.RunWith(
//	    stream.GroupBy(src, 2, func(n int) string {
//	        if n%2 == 0 { return "even" }
//	        return "odd"
//	    }),
//	    stream.Collect[stream.SubStream[int]](),
//	    stream.ActorMaterializer{},
//	)
func GroupBy[T any](src Source[T, NotUsed], maxSubstreams int, key func(T) string) Source[SubStream[T], NotUsed] {
	return Source[SubStream[T], NotUsed]{
		factory: func() (iterator[SubStream[T]], NotUsed) {
			upstream, _ := src.factory()
			return newGroupByIterator(upstream, maxSubstreams, key), NotUsed{}
		},
	}
}

// ─── Log ──────────────────────────────────────────────────────────────────

// Log inserts a logging stage after this Flow's output port.  Each element
// that passes through is printed to the standard logger as:
//
//	[stream] <name>: <element>
//
// Elements are forwarded unchanged; both the element type and the materialized
// value are preserved.
func (f Flow[In, Out, Mat]) Log(name string) Flow[In, Out, Mat] {
	return Flow[In, Out, Mat]{
		attach: func(upstream iterator[In]) (iterator[Out], Mat) {
			inner, mat := f.attach(upstream)
			return &logIterator[Out]{upstream: inner, name: name}, mat
		},
	}
}
