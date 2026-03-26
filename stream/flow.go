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

// Shape returns the [FlowShape] of this Flow.
func (f Flow[In, Out, Mat]) Shape() FlowShape[In, Out] {
	return FlowShape[In, Out]{In: Inlet[In]{}, Out: Outlet[Out]{}}
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

// ─── MapAsync ─────────────────────────────────────────────────────────────

// MapAsync applies fn to each element of src concurrently (up to parallelism
// goroutines in-flight) and emits results in the original input order.
//
// fn must return a channel that will deliver exactly one value (the result)
// and then close.  If fn closes the channel without sending a value the
// corresponding element is silently dropped.
//
// The parallelism parameter controls how many calls to fn may be outstanding
// simultaneously.  When the in-flight queue is full MapAsync blocks on the
// oldest pending result before pulling the next upstream element, so
// back-pressure is preserved.
//
// MapAsync is a package-level function rather than a method on [Flow] because
// Go generics do not allow methods to introduce additional type parameters.
// Use [Via] to compose it with an existing flow:
//
//	result, err := stream.RunWith(
//	    stream.MapAsync(stream.Via(src, preprocess), 4, asyncFn),
//	    stream.Collect[Out](),
//	    stream.ActorMaterializer{},
//	)
func MapAsync[In, Out any](src Source[In, NotUsed], parallelism int, fn func(In) <-chan Out) Source[Out, NotUsed] {
	return Source[Out, NotUsed]{
		factory: func() (iterator[Out], NotUsed) {
			upstream, _ := src.factory()
			return &mapAsyncIterator[In, Out]{
				upstream:    upstream,
				fn:          fn,
				parallelism: parallelism,
				pending:     make([]<-chan Out, 0, parallelism),
			}, NotUsed{}
		},
	}
}

// ─── StatefulMapConcat ────────────────────────────────────────────────────

// StatefulMapConcat threads explicit state through a flat-map transformation.
//
//   - create is called once per materialization to initialise state S.
//   - fn is called for each upstream element with the current state and the
//     element.  It returns the next state and a (possibly empty) slice of
//     output elements.
//   - Output elements are emitted one by one before the next upstream element
//     is pulled.
//
// This operator subsumes both stateful Map (return a singleton slice) and
// stateful Filter (return an empty or singleton slice).
//
// StatefulMapConcat is a package-level function rather than a method on [Flow]
// because Go generics do not allow methods to introduce additional type
// parameters.
//
// Example — emit a running sum after each element:
//
//	src := stream.FromSlice([]int{1, 2, 3, 4})
//	out := stream.StatefulMapConcat(src,
//	    func() int { return 0 },
//	    func(sum, n int) (int, []int) { return sum + n, []int{sum + n} },
//	)
//	// emits 1, 3, 6, 10
func StatefulMapConcat[In, S, Out any](
	src Source[In, NotUsed],
	create func() S,
	fn func(S, In) (S, []Out),
) Source[Out, NotUsed] {
	return Source[Out, NotUsed]{
		factory: func() (iterator[Out], NotUsed) {
			upstream, _ := src.factory()
			return &statefulMapConcatIterator[In, S, Out]{
				upstream: upstream,
				state:    create(),
				fn:       fn,
			}, NotUsed{}
		},
	}
}

// ─── FilterMap ────────────────────────────────────────────────────────────

// FilterMap applies a partial function to each element of src.  Elements for
// which pf returns false are dropped; elements for which it returns true are
// emitted with the transformed value.
//
// This operator is analogous to Scala's collect on a stream: it combines
// type-safe filtering and transformation in a single pass.
//
// FilterMap is named to avoid shadowing the [Collect] sink constructor.
//
// Example — keep only even numbers and halve them:
//
//	src := stream.FromSlice([]int{1, 2, 3, 4, 5, 6})
//	out := stream.FilterMap(src, func(n int) (int, bool) {
//	    if n%2 == 0 { return n / 2, true }
//	    return 0, false
//	})
//	// emits 1, 2, 3
func FilterMap[In, Out any](src Source[In, NotUsed], pf func(In) (Out, bool)) Source[Out, NotUsed] {
	return Source[Out, NotUsed]{
		factory: func() (iterator[Out], NotUsed) {
			upstream, _ := src.factory()
			return &filterMapIterator[In, Out]{upstream: upstream, pf: pf}, NotUsed{}
		},
	}
}

// ─── Grouped / GroupedWithin ──────────────────────────────────────────────

// Grouped batches elements from src into slices of exactly n elements.
// The last batch may contain fewer than n elements if the source is exhausted
// before a full batch accumulates.  An empty source produces no batches.
//
// Grouped is a package-level function rather than a method on [Flow] because
// the return type Flow[In,[]Out,NotUsed] creates an instantiation cycle in Go's
// generic type system when expressed as a method.
//
// Example:
//
//	stream.Grouped(stream.FromSlice([]int{1,2,3,4,5}), 2)
//	// emits [1 2], [3 4], [5]
func Grouped[T any](src Source[T, NotUsed], n int) Source[[]T, NotUsed] {
	return Source[[]T, NotUsed]{
		factory: func() (iterator[[]T], NotUsed) {
			upstream, _ := src.factory()
			return &groupedIterator[T]{upstream: upstream, n: n}, NotUsed{}
		},
	}
}

// GroupedWithin batches elements from src into slices emitted when either n
// elements have accumulated OR the duration d has elapsed since the batch was
// started, whichever occurs first.
//
// Empty batches at timer expiry are suppressed — no zero-length slice is ever
// emitted.  An upstream error terminates the stream after any partial batch
// already in-flight has been emitted.
//
// GroupedWithin is a package-level function for the same reason as [Grouped].
//
// Example — collect up to 10 events or flush every 500 ms:
//
//	stream.GroupedWithin(eventSrc, 10, 500*time.Millisecond)
func GroupedWithin[T any](src Source[T, NotUsed], n int, d time.Duration) Source[[]T, NotUsed] {
	return Source[[]T, NotUsed]{
		factory: func() (iterator[[]T], NotUsed) {
			upstream, _ := src.factory()
			return newGroupedWithinIterator(upstream, n, d), NotUsed{}
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

// ─── FlattenMerge ─────────────────────────────────────────────────────────

// FlattenMerge flattens a stream of streams into a single stream, merging
// up to breadth sub-streams concurrently.
//
// Elements from sub-streams are emitted as they become available.  The
// breadth parameter limits the number of active sub-streams; when breadth
// is reached no more sub-streams are pulled from the outer source until an
// active sub-stream completes.
func FlattenMerge[T any](src Source[Source[T, NotUsed], NotUsed], breadth int) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			upstream, _ := src.factory()
			return newFlattenMergeIterator(upstream, breadth), NotUsed{}
		},
	}
}

// ─── Conflate ─────────────────────────────────────────────────────────────

// Conflate applies flow control that combines elements when the downstream
// is slow.
//
//   - seed is called with the first element of a conflation batch to create
//     the initial accumulated value S.
//   - aggregate is called for subsequent elements, merging them into the
//     current accumulator.
//
// When the downstream pulls, the current accumulator is emitted and reset.
// This operator never applies back-pressure to the upstream; instead it
// "compacts" elements.
func Conflate[In, S any](src Source[In, NotUsed], seed func(In) S, aggregate func(S, In) S) Source[S, NotUsed] {
	return Source[S, NotUsed]{
		factory: func() (iterator[S], NotUsed) {
			upstream, _ := src.factory()
			return newConflateIterator(upstream, seed, aggregate), NotUsed{}
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
