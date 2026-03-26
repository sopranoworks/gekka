/*
 * source.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import "time"

// Source[T, Mat] is a stream stage with exactly one output port that emits
// elements of type T and materializes to a value of type Mat.
//
// Build pipelines by attaching a [Flow] via the package-level [Via] function,
// then connect to a [Sink] with [Source.To]:
//
//	src := stream.FromSlice([]int{1, 2, 3})
//	graph := stream.Via(src, stream.Map(double)).To(stream.Foreach(print))
//	graph.Run(stream.SyncMaterializer{})
type Source[T, Mat any] struct {
	// factory creates a fresh iterator and the materialized value each time
	// the graph is run.
	factory func() (iterator[T], Mat)
}

// Shape returns the [SourceShape] of this Source.
func (s Source[T, Mat]) Shape() SourceShape[T] {
	return SourceShape[T]{Out: Outlet[T]{}}
}

// ─── Constructors ─────────────────────────────────────────────────────────

// FromSlice creates a Source that emits all elements from s in order, then
// completes.  The materialized value is NotUsed.
func FromSlice[T any](s []T) Source[T, NotUsed] {
	cp := make([]T, len(s)) // defensive copy
	copy(cp, s)
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			return &sliceIterator[T]{elems: cp}, NotUsed{}
		},
	}
}

// FromIteratorFunc creates a Source backed by fn.  fn should return
// (element, true, nil) for each element, then (zero, false, nil) when
// exhausted, or (zero, false, err) on failure.
func FromIteratorFunc[T any](fn func() (T, bool, error)) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			return &funcIterator[T]{pullFn: fn}, NotUsed{}
		},
	}
}

// Failed creates a Source that immediately fails with err when run.
func Failed[T any](err error) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			return &errorIterator[T]{err: err}, NotUsed{}
		},
	}
}

// Repeat creates an infinite Source that emits elem forever.
// Combine with [Source.Take] to bound the output:
//
//	stream.Repeat(42).Take(5) // emits 42 five times
func Repeat[T any](elem T) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			return &repeatIterator[T]{elem: elem}, NotUsed{}
		},
	}
}

// ─── Stage methods (element type preserved) ───────────────────────────────

// Filter returns a Source that only emits elements for which pred returns true.
// The materialized value is forwarded unchanged.
func (s Source[T, Mat]) Filter(pred func(T) bool) Source[T, Mat] {
	return Source[T, Mat]{
		factory: func() (iterator[T], Mat) {
			up, mat := s.factory()
			return &filterIterator[T]{upstream: up, pred: pred}, mat
		},
	}
}

// Take returns a Source that emits at most n elements, then completes.
func (s Source[T, Mat]) Take(n int) Source[T, Mat] {
	return Source[T, Mat]{
		factory: func() (iterator[T], Mat) {
			up, mat := s.factory()
			return &takeIterator[T]{upstream: up, n: n}, mat
		},
	}
}

// ─── Supervision ──────────────────────────────────────────────────────────

// WithSupervisionStrategy wraps this Source with a [supervisedIterator] that
// applies decider to every error emitted by the Source's iterator.
// See [Flow.WithSupervisionStrategy] for directive semantics.
func (s Source[T, Mat]) WithSupervisionStrategy(decider Decider) Source[T, Mat] {
	return Source[T, Mat]{
		factory: func() (iterator[T], Mat) {
			inner, mat := s.factory()
			remake := func() iterator[T] {
				newInner, _ := s.factory()
				return newInner
			}
			return &supervisedIterator[T]{
				inner:   inner,
				decider: decider,
				remake:  remake,
			}, mat
		},
	}
}

// ─── Async boundary ───────────────────────────────────────────────────────

// Async inserts an asynchronous boundary after this Source.  Elements are
// buffered in a channel of capacity [DefaultAsyncBufSize]; the upstream source
// runs in a dedicated goroutine.  When the buffer is full the goroutine blocks,
// propagating demand-driven back-pressure to the original source.
//
// Use [ActorMaterializer] when running graphs that contain async boundaries.
func (s Source[T, Mat]) Async() Source[T, Mat] {
	return Source[T, Mat]{
		factory: func() (iterator[T], Mat) {
			up, mat := s.factory()
			return newAsyncBoundary[T](up, DefaultAsyncBufSize), mat
		},
	}
}

// ─── Flow-control helpers ─────────────────────────────────────────────────

// Buffer appends a [Buffer] stage after this Source.
// See the package-level [Buffer] function for parameter semantics.
func (s Source[T, Mat]) Buffer(size int, strategy OverflowStrategy) Source[T, Mat] {
	return Source[T, Mat]{
		factory: func() (iterator[T], Mat) {
			inner, mat := s.factory()
			return newBufferIterator(inner, size, strategy), mat
		},
	}
}

// Throttle appends a [Throttle] stage after this Source.
// See the package-level [Throttle] function for parameter semantics.
func (s Source[T, Mat]) Throttle(elements int, per time.Duration, burst int) Source[T, Mat] {
	return Source[T, Mat]{
		factory: func() (iterator[T], Mat) {
			inner, mat := s.factory()
			return newThrottleIterator(inner, elements, per, burst), mat
		},
	}
}

// Delay appends a [Delay] stage after this Source.
// See the package-level [Delay] function for parameter semantics.
func (s Source[T, Mat]) Delay(d time.Duration) Source[T, Mat] {
	return Source[T, Mat]{
		factory: func() (iterator[T], Mat) {
			inner, mat := s.factory()
			return &delayIterator[T]{upstream: inner, delay: d}, mat
		},
	}
}

// ─── Graph assembly ───────────────────────────────────────────────────────

// To connects this Source to sink, returning a [RunnableGraph] that keeps the
// Source's materialized value.  The sink's materialized value is discarded.
//
// To obtain the sink's materialized value use the package-level [RunWith].
func (s Source[T, sMat]) To(sink sinkConnector[T]) RunnableGraph[sMat] {
	return RunnableGraph[sMat]{
		run: func(_ Materializer) (sMat, error) {
			up, mat := s.factory()
			err := sink.connect(up)
			return mat, err
		},
	}
}

// ─── Via (package-level — required by Go generics) ────────────────────────

// Via attaches flow to source, returning a new Source whose element type is
// the Flow's output type.  The Source's materialized value is kept; the
// Flow's materialized value is discarded.
//
// Because Go does not support additional type parameters on methods, Via is a
// package-level function rather than a method on Source:
//
//	src2 := stream.Via(src, flow)
func Via[T, U, SMat, FMat any](source Source[T, SMat], flow Flow[T, U, FMat]) Source[U, SMat] {
	return Source[U, SMat]{
		factory: func() (iterator[U], SMat) {
			up, srcMat := source.factory()
			down, _ := flow.attach(up)
			return down, srcMat
		},
	}
}

// RunWith connects source to sink and runs the resulting graph using m.
// Unlike [Source.To], RunWith returns the Sink's materialized value.
func RunWith[T, SMat, DMat any](source Source[T, SMat], sink Sink[T, DMat], m Materializer) (DMat, error) {
	up, _ := source.factory()
	return sink.runWith(up)
}
