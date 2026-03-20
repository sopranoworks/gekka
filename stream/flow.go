/*
 * flow.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

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
