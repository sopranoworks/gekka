/*
 * sink.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

// Sink[In, Mat] is a stream stage with exactly one input port.  It consumes
// elements of type In and materializes to a value of type Mat.
//
// Connect a Sink to a Source or composite Source using [Source.To] (to keep
// the Source's mat) or the package-level [RunWith] (to obtain the Sink's mat).
type Sink[In, Mat any] struct {
	// runWith is the internal runner: drains upstream and returns Mat.
	runWith func(upstream iterator[In]) (Mat, error)
}

// Shape returns the [SinkShape] of this Sink.
func (s Sink[In, Mat]) Shape() SinkShape[In] {
	return SinkShape[In]{In: &Inlet[In]{}}
}

func (s Sink[In, Mat]) materialize(m Materializer, shape Shape) materializedStage {
	lazy := &lazyIterator[In]{}
	return materializedStage{
		inConns: map[int]func(any){
			shape.(SinkShape[In]).In.id: func(up any) {
				lazy.inner = up.(iterator[In])
			},
		},
		runners: []func() error{
			func() error {
				_, err := s.runWith(lazy)
				return err
			},
		},
	}
}

// ─── sinkConnector implementation ─────────────────────────────────────────

// connect implements [sinkConnector], allowing Sink to be passed to [Source.To].
// The Sink's materialized value is discarded.
//
//nolint:unused
func (s Sink[In, Mat]) connect(upstream iterator[In]) error {
	_, err := s.runWith(upstream)
	return err
}

// ─── Async boundary ───────────────────────────────────────────────────────

// Async inserts an asynchronous boundary before this Sink.  Upstream runs in a
// dedicated goroutine and pushes elements into a bounded channel of capacity
// [DefaultAsyncBufSize]; the Sink drains from that channel.  When the channel
// is full the upstream goroutine blocks, enforcing demand-driven back-pressure
// on the producer even when the Sink is the slower party.
func (s Sink[In, Mat]) Async() Sink[In, Mat] {
	return Sink[In, Mat]{
		runWith: func(upstream iterator[In]) (Mat, error) {
			ab := newAsyncBoundary[In](upstream, DefaultAsyncBufSize)
			return s.runWith(ab)
		},
	}
}

// ─── Constructors ─────────────────────────────────────────────────────────

// Foreach creates a Sink that calls fn for each incoming element.
// The materialized value is NotUsed.  If fn panics the panic propagates.
func Foreach[T any](fn func(T)) Sink[T, NotUsed] {
	return Sink[T, NotUsed]{
		runWith: func(upstream iterator[T]) (NotUsed, error) {
			for {
				elem, ok, err := upstream.next()
				if err != nil {
					return NotUsed{}, err
				}
				if !ok {
					break
				}
				fn(elem)
			}
			return NotUsed{}, nil
		},
	}
}

// ForeachErr creates a Sink that calls fn for each incoming element.
// If fn returns an error, the stream fails with that error.
func ForeachErr[T any](fn func(T) error) Sink[T, NotUsed] {
	return Sink[T, NotUsed]{
		runWith: func(upstream iterator[T]) (NotUsed, error) {
			for {
				elem, ok, err := upstream.next()
				if err != nil {
					return NotUsed{}, err
				}
				if !ok {
					break
				}
				if err := fn(elem); err != nil {
					return NotUsed{}, err
				}
			}
			return NotUsed{}, nil
		},
	}
}

// Ignore creates a Sink that discards all elements.
// Equivalent to Foreach with a no-op function.
func Ignore[T any]() Sink[T, NotUsed] {
	return Foreach[T](func(T) {})
}

// Collect creates a Sink that accumulates all elements into a slice.
// The materialized value is the collected slice.
func Collect[T any]() Sink[T, []T] {
	return Sink[T, []T]{
		runWith: func(upstream iterator[T]) ([]T, error) {
			var out []T
			for {
				elem, ok, err := upstream.next()
				if err != nil {
					return out, err
				}
				if !ok {
					break
				}
				out = append(out, elem)
			}
			return out, nil
		},
	}
}

// SinkFromFunc creates a Sink backed by a custom drain function.  fn receives
// a pull function that follows the iterator contract: returning (elem, true,
// nil) for each element, (zero, false, nil) at completion, and (zero, false,
// err) on error.  fn should drain the upstream fully and return a non-nil
// error only on failure.  The materialized value is NotUsed.
//
// This constructor is intended for advanced use-cases such as test probes and
// custom back-pressure controllers.  Most applications should use [Foreach],
// [Collect], or [Head] instead.
func SinkFromFunc[T any](fn func(pull func() (T, bool, error)) error) Sink[T, NotUsed] {
	return Sink[T, NotUsed]{
		runWith: func(upstream iterator[T]) (NotUsed, error) {
			return NotUsed{}, fn(upstream.next)
		},
	}
}

// Head creates a Sink that captures the first element and completes.
// Returns the zero value if the source is empty.
func Head[T any]() Sink[T, *T] {
	return Sink[T, *T]{
		runWith: func(upstream iterator[T]) (*T, error) {
			elem, ok, err := upstream.next()
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, nil
			}
			v := elem
			return &v, nil
		},
	}
}
