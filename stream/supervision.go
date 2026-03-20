/*
 * supervision.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

// Directive is the instruction a [Decider] returns when a stage encounters an
// error.
type Directive int

const (
	// Stop propagates the error downstream, failing the stream.  This is the
	// default behaviour and matches the semantics of unrecoverable failures.
	Stop Directive = iota

	// Resume drops the element that caused the error and continues processing
	// the next element from upstream.  The error is silently swallowed.
	Resume

	// Restart resets the failing stage to its initial state and continues
	// processing the next element from upstream.  For stateless stages (e.g.
	// [MapE]) this is equivalent to [Resume].  For stateful stages the
	// accumulated state is discarded.
	Restart
)

// Decider maps an error produced by a stage to the supervision [Directive] that
// determines how the stage recovers (or not).
//
//	decider := func(err error) stream.Directive {
//	    if errors.Is(err, myTransientError) {
//	        return stream.Resume
//	    }
//	    return stream.Stop
//	}
type Decider func(error) Directive

// DefaultDecider always returns [Stop], which preserves the default stream
// semantics: any error immediately fails the stream.
var DefaultDecider Decider = func(error) Directive { return Stop }

// ResumeDecider always returns [Resume].  Every error is swallowed and the
// failing element is skipped.
var ResumeDecider Decider = func(error) Directive { return Resume }

// RestartDecider always returns [Restart].  Every error causes the stage to
// reset its state and continue.
var RestartDecider Decider = func(error) Directive { return Restart }

// ─── supervisedIterator ───────────────────────────────────────────────────

// supervisedIterator wraps any [iterator][T] and applies a [Decider] to errors
// returned by next().
//
//   - [Stop]:    the error is forwarded to the caller.
//   - [Resume]:  next() is called again on the inner iterator (the failing
//     element is skipped; the upstream has already advanced past it).
//   - [Restart]: the inner iterator is recreated via remake(), then next() is
//     called again.  For stateless stages remake() produces a functionally
//     identical iterator sharing the same upstream, so Restart == Resume.
//     For stateful stages remake() can return a freshly initialised iterator.
type supervisedIterator[T any] struct {
	inner   iterator[T]
	decider Decider
	// remake recreates the inner iterator (used for Restart).  If nil, Restart
	// falls back to Resume behaviour.
	remake func() iterator[T]
}

func (s *supervisedIterator[T]) next() (T, bool, error) {
	for {
		elem, ok, err := s.inner.next()
		if err != nil {
			switch s.decider(err) {
			case Stop:
				return elem, false, err
			case Resume:
				continue // pull next element from inner (which advances upstream)
			case Restart:
				if s.remake != nil {
					s.inner = s.remake()
				}
				continue
			}
		}
		return elem, ok, err
	}
}

// ─── recoverIterator ──────────────────────────────────────────────────────

// recoverIterator catches the first error from upstream, emits the value
// returned by fn, then signals normal stream completion.
type recoverIterator[T any] struct {
	upstream  iterator[T]
	fn        func(error) T
	recovered bool // true once the recovery element has been emitted
}

func (r *recoverIterator[T]) next() (T, bool, error) {
	if r.recovered {
		var zero T
		return zero, false, nil
	}
	elem, ok, err := r.upstream.next()
	if err != nil {
		r.recovered = true
		return r.fn(err), true, nil
	}
	return elem, ok, nil
}

// ─── recoverWithIterator ──────────────────────────────────────────────────

// recoverWithIterator catches the first error from upstream and seamlessly
// switches to a backup source produced by fn.  The backup is drained to
// completion as if it were the original source.
type recoverWithIterator[T any] struct {
	upstream iterator[T]
	fn       func(error) Source[T, NotUsed]
	backup   iterator[T] // non-nil after the first error
}

func (r *recoverWithIterator[T]) next() (T, bool, error) {
	if r.backup != nil {
		return r.backup.next()
	}
	elem, ok, err := r.upstream.next()
	if err != nil {
		src := r.fn(err)
		r.backup, _ = src.factory()
		return r.backup.next()
	}
	return elem, ok, nil
}

// ─── Recover / RecoverWith on Flow ────────────────────────────────────────

// Recover intercepts the first error produced by this Flow.  fn is called with
// the error and must return a single fallback element that is emitted before
// the stream completes normally.
//
// Subsequent errors (if any) are forwarded without interception because the
// stream terminates after the fallback element.
//
// Example — emit -1 on any error:
//
//	stream.MapE(riskyFn).Recover(func(error) int { return -1 })
func (f Flow[In, Out, Mat]) Recover(fn func(error) Out) Flow[In, Out, Mat] {
	return Flow[In, Out, Mat]{
		attach: func(upstream iterator[In]) (iterator[Out], Mat) {
			inner, mat := f.attach(upstream)
			return &recoverIterator[Out]{upstream: inner, fn: fn}, mat
		},
	}
}

// RecoverWith intercepts the first error produced by this Flow and switches to
// the backup [Source] returned by fn.  The backup is drained to completion as
// if it were the original source.
//
// Example — failover to a cached source on error:
//
//	stream.MapE(fetchRemote).RecoverWith(func(error) stream.Source[Data, stream.NotUsed] {
//	    return cachedSource
//	})
func (f Flow[In, Out, Mat]) RecoverWith(fn func(error) Source[Out, NotUsed]) Flow[In, Out, Mat] {
	return Flow[In, Out, Mat]{
		attach: func(upstream iterator[In]) (iterator[Out], Mat) {
			inner, mat := f.attach(upstream)
			return &recoverWithIterator[Out]{upstream: inner, fn: fn}, mat
		},
	}
}
