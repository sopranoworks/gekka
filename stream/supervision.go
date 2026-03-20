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
