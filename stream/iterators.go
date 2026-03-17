/*
 * iterators.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

// ─── funcIterator ─────────────────────────────────────────────────────────

// funcIterator wraps a closure as an [iterator].
type funcIterator[T any] struct {
	pullFn func() (T, bool, error)
}

func (f *funcIterator[T]) next() (T, bool, error) { return f.pullFn() }

// sliceIterator emits elements from a slice, one per pull.
type sliceIterator[T any] struct {
	elems []T
	pos   int
}

func (s *sliceIterator[T]) next() (T, bool, error) {
	if s.pos >= len(s.elems) {
		var zero T
		return zero, false, nil
	}
	v := s.elems[s.pos]
	s.pos++
	return v, true, nil
}

// ─── mapIterator ──────────────────────────────────────────────────────────

type mapIterator[In, Out any] struct {
	upstream iterator[In]
	fn       func(In) Out
}

func (m *mapIterator[In, Out]) next() (Out, bool, error) {
	in, ok, err := m.upstream.next()
	if !ok || err != nil {
		var zero Out
		return zero, ok, err
	}
	return m.fn(in), true, nil
}

// ─── filterIterator ───────────────────────────────────────────────────────

type filterIterator[T any] struct {
	upstream iterator[T]
	pred     func(T) bool
}

func (f *filterIterator[T]) next() (T, bool, error) {
	for {
		elem, ok, err := f.upstream.next()
		if !ok || err != nil {
			return elem, ok, err
		}
		if f.pred(elem) {
			return elem, true, nil
		}
	}
}

// ─── takeIterator ─────────────────────────────────────────────────────────

type takeIterator[T any] struct {
	upstream iterator[T]
	n        int
	emitted  int
}

func (t *takeIterator[T]) next() (T, bool, error) {
	if t.emitted >= t.n {
		var zero T
		return zero, false, nil
	}
	elem, ok, err := t.upstream.next()
	if ok {
		t.emitted++
	}
	return elem, ok, err
}

// ─── errorIterator ────────────────────────────────────────────────────────

// errorIterator immediately fails with an error on the first pull.
type errorIterator[T any] struct {
	err error
}

func (e *errorIterator[T]) next() (T, bool, error) {
	var zero T
	return zero, false, e.err
}
