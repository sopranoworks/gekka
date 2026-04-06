/*
 * lazy.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

// ─── LazySource ──────────────────────────────────────────────────────────────

// LazySource creates a Source that defers materialization until the first
// element is pulled. The factory function is called exactly once on first
// demand. If factory returns an error, the stream fails immediately.
func LazySource[T any](factory func() (Source[T, NotUsed], error)) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			return &lazySourceIterator[T]{factoryFn: factory}, NotUsed{}
		},
	}
}

type lazySourceIterator[T any] struct {
	factoryFn   func() (Source[T, NotUsed], error)
	initialized bool
	inner       iterator[T]
	err         error
}

func (l *lazySourceIterator[T]) next() (T, bool, error) {
	var zero T
	if !l.initialized {
		l.initialized = true
		src, err := l.factoryFn()
		if err != nil {
			l.err = err
			return zero, false, err
		}
		l.inner, _ = src.factory()
	}
	if l.err != nil {
		return zero, false, l.err
	}
	return l.inner.next()
}

// ─── LazySink ────────────────────────────────────────────────────────────────

// LazySink creates a Sink that defers materialization until the first
// element is pushed. The factory function is called exactly once when
// the first element arrives. If factory returns an error, the stream fails.
func LazySink[T any](factory func() (Sink[T, NotUsed], error)) Sink[T, NotUsed] {
	return Sink[T, NotUsed]{
		runWith: func(upstream iterator[T]) (NotUsed, error) {
			// Pull the first element to trigger lazy init.
			first, ok, err := upstream.next()
			if err != nil {
				return NotUsed{}, err
			}
			if !ok {
				// Empty stream — never materialize the inner sink.
				return NotUsed{}, nil
			}

			sink, err := factory()
			if err != nil {
				return NotUsed{}, err
			}

			// Prepend the first element back and run the inner sink.
			prepended := &prependIterator[T]{first: first, hasFirst: true, rest: upstream}
			return sink.runWith(prepended)
		},
	}
}

// ─── LazyFlow ────────────────────────────────────────────────────────────────

// LazyFlow creates a Flow that defers materialization until the first
// element arrives. The factory function is called exactly once when the
// first element is received. If factory returns an error, the stream fails.
func LazyFlow[T, U any](factory func() (Flow[T, U, NotUsed], error)) Flow[T, U, NotUsed] {
	return Flow[T, U, NotUsed]{
		attach: func(upstream iterator[T]) (iterator[U], NotUsed) {
			return &lazyFlowIterator[T, U]{
				factoryFn: factory,
				upstream:  upstream,
			}, NotUsed{}
		},
	}
}

type lazyFlowIterator[T, U any] struct {
	factoryFn   func() (Flow[T, U, NotUsed], error)
	upstream    iterator[T]
	initialized bool
	inner       iterator[U]
	err         error
}

func (l *lazyFlowIterator[T, U]) next() (U, bool, error) {
	var zero U
	if !l.initialized {
		l.initialized = true
		flow, err := l.factoryFn()
		if err != nil {
			l.err = err
			return zero, false, err
		}
		l.inner, _ = flow.attach(l.upstream)
	}
	if l.err != nil {
		return zero, false, l.err
	}
	return l.inner.next()
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// prependIterator prepends a single element before the rest of the iterator.
type prependIterator[T any] struct {
	first    T
	hasFirst bool
	rest     iterator[T]
}

func (p *prependIterator[T]) next() (T, bool, error) {
	if p.hasFirst {
		p.hasFirst = false
		return p.first, true, nil
	}
	return p.rest.next()
}
