/*
 * sink_ops.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import "errors"

// ErrEmptyStream is returned by sinks that require at least one element (such
// as [Last] and [Reduce]) when the upstream source is empty.
var ErrEmptyStream = errors.New("empty stream")

// Fold creates a [Sink] that reduces all upstream elements to a single value
// using f, starting from zero.  The materialized value is the final accumulator.
func Fold[T, U any](zero U, f func(U, T) U) Sink[T, U] {
	return Sink[T, U]{
		runWith: func(upstream iterator[T]) (U, error) {
			acc := zero
			for {
				elem, ok, err := upstream.next()
				if err != nil {
					return acc, err
				}
				if !ok {
					return acc, nil
				}
				acc = f(acc, elem)
			}
		},
	}
}

// Reduce creates a [Sink] that reduces all upstream elements using f.  The
// first element is used as the initial accumulator.  Returns [ErrEmptyStream]
// if the source is empty.
func Reduce[T any](f func(T, T) T) Sink[T, T] {
	return Sink[T, T]{
		runWith: func(upstream iterator[T]) (T, error) {
			first, ok, err := upstream.next()
			if err != nil {
				var zero T
				return zero, err
			}
			if !ok {
				var zero T
				return zero, ErrEmptyStream
			}
			acc := first
			for {
				elem, ok, err := upstream.next()
				if err != nil {
					return acc, err
				}
				if !ok {
					return acc, nil
				}
				acc = f(acc, elem)
			}
		},
	}
}

// Last creates a [Sink] that materializes the last element from upstream.
// Returns [ErrEmptyStream] if the source is empty.
func Last[T any]() Sink[T, T] {
	return Sink[T, T]{
		runWith: func(upstream iterator[T]) (T, error) {
			var last T
			seen := false
			for {
				elem, ok, err := upstream.next()
				if err != nil {
					return last, err
				}
				if !ok {
					if !seen {
						var zero T
						return zero, ErrEmptyStream
					}
					return last, nil
				}
				last = elem
				seen = true
			}
		},
	}
}

// LastOption creates a [Sink] that materializes a pointer to the last element,
// or nil if the source is empty.
func LastOption[T any]() Sink[T, *T] {
	return Sink[T, *T]{
		runWith: func(upstream iterator[T]) (*T, error) {
			var last T
			seen := false
			for {
				elem, ok, err := upstream.next()
				if err != nil {
					if seen {
						return &last, err
					}
					return nil, err
				}
				if !ok {
					if !seen {
						return nil, nil
					}
					v := last
					return &v, nil
				}
				last = elem
				seen = true
			}
		},
	}
}

// HeadOption creates a [Sink] that materializes a pointer to the first element,
// or nil if the source is empty.  This is equivalent to [Head] but with a
// clearer name alongside [LastOption].
func HeadOption[T any]() Sink[T, *T] {
	return Head[T]()
}

// Cancelled creates a [Sink] that immediately cancels upstream without
// consuming any elements.  The materialized value is [NotUsed].
func Cancelled[T any]() Sink[T, NotUsed] {
	return Sink[T, NotUsed]{
		runWith: func(_ iterator[T]) (NotUsed, error) {
			return NotUsed{}, nil
		},
	}
}
