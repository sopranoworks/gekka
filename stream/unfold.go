/*
 * unfold.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

// UnfoldResult holds the outcome of one step in an [UnfoldAsync] computation.
type UnfoldResult[T, S any] struct {
	Elem     T
	State    S
	Continue bool
}

// Unfold creates a [Source] that generates elements by repeatedly applying f to
// a state value, starting with s0.  f returns (element, nextState, continue).
// The source completes when continue is false.
//
// Example — Fibonacci:
//
//	fib := stream.Unfold(
//	    [2]int{0, 1},
//	    func(s [2]int) (int, [2]int, bool) {
//	        return s[0], [2]int{s[1], s[0] + s[1]}, true
//	    },
//	).Take(10)
func Unfold[S, T any](s0 S, f func(S) (T, S, bool)) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			return &unfoldIterator[S, T]{state: s0, f: f}, NotUsed{}
		},
	}
}

type unfoldIterator[S, T any] struct {
	state S
	f     func(S) (T, S, bool)
	done  bool
}

func (u *unfoldIterator[S, T]) next() (T, bool, error) {
	if u.done {
		var zero T
		return zero, false, nil
	}
	elem, nextState, cont := u.f(u.state)
	if !cont {
		u.done = true
		var zero T
		return zero, false, nil
	}
	u.state = nextState
	return elem, true, nil
}

// UnfoldAsync creates a [Source] that generates elements by repeatedly applying
// f to a state value asynchronously.  f returns a channel delivering a single
// [UnfoldResult].  The source completes when Continue is false.
func UnfoldAsync[S, T any](s0 S, f func(S) chan UnfoldResult[T, S]) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			return &unfoldAsyncIterator[S, T]{state: s0, f: f}, NotUsed{}
		},
	}
}

type unfoldAsyncIterator[S, T any] struct {
	state S
	f     func(S) chan UnfoldResult[T, S]
	done  bool
}

func (u *unfoldAsyncIterator[S, T]) next() (T, bool, error) {
	if u.done {
		var zero T
		return zero, false, nil
	}
	ch := u.f(u.state)
	result := <-ch
	if !result.Continue {
		u.done = true
		var zero T
		return zero, false, nil
	}
	u.state = result.State
	return result.Elem, true, nil
}
