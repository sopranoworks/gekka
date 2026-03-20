/*
 * kill_switch.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import "sync/atomic"

// ─── KillSwitch ───────────────────────────────────────────────────────────

// KillSwitch provides manual control over a running stream.
//
// Shutdown causes the controlled stream to complete normally (no error).
// Abort causes the controlled stream to fail with the provided error.
//
// Both methods are safe to call from any goroutine and are idempotent — only
// the first call has any effect.
type KillSwitch interface {
	// Shutdown completes the stream cleanly.
	Shutdown()
	// Abort fails the stream with err.
	Abort(err error)
}

// ─── killSwitchState ──────────────────────────────────────────────────────

// killSwitchState is the shared control block used by killSwitchIterator.
// It implements the KillSwitch interface and may be shared across multiple
// iterators (SharedKillSwitch).
type killSwitchState struct {
	once   atomicOnce
	doneCh chan struct{}
	err    atomic.Value // stores a non-nil error on Abort
}

func newKillSwitchState() *killSwitchState {
	return &killSwitchState{doneCh: make(chan struct{})}
}

// atomicOnce is a thin wrapper so we can have an atomic "triggered" flag
// without an embedded sync.Once that would prevent struct copying.
type atomicOnce struct{ triggered atomic.Int32 }

func (o *atomicOnce) Do(f func()) {
	if o.triggered.CompareAndSwap(0, 1) {
		f()
	}
}

func (k *killSwitchState) Shutdown() {
	k.once.Do(func() { close(k.doneCh) })
}

func (k *killSwitchState) Abort(err error) {
	k.once.Do(func() {
		k.err.Store(err)
		close(k.doneCh)
	})
}

func (k *killSwitchState) getError() error {
	v := k.err.Load()
	if v == nil {
		return nil
	}
	return v.(error)
}

// ─── killSwitchIterator ───────────────────────────────────────────────────

// killSwitchIterator is a pass-through iterator backed by an upstream goroutine
// that can be terminated by a KillSwitch signal.
//
// The upstream goroutine pulls from the (potentially blocking) upstream
// iterator and forwards elements on elemCh.  When doneCh is closed the
// goroutine stops, and the iterator's next() returns completion (Shutdown)
// or failure (Abort).
type killSwitchIterator[T any] struct {
	state   *killSwitchState
	elemCh  <-chan T
	upErrCh <-chan error
}

func newKillSwitchIterator[T any](upstream iterator[T], state *killSwitchState) *killSwitchIterator[T] {
	elemCh := make(chan T, DefaultAsyncBufSize)
	upErrCh := make(chan error, 1)

	go func() {
		defer close(elemCh)
		for {
			// Fast-path: honour kill before blocking on upstream.
			select {
			case <-state.doneCh:
				return
			default:
			}

			elem, ok, err := upstream.next()
			if err != nil {
				select {
				case upErrCh <- err:
				default:
				}
				return
			}
			if !ok {
				return
			}

			// Forward element or exit on kill.
			select {
			case elemCh <- elem:
			case <-state.doneCh:
				return
			}
		}
	}()

	return &killSwitchIterator[T]{state: state, elemCh: elemCh, upErrCh: upErrCh}
}

func (k *killSwitchIterator[T]) next() (T, bool, error) {
	// Priority check: if kill is already signalled, stop immediately.
	select {
	case <-k.state.doneCh:
		var zero T
		return zero, false, k.state.getError()
	default:
	}

	select {
	case <-k.state.doneCh:
		var zero T
		return zero, false, k.state.getError()
	case err := <-k.upErrCh:
		var zero T
		return zero, false, err
	case elem, ok := <-k.elemCh:
		if !ok {
			// elemCh closed: check for a final upstream error.
			select {
			case err := <-k.upErrCh:
				var zero T
				return zero, false, err
			default:
				var zero T
				return zero, false, k.state.getError()
			}
		}
		return elem, true, nil
	}
}

// ─── NewKillSwitch ────────────────────────────────────────────────────────

// NewKillSwitch returns a pass-through [Flow] and a [KillSwitch] handle that
// controls it.
//
// The flow is transparent under normal operation: every upstream element is
// forwarded downstream unchanged.  Calling Shutdown() on the handle causes
// the flow to complete cleanly; calling Abort(err) causes it to fail with err.
// Both methods are idempotent and safe to call from any goroutine.
//
// Because [RunWith] discards the flow's materialized value, the KillSwitch
// handle is returned directly from this constructor:
//
//	flow, ks := stream.NewKillSwitch[int]()
//
//	go func() {
//	    stream.RunWith(stream.Via(src, flow), sink, m)
//	}()
//
//	// Later, from any goroutine:
//	ks.Shutdown()
func NewKillSwitch[T any]() (Flow[T, T, NotUsed], KillSwitch) {
	state := newKillSwitchState()
	flow := Flow[T, T, NotUsed]{
		attach: func(upstream iterator[T]) (iterator[T], NotUsed) {
			return newKillSwitchIterator(upstream, state), NotUsed{}
		},
	}
	return flow, state
}

// ─── SharedKillSwitch ─────────────────────────────────────────────────────

// SharedKillSwitch controls multiple streams simultaneously.
// Create one with [NewSharedKillSwitch], attach it to multiple flows using
// [SharedKillSwitchFlow], then call Shutdown or Abort to stop all of them.
//
//	ks := stream.NewSharedKillSwitch()
//
//	go stream.RunWith(stream.Via(src1, stream.SharedKillSwitchFlow[int](ks)), sink1, m)
//	go stream.RunWith(stream.Via(src2, stream.SharedKillSwitchFlow[string](ks)), sink2, m)
//
//	ks.Shutdown() // stops both streams
type SharedKillSwitch struct {
	state *killSwitchState
}

// NewSharedKillSwitch creates a SharedKillSwitch that can be attached to any
// number of streams via [SharedKillSwitchFlow].
func NewSharedKillSwitch() *SharedKillSwitch {
	return &SharedKillSwitch{state: newKillSwitchState()}
}

// Shutdown completes all streams controlled by this SharedKillSwitch.
func (s *SharedKillSwitch) Shutdown() { s.state.Shutdown() }

// Abort fails all streams controlled by this SharedKillSwitch with err.
func (s *SharedKillSwitch) Abort(err error) { s.state.Abort(err) }

// SharedKillSwitchFlow returns a pass-through [Flow] controlled by ks.
// Multiple flows created from the same SharedKillSwitch all stop when
// Shutdown() or Abort() is called on it.
//
// This is a package-level function because Go generics do not allow methods
// to introduce additional type parameters.
func SharedKillSwitchFlow[T any](ks *SharedKillSwitch) Flow[T, T, NotUsed] {
	return Flow[T, T, NotUsed]{
		attach: func(upstream iterator[T]) (iterator[T], NotUsed) {
			return newKillSwitchIterator(upstream, ks.state), NotUsed{}
		},
	}
}
