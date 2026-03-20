/*
 * restart.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"math/rand"
	"time"
)

// ─── RestartSettings ──────────────────────────────────────────────────────

// RestartSettings controls the exponential-backoff policy applied by
// [RestartSource] and [RestartFlow] when the inner factory fails.
type RestartSettings struct {
	// MinBackoff is the initial delay before the first restart attempt.
	MinBackoff time.Duration

	// MaxBackoff caps the delay so it does not grow without bound.
	MaxBackoff time.Duration

	// RandomFactor adds jitter to each backoff to avoid the thundering-herd
	// problem.  The actual delay is multiplied by a random value drawn from
	// [1.0, 1.0+RandomFactor].  A value of 0.2 gives ±20 % spread.
	RandomFactor float64

	// MaxRestarts is the maximum number of times the inner factory may be
	// restarted after a failure.  -1 means unlimited.
	MaxRestarts int
}

// calcBackoff returns the next backoff duration, doubling current and capping
// at settings.MaxBackoff, then applying RandomFactor jitter.
func calcBackoff(current time.Duration, settings RestartSettings) time.Duration {
	next := min(current*2, settings.MaxBackoff)
	if settings.RandomFactor > 0 {
		// Multiply by a uniform value in [1.0, 1.0+RandomFactor].
		next = min(time.Duration(float64(next)*(1.0+rand.Float64()*settings.RandomFactor)), settings.MaxBackoff)
	}
	return next
}

// ─── restartSourceIterator ────────────────────────────────────────────────

type restartSourceIterator[T any] struct {
	settings RestartSettings
	factory  func() Source[T, NotUsed]
	inner    iterator[T]
	restarts int
	backoff  time.Duration
}

func newRestartSourceIterator[T any](settings RestartSettings, factory func() Source[T, NotUsed]) *restartSourceIterator[T] {
	src := factory()
	inner, _ := src.factory()
	return &restartSourceIterator[T]{
		settings: settings,
		factory:  factory,
		inner:    inner,
		backoff:  settings.MinBackoff,
	}
}

func (r *restartSourceIterator[T]) next() (T, bool, error) {
	for {
		elem, ok, err := r.inner.next()
		if err == nil {
			// Normal element or clean completion.
			return elem, ok, nil
		}

		// The inner source failed.  Decide whether to restart.
		if r.settings.MaxRestarts >= 0 && r.restarts >= r.settings.MaxRestarts {
			// Budget exhausted — propagate the error.
			var zero T
			return zero, false, err
		}

		// Sleep the current backoff, then restart.
		time.Sleep(r.backoff)
		r.backoff = calcBackoff(r.backoff, r.settings)
		r.restarts++

		newSrc := r.factory()
		r.inner, _ = newSrc.factory()
	}
}

// ─── RestartSource ────────────────────────────────────────────────────────

// RestartSource wraps factory so that if the produced Source fails, it is
// transparently restarted after an exponential-backoff delay.
//
// On each failure the delay is doubled (starting at settings.MinBackoff),
// capped at settings.MaxBackoff, and jittered by settings.RandomFactor.
// After settings.MaxRestarts failures the error is propagated downstream.
// Use MaxRestarts = -1 for unlimited restarts.
//
// Example — restart a flaky HTTP source up to 5 times:
//
//	src := stream.RestartSource(
//	    stream.RestartSettings{
//	        MinBackoff:   100*time.Millisecond,
//	        MaxBackoff:   10*time.Second,
//	        RandomFactor: 0.2,
//	        MaxRestarts:  5,
//	    },
//	    func() stream.Source[[]byte, stream.NotUsed] { return fetchHTTP(url) },
//	)
func RestartSource[T any](settings RestartSettings, factory func() Source[T, NotUsed]) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			return newRestartSourceIterator(settings, factory), NotUsed{}
		},
	}
}

// ─── restartFlowIterator ──────────────────────────────────────────────────

type restartFlowIterator[In, Out any] struct {
	settings RestartSettings
	factory  func() Flow[In, Out, NotUsed]
	upstream iterator[In]
	inner    iterator[Out]
	restarts int
	backoff  time.Duration
}

func newRestartFlowIterator[In, Out any](
	settings RestartSettings,
	factory func() Flow[In, Out, NotUsed],
	upstream iterator[In],
) *restartFlowIterator[In, Out] {
	flow := factory()
	inner, _ := flow.attach(upstream)
	return &restartFlowIterator[In, Out]{
		settings: settings,
		factory:  factory,
		upstream: upstream,
		inner:    inner,
		backoff:  settings.MinBackoff,
	}
}

func (r *restartFlowIterator[In, Out]) next() (Out, bool, error) {
	for {
		elem, ok, err := r.inner.next()
		if err == nil {
			return elem, ok, nil
		}

		// The inner flow failed.
		if r.settings.MaxRestarts >= 0 && r.restarts >= r.settings.MaxRestarts {
			var zero Out
			return zero, false, err
		}

		time.Sleep(r.backoff)
		r.backoff = calcBackoff(r.backoff, r.settings)
		r.restarts++

		// Re-attach the flow to the same upstream.  Elements consumed by the
		// failed flow are lost; the restart resumes from wherever the upstream
		// currently is.
		newFlow := r.factory()
		r.inner, _ = newFlow.attach(r.upstream)
	}
}

// ─── RestartFlow ──────────────────────────────────────────────────────────

// RestartFlow wraps factory so that if the produced Flow fails, it is
// re-attached to the same upstream after an exponential-backoff delay.
// The restart semantics mirror those of [RestartSource].
//
// Note: elements that were in-flight through the failed Flow instance are
// lost on restart.  If loss-free semantics are required, use supervision
// strategies ([Flow.WithSupervisionStrategy]) instead.
func RestartFlow[In, Out any](settings RestartSettings, factory func() Flow[In, Out, NotUsed]) Flow[In, Out, NotUsed] {
	return Flow[In, Out, NotUsed]{
		attach: func(upstream iterator[In]) (iterator[Out], NotUsed) {
			return newRestartFlowIterator(settings, factory, upstream), NotUsed{}
		},
	}
}
