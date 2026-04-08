/*
 * supervise.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"fmt"
	"log/slog"
	"time"
)

// SupervisorStrategy defines how the actor should react to failures.
type SupervisorStrategy int

const (
	// StrategyRestart restarts the actor on failure, resetting its state.
	StrategyRestart SupervisorStrategy = iota
	// StrategyStop stops the actor permanently on failure.
	StrategyStop
	// StrategyResume resumes the actor, keeping its state.
	StrategyResume
)

// SuperviseBuilder provides a fluent API for configuring supervision.
type SuperviseBuilder[T any] struct {
	behavior Behavior[T]
}

// Supervise wraps a behavior with supervision configuration.
// Use the returned [SuperviseBuilder] to specify the failure strategy:
//
//	supervised := typed.Supervise(myBehavior).OnFailure(typed.StrategyRestart)
func Supervise[T any](behavior Behavior[T]) SuperviseBuilder[T] {
	return SuperviseBuilder[T]{behavior: behavior}
}

// OnFailure sets the strategy to apply when the wrapped behavior panics.
func (sb SuperviseBuilder[T]) OnFailure(strategy SupervisorStrategy) Behavior[T] {
	inner := sb.behavior
	return supervisedBehavior(inner, strategy)
}

// OnFailureWithBackoff configures backoff-based restart supervision.
func (sb SuperviseBuilder[T]) OnFailureWithBackoff(opts BackoffOptions) Behavior[T] {
	inner := sb.behavior
	return supervisedBackoffBehavior(inner, opts)
}

func supervisedBehavior[T any](inner Behavior[T], strategy SupervisorStrategy) Behavior[T] {
	return func(ctx TypedContext[T], msg T) Behavior[T] {
		next, panicked := safeInvoke(ctx, msg, inner)
		if !panicked {
			if next == nil {
				return nil // Same — keep supervised wrapper
			}
			if isStopped(next) {
				return Stopped[T]()
			}
			return supervisedBehavior(next, strategy)
		}

		// Handle panic according to strategy
		switch strategy {
		case StrategyStop:
			return Stopped[T]()
		case StrategyResume:
			// Keep current behavior, swallow panic
			return nil
		default: // StrategyRestart
			return supervisedBehavior(inner, strategy)
		}
	}
}

func supervisedBackoffBehavior[T any](inner Behavior[T], opts BackoffOptions) Behavior[T] {
	failures := 0
	var lastFailure time.Time
	current := inner

	return func(ctx TypedContext[T], msg T) Behavior[T] {
		next, panicked := safeInvoke(ctx, msg, current)
		if !panicked {
			if next == nil {
				return nil
			}
			if isStopped(next) {
				return Stopped[T]()
			}
			current = next
			return nil
		}

		// Reset failure count if enough time has passed
		now := time.Now()
		if !lastFailure.IsZero() && opts.ResetInterval > 0 && now.Sub(lastFailure) > opts.ResetInterval {
			failures = 0
		}

		delay := opts.NextDelay(failures)
		failures++
		lastFailure = now

		time.Sleep(delay)
		current = inner
		return nil
	}
}

// safeInvoke calls behavior, recovering from panics.
func safeInvoke[T any](ctx TypedContext[T], msg T, b Behavior[T]) (next Behavior[T], panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			log := slog.Default()
			if ctx != nil {
				func() {
					defer func() { recover() }()
					log = ctx.Log()
				}()
			}
			log.Error(fmt.Sprintf("actor panicked: %v", r))
			panicked = true
		}
	}()
	return b(ctx, msg), false
}
