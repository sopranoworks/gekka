/*
 * interceptor.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"context"
	"fmt"
	"log/slog"
)

// BehaviorInterceptor is a hook that wraps a Behavior[T], allowing pre- and
// post-message logic to be inserted.  It mirrors Pekko's BehaviorInterceptor.
//
// AroundReceive is called instead of forwarding msg directly to target.
// The implementation MUST call target(ctx, msg) to continue message processing,
// or may choose to swallow the message and return the current behavior.
// It may return a different Behavior[T].
type BehaviorInterceptor[T any] interface {
	AroundReceive(ctx TypedContext[T], msg T, target Behavior[T]) Behavior[T]
}

// Intercept wraps behavior with interceptor.  Every message delivered to the
// resulting behavior is first handed to interceptor.AroundReceive; the
// interceptor decides whether (and how) to forward the message to the inner
// behavior.
//
// If the inner behavior transitions to a new behavior, the interceptor layer
// is re-applied so that subsequent messages are still intercepted.
func Intercept[T any](interceptor BehaviorInterceptor[T], behavior Behavior[T]) Behavior[T] {
	return interceptBehavior(interceptor, behavior)
}

// interceptBehavior returns a Behavior[T] that wraps inner with interceptor.
func interceptBehavior[T any](interceptor BehaviorInterceptor[T], inner Behavior[T]) Behavior[T] {
	return func(ctx TypedContext[T], msg T) Behavior[T] {
		next := interceptor.AroundReceive(ctx, msg, inner)
		if next == nil {
			// Same — keep current interceptor-wrapped inner.
			return nil
		}
		if isStopped(next) {
			return next
		}
		// Re-wrap the new behavior so future messages are still intercepted.
		return interceptBehavior(interceptor, next)
	}
}

// ── Built-in interceptors ────────────────────────────────────────────────────

// logMessagesInterceptor logs each received message at the configured level.
type logMessagesInterceptor[T any] struct {
	level slog.Level
}

func (l *logMessagesInterceptor[T]) AroundReceive(ctx TypedContext[T], msg T, target Behavior[T]) Behavior[T] {
	ctx.Log().Log(context.Background(), l.level,
		fmt.Sprintf("actor received message: %T", msg),
		"message", fmt.Sprintf("%+v", msg),
	)
	return target(ctx, msg)
}

// LogMessages wraps behavior with a built-in interceptor that logs every
// received message at the given slog level.
func LogMessages[T any](level slog.Level, behavior Behavior[T]) Behavior[T] {
	return Intercept[T](&logMessagesInterceptor[T]{level: level}, behavior)
}
