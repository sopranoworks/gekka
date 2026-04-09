/*
 * receive_partial_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReceivePartial_HandledMessage(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	handled := false
	behavior := ReceivePartial[any](func(ctx TypedContext[any], msg any) (Behavior[any], bool) {
		if _, ok := msg.(int); ok {
			handled = true
			return Same[any](), true
		}
		return nil, false
	})

	ctx := &behaviorTestCtx[any]{logger: logger}

	// int should be handled
	next := behavior(ctx, 42)
	assert.True(t, handled, "int message should be handled")
	assert.Nil(t, next, "handled message should return Same (nil)")
	assert.NotContains(t, buf.String(), "unhandled")
}

func TestReceivePartial_UnhandledMessage(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	behavior := ReceivePartial[any](func(ctx TypedContext[any], msg any) (Behavior[any], bool) {
		if _, ok := msg.(int); ok {
			return Same[any](), true
		}
		return nil, false
	})

	ctx := &behaviorTestCtx[any]{logger: logger}

	// string should be unhandled
	next := behavior(ctx, "hello")
	assert.Nil(t, next, "unhandled should return Same (nil)")
	assert.Contains(t, buf.String(), "unhandled message in receivePartial")
}

func TestReceivePartial_BehaviorTransition(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Handler that transitions to Stopped on negative int
	behavior := ReceivePartial[any](func(ctx TypedContext[any], msg any) (Behavior[any], bool) {
		if n, ok := msg.(int); ok {
			if n < 0 {
				return Stopped[any](), true
			}
			return Same[any](), true
		}
		return nil, false
	})

	ctx := &behaviorTestCtx[any]{logger: logger}

	// Positive int → Same
	next := behavior(ctx, 1)
	assert.Nil(t, next, "positive int should return Same")

	// Negative int → Stopped
	next = behavior(ctx, -1)
	assert.NotNil(t, next, "negative int should return Stopped (non-nil)")
	assert.True(t, isStopped(next), "should be Stopped behavior")
}
