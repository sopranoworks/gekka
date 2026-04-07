/*
 * behaviors_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/sopranoworks/gekka/actor"

	"github.com/stretchr/testify/assert"
)

// ─── Empty ────────────────────────────────────────────────────────────────

func TestEmpty_LogsWarning(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	behavior := Empty[string]()
	ctx := &behaviorTestCtx[string]{logger: logger}
	next := behavior(ctx, "hello")

	assert.Nil(t, next, "Empty should return Same (nil)")
	assert.Contains(t, buf.String(), "empty behavior received message")
}

// ─── Ignore ───────────────────────────────────────────────────────────────

func TestIgnore_SilentlyDrops(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	behavior := Ignore[string]()
	ctx := &behaviorTestCtx[string]{logger: logger}
	next := behavior(ctx, "hello")

	assert.Nil(t, next, "Ignore should return Same (nil)")
	assert.Empty(t, buf.String(), "Ignore should produce no log output")
}

// ─── Unhandled ────────────────────────────────────────────────────────────

func TestUnhandled_LogsDebug(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	behavior := Unhandled[string]()
	ctx := &behaviorTestCtx[string]{logger: logger}
	next := behavior(ctx, "hello")

	assert.Nil(t, next, "Unhandled should return Same (nil)")
	assert.Contains(t, buf.String(), "unhandled message")
}

// ─── WithMdc ──────────────────────────────────────────────────────────────

func TestWithMdc_StaticKeys(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	inner := func(ctx TypedContext[string], msg string) Behavior[string] {
		ctx.Log().Info("processing", "msg", msg)
		return Same[string]()
	}

	wrapped := WithMdc[string](
		map[string]string{"actor-id": "test-123"},
		nil,
		inner,
	)

	ctx := newMdcTestCtx[string](logger)
	wrapped(ctx, "hello")

	output := buf.String()
	assert.Contains(t, output, "actor-id")
	assert.Contains(t, output, "test-123")
	assert.Contains(t, output, "processing")
}

func TestWithMdc_MessageSpecificKeys(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	inner := func(ctx TypedContext[string], msg string) Behavior[string] {
		ctx.Log().Info("got message")
		return Same[string]()
	}

	wrapped := WithMdc[string](
		nil,
		func(msg string) map[string]string {
			return map[string]string{"msg-type": strings.ToUpper(msg)}
		},
		inner,
	)

	ctx := newMdcTestCtx[string](logger)
	wrapped(ctx, "hello")

	output := buf.String()
	assert.Contains(t, output, "msg-type")
	assert.Contains(t, output, "HELLO")
}

// ─── test helpers ─────────────────────────────────────────────────────────

// behaviorTestCtx implements TypedContext for testing behavior functions.
type behaviorTestCtx[T any] struct {
	logger *slog.Logger
}

func (m *behaviorTestCtx[T]) Self() TypedActorRef[T]    { return TypedActorRef[T]{} }
func (m *behaviorTestCtx[T]) System() actor.ActorContext { return nil }
func (m *behaviorTestCtx[T]) Log() *slog.Logger         { return m.logger }
func (m *behaviorTestCtx[T]) Watch(target actor.Ref)    {}
func (m *behaviorTestCtx[T]) Unwatch(target actor.Ref)  {}
func (m *behaviorTestCtx[T]) Stop(target actor.Ref)     {}
func (m *behaviorTestCtx[T]) Passivate()                {}
func (m *behaviorTestCtx[T]) Timers() TimerScheduler[T] { return nil }
func (m *behaviorTestCtx[T]) Stash() StashBuffer[T]     { return nil }
func (m *behaviorTestCtx[T]) Sender() actor.Ref         { return nil }
func (m *behaviorTestCtx[T]) Ask(target actor.Ref, msgFactory func(replyTo actor.Ref) any, transform func(any, error) T) {
}
func (m *behaviorTestCtx[T]) Spawn(behavior any, name string) (actor.Ref, error) {
	return nil, nil
}
func (m *behaviorTestCtx[T]) SpawnAnonymous(behavior any) (actor.Ref, error) {
	return nil, nil
}
func (m *behaviorTestCtx[T]) SystemActorOf(behavior any, name string) (actor.Ref, error) {
	return nil, nil
}

// mdcTestCtx is a typedContext that supports MDC logger swapping for WithMdc tests.
type mdcTestCtx[T any] struct {
	typedContext[T]
	fallbackLogger *slog.Logger
}

func newMdcTestCtx[T any](logger *slog.Logger) *mdcTestCtx[T] {
	return &mdcTestCtx[T]{
		fallbackLogger: logger,
	}
}

func (m *mdcTestCtx[T]) Self() TypedActorRef[T]    { return TypedActorRef[T]{} }
func (m *mdcTestCtx[T]) System() actor.ActorContext { return nil }
func (m *mdcTestCtx[T]) Watch(target actor.Ref)    {}
func (m *mdcTestCtx[T]) Unwatch(target actor.Ref)  {}
func (m *mdcTestCtx[T]) Stop(target actor.Ref)     {}
func (m *mdcTestCtx[T]) Passivate()                {}
func (m *mdcTestCtx[T]) Timers() TimerScheduler[T] { return nil }
func (m *mdcTestCtx[T]) Stash() StashBuffer[T]     { return nil }
func (m *mdcTestCtx[T]) Sender() actor.Ref         { return nil }
func (m *mdcTestCtx[T]) Ask(target actor.Ref, msgFactory func(replyTo actor.Ref) any, transform func(any, error) T) {
}
func (m *mdcTestCtx[T]) Spawn(behavior any, name string) (actor.Ref, error) {
	return nil, nil
}
func (m *mdcTestCtx[T]) SpawnAnonymous(behavior any) (actor.Ref, error) {
	return nil, nil
}
func (m *mdcTestCtx[T]) SystemActorOf(behavior any, name string) (actor.Ref, error) {
	return nil, nil
}

func (m *mdcTestCtx[T]) setMdcLogger(l *slog.Logger) { m.mdcLogger = l }

func (m *mdcTestCtx[T]) Log() *slog.Logger {
	if m.mdcLogger != nil {
		return m.mdcLogger
	}
	return m.fallbackLogger
}
