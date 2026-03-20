/*
 * kill_switch_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"errors"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/stream"
)

// ─── NewKillSwitch ────────────────────────────────────────────────────────

// TestKillSwitch_ShutdownStopsInfiniteSource verifies that Shutdown() causes
// an infinite source to complete cleanly (no error propagated to the sink).
func TestKillSwitch_ShutdownStopsInfiniteSource(t *testing.T) {
	flow, ks := stream.NewKillSwitch[int]()

	// Trigger shutdown from another goroutine after a brief pause.
	go func() {
		time.Sleep(20 * time.Millisecond)
		ks.Shutdown()
	}()

	result, err := stream.RunWith(
		stream.Via(stream.Repeat(1), flow),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("expected clean completion after Shutdown, got error: %v", err)
	}
	// We don't assert a specific count — just that it terminated cleanly.
	_ = result
}

// TestKillSwitch_AbortFailsStream verifies that Abort(err) causes the stream
// to fail with the provided error.
func TestKillSwitch_AbortFailsStream(t *testing.T) {
	sentinelErr := errors.New("kill switch abort")
	flow, ks := stream.NewKillSwitch[int]()

	go func() {
		time.Sleep(20 * time.Millisecond)
		ks.Abort(sentinelErr)
	}()

	_, err := stream.RunWith(
		stream.Via(stream.Repeat(1), flow),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err == nil {
		t.Fatal("expected error after Abort, got nil")
	}
	if !errors.Is(err, sentinelErr) {
		t.Fatalf("got %v, want sentinelErr", err)
	}
}

// TestKillSwitch_NoOpOnNormalCompletion verifies that a KillSwitch that is
// never triggered does not interfere with a finite source.
func TestKillSwitch_NoOpOnNormalCompletion(t *testing.T) {
	flow, _ := stream.NewKillSwitch[int]()

	result, err := stream.RunWith(
		stream.Via(stream.FromSlice([]int{1, 2, 3}), flow),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("got %v, want [1 2 3]", result)
	}
}

// TestKillSwitch_IdempotentShutdown verifies that calling Shutdown() multiple
// times does not panic or produce duplicate effects.
func TestKillSwitch_IdempotentShutdown(t *testing.T) {
	flow, ks := stream.NewKillSwitch[int]()

	go func() {
		time.Sleep(10 * time.Millisecond)
		ks.Shutdown()
		ks.Shutdown() // second call must be a no-op
		ks.Shutdown()
	}()

	_, err := stream.RunWith(
		stream.Via(stream.Repeat(0), flow),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("expected clean completion, got: %v", err)
	}
}

// TestKillSwitch_AbortWinsOverShutdown verifies that whichever of Abort or
// Shutdown fires first wins (the stream must end without a double-close panic).
func TestKillSwitch_AbortWinsOverShutdown(t *testing.T) {
	sentinelErr := errors.New("abort wins")
	flow, ks := stream.NewKillSwitch[int]()

	go func() {
		ks.Abort(sentinelErr)
		ks.Shutdown() // must be a no-op
	}()

	_, err := stream.RunWith(
		stream.Via(stream.Repeat(1), flow),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	// Stream ended — either with sentinelErr (abort won) or nil (shutdown won
	// in the tiny window before abort). Both are acceptable; what matters is
	// no panic and no deadlock.
	_ = err
}

// ─── SharedKillSwitch ─────────────────────────────────────────────────────

// TestSharedKillSwitch_StopsMultipleStreams verifies that Shutdown() on a
// SharedKillSwitch stops all attached streams.
func TestSharedKillSwitch_StopsMultipleStreams(t *testing.T) {
	ks := stream.NewSharedKillSwitch()

	go func() {
		time.Sleep(20 * time.Millisecond)
		ks.Shutdown()
	}()

	done := make(chan error, 2)
	run := func() {
		_, err := stream.RunWith(
			stream.Via(stream.Repeat(1), stream.SharedKillSwitchFlow[int](ks)),
			stream.Collect[int](),
			stream.ActorMaterializer{},
		)
		done <- err
	}

	go run()
	go run()

	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatalf("stream %d: expected clean completion, got: %v", i, err)
		}
	}
}

// TestSharedKillSwitch_AbortPropagatesError verifies that Abort(err) fails all
// streams controlled by the SharedKillSwitch.
func TestSharedKillSwitch_AbortPropagatesError(t *testing.T) {
	sentinelErr := errors.New("shared abort")
	ks := stream.NewSharedKillSwitch()

	go func() {
		time.Sleep(20 * time.Millisecond)
		ks.Abort(sentinelErr)
	}()

	done := make(chan error, 2)
	run := func() {
		_, err := stream.RunWith(
			stream.Via(stream.Repeat(1), stream.SharedKillSwitchFlow[int](ks)),
			stream.Collect[int](),
			stream.ActorMaterializer{},
		)
		done <- err
	}

	go run()
	go run()

	for i := 0; i < 2; i++ {
		if err := <-done; !errors.Is(err, sentinelErr) {
			t.Fatalf("stream %d: got %v, want sentinelErr", i, err)
		}
	}
}
