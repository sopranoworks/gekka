/*
 * buffer_pool_wiring_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestStreamBufferPool_Independence asserts the wiring contract that streams
// 1 (control) and 2 (ordinary) share the regular pool while stream 3 (large)
// gets its own. A single NodeManager must hand out exactly two distinct
// pool instances regardless of how many times each stream is queried, and
// the two pools must use the per-stream maximum-frame-size as their
// per-buffer length.
func TestStreamBufferPool_Independence(t *testing.T) {
	nm := &NodeManager{
		BufferPoolSize:        4,
		LargeBufferPoolSize:   2,
		MaxFrameSize:          128 * 1024,
		MaximumLargeFrameSize: 1 * 1024 * 1024,
	}

	control := nm.streamBufferPool(AeronStreamControl)
	ordinary := nm.streamBufferPool(AeronStreamOrdinary)
	large := nm.streamBufferPool(AeronStreamLarge)

	if control == nil || ordinary == nil || large == nil {
		t.Fatalf("streamBufferPool returned nil for one or more streams")
	}
	if control != ordinary {
		t.Errorf("control and ordinary streams must share the regular pool; got %p vs %p", control, ordinary)
	}
	if control == large {
		t.Errorf("regular and large pools must be distinct instances")
	}

	if got, want := control.Capacity(), 4; got != want {
		t.Errorf("regular pool capacity = %d want %d", got, want)
	}
	if got, want := control.BufferSize(), 128*1024; got != want {
		t.Errorf("regular pool bufferSize = %d want %d", got, want)
	}
	if got, want := large.Capacity(), 2; got != want {
		t.Errorf("large pool capacity = %d want %d", got, want)
	}
	if got, want := large.BufferSize(), 1*1024*1024; got != want {
		t.Errorf("large pool bufferSize = %d want %d", got, want)
	}

	// Re-querying must return the same pool instance (lazy-init memoised).
	if again := nm.streamBufferPool(AeronStreamOrdinary); again != ordinary {
		t.Errorf("streamBufferPool(ordinary) returned a fresh pool on the second call")
	}
	if again := nm.streamBufferPool(AeronStreamLarge); again != large {
		t.Errorf("streamBufferPool(large) returned a fresh pool on the second call")
	}
}

// TestStreamBufferPool_DefaultsWhenUnconfigured covers the zero-value path:
// when NodeManager is built without explicit config the pool sizes fall
// back to the Pekko defaults (DefaultBufferPoolSize / DefaultLargeBufferPoolSize)
// and the per-buffer size falls back to DefaultMaxFrameSize /
// DefaultMaximumLargeFrameSize.
func TestStreamBufferPool_DefaultsWhenUnconfigured(t *testing.T) {
	nm := &NodeManager{}

	regular := nm.streamBufferPool(AeronStreamOrdinary)
	large := nm.streamBufferPool(AeronStreamLarge)

	if got, want := regular.Capacity(), DefaultBufferPoolSize; got != want {
		t.Errorf("regular pool default capacity = %d want %d", got, want)
	}
	if got, want := regular.BufferSize(), DefaultMaxFrameSize; got != want {
		t.Errorf("regular pool default bufferSize = %d want %d", got, want)
	}
	if got, want := large.Capacity(), DefaultLargeBufferPoolSize; got != want {
		t.Errorf("large pool default capacity = %d want %d", got, want)
	}
	if got, want := large.BufferSize(), DefaultMaximumLargeFrameSize; got != want {
		t.Errorf("large pool default bufferSize = %d want %d", got, want)
	}
}

// TestTcpArteryReadLoop_PoolRoundtrip drives a real Artery frame through
// tcpArteryReadLoop with a pool attached and confirms the post-loop
// invariants: AllocCount bumped exactly once (the loop's initial Get),
// pool depth back to 1 after the loop exits (the buffer was returned),
// and the dispatched payload reached the handler intact.
func TestTcpArteryReadLoop_PoolRoundtrip(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pool := NewBufferPool(2, 64*1024)
	got := make(chan int, 1)
	handler := func(ctx context.Context, meta *ArteryMetadata) error {
		got <- len(meta.Payload)
		return nil
	}

	done := make(chan struct{})
	go func() {
		_ = tcpArteryReadLoop(ctx, server, handler, nil, 0, AeronStreamOrdinary, 64*1024, pool)
		close(done)
	}()

	frame, err := BuildArteryFrame(7, 4, "", "/user/x", "B", []byte("hello"), false)
	if err != nil {
		t.Fatalf("BuildArteryFrame: %v", err)
	}
	if err := WriteFrame(client, frame); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	select {
	case n := <-got:
		if n != len("hello") {
			t.Errorf("dispatched payload size = %d want %d", n, len("hello"))
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("read loop did not dispatch the frame")
	}

	// Allocate-then-return: the pool was empty on entry so Get allocated
	// fresh once. The loop must Put it back when it exits.
	if got := pool.AllocCount(); got != 1 {
		t.Errorf("AllocCount = %d want 1 (single fresh alloc on cold pool)", got)
	}

	// Trigger loop exit.
	client.Close()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatalf("read loop did not exit after client close")
	}

	if got := pool.Depth(); got != 1 {
		t.Errorf("pool Depth after loop exit = %d want 1 (buffer must be returned)", got)
	}
}

// TestTcpArteryReadLoop_PoolReuseAcrossLoops is the steady-load regression
// guard: after warming up the pool, running additional read loops must
// hit the cache rather than allocate fresh — which is the entire reason
// the pool exists. AllocCount must equal the number of buffers actually
// resident in the pool at warmup, and stay there across subsequent loops.
func TestTcpArteryReadLoop_PoolReuseAcrossLoops(t *testing.T) {
	const iterations = 8
	pool := NewBufferPool(1, 64*1024) // tight pool so every iteration reuses

	for i := 0; i < iterations; i++ {
		runOneFrameRoundtrip(t, pool)
	}

	// First loop allocates fresh (cold pool); every subsequent loop must
	// hit the cache. AllocCount therefore must equal exactly 1 even after
	// `iterations` cycles. Anything higher means the pool is leaking and
	// the receive path is back to per-frame allocations — which is the
	// regression Phase 3 exists to prevent.
	if got := pool.AllocCount(); got != 1 {
		t.Errorf("AllocCount after %d loops = %d want 1 (cache-hit dominated)", iterations, got)
	}
	if got := pool.Depth(); got != 1 {
		t.Errorf("pool Depth at rest = %d want 1", got)
	}
}

// TestTcpArteryReadLoop_LargePoolUnaffectedByRegularLoad covers the
// independence requirement: stressing the regular-stream pool must not
// drain the large-stream pool, and vice versa. Pekko keeps the two pools
// fully separate so a burst of large frames cannot starve the ordinary
// path of buffers.
func TestTcpArteryReadLoop_LargePoolUnaffectedByRegularLoad(t *testing.T) {
	regular := NewBufferPool(2, 64*1024)
	large := NewBufferPool(2, 1*1024*1024)

	// Hammer the regular pool with several roundtrips.
	for i := 0; i < 5; i++ {
		runOneFrameRoundtrip(t, regular)
	}

	if regular.AllocCount() < 1 {
		t.Fatalf("regular pool AllocCount = 0 (expected >= 1 from warmup)")
	}
	if got := large.AllocCount(); got != 0 {
		t.Errorf("large pool AllocCount = %d want 0 (must be untouched by regular-stream traffic)", got)
	}
	if got := large.Depth(); got != 0 {
		t.Errorf("large pool Depth = %d want 0 (no traffic on this pool yet)", got)
	}
}

// runOneFrameRoundtrip spins up a tcpArteryReadLoop on one side of a pipe,
// pushes a single frame from the other side, waits for dispatch, then
// closes the client to drive the loop to exit. The pool's lifecycle
// hooks (Get on entry, Put on exit) are exercised end-to-end.
func runOneFrameRoundtrip(t *testing.T, pool *BufferPool) {
	t.Helper()
	server, client := net.Pipe()
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	dispatched := make(chan struct{}, 1)
	handler := func(ctx context.Context, meta *ArteryMetadata) error {
		dispatched <- struct{}{}
		return nil
	}

	done := make(chan struct{})
	go func() {
		_ = tcpArteryReadLoop(ctx, server, handler, nil, 0, AeronStreamOrdinary, int32(pool.BufferSize()), pool)
		close(done)
	}()

	frame, err := BuildArteryFrame(7, 4, "", "/user/x", "B", []byte("ping"), false)
	if err != nil {
		t.Fatalf("BuildArteryFrame: %v", err)
	}
	if err := WriteFrame(client, frame); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	select {
	case <-dispatched:
	case <-time.After(1 * time.Second):
		t.Fatalf("read loop did not dispatch the frame")
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatalf("read loop did not exit after client close")
	}
}

// TestTcpArteryReadLoop_NilPoolFallback documents the legacy-callers
// guarantee: passing nil as the pool keeps the read loop running with a
// per-call scratch buffer, identical to the pre-Phase-3 behaviour. This
// is what the public TcpArteryHandlerWithCallback wrapper does for every
// existing test that has not been migrated to a pool yet.
func TestTcpArteryReadLoop_NilPoolFallback(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	got := make(chan int, 1)
	handler := func(ctx context.Context, meta *ArteryMetadata) error {
		got <- len(meta.Payload)
		return nil
	}

	go func() {
		_ = tcpArteryReadLoop(ctx, server, handler, nil, 0, AeronStreamOrdinary, 64*1024, nil)
	}()

	// Drive a frame through and assert the loop processed it as expected.
	frame, err := BuildArteryFrame(7, 4, "", "/user/x", "B", []byte("legacy"), false)
	if err != nil {
		t.Fatalf("BuildArteryFrame: %v", err)
	}
	if err := WriteFrame(client, frame); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	select {
	case n := <-got:
		if n != len("legacy") {
			t.Errorf("dispatched payload size = %d want %d", n, len("legacy"))
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("nil-pool read loop did not dispatch the frame")
	}
}
