/*
 * flush_production_wiring_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"context"
	"testing"
	"time"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// flushTestAssoc registers a buffered OUTBOUND ASSOCIATED association on nm
// with the given UID and outbox capacity, returning the registered association
// for further inspection.
func flushTestAssoc(t *testing.T, nm *NodeManager, uid uint64, outboxCap int) *GekkaAssociation {
	t.Helper()
	remote := &gproto_remote.UniqueAddress{
		Address: lifecycleTestAddr("10.0.0.1", 2551, "Remote"),
		Uid:     proto.Uint64(uid),
	}
	assoc := &GekkaAssociation{
		state:    ASSOCIATED,
		role:     OUTBOUND,
		nodeMgr:  nm,
		localUid: nm.localUid,
		outbox:   make(chan []byte, outboxCap),
		remote:   remote,
		streamId: 1,
		lastSeen: time.Now(),
	}
	nm.RegisterAssociation(remote, assoc)
	return assoc
}

// TestWaitForOutboundFlush_DrainsBeforeTimeout verifies that the function
// returns true as soon as the outbox empties, well before the timeout.
func TestWaitForOutboundFlush_DrainsBeforeTimeout(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)
	assoc := flushTestAssoc(t, nm, 1001, 1)
	assoc.outbox <- []byte("frame")

	// Drain the outbox after 30ms.
	go func() {
		time.Sleep(30 * time.Millisecond)
		<-assoc.outbox
	}()

	start := time.Now()
	ok := WaitForOutboundFlush(context.Background(), nm, 500*time.Millisecond)
	elapsed := time.Since(start)
	if !ok {
		t.Fatalf("WaitForOutboundFlush returned false; want true (drained)")
	}
	if elapsed >= 500*time.Millisecond {
		t.Errorf("elapsed = %v; want < 500ms", elapsed)
	}
	if elapsed < 25*time.Millisecond {
		t.Errorf("elapsed = %v; want >= 25ms (drain delay)", elapsed)
	}
}

// TestWaitForOutboundFlush_TimesOutWhenStuck verifies the function returns
// false near the timeout when the outbox never drains.
func TestWaitForOutboundFlush_TimesOutWhenStuck(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)
	assoc := flushTestAssoc(t, nm, 1002, 1)
	assoc.outbox <- []byte("stuck")

	start := time.Now()
	ok := WaitForOutboundFlush(context.Background(), nm, 80*time.Millisecond)
	elapsed := time.Since(start)
	if ok {
		t.Fatalf("WaitForOutboundFlush returned true; want false (timed out)")
	}
	if elapsed < 80*time.Millisecond {
		t.Errorf("elapsed = %v; want >= 80ms", elapsed)
	}
	if elapsed > 300*time.Millisecond {
		t.Errorf("elapsed = %v; want <= 300ms", elapsed)
	}
}

// TestWaitForOutboundFlush_ReturnsImmediatelyOnEmpty verifies the empty-state
// fast path: an already-empty registry returns true with no polling.
func TestWaitForOutboundFlush_ReturnsImmediatelyOnEmpty(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)
	flushTestAssoc(t, nm, 1003, 1)

	start := time.Now()
	ok := WaitForOutboundFlush(context.Background(), nm, time.Hour)
	elapsed := time.Since(start)
	if !ok {
		t.Fatalf("WaitForOutboundFlush returned false on empty outbox; want true")
	}
	if elapsed >= 50*time.Millisecond {
		t.Errorf("elapsed = %v; want < 50ms (fast-path empty check)", elapsed)
	}
}

// TestWaitForOutboundFlush_ContextCancellationShortCircuits verifies that
// cancelling ctx returns false quickly even when timeout is far in the
// future.
func TestWaitForOutboundFlush_ContextCancellationShortCircuits(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)
	assoc := flushTestAssoc(t, nm, 1004, 1)
	assoc.outbox <- []byte("stuck")

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	ok := WaitForOutboundFlush(ctx, nm, 5*time.Second)
	elapsed := time.Since(start)
	if ok {
		t.Fatalf("WaitForOutboundFlush returned true after cancel; want false")
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("elapsed = %v; want <= 200ms (cancellation short-circuit)", elapsed)
	}
}

// TestWaitForOutboundFlush_TimeoutZeroPollsOnce verifies the timeout<=0 path:
// returns the current state without polling.
func TestWaitForOutboundFlush_TimeoutZeroPollsOnce(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)
	assoc := flushTestAssoc(t, nm, 1005, 1)
	assoc.outbox <- []byte("frame")

	if ok := WaitForOutboundFlush(context.Background(), nm, 0); ok {
		t.Errorf("timeout=0 with non-empty outbox should return false")
	}

	<-assoc.outbox
	if ok := WaitForOutboundFlush(context.Background(), nm, 0); !ok {
		t.Errorf("timeout=0 with empty outbox should return true")
	}
}

// TestDeathWatchNotificationFlushDelay_RunsToCompletion verifies the function
// blocks for at least the requested timeout when ctx stays live.
func TestDeathWatchNotificationFlushDelay_RunsToCompletion(t *testing.T) {
	start := time.Now()
	ok := DeathWatchNotificationFlushDelay(context.Background(), 80*time.Millisecond)
	elapsed := time.Since(start)
	if !ok {
		t.Fatalf("DeathWatchNotificationFlushDelay returned false; want true (full delay)")
	}
	if elapsed < 80*time.Millisecond {
		t.Errorf("elapsed = %v; want >= 80ms", elapsed)
	}
	if elapsed > 300*time.Millisecond {
		t.Errorf("elapsed = %v; want <= 300ms", elapsed)
	}
}

// TestDeathWatchNotificationFlushDelay_ZeroTimeoutSkipsSleep verifies the
// zero-timeout fast path matches Pekko's "no delay" semantic.
func TestDeathWatchNotificationFlushDelay_ZeroTimeoutSkipsSleep(t *testing.T) {
	start := time.Now()
	ok := DeathWatchNotificationFlushDelay(context.Background(), 0)
	elapsed := time.Since(start)
	if !ok {
		t.Fatalf("DeathWatchNotificationFlushDelay(timeout=0) returned false; want true")
	}
	if elapsed >= 5*time.Millisecond {
		t.Errorf("elapsed = %v; want < 5ms (no-delay fast path)", elapsed)
	}
}

// TestDeathWatchNotificationFlushDelay_ContextCancellationShortCircuits
// verifies that cancelling ctx returns false quickly without consuming the
// full delay.
func TestDeathWatchNotificationFlushDelay_ContextCancellationShortCircuits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	ok := DeathWatchNotificationFlushDelay(ctx, 5*time.Second)
	elapsed := time.Since(start)
	if ok {
		t.Fatalf("DeathWatchNotificationFlushDelay returned true after cancel; want false")
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("elapsed = %v; want <= 200ms (cancellation short-circuit)", elapsed)
	}
}
