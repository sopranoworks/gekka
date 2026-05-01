/*
 * dial_remote_restart_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// closedListenerTarget returns a target Address pointing at a port that
// briefly held a listener and was then closed — connect attempts return
// ECONNREFUSED almost immediately, exercising the dial-failure path
// without depending on a slow network timeout.
func closedListenerTarget(t *testing.T) (host string, port uint32) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	hostStr, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	p, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse port %q: %v", portStr, err)
	}
	return hostStr, uint32(p)
}

// TestDialRemoteWithRestart_RetriesUpToCapAndStops verifies that the new
// production wrapper exhausts OutboundMaxRestarts retries before giving
// up, separating attempts by OutboundRestartBackoff. With cap=2,
// TryRecordOutboundRestart returns true on the 1st and 2nd record and
// false on the 3rd (count > max), so DialRemoteWithRestart performs
// exactly 3 dial attempts before returning "outbound restart cap
// exceeded".
func TestDialRemoteWithRestart_RetriesUpToCapAndStops(t *testing.T) {
	host, port := closedListenerTarget(t)

	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "Local"), 1)
	nm.TcpConnectionTimeout = 50 * time.Millisecond
	nm.OutboundMaxRestarts = 2
	nm.OutboundRestartBackoff = 10 * time.Millisecond
	nm.OutboundRestartTimeout = 5 * time.Second

	target := lifecycleTestAddr(host, port, "Remote")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	assoc, err := nm.DialRemoteWithRestart(ctx, target)
	elapsed := time.Since(start)

	if assoc != nil {
		t.Fatalf("DialRemoteWithRestart: expected nil association, got %v", assoc)
	}
	if err == nil {
		t.Fatal("DialRemoteWithRestart: expected non-nil error, got nil")
	}
	if !strings.Contains(err.Error(), "outbound restart cap exceeded") {
		t.Errorf("DialRemoteWithRestart err = %v; want substring %q",
			err, "outbound restart cap exceeded")
	}

	// 3 dial attempts × ~50ms timeout (often faster on connect-refused) +
	// 2 backoffs × 10ms ≈ 70-200ms. Allow generous slack for CI machines
	// but reject anything close to a no-retry single attempt.
	if elapsed > 5*time.Second {
		t.Errorf("DialRemoteWithRestart elapsed = %v; want < 5s", elapsed)
	}

	nm.restartMu.Lock()
	got := len(nm.outboundRestarts)
	nm.restartMu.Unlock()
	// 3 records: each TryRecord call appends, regardless of return value.
	if got != 3 {
		t.Errorf("len(nm.outboundRestarts) = %d; want 3 (1 per attempt)", got)
	}
}

// TestDialRemoteWithRestart_TightCapStopsAfterTwo verifies that
// OutboundMaxRestarts = 1 produces exactly 2 dial attempts: the 1st
// failure records 1 (1 <= 1, retry), the 2nd records 2 (2 > 1,
// cap-exceeded). Establishes the boundary case for the smallest
// non-default cap.
func TestDialRemoteWithRestart_TightCapStopsAfterTwo(t *testing.T) {
	host, port := closedListenerTarget(t)

	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "Local"), 1)
	nm.TcpConnectionTimeout = 50 * time.Millisecond
	nm.OutboundMaxRestarts = 1
	nm.OutboundRestartBackoff = 5 * time.Millisecond
	nm.OutboundRestartTimeout = 5 * time.Second

	target := lifecycleTestAddr(host, port, "Remote")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := nm.DialRemoteWithRestart(ctx, target)
	if err == nil {
		t.Fatal("expected non-nil error for unreachable target")
	}
	if !strings.Contains(err.Error(), "outbound restart cap exceeded") {
		t.Errorf("err = %v; want cap-exceeded", err)
	}

	nm.restartMu.Lock()
	got := len(nm.outboundRestarts)
	nm.restartMu.Unlock()
	// Cap=1: 1st record returns true (1 <= 1) → retry. 2nd record
	// returns false (2 > 1) → return. So 2 dial attempts total, 2 records.
	if got != 2 {
		t.Errorf("len(nm.outboundRestarts) = %d; want 2", got)
	}
}

// TestDialRemoteWithRestart_RespectsContextCancel verifies that a
// context cancelled during the backoff sleep aborts the loop with a
// wrapped ctx.Err() rather than continuing to spin attempts.
func TestDialRemoteWithRestart_RespectsContextCancel(t *testing.T) {
	host, port := closedListenerTarget(t)

	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "Local"), 1)
	nm.TcpConnectionTimeout = 50 * time.Millisecond
	nm.OutboundMaxRestarts = 100
	nm.OutboundRestartBackoff = 200 * time.Millisecond
	nm.OutboundRestartTimeout = 30 * time.Second

	target := lifecycleTestAddr(host, port, "Remote")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := nm.DialRemoteWithRestart(ctx, target)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected non-nil error after ctx cancel")
	}
	// Either "cancelled during backoff" (we hit the backoff sleep) or
	// "cancelled after N attempts" (ctx fired between dial returns) is
	// acceptable — both prove the loop stopped honoring ctx.
	if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "cancelled") {
		t.Errorf("err = %v; want ctx-cancelled wrapping", err)
	}
	// The 100ms ctx must not allow 100 retries × 200ms each.
	if elapsed > 2*time.Second {
		t.Errorf("DialRemoteWithRestart elapsed = %v under cancelled ctx; want < 2s", elapsed)
	}
}
