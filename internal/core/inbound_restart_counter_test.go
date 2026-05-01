/*
 * inbound_restart_counter_test.go
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

// feedGarbageHandshake writes 5 non-Artery bytes to the client side of a
// net.Pipe so that ProcessConnection's INBOUND magic check fails with
// "invalid artery magic" — the canonical inbound stream-failure error.
func feedGarbageHandshake(t *testing.T) (handlerSide net.Conn) {
	t.Helper()
	client, server := net.Pipe()
	go func() {
		// "XYZAB" is neither "AKKA"+stream nor "ART"+ver+stream — magic
		// check rejects on the first 3 bytes.
		_, _ = client.Write([]byte{'X', 'Y', 'Z', 'A', 'B'})
		// Hold the conn open so the handler returns the magic error
		// rather than a read EOF.
		time.Sleep(50 * time.Millisecond)
		_ = client.Close()
	}()
	return server
}

// TestInboundRestartCounter_IncrementsOnHandlerError verifies that every
// non-nil error return from ProcessConnection (driven via the
// TcpArteryHandlerWithNodeManager wrapper) increments the inbound
// restart counter. Each garbage-bytes connection is one "restart".
func TestInboundRestartCounter_IncrementsOnHandlerError(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "Local"), 1)
	nm.InboundMaxRestarts = 100
	nm.InboundRestartTimeout = 10 * time.Second

	handler := TcpArteryHandlerWithNodeManager(nm)

	const garbageConns = 3
	for i := 0; i < garbageConns; i++ {
		conn := feedGarbageHandshake(t)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		err := handler(ctx, conn)
		cancel()
		_ = conn.Close()
		if err == nil {
			t.Fatalf("attempt %d: expected magic-check error, got nil", i)
		}
	}

	nm.restartMu.Lock()
	got := len(nm.inboundRestarts)
	nm.restartMu.Unlock()
	if got != garbageConns {
		t.Errorf("len(nm.inboundRestarts) = %d; want %d", got, garbageConns)
	}
}

// TestInboundRestartCounter_CapExceededEmitsFlightEvent verifies the
// surface chosen for cap-exceeded inbound restarts: a flight-recorder
// event under CatInboundRestartExceeded plus an slog.Warn. The
// listener intentionally keeps accepting connections — gekka is more
// conservative than Pekko, which terminates the ActorSystem.
func TestInboundRestartCounter_CapExceededEmitsFlightEvent(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "Local"), 1)
	nm.InboundMaxRestarts = 2
	nm.InboundRestartTimeout = 10 * time.Second
	nm.FlightRec = NewFlightRecorder(true, LevelFull)

	handler := TcpArteryHandlerWithNodeManager(nm)

	// Send 4 garbage handshakes; the 3rd and 4th exceed the cap of 2.
	for i := 0; i < 4; i++ {
		conn := feedGarbageHandshake(t)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		err := handler(ctx, conn)
		cancel()
		_ = conn.Close()
		if err == nil {
			t.Fatalf("attempt %d: expected magic-check error, got nil", i)
		}
	}

	// The wrapper records every error; the cap fires on records 3 and 4.
	nm.restartMu.Lock()
	got := len(nm.inboundRestarts)
	nm.restartMu.Unlock()
	if got != 4 {
		t.Errorf("len(nm.inboundRestarts) = %d; want 4 (one per attempt)", got)
	}

	allEvents := nm.FlightRec.SnapshotAll()
	exceededCount := 0
	for _, evs := range allEvents {
		for _, ev := range evs {
			if ev.Category == CatInboundRestartExceeded {
				exceededCount++
			}
		}
	}
	// Records 3 and 4 each exceed the cap → at least 2 flight events.
	if exceededCount < 2 {
		t.Errorf("CatInboundRestartExceeded flight events = %d; want >= 2", exceededCount)
	}
}
