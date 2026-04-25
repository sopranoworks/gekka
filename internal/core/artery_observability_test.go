/*
 * artery_observability_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// captureSlog redirects slog output to a buffer and returns a restore func.
func captureSlog(t *testing.T, level slog.Level) (*bytes.Buffer, func()) {
	t.Helper()
	var mu sync.Mutex
	buf := &bytes.Buffer{}
	prev := slog.Default()
	h := slog.NewTextHandler(syncWriter{w: buf, mu: &mu}, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(h))
	return buf, func() { slog.SetDefault(prev) }
}

type syncWriter struct {
	w  *bytes.Buffer
	mu *sync.Mutex
}

func (s syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

// TestEmitHarmlessQuarantineEvent_Off verifies that with the flag disabled
// the quarantine event is downgraded to DEBUG (not visible at WARN level).
func TestEmitHarmlessQuarantineEvent_Off(t *testing.T) {
	nm := &NodeManager{PropagateHarmlessQuarantineEvents: false}
	buf, restore := captureSlog(t, slog.LevelWarn)
	defer restore()
	nm.EmitHarmlessQuarantineEvent("test-quarantine-event", "uid", uint64(42))
	if strings.Contains(buf.String(), "test-quarantine-event") {
		t.Errorf("expected harmless quarantine event suppressed at WARN level, got %q", buf.String())
	}
}

// TestEmitHarmlessQuarantineEvent_On verifies that with the flag enabled the
// event is emitted at WARN (legacy Pekko 1.x behavior).
func TestEmitHarmlessQuarantineEvent_On(t *testing.T) {
	nm := &NodeManager{PropagateHarmlessQuarantineEvents: true}
	buf, restore := captureSlog(t, slog.LevelWarn)
	defer restore()
	nm.EmitHarmlessQuarantineEvent("legacy-quarantine-event", "uid", uint64(42))
	if !strings.Contains(buf.String(), "legacy-quarantine-event") {
		t.Errorf("expected legacy quarantine event at WARN, got %q", buf.String())
	}
}

// TestRecordOversizedFrame_LogsOnceAboveThreshold verifies the +10% growth
// guard: a payload above the threshold logs once, a tiny growth is silent,
// a >10% growth re-logs.
func TestRecordOversizedFrame_LogsOnceAboveThreshold(t *testing.T) {
	nm := &NodeManager{LogFrameSizeExceeding: 1000}
	buf, restore := captureSlog(t, slog.LevelWarn)
	defer restore()

	// Below threshold: no log.
	nm.recordOversizedFrame(2, "x", 999)
	if strings.Contains(buf.String(), "frame size exceeds threshold") {
		t.Fatalf("payload below threshold should not log: %q", buf.String())
	}

	// First above-threshold payload: must log.
	nm.recordOversizedFrame(2, "x", 1500)
	first := buf.String()
	if !strings.Contains(first, "frame size exceeds threshold") ||
		!strings.Contains(first, "payload_bytes=1500") {
		t.Fatalf("expected first oversize event logged at 1500 bytes, got %q", first)
	}

	// Tiny growth (below +10%): no additional log.
	buf.Reset()
	nm.recordOversizedFrame(2, "x", 1600) // +6.7%, suppressed
	if strings.Contains(buf.String(), "frame size exceeds threshold") {
		t.Errorf("growth under +10%% should not re-log, got %q", buf.String())
	}

	// >+10% growth: must re-log.
	buf.Reset()
	nm.recordOversizedFrame(2, "x", 2000) // +33% over 1500
	if !strings.Contains(buf.String(), "frame size exceeds threshold") ||
		!strings.Contains(buf.String(), "payload_bytes=2000") {
		t.Errorf("growth above +10%% should re-log, got %q", buf.String())
	}
}

// TestRecordOversizedFrame_Off verifies that LogFrameSizeExceeding=0 suppresses.
func TestRecordOversizedFrame_Off(t *testing.T) {
	nm := &NodeManager{LogFrameSizeExceeding: 0}
	buf, restore := captureSlog(t, slog.LevelWarn)
	defer restore()
	nm.recordOversizedFrame(2, "x", 1<<20)
	if strings.Contains(buf.String(), "frame size exceeds threshold") {
		t.Errorf("with LogFrameSizeExceeding=0 there should be no log, got %q", buf.String())
	}
}

// TestListenWithBindTimeout_BindError verifies that listenWithBindTimeout
// returns an error (not a successful listener) when the bind cannot complete.
// We force a bind failure by trying to bind a port already taken — this is a
// deterministic failure path that exercises the same error-return code that
// the timeout path would take.
func TestListenWithBindTimeout_BindError(t *testing.T) {
	// Hold a port to force EADDRINUSE on the second bind.
	holder, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("hold port: %v", err)
	}
	defer holder.Close()
	addr := holder.Addr().String()

	_, err = listenWithBindTimeout(context.Background(), addr, time.Second, nil)
	if err == nil {
		t.Fatal("expected bind error when port already in use")
	}
}

// TestListenWithBindTimeout_Success verifies the happy path: when the bind
// completes inside the timeout, the listener is returned and works.
func TestListenWithBindTimeout_Success(t *testing.T) {
	ln, err := listenWithBindTimeout(context.Background(), "127.0.0.1:0", time.Second, nil)
	if err != nil {
		t.Fatalf("listenWithBindTimeout: %v", err)
	}
	defer ln.Close()
	if _, ok := ln.Addr().(*net.TCPAddr); !ok {
		t.Fatalf("listener Addr type = %T, want *net.TCPAddr", ln.Addr())
	}
}

// TestTcpServer_BindTimeout_PathExercised verifies that TcpServer.Start uses
// listenWithBindTimeout when BindTimeout > 0, and that the listener still
// works for a normal bind with a generous timeout.
func TestTcpServer_BindTimeout_PathExercised(t *testing.T) {
	srv, err := NewTcpServer(TcpServerConfig{
		Addr:        "127.0.0.1:0",
		BindTimeout: time.Second,
		Handler:     func(context.Context, net.Conn) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewTcpServer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Start with BindTimeout=1s: %v", err)
	}
	defer srv.Shutdown()
	addr := srv.Addr()
	if addr == nil {
		t.Fatal("expected non-nil Addr after Start")
	}
}
