/*
 * actor_debug_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

// captureSlogDebug redirects slog to a buffer at DEBUG level for the duration
// of the test and returns the buffer plus a restore function.
func captureSlogDebug(t *testing.T) (*bytes.Buffer, func()) {
	t.Helper()
	var mu sync.Mutex
	buf := &bytes.Buffer{}
	prev := slog.Default()
	w := lockedWriter{w: buf, mu: &mu}
	h := slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug})
	slog.SetDefault(slog.New(h))
	return buf, func() { slog.SetDefault(prev) }
}

type lockedWriter struct {
	w  *bytes.Buffer
	mu *sync.Mutex
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// TestActorDebug_LogActorReceive verifies the receive flag gates DEBUG output.
func TestActorDebug_LogActorReceive(t *testing.T) {
	tcs := []struct {
		flag bool
		want bool
	}{
		{flag: false, want: false},
		{flag: true, want: true},
	}
	for _, tc := range tcs {
		buf, restore := captureSlogDebug(t)
		c := ActorDebugConfig{Receive: tc.flag}
		c.LogActorReceive("/user/foo", "hello")
		got := strings.Contains(buf.String(), "actor: received message")
		if got != tc.want {
			t.Errorf("flag=%v: log emitted=%v, want %v (output=%q)", tc.flag, got, tc.want, buf.String())
		}
		restore()
	}
}

// TestActorDebug_LogActorAutoreceive verifies autoreceive gating.
func TestActorDebug_LogActorAutoreceive(t *testing.T) {
	for _, on := range []bool{false, true} {
		buf, restore := captureSlogDebug(t)
		ActorDebugConfig{Autoreceive: on}.LogActorAutoreceive("/user/x", "PoisonPill")
		emitted := strings.Contains(buf.String(), "actor: auto-received system message")
		if emitted != on {
			t.Errorf("flag=%v: emitted=%v want=%v", on, emitted, on)
		}
		restore()
	}
}

// TestActorDebug_LogActorLifecycle verifies lifecycle gating.
func TestActorDebug_LogActorLifecycle(t *testing.T) {
	for _, on := range []bool{false, true} {
		buf, restore := captureSlogDebug(t)
		ActorDebugConfig{Lifecycle: on}.LogActorLifecycle("/user/x", "started")
		emitted := strings.Contains(buf.String(), "actor: lifecycle event")
		if emitted != on {
			t.Errorf("flag=%v: emitted=%v want=%v", on, emitted, on)
		}
		restore()
	}
}

// TestActorDebug_LogActorFSM verifies FSM gating.
func TestActorDebug_LogActorFSM(t *testing.T) {
	for _, on := range []bool{false, true} {
		buf, restore := captureSlogDebug(t)
		ActorDebugConfig{FSM: on}.LogActorFSM("/user/x", "transition", "S1->S2")
		emitted := strings.Contains(buf.String(), "actor: FSM event")
		if emitted != on {
			t.Errorf("flag=%v: emitted=%v want=%v", on, emitted, on)
		}
		restore()
	}
}

// TestActorDebug_LogActorEventStream verifies event-stream gating.
func TestActorDebug_LogActorEventStream(t *testing.T) {
	for _, on := range []bool{false, true} {
		buf, restore := captureSlogDebug(t)
		ActorDebugConfig{EventStream: on}.LogActorEventStream("subscribe", "MyChannel")
		emitted := strings.Contains(buf.String(), "actor: event-stream event")
		if emitted != on {
			t.Errorf("flag=%v: emitted=%v want=%v", on, emitted, on)
		}
		restore()
	}
}

// TestActorDebug_LogActorUnhandled verifies unhandled gating.
func TestActorDebug_LogActorUnhandled(t *testing.T) {
	for _, on := range []bool{false, true} {
		buf, restore := captureSlogDebug(t)
		ActorDebugConfig{Unhandled: on}.LogActorUnhandled("/user/x", 42)
		emitted := strings.Contains(buf.String(), "actor: unhandled message")
		if emitted != on {
			t.Errorf("flag=%v: emitted=%v want=%v", on, emitted, on)
		}
		restore()
	}
}

// TestActorDebug_LogRouterMisconfiguration verifies the router-misconfig
// flag emits at WARN (matching Pekko semantics).
func TestActorDebug_LogRouterMisconfiguration(t *testing.T) {
	for _, on := range []bool{false, true} {
		var mu sync.Mutex
		buf := &bytes.Buffer{}
		prev := slog.Default()
		// WARN-level handler so we verify the elevated severity.
		h := slog.NewTextHandler(lockedWriter{w: buf, mu: &mu}, &slog.HandlerOptions{Level: slog.LevelWarn})
		slog.SetDefault(slog.New(h))
		ActorDebugConfig{RouterMisconfiguration: on}.LogRouterMisconfiguration("/user/router", "missing routees")
		emitted := strings.Contains(buf.String(), "actor: router misconfigured")
		if emitted != on {
			t.Errorf("flag=%v: emitted=%v want=%v (output=%q)", on, emitted, on, buf.String())
		}
		slog.SetDefault(prev)
	}
}

// TestNewCluster_LogConfigOnStart_Dumps verifies that when
// pekko.log-config-on-start is enabled, NewCluster emits an INFO log line
// containing the resolved config at startup. We use a minimal config with
// LogConfigOnStart=true; the cluster startup may proceed past the log line
// and then fail to bind/etc., but the test only inspects the dump.
func TestNewCluster_LogConfigOnStart_Dumps(t *testing.T) {
	var mu sync.Mutex
	buf := &bytes.Buffer{}
	prev := slog.Default()
	h := slog.NewTextHandler(lockedWriter{w: buf, mu: &mu}, &slog.HandlerOptions{Level: slog.LevelInfo})
	slog.SetDefault(slog.New(h))
	defer slog.SetDefault(prev)

	cfg := ClusterConfig{
		Host:             "127.0.0.1",
		Port:             0,
		SystemName:       "TestSystem",
		LogConfigOnStart: true,
	}
	node, err := NewCluster(cfg)
	if err == nil && node != nil {
		defer node.Shutdown()
	}
	out := buf.String()
	if !strings.Contains(out, "gekka: resolved cluster configuration") {
		t.Errorf("expected log-config-on-start INFO line, got %q", out)
	}
	// And it must include at least one identifiable struct field name from
	// the dumped %+v rendering (proves the resolved config is in the line).
	if !strings.Contains(out, "LogConfigOnStart:true") {
		t.Errorf("expected dumped config to include LogConfigOnStart:true, got %q", out)
	}
}

// TestNewCluster_LogConfigOnStart_Off verifies the dump is suppressed when
// the flag is disabled (default).
func TestNewCluster_LogConfigOnStart_Off(t *testing.T) {
	var mu sync.Mutex
	buf := &bytes.Buffer{}
	prev := slog.Default()
	h := slog.NewTextHandler(lockedWriter{w: buf, mu: &mu}, &slog.HandlerOptions{Level: slog.LevelInfo})
	slog.SetDefault(slog.New(h))
	defer slog.SetDefault(prev)

	cfg := ClusterConfig{
		Host:       "127.0.0.1",
		Port:       0,
		SystemName: "TestSystem",
	}
	node, err := NewCluster(cfg)
	if err == nil && node != nil {
		defer node.Shutdown()
	}
	if strings.Contains(buf.String(), "gekka: resolved cluster configuration") {
		t.Errorf("config dump should not appear when LogConfigOnStart=false, got %q", buf.String())
	}
}
