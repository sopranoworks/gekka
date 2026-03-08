/*
 * logger_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

// ── captureHandler ────────────────────────────────────────────────────────────

// captureHandler is a slog.Handler that records formatted log lines.
type captureHandler struct {
	buf bytes.Buffer
	h   slog.Handler
}

func newCaptureHandler() *captureHandler {
	c := &captureHandler{}
	c.h = slog.NewTextHandler(&c.buf, &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: false,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Strip the time attribute so output is deterministic.
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	})
	return c
}

func (c *captureHandler) output() string { return c.buf.String() }

// withAttr is a passthrough handler that pre-sets additional attributes.
type withAttr struct {
	inner slog.Handler
	attrs []slog.Attr
}

func (h *captureHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.h.Enabled(ctx, l)
}
func (h *captureHandler) Handle(ctx context.Context, r slog.Record) error {
	return h.h.Handle(ctx, r)
}
func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h.h.WithAttrs(attrs)
}
func (h *captureHandler) WithGroup(name string) slog.Handler {
	return h.h.WithGroup(name)
}

// ── systemName ────────────────────────────────────────────────────────────────

func TestSystemName_Pekko(t *testing.T) {
	got := systemName("pekko://ClusterSystem@host:2552/user/foo")
	if got != "ClusterSystem" {
		t.Errorf("systemName = %q, want %q", got, "ClusterSystem")
	}
}

func TestSystemName_Akka(t *testing.T) {
	got := systemName("akka://MySystem@10.0.0.1:2552/user/bar")
	if got != "MySystem" {
		t.Errorf("systemName = %q, want %q", got, "MySystem")
	}
}

func TestSystemName_Invalid(t *testing.T) {
	for _, path := range []string{"", "/user/foo", "http://example.com"} {
		if got := systemName(path); got != "" {
			t.Errorf("systemName(%q) = %q, want empty", path, got)
		}
	}
}

// ── newActorLogger — attribute injection ──────────────────────────────────────

func TestActorLogger_ActorAttr(t *testing.T) {
	h := newCaptureHandler()
	ref := &mockRef{path: "pekko://Sys@host:2552/user/test"}
	l := newActorLogger(h, ref, func() Ref { return nil })

	l.Info("hello")

	out := h.output()
	if !strings.Contains(out, `actor=pekko://Sys@host:2552/user/test`) {
		t.Errorf("missing actor attr in: %s", out)
	}
	if !strings.Contains(out, `system=Sys`) {
		t.Errorf("missing system attr in: %s", out)
	}
}

func TestActorLogger_SenderAttr_WhenSenderSet(t *testing.T) {
	h := newCaptureHandler()
	ref := &mockRef{path: "pekko://Sys@host:2552/user/actor"}
	sender := &mockRef{path: "pekko://Sys@host:2552/user/sender"}

	var currentSender Ref = sender
	l := newActorLogger(h, ref, func() Ref { return currentSender })

	l.Info("got message")

	out := h.output()
	if !strings.Contains(out, `sender=pekko://Sys@host:2552/user/sender`) {
		t.Errorf("missing sender attr in: %s", out)
	}
}

func TestActorLogger_NoSenderAttr_WhenSenderNil(t *testing.T) {
	h := newCaptureHandler()
	ref := &mockRef{path: "pekko://Sys@host:2552/user/actor"}

	l := newActorLogger(h, ref, func() Ref { return nil })
	l.Info("no sender")

	out := h.output()
	if strings.Contains(out, "sender=") {
		t.Errorf("unexpected sender attr in: %s", out)
	}
}

// ── ActorLogger.With ──────────────────────────────────────────────────────────

func TestActorLogger_With(t *testing.T) {
	h := newCaptureHandler()
	ref := &mockRef{path: "pekko://Sys@host:2552/user/actor"}
	l := newActorLogger(h, ref, func() Ref { return nil })
	l2 := l.With("requestID", "abc-123")

	l2.Info("handling request")

	out := h.output()
	if !strings.Contains(out, "requestID=abc-123") {
		t.Errorf("missing requestID attr in: %s", out)
	}
}

// ── ActorLogger.Enabled ───────────────────────────────────────────────────────

func TestActorLogger_Enabled(t *testing.T) {
	h := newCaptureHandler()
	ref := &mockRef{path: "pekko://Sys@host:2552/user/actor"}
	l := newActorLogger(h, ref, func() Ref { return nil })

	if !l.Enabled(context.Background(), slog.LevelInfo) {
		t.Error("Enabled(Info) should be true")
	}
	if !l.Enabled(context.Background(), slog.LevelDebug) {
		t.Error("Enabled(Debug) should be true with debug handler")
	}
}

// ── BaseActor.Log — uninitialized fallback ────────────────────────────────────

func TestBaseActor_Log_Uninitialised(t *testing.T) {
	var b BaseActor
	l := b.Log()
	// Must not panic, must return a usable logger.
	l.Info("uninitialised log call")
}

// ── BaseActor.Log — integration via initLog ───────────────────────────────────

func TestBaseActor_Log_AfterInitLog(t *testing.T) {
	h := newCaptureHandler()
	ref := &mockRef{path: "pekko://Sys@host:2552/user/init-test"}

	var b BaseActor
	b.initLog(h, ref)

	b.Log().Info("after init")

	out := h.output()
	if !strings.Contains(out, "actor=pekko://Sys@host:2552/user/init-test") {
		t.Errorf("missing actor attr in: %s", out)
	}
}

// ── Sender attribute is dynamic (captured at log time) ────────────────────────

func TestActorLogger_SenderAttr_IsLazy(t *testing.T) {
	h := newCaptureHandler()
	ref := &mockRef{path: "pekko://Sys@host:2552/user/actor"}

	var currentSender Ref
	l := newActorLogger(h, ref, func() Ref { return currentSender })

	// First call — no sender.
	l.Info("no sender yet")
	out1 := h.output()
	if strings.Contains(out1, "sender=") {
		t.Errorf("unexpected sender attr before it was set: %s", out1)
	}

	// Set sender and log again.
	currentSender = &mockRef{path: "pekko://Sys@host:2552/user/s"}
	h.buf.Reset()
	l.Info("sender now set")
	out2 := h.output()
	if !strings.Contains(out2, "sender=pekko://Sys@host:2552/user/s") {
		t.Errorf("missing sender attr after setting it: %s", out2)
	}
}
