/*
 * logging_test_kit.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package testkit

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
)

// LoggingTestKit captures log output during a test function and provides
// assertion helpers to verify that expected (or unexpected) log messages
// were emitted.
//
// Usage:
//
//	ltk := testkit.NewLoggingTestKit()
//	ltk.Intercept(func() {
//	    slog.New(ltk.Handler()).Warn("disk is 90% full")
//	})
//	if err := ltk.ExpectLog(slog.LevelWarn, "disk.*full"); err != nil {
//	    t.Fatal(err)
//	}
type LoggingTestKit struct {
	mu      sync.Mutex
	entries []logEntry
	handler *capturingHandler
}

type logEntry struct {
	Level   slog.Level
	Message string
}

// NewLoggingTestKit creates a new LoggingTestKit.
func NewLoggingTestKit() *LoggingTestKit {
	ltk := &LoggingTestKit{}
	ltk.handler = &capturingHandler{ltk: ltk}
	return ltk
}

// Handler returns a slog.Handler that captures log records into this test kit.
// Pass it to slog.New() or inject it into actor logger configuration.
func (ltk *LoggingTestKit) Handler() slog.Handler {
	return ltk.handler
}

// Intercept runs fn and captures all log records emitted through the
// Handler() during execution. Prior entries are NOT cleared — call Clear()
// first if you want a fresh capture window.
func (ltk *LoggingTestKit) Intercept(fn func()) {
	fn()
}

// ExpectLog asserts that at least one captured log entry matches the given
// level and message pattern (regexp). Returns nil on success, an error
// describing the mismatch on failure.
func (ltk *LoggingTestKit) ExpectLog(level slog.Level, messagePattern string) error {
	ltk.mu.Lock()
	defer ltk.mu.Unlock()

	re, err := regexp.Compile(messagePattern)
	if err != nil {
		return fmt.Errorf("invalid pattern %q: %w", messagePattern, err)
	}

	for _, e := range ltk.entries {
		if e.Level == level && re.MatchString(e.Message) {
			return nil
		}
	}

	return fmt.Errorf("expected log at level %s matching %q, but got %d entries: %v",
		level, messagePattern, len(ltk.entries), ltk.entries)
}

// ExpectNoLog asserts that no captured log entry matches the given level
// and message pattern. Returns nil on success, an error if a match is found.
func (ltk *LoggingTestKit) ExpectNoLog(level slog.Level, messagePattern string) error {
	ltk.mu.Lock()
	defer ltk.mu.Unlock()

	re, err := regexp.Compile(messagePattern)
	if err != nil {
		return fmt.Errorf("invalid pattern %q: %w", messagePattern, err)
	}

	for _, e := range ltk.entries {
		if e.Level == level && re.MatchString(e.Message) {
			return fmt.Errorf("unexpected log at level %s matching %q: %q",
				level, messagePattern, e.Message)
		}
	}
	return nil
}

// Entries returns a copy of all captured log entries.
func (ltk *LoggingTestKit) Entries() []logEntry {
	ltk.mu.Lock()
	defer ltk.mu.Unlock()
	out := make([]logEntry, len(ltk.entries))
	copy(out, ltk.entries)
	return out
}

// Clear removes all captured log entries.
func (ltk *LoggingTestKit) Clear() {
	ltk.mu.Lock()
	defer ltk.mu.Unlock()
	ltk.entries = nil
}

// ── capturingHandler ─────────────────────────────────────────────────────────

type capturingHandler struct {
	ltk   *LoggingTestKit
	attrs []slog.Attr
	group string
}

func (h *capturingHandler) Enabled(_ context.Context, _ slog.Level) bool {
	return true // capture everything
}

func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.ltk.mu.Lock()
	defer h.ltk.mu.Unlock()
	h.ltk.entries = append(h.ltk.entries, logEntry{
		Level:   r.Level,
		Message: r.Message,
	})
	return nil
}

func (h *capturingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &capturingHandler{
		ltk:   h.ltk,
		attrs: append(h.attrs, attrs...),
		group: h.group,
	}
}

func (h *capturingHandler) WithGroup(name string) slog.Handler {
	return &capturingHandler{
		ltk:   h.ltk,
		attrs: h.attrs,
		group: name,
	}
}
