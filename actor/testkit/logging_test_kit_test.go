/*
 * logging_test_kit_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package testkit

import (
	"log/slog"
	"testing"
)

func TestLoggingTestKit_ExpectLog(t *testing.T) {
	ltk := NewLoggingTestKit()
	logger := slog.New(ltk.Handler())

	ltk.Intercept(func() {
		logger.Warn("disk is 90% full")
		logger.Info("all good")
	})

	if err := ltk.ExpectLog(slog.LevelWarn, "disk.*full"); err != nil {
		t.Fatal(err)
	}
	if err := ltk.ExpectLog(slog.LevelInfo, "all good"); err != nil {
		t.Fatal(err)
	}
}

func TestLoggingTestKit_ExpectLog_NoMatch(t *testing.T) {
	ltk := NewLoggingTestKit()
	logger := slog.New(ltk.Handler())

	ltk.Intercept(func() {
		logger.Info("something else")
	})

	if err := ltk.ExpectLog(slog.LevelError, "fatal"); err == nil {
		t.Fatal("expected error for missing log")
	}
}

func TestLoggingTestKit_ExpectNoLog(t *testing.T) {
	ltk := NewLoggingTestKit()
	logger := slog.New(ltk.Handler())

	ltk.Intercept(func() {
		logger.Info("normal operation")
	})

	// No error log should have been emitted.
	if err := ltk.ExpectNoLog(slog.LevelError, ".*"); err != nil {
		t.Fatal(err)
	}
}

func TestLoggingTestKit_ExpectNoLog_Found(t *testing.T) {
	ltk := NewLoggingTestKit()
	logger := slog.New(ltk.Handler())

	ltk.Intercept(func() {
		logger.Error("bad thing happened")
	})

	if err := ltk.ExpectNoLog(slog.LevelError, "bad.*happened"); err == nil {
		t.Fatal("expected error because log was found")
	}
}

func TestLoggingTestKit_Clear(t *testing.T) {
	ltk := NewLoggingTestKit()
	logger := slog.New(ltk.Handler())

	logger.Warn("first")
	if len(ltk.Entries()) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(ltk.Entries()))
	}

	ltk.Clear()
	if len(ltk.Entries()) != 0 {
		t.Fatalf("expected 0 entries after clear, got %d", len(ltk.Entries()))
	}

	logger.Error("second")
	if len(ltk.Entries()) != 1 {
		t.Fatalf("expected 1 entry after second log, got %d", len(ltk.Entries()))
	}
}

func TestLoggingTestKit_Entries(t *testing.T) {
	ltk := NewLoggingTestKit()
	logger := slog.New(ltk.Handler())

	logger.Debug("d")
	logger.Info("i")
	logger.Warn("w")
	logger.Error("e")

	entries := ltk.Entries()
	if len(entries) != 4 {
		t.Fatalf("expected 4 entries, got %d", len(entries))
	}

	levels := []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError}
	msgs := []string{"d", "i", "w", "e"}
	for i, e := range entries {
		if e.Level != levels[i] {
			t.Errorf("entry %d: level=%s, want %s", i, e.Level, levels[i])
		}
		if e.Message != msgs[i] {
			t.Errorf("entry %d: msg=%q, want %q", i, e.Message, msgs[i])
		}
	}
}

func TestLoggingTestKit_WithAttrs(t *testing.T) {
	ltk := NewLoggingTestKit()
	logger := slog.New(ltk.Handler()).With("key", "val")

	logger.Info("with attrs")

	if err := ltk.ExpectLog(slog.LevelInfo, "with attrs"); err != nil {
		t.Fatal(err)
	}
}
