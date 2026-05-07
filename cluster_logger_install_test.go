/*
 * cluster_logger_install_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/sopranoworks/gekka/logger"
)

// recordingHandler captures every slog.Record routed through it. The handler
// is the test's stand-in for cluster.ClusterConfig.LogHandler — A4 must thread
// it through into logger.Install as Options.Main so records emitted on the
// main leg are observable from the test.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *recordingHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(_ string) slog.Handler      { return h }

func (h *recordingHandler) snapshot() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]slog.Record, len(h.records))
	copy(out, h.records)
	return out
}

// TestNewCluster_InstallsLoggerFromConfig — A4.1 RED.
//
// After NewCluster returns, MainLevelVar() and StdoutLevelVar() must reflect
// the configured Pekko levels, and a Debug record emitted via logger.Default()
// must reach the custom main handler supplied through ClusterConfig.LogHandler.
// stdout-loglevel = "OFF" silences the stdout leg while the main leg keeps
// emitting at LogLevel = "DEBUG".
func TestNewCluster_InstallsLoggerFromConfig(t *testing.T) {
	rec := &recordingHandler{}
	cfg := ClusterConfig{
		SystemName:     "LoggerInstallTest",
		Host:           "127.0.0.1",
		Port:           0,
		LogLevel:       "DEBUG",
		StdoutLogLevel: "OFF",
		LogHandler:     rec,
	}
	c, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer func() { _ = c.Shutdown() }()

	if got, want := logger.MainLevelVar().Level(), slog.LevelDebug; got != want {
		t.Fatalf("MainLevelVar after Spawn: got %v, want %v", got, want)
	}
	if got, want := logger.StdoutLevelVar().Level(), logger.LevelOff; got != want {
		t.Fatalf("StdoutLevelVar after Spawn: got %v, want %v", got, want)
	}

	before := len(rec.snapshot())
	logger.Default().LogAttrs(context.Background(), slog.LevelDebug, "a4-debug-probe", slog.String("k", "v"))
	got := rec.snapshot()
	if len(got) != before+1 {
		t.Fatalf("recording handler: got %d records, want %d", len(got), before+1)
	}
	last := got[len(got)-1]
	if last.Message != "a4-debug-probe" {
		t.Fatalf("recording handler last message: got %q, want %q", last.Message, "a4-debug-probe")
	}
	if last.Level != slog.LevelDebug {
		t.Fatalf("recording handler last level: got %v, want %v", last.Level, slog.LevelDebug)
	}
}

// TestNewCluster_InvalidLogLevel_ReturnsError — A4.1 RED.
//
// ParseLevel failures at the Install call site MUST surface as an error from
// NewCluster — no silent fallback. The error message must mention "log level"
// so operators can tell which knob is malformed.
func TestNewCluster_InvalidLogLevel_ReturnsError(t *testing.T) {
	cfg := ClusterConfig{
		SystemName: "LoggerInstallTest",
		Host:       "127.0.0.1",
		Port:       0,
		LogLevel:   "NOT-A-LEVEL",
	}
	c, err := NewCluster(cfg)
	if err == nil {
		_ = c.Shutdown()
		t.Fatalf("NewCluster: expected error for invalid LogLevel, got nil")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "log level") {
		t.Fatalf("NewCluster error message: got %q, want it to mention 'log level'", err.Error())
	}
}

// TestNewCluster_InvalidStdoutLogLevel_ReturnsError — A4.1 RED.
//
// Same contract as TestNewCluster_InvalidLogLevel_ReturnsError, applied to
// the stdout-loglevel knob. The Install call site validates both Pekko knobs
// before any Install side-effect lands.
func TestNewCluster_InvalidStdoutLogLevel_ReturnsError(t *testing.T) {
	cfg := ClusterConfig{
		SystemName:     "LoggerInstallTest",
		Host:           "127.0.0.1",
		Port:           0,
		StdoutLogLevel: "NOT-A-LEVEL",
	}
	c, err := NewCluster(cfg)
	if err == nil {
		_ = c.Shutdown()
		t.Fatalf("NewCluster: expected error for invalid StdoutLogLevel, got nil")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "log level") {
		t.Fatalf("NewCluster error message: got %q, want it to mention 'log level'", err.Error())
	}
}

// TestCluster_Shutdown_UninstallsLogger — A4.1 RED.
//
// After Shutdown(), the custom main handler supplied via LogHandler must
// stop receiving records routed through logger.Default(). install.Install
// (A2) replaces the main leg with a fresh stdout JSON handler when the
// uninstall fn fires, so the recorder is unreachable from logger.Default()
// after Shutdown.
func TestCluster_Shutdown_UninstallsLogger(t *testing.T) {
	rec := &recordingHandler{}
	cfg := ClusterConfig{
		SystemName: "LoggerInstallTest",
		Host:       "127.0.0.1",
		Port:       0,
		LogLevel:   "DEBUG",
		LogHandler: rec,
	}
	c, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}

	logger.Default().LogAttrs(context.Background(), slog.LevelDebug, "before-shutdown")
	beforeShutdown := len(rec.snapshot())
	if beforeShutdown == 0 {
		t.Fatalf("expected recorder to capture pre-shutdown record, got 0")
	}

	if err := c.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	logger.Default().LogAttrs(context.Background(), slog.LevelDebug, "after-shutdown")
	afterShutdown := len(rec.snapshot())
	if afterShutdown != beforeShutdown {
		t.Fatalf("recorder grew across Shutdown: before=%d after=%d (uninstall did not detach main leg)",
			beforeShutdown, afterShutdown)
	}
}
