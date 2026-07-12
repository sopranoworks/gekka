/*
 * test_slog_silence_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"io"
	"log/slog"
	"os"
	"testing"
)

// TestMain installs a Discard handler as slog.Default for the entire
// sharding test package. Every test in this package exercises shard
// lifecycle and rebalance code paths whose WARN/ERROR emissions are
// the very behaviour the test asserts has occurred — but the assertion
// is on the resulting actor state, not on log capture. Silencing slog
// at package scope keeps the reliability gate's stdout free of these
// by-design log lines without requiring every individual test to wire
// silenceSlogForTest manually.
//
// No test in this package asserts on slog output (verified by grep at
// the time this file was added); if a future test needs to inspect
// logs, it can install its own handler via slog.SetDefault and use
// t.Cleanup to restore the discard sink.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}
