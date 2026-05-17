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

// silenceSlogForTest swaps slog.Default to a Discard handler for the
// duration of t, restoring the previous default in t.Cleanup. Sharding
// test paths that exercise warning code (RebalanceShard unknown target/
// shard, shard-start-timeout deadlines, lease-acquire failures,
// invalid state transitions, etc.) emit WARN/ERROR via BaseActor.Log,
// which routes through Go's slog default. The assertions in those tests
// are on behaviour (no message sent, state retained, etc.), not on log
// output, so the WARN/ERROR are reliability-gate noise rather than
// signal.
func silenceSlogForTest(t *testing.T) {
	t.Helper()
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
}

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
