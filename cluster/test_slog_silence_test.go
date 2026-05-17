/*
 * test_slog_silence_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/sopranoworks/gekka/logger"
)

// TestMain installs Discard handlers for the cluster test package.
// Tests exercise SBR strategies, multi-DC gossip, config compatibility
// checks, and reachability accounting; many code paths emit WARN by
// design (e.g. "Cluster: config compat check failed",
// "Cluster: gossip received from unknown sender") and assertions are
// on cluster state, not on log capture.
//
// Tests that need to capture log output for assertions
// (cluster_manager_test.go, downing_test.go) still work because
// logger.SetDefaultForTest returns a restore closure that puts the
// previous (here: discard) handler back, so per-test swaps compose
// correctly with this package-wide baseline.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	logger.SetDefaultForTest(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}
