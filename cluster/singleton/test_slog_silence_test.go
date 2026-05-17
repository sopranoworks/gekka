/*
 * test_slog_silence_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package singleton

import (
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/sopranoworks/gekka/logger"
)

// TestMain installs Discard handlers for the cluster/singleton test
// package. Tests exercise the lease-acquisition-failed retry path
// (emits a WARN by design) and the singleton handover/role assignment
// flow; the assertions are on singleton state, not on log output.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	logger.SetDefaultForTest(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}
