/*
 * test_slog_silence_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/sopranoworks/gekka/logger"
)

// TestMain installs Discard handlers as both slog.Default and gekka's
// logger.Default for the entire internal/core test package. Tests in
// this package exercise inbound-restart cap, write classifier branches,
// quarantine handlers, and other code paths whose WARN/ERROR emissions
// are exactly the contract being asserted — but the assertions
// themselves are on counters, flight-recorder events, and association
// state, not on log capture.
//
// Two tests deliberately wrap logger.SetDefaultForTest to capture log
// output (compression_production_wiring_test.go,
// artery_observability_test.go); their per-test swaps work alongside
// this package-wide baseline because the per-test cleanup restores the
// previous logger.Default — which, after this TestMain runs, is the
// discard handler installed here.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	logger.SetDefaultForTest(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}
