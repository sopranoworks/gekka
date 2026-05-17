/*
 * test_slog_silence_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/sopranoworks/gekka/logger"
)

// TestMain installs Discard handlers as both slog.Default and gekka's
// logger.Default for the entire persistence test package. Tests in this
// package exercise PersistentFSM unhandled-message handling, ReplayFilter
// anomaly modes, and recovery code paths whose WARN emissions are part
// of the behaviour under test — the assertions are on FSM state and
// recovery outcomes, not on log capture.
//
// No test in this package asserts on slog output (verified by grep at
// the time this file was added).
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	logger.SetDefaultForTest(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}
