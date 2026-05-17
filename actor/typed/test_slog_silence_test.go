/*
 * test_slog_silence_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"io"
	"log/slog"
	"os"
	"testing"
)

// TestMain installs a Discard handler as slog.Default for the entire
// actor/typed test package. Tests in this package exercise actor
// lifecycle and dispatch code paths (TestSpawnProtocol_UnexpectedMessage,
// typed-actor panic recovery, etc.) whose WARN/ERROR emissions are part
// of the contract being tested; the assertions are on actor state and
// behaviour, not on log output. Silencing slog at package scope keeps
// the reliability gate's stdout free of these by-design log lines.
//
// No test in this package asserts on slog output (verified by grep at
// the time this file was added).
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}
