/*
 * test_slog_silence_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package receptionist

import (
	"io"
	"log/slog"
	"os"
	"testing"
)

// TestMain installs a Discard handler as slog.Default for the entire
// actor/typed/receptionist test package. Tests in this package
// exercise the no-routees-available drop path (deliberately a WARN);
// the assertions are on routing behaviour, not on log output.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}
