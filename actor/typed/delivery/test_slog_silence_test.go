/*
 * test_slog_silence_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package delivery

import (
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/sopranoworks/gekka/logger"
)

// TestMain installs Discard handlers for the actor/typed/delivery test
// package. The work-pulling buffer-full drop path emits a WARN by
// design when the producer outpaces the consumer; the assertions are
// on item-counting and routing, not on log output.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	logger.SetDefaultForTest(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}
