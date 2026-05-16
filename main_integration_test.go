//go:build integration

/*
 * main_integration_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// TestMain for the integration test binary.  Its sole job: SIGKILL any
// `java` process left over from a previous test binary run that still
// holds one of our well-known test ports.  Without this pre-pass, a
// crash or `-timeout` abort in run N inherits its zombie JVMs into
// run N+1, where every cluster-binding test fails on
// "bind: address already in use".
//
// The integration suite is the authoritative end-to-end gate; making it
// self-recovering eliminates the manual "kill 12345" step that used to
// precede every full run.

package gekka

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/test/jvmproc"
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	killed, err := jvmproc.ReapStaleTestJVMs(ctx)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration TestMain: reap failed: %v\n", err)
		os.Exit(2)
	}
	if killed > 0 {
		fmt.Fprintf(os.Stderr, "integration TestMain: reaped %d stale JVMs before suite start\n", killed)
	}
	os.Exit(m.Run())
}
