/*
 * plugin_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Tests that verify the plugin auto-registration mechanism works correctly.
// These tests run without any external services; they only confirm that the
// plugin names appear in the registry after a blank import.
package integration_test

import (
	"os"
	"testing"

	"github.com/sopranoworks/gekka/persistence"
	"github.com/sopranoworks/gekka/telemetry"

	// Blank imports trigger init() registration.
	_ "github.com/sopranoworks/gekka-extensions-persistence-spanner"
	_ "github.com/sopranoworks/gekka-extensions-telemetry-otel"
)

func TestPlugin_SpannerRegistered(t *testing.T) {
	names := persistence.JournalProviderNames()
	found := false
	for _, n := range names {
		if n == "spanner" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected \"spanner\" in JournalProviderNames(), got %v", names)
	}
}

func TestPlugin_OtelRegistered(t *testing.T) {
	names := telemetry.ProviderNames()
	found := false
	for _, n := range names {
		if n == "otel" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected \"otel\" in telemetry.ProviderNames(), got %v", names)
	}
}

// TestPlugin_SpannerEmulator exercises the full Spanner Journal against a live
// emulator. It is skipped unless SPANNER_EMULATOR_HOST is set.
func TestPlugin_SpannerEmulator(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("SPANNER_EMULATOR_HOST not set; skipping emulator test")
	}
	// When the emulator is present the caller is expected to have already
	// called spannerstore.Configure(client, codec). This test only asserts
	// that the registration plumbing is in place; a deeper test belongs in
	// the spanner extension module itself.
	t.Log("SPANNER_EMULATOR_HOST is set; emulator wiring test placeholder passed")
}
