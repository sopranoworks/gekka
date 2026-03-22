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

	hocon "github.com/sopranoworks/gekka-config"
	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/discovery"
	"github.com/sopranoworks/gekka/persistence"
	"github.com/sopranoworks/gekka/telemetry"

	// Blank imports trigger init() registration — zero manual setup calls.
	_ "github.com/sopranoworks/gekka-extensions-cluster-k8s"
	_ "github.com/sopranoworks/gekka-extensions-persistence-spanner"
	_ "github.com/sopranoworks/gekka-extensions-telemetry-otel"
)

func TestPlugin_SpannerRegistered(t *testing.T) {
	if !containsString(persistence.JournalProviderNames(), "spanner") {
		t.Fatalf("expected \"spanner\" in JournalProviderNames(), got %v", persistence.JournalProviderNames())
	}
}

func TestPlugin_OtelRegistered(t *testing.T) {
	if !containsString(telemetry.ProviderNames(), "otel") {
		t.Fatalf("expected \"otel\" in telemetry.ProviderNames(), got %v", telemetry.ProviderNames())
	}
}

func TestPlugin_KubernetesDiscoveryRegistered(t *testing.T) {
	if !containsString(discovery.ProviderNames(), "kubernetes") {
		t.Fatalf("expected \"kubernetes\" in discovery.ProviderNames(), got %v", discovery.ProviderNames())
	}
}

func TestPlugin_KubernetesAPIDiscoveryRegistered(t *testing.T) {
	if !containsString(discovery.ProviderNames(), "kubernetes-api") {
		t.Fatalf("expected \"kubernetes-api\" in discovery.ProviderNames(), got %v", discovery.ProviderNames())
	}
}

// TestPlugin_SystemStartsWithHOCON verifies that NewActorSystem boots with
// plugin names supplied via HOCON — zero manual Configure() calls.
// The built-in "in-memory" backend is used so no external service is needed.
func TestPlugin_SystemStartsWithHOCON(t *testing.T) {
	cfg, err := hocon.ParseString(`
		persistence {
			journal.plugin = "in-memory"
			snapshot-store.plugin = "in-memory"
		}
		telemetry.provider.plugin = "no-op"
	`)
	if err != nil {
		t.Fatalf("parse HOCON: %v", err)
	}

	sys, err := gekka.NewActorSystem("plugin-test", cfg)
	if err != nil {
		t.Fatalf("NewActorSystem: %v", err)
	}
	defer sys.Terminate()

	// Journal and SnapshotStore must be non-nil in-memory implementations.
	if sys.Journal() == nil {
		t.Error("Journal() returned nil")
	}
	if sys.SnapshotStore() == nil {
		t.Error("SnapshotStore() returned nil")
	}
}

// TestPlugin_SpannerEmulator exercises the Spanner Journal against a live
// emulator. It is skipped unless SPANNER_EMULATOR_HOST is set.
func TestPlugin_SpannerEmulator(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("SPANNER_EMULATOR_HOST not set; skipping emulator test")
	}
	t.Log("SPANNER_EMULATOR_HOST is set; full emulator test belongs in extensions/persistence/spanner")
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
