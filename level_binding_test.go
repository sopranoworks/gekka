/*
 * level_binding_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"log/slog"
	"testing"

	"github.com/sopranoworks/gekka/logger"
)

// spawnFromHOCON parses the given HOCON, fills minimal node-identity fields
// so NewCluster can bind, and arranges Shutdown via t.Cleanup. The returned
// Cluster is the one whose NewCluster call drove logger.Install — callers can
// inspect logger.MainLevelVar() / StdoutLevelVar() afterwards to verify the
// HOCON-to-LevelVar binding contract Phase 10 closes.
func spawnFromHOCON(t *testing.T, hoconText string) *Cluster {
	t.Helper()
	cfg, err := ParseHOCONString(hoconText)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if cfg.SystemName == "" {
		cfg.SystemName = "LevelBindingTest"
	}
	if cfg.Host == "" {
		cfg.Host = "127.0.0.1"
	}
	c, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })
	return c
}

// TestNodeConfig_LogLevelDriversMainLevelVar — B1.1.
//
// pekko.loglevel = "DEBUG" must drive the package-global MainLevelVar to
// slog.LevelDebug after NewCluster runs Install. End-to-end binding from
// HOCON through ClusterConfig.LogLevel through logger.Install.
func TestNodeConfig_LogLevelDriversMainLevelVar(t *testing.T) {
	spawnFromHOCON(t, `pekko.loglevel = "DEBUG"`)
	if got, want := logger.MainLevelVar().Level(), slog.LevelDebug; got != want {
		t.Fatalf("MainLevelVar: got %v, want %v", got, want)
	}
}

// TestNodeConfig_StdoutLogLevelDriversStdoutLevelVar — B1.1.
//
// pekko.stdout-loglevel = "ERROR" must drive the package-global StdoutLevelVar
// to slog.LevelError after NewCluster runs Install.
func TestNodeConfig_StdoutLogLevelDriversStdoutLevelVar(t *testing.T) {
	spawnFromHOCON(t, `pekko.stdout-loglevel = "ERROR"`)
	if got, want := logger.StdoutLevelVar().Level(), slog.LevelError; got != want {
		t.Fatalf("StdoutLevelVar: got %v, want %v", got, want)
	}
}

// TestStdoutLogLevel_OFF_SilencesStdoutLeg — B1.1.
//
// pekko.stdout-loglevel = "OFF" must drive StdoutLevelVar to logger.LevelOff
// (math.MaxInt32). The composite handler's Handle gates stdout-leg dispatch
// behind r.Level >= stdoutLevelVar.Level(); since no record level satisfies
// that with LevelOff, the stdout leg is contractually silenced. Byte-level
// behavior of the level filter is unit-tested in logger/handler_test.go
// (TestCompositeHandler_RoutesByLevel and TestCompositeHandler_Enabled_*);
// this test verifies that the OFF HOCON value reaches the LevelVar end-to-
// end, which is the binding contract Phase 10 closes.
//
// The routing-path exercise (calling LogAttrs at WARN) that used to live
// here leaked a "stdout-off-probe" record to stdout via the main leg
// (which still defaults to stdout until external-log-server transport
// lands) and surfaced as a WARN in the integration log. The byte-level
// routing assertion is fully covered by the dedicated handler unit tests
// referenced above.
func TestStdoutLogLevel_OFF_SilencesStdoutLeg(t *testing.T) {
	spawnFromHOCON(t, `pekko.stdout-loglevel = "OFF"`)
	if got, want := logger.StdoutLevelVar().Level(), logger.LevelOff; got != want {
		t.Fatalf("StdoutLevelVar after OFF: got %v, want %v", got, want)
	}
}

// TestLogLevel_DefaultsAreInfoAndWarning — B1.1.
//
// With no log-level keys present in HOCON, NewCluster must install with
// MainLevel = slog.LevelInfo and StdoutLevel = slog.LevelWarn, matching
// Pekko reference.conf defaults (loglevel=INFO, stdout-loglevel=WARNING).
// Per the Phase 10 plan B2.2 the defaulting lives at the Install call site
// in cluster.go, not at the HOCON parse site.
func TestLogLevel_DefaultsAreInfoAndWarning(t *testing.T) {
	spawnFromHOCON(t, ``)
	if got, want := logger.MainLevelVar().Level(), slog.LevelInfo; got != want {
		t.Fatalf("MainLevelVar default: got %v, want %v", got, want)
	}
	if got, want := logger.StdoutLevelVar().Level(), slog.LevelWarn; got != want {
		t.Fatalf("StdoutLevelVar default: got %v, want %v", got, want)
	}
}

// TestPekkoAlias_AkkaLogLevel — B1.1.
//
// akka.loglevel = "WARN" is an alias for pekko.loglevel. detectProtocol
// returns "akka" when an akka identity key is present (here
// akka.actor.provider), and the parser routes <prefix>.loglevel into
// NodeConfig.LogLevel, which Install then writes to MainLevelVar. This
// matches the convention established by TestParseHOCON_StdoutLogLevel_AkkaAlias
// in hocon_config_test.go.
func TestPekkoAlias_AkkaLogLevel(t *testing.T) {
	spawnFromHOCON(t, `
akka.actor.provider = "cluster"
akka.loglevel = "WARN"
`)
	if got, want := logger.MainLevelVar().Level(), slog.LevelWarn; got != want {
		t.Fatalf("MainLevelVar via akka alias: got %v, want %v", got, want)
	}
}
