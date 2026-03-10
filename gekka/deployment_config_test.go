/*
 * deployment_config_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"testing"

	"gekka/gekka/actor"

	hocon "github.com/sopranoworks/gekka-config"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// mustParseHOCON is a test helper that fails immediately on parse error.
func mustParseHOCON(t *testing.T, text string) *hocon.Config {
	t.Helper()
	cfg, err := hocon.ParseString(text)
	if err != nil {
		t.Fatalf("hocon.ParseString: %v", err)
	}
	return cfg
}

// ── LookupDeployment ─────────────────────────────────────────────────────────

func TestLookupDeployment_RoundRobinPool_FullPath(t *testing.T) {
	cfg := mustParseHOCON(t, `
pekko.actor.deployment {
  "/user/myRouter" {
    router          = round-robin-pool
    nr-of-instances = 5
  }
}
`)
	d, ok := LookupDeployment(cfg, "/user/myRouter")
	if !ok {
		t.Fatal("LookupDeployment returned false, want true")
	}
	if d.Router != "round-robin-pool" {
		t.Errorf("Router = %q, want %q", d.Router, "round-robin-pool")
	}
	if d.NrOfInstances != 5 {
		t.Errorf("NrOfInstances = %d, want 5", d.NrOfInstances)
	}
}

// TestLookupDeployment_ShortPath verifies that "/myRouter" resolves to a
// deployment block keyed by the short form (without /user prefix).
func TestLookupDeployment_ShortPath(t *testing.T) {
	cfg := mustParseHOCON(t, `
pekko.actor.deployment {
  "/myRouter" {
    router          = round-robin-pool
    nr-of-instances = 3
  }
}
`)
	// Query with short form directly.
	d, ok := LookupDeployment(cfg, "/myRouter")
	if !ok {
		t.Fatal("LookupDeployment returned false for short-form key, want true")
	}
	if d.NrOfInstances != 3 {
		t.Errorf("NrOfInstances = %d, want 3", d.NrOfInstances)
	}
}

// TestLookupDeployment_FullPathFindsShortKey verifies that querying with the
// full path "/user/myRouter" finds a deployment block keyed by "/myRouter".
func TestLookupDeployment_FullPathFindsShortKey(t *testing.T) {
	cfg := mustParseHOCON(t, `
pekko.actor.deployment {
  "/myRouter" {
    router          = round-robin-pool
    nr-of-instances = 4
  }
}
`)
	d, ok := LookupDeployment(cfg, "/user/myRouter")
	if !ok {
		t.Fatal("LookupDeployment returned false, want true (full path should find short key)")
	}
	if d.NrOfInstances != 4 {
		t.Errorf("NrOfInstances = %d, want 4", d.NrOfInstances)
	}
}

// TestLookupDeployment_ShortPathFindsFullKey verifies the reverse: querying
// with the short form "/myRouter" locates a block keyed by "/user/myRouter".
func TestLookupDeployment_ShortPathFindsFullKey(t *testing.T) {
	cfg := mustParseHOCON(t, `
pekko.actor.deployment {
  "/user/myRouter" {
    router          = round-robin-pool
    nr-of-instances = 7
  }
}
`)
	d, ok := LookupDeployment(cfg, "/myRouter")
	if !ok {
		t.Fatal("LookupDeployment returned false, want true (short path should find full key)")
	}
	if d.NrOfInstances != 7 {
		t.Errorf("NrOfInstances = %d, want 7", d.NrOfInstances)
	}
}

// TestLookupDeployment_AkkaPrefix verifies that the akka.* prefix is also searched.
func TestLookupDeployment_AkkaPrefix(t *testing.T) {
	cfg := mustParseHOCON(t, `
akka.actor.deployment {
  "/user/myWorker" {
    router          = round-robin-pool
    nr-of-instances = 2
  }
}
`)
	d, ok := LookupDeployment(cfg, "/user/myWorker")
	if !ok {
		t.Fatal("LookupDeployment returned false for akka prefix, want true")
	}
	if d.Router != "round-robin-pool" {
		t.Errorf("Router = %q, want round-robin-pool", d.Router)
	}
	if d.NrOfInstances != 2 {
		t.Errorf("NrOfInstances = %d, want 2", d.NrOfInstances)
	}
}

// TestLookupDeployment_MissingBlock verifies that a missing deployment block
// returns (zero, false) without panicking — the actor should be treated as plain.
func TestLookupDeployment_MissingBlock(t *testing.T) {
	cfg := mustParseHOCON(t, `
pekko.actor.deployment {
  "/user/other" {
    router          = round-robin-pool
    nr-of-instances = 1
  }
}
`)
	_, ok := LookupDeployment(cfg, "/user/myRouter")
	if ok {
		t.Error("LookupDeployment returned true for absent actor, want false")
	}
}

// TestLookupDeployment_NoDeploymentBlock verifies graceful handling when the
// entire deployment section is absent from the config.
func TestLookupDeployment_NoDeploymentBlock(t *testing.T) {
	cfg := mustParseHOCON(t, `
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port     = 2552
  }
}
`)
	_, ok := LookupDeployment(cfg, "/user/myRouter")
	if ok {
		t.Error("LookupDeployment returned true with no deployment section, want false")
	}
}

// TestLookupDeployment_MultipleRouters checks that the correct block is selected
// when several deployment entries are present.
func TestLookupDeployment_MultipleRouters(t *testing.T) {
	cfg := mustParseHOCON(t, `
pekko.actor.deployment {
  "/user/workerPool" {
    router          = round-robin-pool
    nr-of-instances = 8
  }
  "/user/otherPool" {
    router          = round-robin-pool
    nr-of-instances = 2
  }
}
`)
	d, ok := LookupDeployment(cfg, "/user/workerPool")
	if !ok {
		t.Fatal("LookupDeployment returned false")
	}
	if d.NrOfInstances != 8 {
		t.Errorf("NrOfInstances = %d, want 8", d.NrOfInstances)
	}

	d2, ok := LookupDeployment(cfg, "/user/otherPool")
	if !ok {
		t.Fatal("LookupDeployment returned false for otherPool")
	}
	if d2.NrOfInstances != 2 {
		t.Errorf("NrOfInstances = %d, want 2", d2.NrOfInstances)
	}
}

// ── DeploymentToPoolRouter ────────────────────────────────────────────────────

func TestDeploymentToPoolRouter_RoundRobin(t *testing.T) {
	d := DeploymentConfig{
		Router:        "round-robin-pool",
		NrOfInstances: 3,
	}
	props := actor.Props{New: func() actor.Actor {
		return &deployTestActor{BaseActor: actor.NewBaseActor()}
	}}

	pool, err := DeploymentToPoolRouter(nil, d, props)
	if err != nil {
		t.Fatalf("DeploymentToPoolRouter: %v", err)
	}
	if pool == nil {
		t.Fatal("pool is nil")
	}
	if pool.Mailbox() == nil {
		t.Error("pool.Mailbox() is nil")
	}
}

func TestDeploymentToPoolRouter_UnknownRouter(t *testing.T) {
	d := DeploymentConfig{
		Router:        "bogus-pool",
		NrOfInstances: 3,
	}
	_, err := DeploymentToPoolRouter(nil, d, actor.Props{New: func() actor.Actor {
		return &deployTestActor{BaseActor: actor.NewBaseActor()}
	}})
	if err == nil {
		t.Error("expected error for unknown router type, got nil")
	}
}

func TestDeploymentToPoolRouter_ZeroInstances(t *testing.T) {
	d := DeploymentConfig{
		Router:        "round-robin-pool",
		NrOfInstances: 0,
	}
	_, err := DeploymentToPoolRouter(nil, d, actor.Props{New: func() actor.Actor {
		return &deployTestActor{BaseActor: actor.NewBaseActor()}
	}})
	if err == nil {
		t.Error("expected error for zero NrOfInstances, got nil")
	}
}

// ── End-to-end: parse HOCON → pool router ────────────────────────────────────

// TestDeploymentEndToEnd_ParseAndSpawn exercises the full pipeline:
// HOCON text → LookupDeployment → DeploymentToPoolRouter.
func TestDeploymentEndToEnd_ParseAndSpawn(t *testing.T) {
	const hoconText = `
pekko.actor.deployment {
  "/user/myRouter" {
    router          = round-robin-pool
    nr-of-instances = 5
  }
}
`
	cfg := mustParseHOCON(t, hoconText)

	d, ok := LookupDeployment(cfg, "/user/myRouter")
	if !ok {
		t.Fatal("deployment block not found")
	}

	props := actor.Props{New: func() actor.Actor {
		return &deployTestActor{BaseActor: actor.NewBaseActor()}
	}}
	pool, err := DeploymentToPoolRouter(nil, d, props)
	if err != nil {
		t.Fatalf("DeploymentToPoolRouter: %v", err)
	}
	if pool == nil {
		t.Fatal("pool is nil")
	}
	// The pool has not been started yet; nrOfInstances should reflect the config.
	if pool.(*actor.PoolRouter).NrOfInstances() != 5 {
		t.Errorf("nrOfInstances = %d, want 5", pool.(*actor.PoolRouter).NrOfInstances())
	}
}

// ── minimal actor for tests ───────────────────────────────────────────────────

type deployTestActor struct{ actor.BaseActor }

func (a *deployTestActor) Receive(_ any) {}
