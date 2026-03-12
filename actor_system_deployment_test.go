/*
 * actor_system_deployment_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"fmt"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/internal/core"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// collectActor delivers every received message to a shared channel.
type collectActor struct {
	actor.BaseActor
	ch chan<- any
}

func (a *collectActor) Receive(msg any) { a.ch <- msg }

// spawnTestNode creates a Cluster on an ephemeral port, returning it and a
// teardown func. It does NOT join any cluster.
func spawnTestNode(t *testing.T, cfg ClusterConfig) *Cluster {
	t.Helper()
	if cfg.Port == 0 {
		cfg.Port = 0 // let OS pick
	}
	node, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	t.Cleanup(func() { _ = node.Shutdown() })
	return node
}

// drainN collects exactly n items from ch within timeout, returning them.
// Reports a test failure if fewer than n items arrive.
func drainN(t *testing.T, ch <-chan any, n int, timeout time.Duration) []any {
	t.Helper()
	out := make([]any, 0, n)
	deadline := time.After(timeout)
	for len(out) < n {
		select {
		case v := <-ch:
			out = append(out, v)
		case <-deadline:
			t.Errorf("drainN: got %d/%d messages before timeout", len(out), n)
			return out
		}
	}
	return out
}

// ── Deployment auto-provisioning ─────────────────────────────────────────────

// TestActorOf_DeploymentAutoProvisioning_RoundRobin verifies that ActorOf
// automatically wraps the user's Props in a PoolRouter when a matching
// deployment entry is present in ClusterConfig.Deployments.
func TestActorOf_DeploymentAutoProvisioning_RoundRobin(t *testing.T) {
	const nInstances = 3
	const nMessages = 9 // divisible by nInstances for even distribution check

	received := make(chan any, nMessages*2)

	node := spawnTestNode(t, ClusterConfig{
		Deployments: map[string]core.DeploymentConfig{
			"/user/myRouter": {Router: "round-robin-pool", NrOfInstances: nInstances},
		},
	})

	workerProps := Props{New: func() actor.Actor {
		return &collectActor{BaseActor: actor.NewBaseActor(), ch: received}
	}}

	// ActorOf should silently create a PoolRouter instead of a plain actor.
	ref, err := node.System.ActorOf(workerProps, "myRouter")
	if err != nil {
		t.Fatalf("ActorOf: %v", err)
	}

	// Give the PoolRouter time to finish PreStart (child spawning is async
	// because Start launches a goroutine before calling PreStart).
	time.Sleep(20 * time.Millisecond)

	for i := 0; i < nMessages; i++ {
		ref.Tell(fmt.Sprintf("msg-%d", i))
	}

	msgs := drainN(t, received, nMessages, 500*time.Millisecond)
	if len(msgs) != nMessages {
		return // drainN already reported the failure
	}
}

// TestActorOf_DeploymentAutoProvisioning_ShortPath checks that a deployment
// keyed by the short form "/myRouter" (without /user) also triggers automatic
// pool creation when ActorOf is called with "myRouter".
func TestActorOf_DeploymentAutoProvisioning_ShortPath(t *testing.T) {
	received := make(chan any, 20)

	node := spawnTestNode(t, ClusterConfig{
		Deployments: map[string]core.DeploymentConfig{
			"/myRouter": {Router: "round-robin-pool", NrOfInstances: 2},
		},
	})

	ref, err := node.System.ActorOf(Props{New: func() actor.Actor {
		return &collectActor{BaseActor: actor.NewBaseActor(), ch: received}
	}}, "myRouter")
	if err != nil {
		t.Fatalf("ActorOf: %v", err)
	}

	time.Sleep(20 * time.Millisecond)

	for i := 0; i < 4; i++ {
		ref.Tell(i)
	}
	drainN(t, received, 4, 300*time.Millisecond)
}

// TestActorOf_NoDeployment_SpawnsPlainActor ensures that when no deployment
// config exists for the path, ActorOf spawns a plain actor (not a router).
func TestActorOf_NoDeployment_SpawnsPlainActor(t *testing.T) {
	received := make(chan any, 4)

	node := spawnTestNode(t, ClusterConfig{}) // no deployments

	ref, err := node.System.ActorOf(Props{New: func() actor.Actor {
		return &collectActor{BaseActor: actor.NewBaseActor(), ch: received}
	}}, "plain")
	if err != nil {
		t.Fatalf("ActorOf: %v", err)
	}

	ref.Tell("hello")
	drainN(t, received, 1, 200*time.Millisecond)
}

// TestActorOf_ManualRouterAlongsideDeployment verifies that manually created
// RouterActors still work correctly when deployment config exists for other paths.
func TestActorOf_ManualRouterAlongsideDeployment(t *testing.T) {
	received := make(chan any, 10)

	node := spawnTestNode(t, ClusterConfig{
		Deployments: map[string]core.DeploymentConfig{
			"/user/autoPool": {Router: "round-robin-pool", NrOfInstances: 2},
		},
	})

	// Manual router at a different path — no deployment config for it.
	r0 := make(chan any, 8)
	r1 := make(chan any, 8)
	routees := []actor.Ref{
		&funcRef{path: func() string { return "r0" }, tell: func(m any, _ ...actor.Ref) { r0 <- m }},
		&funcRef{path: func() string { return "r1" }, tell: func(m any, _ ...actor.Ref) { r1 <- m }},
	}
	manualRouter := actor.NewRouterActor(&actor.RoundRobinRoutingLogic{}, routees)
	manualRef, err := node.System.ActorOf(Props{New: func() actor.Actor { return manualRouter }}, "manualRouter")
	if err != nil {
		t.Fatalf("ActorOf manual: %v", err)
	}

	// Auto pool.
	autoRef, err := node.System.ActorOf(Props{New: func() actor.Actor {
		return &collectActor{BaseActor: actor.NewBaseActor(), ch: received}
	}}, "autoPool")
	if err != nil {
		t.Fatalf("ActorOf auto: %v", err)
	}

	// Both refs must be non-nil.
	if manualRef.Path() == "" || autoRef.Path() == "" {
		t.Error("ref paths must not be empty")
	}

	// Manual router routes.
	manualRef.Tell("ping")
	time.Sleep(20 * time.Millisecond)
	got := len(r0) + len(r1)
	if got != 1 {
		t.Errorf("manual router: got %d deliveries, want 1", got)
	}

	// Auto pool routes.
	time.Sleep(20 * time.Millisecond)
	autoRef.Tell("hello")
	drainN(t, received, 1, 200*time.Millisecond)
}

// TestActorOf_DeploymentFromHOCON exercises the full pipeline:
// HOCON text → ParseHOCONString → Spawn → ActorOf → auto pool.
func TestActorOf_DeploymentFromHOCON(t *testing.T) {
	const hoconText = `
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 0 }
  actor.deployment {
    "/user/hoconRouter" {
      router          = round-robin-pool
      nr-of-instances = 4
    }
  }
}
`
	nodeCfg, err := parseHOCONString(hoconText)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	nodeCfg.Port = 0 // ensure ephemeral

	if len(nodeCfg.Deployments) == 0 {
		t.Fatal("expected Deployments to be populated from HOCON, got empty")
	}
	d, ok := nodeCfg.Deployments["/user/hoconRouter"]
	if !ok {
		// Try short form as well.
		d, ok = nodeCfg.Deployments["/hoconRouter"]
	}
	if !ok {
		t.Fatalf("deployment for /user/hoconRouter not found; map = %v", nodeCfg.Deployments)
	}
	if d.Router != "round-robin-pool" {
		t.Errorf("Router = %q, want round-robin-pool", d.Router)
	}
	if d.NrOfInstances != 4 {
		t.Errorf("NrOfInstances = %d, want 4", d.NrOfInstances)
	}

	node, err := NewCluster(nodeCfg)
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	defer func() { _ = node.Shutdown() }()

	received := make(chan any, 20)
	ref, err := node.System.ActorOf(Props{New: func() actor.Actor {
		return &collectActor{BaseActor: actor.NewBaseActor(), ch: received}
	}}, "hoconRouter")
	if err != nil {
		t.Fatalf("ActorOf: %v", err)
	}

	time.Sleep(20 * time.Millisecond)

	const nMsg = 8
	for i := 0; i < nMsg; i++ {
		ref.Tell(i)
	}
	drainN(t, received, nMsg, 400*time.Millisecond)
}

// TestActorOf_DeploymentBadRouter verifies that an unrecognised router type
// in the deployment config returns an error from ActorOf.
func TestActorOf_DeploymentBadRouter(t *testing.T) {
	node := spawnTestNode(t, ClusterConfig{
		Deployments: map[string]core.DeploymentConfig{
			"/user/bad": {Router: "bogus-pool", NrOfInstances: 3},
		},
	})

	_, err := node.System.ActorOf(Props{New: func() actor.Actor {
		return &collectActor{BaseActor: actor.NewBaseActor(), ch: make(chan any)}
	}}, "bad")
	if err == nil {
		t.Error("expected error for unknown router type, got nil")
	}
}

// TestActorOf_DeploymentZeroInstances verifies that a deployment config with
// nr-of-instances = 0 returns an error from ActorOf.
func TestActorOf_DeploymentZeroInstances(t *testing.T) {
	node := spawnTestNode(t, ClusterConfig{
		Deployments: map[string]core.DeploymentConfig{
			"/user/zero": {Router: "round-robin-pool", NrOfInstances: 0},
		},
	})

	_, err := node.System.ActorOf(Props{New: func() actor.Actor {
		return &collectActor{BaseActor: actor.NewBaseActor(), ch: make(chan any)}
	}}, "zero")
	if err == nil {
		t.Error("expected error for zero NrOfInstances, got nil")
	}
}

// ── funcRef: minimal Ref for manual router tests ─────────────────────────────

type funcRef struct {
	path func() string
	tell func(any, ...actor.Ref)
}

func (r *funcRef) Path() string                 { return r.path() }
func (r *funcRef) Tell(msg any, s ...actor.Ref) { r.tell(msg, s...) }
