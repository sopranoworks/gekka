/*
 * actor_system_group_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/internal/core"
)

// ── DeploymentToGroupRouter ───────────────────────────────────────────────────

func TestDeploymentToGroupRouter_RoundRobin(t *testing.T) {
	d := core.DeploymentConfig{
		Router:       "round-robin-group",
		RouteesPaths: []string{"/user/w1", "/user/w2"},
	}
	g, err := core.DeploymentToGroupRouter(nil, d)
	if err != nil {
		t.Fatalf("DeploymentToGroupRouter: %v", err)
	}
	if g == nil {
		t.Fatal("group router is nil")
	}
	if got := g.(*actor.GroupRouter).RouteePathsForTest(); len(got) != 2 {
		t.Errorf("routee paths = %v, want 2", got)
	}
}

func TestDeploymentToGroupRouter_Random(t *testing.T) {
	d := core.DeploymentConfig{
		Router:       "random-group",
		RouteesPaths: []string{"/user/a", "/user/b", "/user/c"},
	}
	g, err := core.DeploymentToGroupRouter(nil, d)
	if err != nil {
		t.Fatalf("DeploymentToGroupRouter: %v", err)
	}
	if g == nil {
		t.Fatal("group router is nil")
	}
}

func TestDeploymentToGroupRouter_EmptyPaths_Error(t *testing.T) {
	d := core.DeploymentConfig{Router: "round-robin-group", RouteesPaths: nil}
	_, err := core.DeploymentToGroupRouter(nil, d)
	if err == nil {
		t.Error("expected error for empty RouteesPaths, got nil")
	}
}

func TestDeploymentToGroupRouter_UnknownRouter_Error(t *testing.T) {
	d := core.DeploymentConfig{Router: "bogus-group", RouteesPaths: []string{"/user/a"}}
	_, err := core.DeploymentToGroupRouter(nil, d)
	if err == nil {
		t.Error("expected error for unknown router type, got nil")
	}
}

// ── isGroupRouter ─────────────────────────────────────────────────────────────

func TestIsGroupRouter(t *testing.T) {
	cases := []struct {
		routerType string
		want       bool
	}{
		{"round-robin-group", true},
		{"random-group", true},
		{"round-robin-pool", false},
		{"random-pool", false},
		{"group", true}, // ends with "-group"? No — "group" doesn't end with "-group". Actually it doesn't.
		{"", false},
	}
	// Rewrite the edge case: "group" ends in "group" but not "-group"
	cases[4] = struct {
		routerType string
		want       bool
	}{"my-group", true}

	for _, tc := range cases {
		got := core.IsGroupRouter(tc.routerType)
		if got != tc.want {
			t.Errorf("isGroupRouter(%q) = %v, want %v", tc.routerType, got, tc.want)
		}
	}
}

// ── ActorOf group router auto-provisioning ────────────────────────────────────

// TestActorOf_GroupRouter_HOCON_DeploymentAndRouting is the primary integration
// test: workers are registered first, then a group router is auto-provisioned
// from a HOCON deployment config. Messages sent to the group ref reach the workers.
func TestActorOf_GroupRouter_HOCON_DeploymentAndRouting(t *testing.T) {
	const hoconText = `
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 0 }
  actor.deployment {
    "/user/myGroup" {
      router        = round-robin-group
      routees.paths = ["/user/worker1", "/user/worker2"]
    }
  }
}
`
	nodeCfg, err := parseHOCONString(hoconText)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	nodeCfg.Port = 0

	node, err := NewCluster(nodeCfg)
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	defer func() { _ = node.Shutdown() }()

	received := make(chan any, 16)

	// Register the worker actors FIRST so that GroupRouter.PreStart can resolve them.
	_, err = node.System.ActorOf(Props{New: func() actor.Actor {
		return &collectActor{BaseActor: actor.NewBaseActor(), ch: received}
	}}, "worker1")
	if err != nil {
		t.Fatalf("ActorOf worker1: %v", err)
	}
	_, err = node.System.ActorOf(Props{New: func() actor.Actor {
		return &collectActor{BaseActor: actor.NewBaseActor(), ch: received}
	}}, "worker2")
	if err != nil {
		t.Fatalf("ActorOf worker2: %v", err)
	}

	// Auto-provision the group router (no Props.New needed).
	groupRef, err := node.System.ActorOf(Props{}, "myGroup")
	if err != nil {
		t.Fatalf("ActorOf myGroup: %v", err)
	}
	if groupRef.Path() == "" {
		t.Fatal("groupRef.Path() is empty")
	}

	// Allow PreStart goroutine to run and resolve routee paths.
	time.Sleep(30 * time.Millisecond)

	const nMsg = 6
	for i := 0; i < nMsg; i++ {
		groupRef.Tell(i)
	}

	got := drainN(t, received, nMsg, 500*time.Millisecond)
	if len(got) != nMsg {
		return // drainN already reported
	}
}

// TestActorOf_GroupRouter_DirectRef exercises group router created with explicit
// Ref values (no path resolution needed).
func TestActorOf_GroupRouter_DirectRef(t *testing.T) {
	node := spawnTestNode(t, ClusterConfig{})

	received := make(chan any, 10)

	// Register workers.
	w1, _ := node.System.ActorOf(Props{New: func() actor.Actor {
		return &collectActor{BaseActor: actor.NewBaseActor(), ch: received}
	}}, "direct-w1")
	w2, _ := node.System.ActorOf(Props{New: func() actor.Actor {
		return &collectActor{BaseActor: actor.NewBaseActor(), ch: received}
	}}, "direct-w2")

	// Build group router manually (no deployment config needed).
	group := actor.NewGroupRouter(&actor.RoundRobinRoutingLogic{}, []actor.Ref{w1, w2})
	groupRef, err := node.System.ActorOf(Props{New: func() actor.Actor { return group }}, "directGroup")
	if err != nil {
		t.Fatalf("ActorOf directGroup: %v", err)
	}

	for i := 0; i < 4; i++ {
		groupRef.Tell(i)
	}
	drainN(t, received, 4, 300*time.Millisecond)
}

// TestActorOf_GroupRouter_NilProps verifies that ActorOf with Props{} (no New)
// succeeds for a group router deployment and returns a non-empty ref.
func TestActorOf_GroupRouter_NilProps(t *testing.T) {
	node := spawnTestNode(t, ClusterConfig{
		Deployments: map[string]core.DeploymentConfig{
			"/user/nilGroup": {
				Router:       "round-robin-group",
				RouteesPaths: []string{"/user/x"},
			},
		},
	})

	ref, err := node.System.ActorOf(Props{}, "nilGroup")
	if err != nil {
		t.Fatalf("ActorOf with Props{}: %v", err)
	}
	if ref.Path() == "" {
		t.Error("ref.Path() is empty, expected group router to be registered")
	}
}

// TestActorOf_GroupRouter_Broadcast verifies Broadcast delivers to all routees.
func TestActorOf_GroupRouter_Broadcast(t *testing.T) {
	node := spawnTestNode(t, ClusterConfig{
		Deployments: map[string]core.DeploymentConfig{
			"/user/bcastGroup": {
				Router:       "round-robin-group",
				RouteesPaths: []string{"/user/bc1", "/user/bc2", "/user/bc3"},
			},
		},
	})

	received := make(chan any, 20)
	for _, name := range []string{"bc1", "bc2", "bc3"} {
		_, _ = node.System.ActorOf(Props{New: func() actor.Actor {
			return &collectActor{BaseActor: actor.NewBaseActor(), ch: received}
		}}, name)
	}

	groupRef, err := node.System.ActorOf(Props{}, "bcastGroup")
	if err != nil {
		t.Fatalf("ActorOf bcastGroup: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	groupRef.Tell(actor.Broadcast{Message: "all"})
	drainN(t, received, 3, 400*time.Millisecond)
}

// TestActorOf_GroupRouter_RandomRouting verifies random-group type resolves.
func TestActorOf_GroupRouter_RandomRouting(t *testing.T) {
	node := spawnTestNode(t, ClusterConfig{
		Deployments: map[string]core.DeploymentConfig{
			"/user/randGroup": {
				Router:       "random-group",
				RouteesPaths: []string{"/user/rw1", "/user/rw2"},
			},
		},
	})

	received := make(chan any, 20)
	for _, name := range []string{"rw1", "rw2"} {
		_, _ = node.System.ActorOf(Props{New: func() actor.Actor {
			return &collectActor{BaseActor: actor.NewBaseActor(), ch: received}
		}}, name)
	}

	groupRef, err := node.System.ActorOf(Props{}, "randGroup")
	if err != nil {
		t.Fatalf("ActorOf randGroup: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	for i := 0; i < 8; i++ {
		groupRef.Tell(i)
	}
	drainN(t, received, 8, 400*time.Millisecond)
}
