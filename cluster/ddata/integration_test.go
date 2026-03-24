/*
 * integration_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"encoding/json"
	"slices"
	"testing"
	"time"
)

// ── gossip helpers ────────────────────────────────────────────────────────────
//
// These helpers replicate exactly what Replicator.gossipAll does over the
// network: encode the local CRDT snapshot into a ReplicatorMsg and call
// dst.HandleIncoming — simulating a one-way push from src to dst.
//
// Using HandleIncoming rather than direct struct access keeps the test honest:
// it exercises the full serialisation/deserialisation path that production
// gossip uses.

// gossipLWWMap pushes src's LWWMap at mapKey to dst.
func gossipLWWMap(t *testing.T, src, dst *Replicator, mapKey string) {
	t.Helper()
	payload, err := json.Marshal(LWWMapPayload{State: src.LWWMap(mapKey).Snapshot()})
	if err != nil {
		t.Fatalf("gossipLWWMap: marshal: %v", err)
	}
	raw, err := json.Marshal(ReplicatorMsg{Type: "lwwmap-gossip", Key: mapKey, Payload: payload})
	if err != nil {
		t.Fatalf("gossipLWWMap: marshal envelope: %v", err)
	}
	if err := dst.HandleIncoming(raw); err != nil {
		t.Fatalf("gossipLWWMap: HandleIncoming: %v", err)
	}
}

// gossipORSet pushes src's ORSet at setKey to dst.
func gossipORSet(t *testing.T, src, dst *Replicator, setKey string) {
	t.Helper()
	snap := src.ORSet(setKey).Snapshot()
	payload, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("gossipORSet: marshal: %v", err)
	}
	raw, err := json.Marshal(ReplicatorMsg{Type: "orset-gossip", Key: setKey, Payload: payload})
	if err != nil {
		t.Fatalf("gossipORSet: marshal envelope: %v", err)
	}
	if err := dst.HandleIncoming(raw); err != nil {
		t.Fatalf("gossipORSet: HandleIncoming: %v", err)
	}
}

// ── TestIntegration_ConfigAndServiceDiscovery ─────────────────────────────────

// TestIntegration_ConfigAndServiceDiscovery is the primary Phase 11/12
// integration test.  It validates cross-node convergence of both
// ConfigRegistry (LWWMap) and ServiceDiscovery (ORSet) after a single
// gossip round.
//
// Scenario:
//  1. Two in-process Replicators (Node A, Node B) with nil routers — gossip is
//     driven manually via the gossip helpers above.
//  2. Node A updates config key "log-level" to "DEBUG" via ConfigRegistry.
//  3. Node B registers service "order-processor" at "localhost:8080" via
//     ServiceDiscovery.
//  4. Before gossip: Node B cannot see "log-level"; Node A cannot see
//     "order-processor".
//  5. Manual gossip round: A→B for the config LWWMap; B→A for the service ORSet.
//  6. After gossip: Node B sees "log-level=DEBUG"; Node A sees
//     "order-processor" at "localhost:8080".
func TestIntegration_ConfigAndServiceDiscovery(t *testing.T) {
	rA := NewReplicator("node-A", nil)
	rB := NewReplicator("node-B", nil)

	cfgA := NewConfigRegistry(rA, "cluster-config")
	cfgB := NewConfigRegistry(rB, "cluster-config")
	sdA := NewServiceDiscovery(rA)
	sdB := NewServiceDiscovery(rB)

	// ── Writes ────────────────────────────────────────────────────────────
	cfgA.UpdateConfig("log-level", "DEBUG")
	sdB.RegisterService("order-processor", "localhost:8080")

	// ── Pre-gossip: isolation check ───────────────────────────────────────
	if _, ok := cfgB.GetConfig("log-level"); ok {
		t.Error("pre-gossip: Node B must not see log-level before sync")
	}
	if addrs := sdA.LookupService("order-processor"); len(addrs) != 0 {
		t.Errorf("pre-gossip: Node A must not see order-processor before sync, got %v", addrs)
	}

	// ── Manual gossip round ───────────────────────────────────────────────
	gossipLWWMap(t, rA, rB, cfgA.MapKey())    // A→B: config
	gossipORSet(t, rB, rA, "order-processor") // B→A: service

	// ── Post-gossip: convergence assertions ───────────────────────────────
	val, ok := cfgB.GetConfig("log-level")
	if !ok {
		t.Fatal("post-gossip: Node B must see log-level after sync")
	}
	if val != "DEBUG" {
		t.Errorf("Node B: expected log-level=DEBUG, got %v", val)
	}

	addrs := sdA.LookupService("order-processor")
	if len(addrs) != 1 || addrs[0] != "localhost:8080" {
		t.Errorf("Node A: expected order-processor@localhost:8080, got %v", addrs)
	}
}

// ── TestIntegration_BidirectionalConfigGossip ─────────────────────────────────

// TestIntegration_BidirectionalConfigGossip verifies that two nodes can each
// update different config keys and converge to a complete view after a
// bidirectional gossip exchange.
func TestIntegration_BidirectionalConfigGossip(t *testing.T) {
	rA := NewReplicator("node-A", nil)
	rB := NewReplicator("node-B", nil)

	cfgA := NewConfigRegistry(rA, "cluster-config")
	cfgB := NewConfigRegistry(rB, "cluster-config")

	cfgA.UpdateConfig("log-level", "INFO")
	cfgA.UpdateConfig("max-connections", 100)
	cfgB.UpdateConfig("timeout-ms", 5000)

	// Bidirectional gossip: A→B then B→A.
	gossipLWWMap(t, rA, rB, cfgA.MapKey())
	gossipLWWMap(t, rB, rA, cfgB.MapKey())

	// Both nodes must see all three keys.
	for _, cfg := range []*ConfigRegistry{cfgA, cfgB} {
		if v, ok := cfg.GetConfig("log-level"); !ok || v != "INFO" {
			t.Errorf("expected log-level=INFO, got %v (ok=%v)", v, ok)
		}
		if v, ok := cfg.GetConfig("timeout-ms"); !ok {
			t.Errorf("expected timeout-ms to be present, got %v (ok=%v)", v, ok)
		}
	}

	// Verify Node A sees timeout-ms written by B (value survives JSON round-trip
	// as float64 for numeric types).
	v, ok := cfgA.GetConfig("timeout-ms")
	if !ok {
		t.Fatal("Node A: missing timeout-ms after bidirectional gossip")
	}
	// JSON unmarshals numbers as float64 when target type is any.
	if vf, _ := v.(float64); vf != 5000 {
		t.Errorf("Node A: expected timeout-ms=5000, got %v (%T)", v, v)
	}
}

// ── TestIntegration_MultiNodeServiceDiscovery ─────────────────────────────────

// TestIntegration_MultiNodeServiceDiscovery exercises multiple services and
// multiple addresses, including unregistration and propagation of removes.
func TestIntegration_MultiNodeServiceDiscovery(t *testing.T) {
	rA := NewReplicator("node-A", nil)
	rB := NewReplicator("node-B", nil)

	sdA := NewServiceDiscovery(rA)
	sdB := NewServiceDiscovery(rB)

	// A registers two addresses for "api-gateway".
	sdA.RegisterService("api-gateway", "10.0.0.1:443")
	sdA.RegisterService("api-gateway", "10.0.0.2:443")

	// B registers one address for "api-gateway" and one for "auth-service".
	sdB.RegisterService("api-gateway", "10.0.0.3:443")
	sdB.RegisterService("auth-service", "10.0.0.5:9090")

	// Gossip api-gateway ORSet in both directions so both nodes see all three
	// addresses.
	gossipORSet(t, rA, rB, "api-gateway")
	gossipORSet(t, rB, rA, "api-gateway")

	// Gossip auth-service from B to A.
	gossipORSet(t, rB, rA, "auth-service")

	// Both nodes must see all three api-gateway addresses.
	for label, sd := range map[string]*ServiceDiscovery{"A": sdA, "B": sdB} {
		addrs := sd.LookupService("api-gateway")
		if len(addrs) != 3 {
			t.Errorf("Node %s: expected 3 api-gateway addresses, got %d: %v", label, len(addrs), addrs)
		}
		for _, want := range []string{"10.0.0.1:443", "10.0.0.2:443", "10.0.0.3:443"} {
			if !slices.Contains(addrs, want) {
				t.Errorf("Node %s: missing api-gateway address %s", label, want)
			}
		}
	}

	// Node A must see auth-service (propagated from B).
	authAddrs := sdA.LookupService("auth-service")
	if len(authAddrs) != 1 || authAddrs[0] != "10.0.0.5:9090" {
		t.Errorf("Node A: expected auth-service@10.0.0.5:9090, got %v", authAddrs)
	}

	// Unregister one api-gateway address on A, then gossip to B.
	sdA.UnregisterService("api-gateway", "10.0.0.1:443")
	gossipORSet(t, rA, rB, "api-gateway")

	// B must no longer see the unregistered address.
	addrsB := sdB.LookupService("api-gateway")
	if slices.Contains(addrsB, "10.0.0.1:443") {
		t.Errorf("Node B: unregistered address 10.0.0.1:443 still visible after gossip")
	}
	if len(addrsB) != 2 {
		t.Errorf("Node B: expected 2 api-gateway addresses after unregister, got %d: %v", len(addrsB), addrsB)
	}
}

// ── TestIntegration_LWWConflictResolution ─────────────────────────────────────

// TestIntegration_LWWConflictResolution verifies that when both nodes write
// the same config key, the entry with the higher timestamp wins after gossip.
func TestIntegration_LWWConflictResolution(t *testing.T) {
	rA := NewReplicator("node-A", nil)
	rB := NewReplicator("node-B", nil)

	cfgA := NewConfigRegistry(rA, "cluster-config")
	cfgB := NewConfigRegistry(rB, "cluster-config")

	// A writes first.
	cfgA.UpdateConfig("log-level", "WARN")
	// B writes second; sleep ensures a strictly higher wall-clock timestamp.
	time.Sleep(time.Millisecond)
	cfgB.UpdateConfig("log-level", "ERROR")

	// Bidirectional gossip.
	gossipLWWMap(t, rA, rB, cfgA.MapKey())
	gossipLWWMap(t, rB, rA, cfgB.MapKey())

	// Both nodes should converge to the higher-timestamp value ("ERROR").
	for label, cfg := range map[string]*ConfigRegistry{"A": cfgA, "B": cfgB} {
		v, ok := cfg.GetConfig("log-level")
		if !ok {
			t.Fatalf("Node %s: log-level missing after gossip", label)
		}
		if v != "ERROR" {
			t.Errorf("Node %s: expected log-level=ERROR (LWW), got %v", label, v)
		}
	}
}
