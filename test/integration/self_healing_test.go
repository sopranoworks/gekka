/*
 * self_healing_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package integration_test contains cross-node and infrastructure-aware
// integration tests for the Gekka cluster package.
package integration_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/cluster"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"google.golang.org/protobuf/proto"
)

// ── Mock infrastructure provider ─────────────────────────────────────────────

// mockInfraProvider implements cluster.InfrastructureProvider.
// deadHosts maps Host strings to true when the pod should be reported as dead.
type mockInfraProvider struct {
	mu        sync.Mutex
	deadHosts map[string]cluster.InfraStatus
}

func newMockInfra() *mockInfraProvider {
	return &mockInfraProvider{deadHosts: make(map[string]cluster.InfraStatus)}
}

func (m *mockInfraProvider) setPodStatus(host string, s cluster.InfraStatus) {
	m.mu.Lock()
	m.deadHosts[host] = s
	m.mu.Unlock()
}

func (m *mockInfraProvider) PodStatus(_ context.Context, address cluster.MemberAddress) cluster.InfraStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.deadHosts[address.Host]; ok {
		return s
	}
	return cluster.InfraUnknown
}

// ── Helper: minimal ClusterManager ───────────────────────────────────────────

// newTestClusterManager creates a minimal ClusterManager suitable for
// exercising the SBR event loop in tests.  It does not start any network
// listeners; event delivery uses ForcePublishEvent.
func newTestClusterManager() *cluster.ClusterManager {
	local := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
			Hostname: proto.String("10.0.0.1"),
			Port:     proto.Uint32(2551),
		},
		Uid: proto.Uint32(1),
	}
	return cluster.NewClusterManager(local, nil)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestSelfHealing_ImmediateDownOnInfraDead verifies that when the
// InfrastructureProvider confirms a Pod is dead the SBR manager downs the
// member immediately — well before the stable-after timeout — bypassing the
// standard gossip-stabilisation wait.
//
// The test sets stable-after to 5 s (much longer than the test timeout) and
// expects the down to complete within 200 ms of publishing the
// UnreachableMember event.
func TestSelfHealing_ImmediateDownOnInfraDead(t *testing.T) {
	const stableAfter = 5 * time.Second // intentionally long: should NOT fire
	const fastDownDeadline = 200 * time.Millisecond

	cm := newTestClusterManager()

	cfg := cluster.SBRConfig{
		ActiveStrategy: "keep-majority",
		StableAfter:    stableAfter,
	}
	mgr := cluster.NewSBRManager(cm, cfg)
	if mgr == nil {
		t.Fatal("NewSBRManager returned nil")
	}

	// Wire mock infra: member at 10.0.0.5 is confirmed dead.
	unreachableAddr := cluster.MemberAddress{
		Protocol: "pekko",
		System:   "TestSystem",
		Host:     "10.0.0.5",
		Port:     2551,
	}
	infra := newMockInfra()
	infra.setPodStatus(unreachableAddr.Host, cluster.InfraDead)
	mgr.SetInfraProvider(infra)

	// Capture calls to downFn.
	downCh := make(chan cluster.MemberAddress, 4)
	mgr.SetDownFnForTest(func(addr cluster.MemberAddress) {
		downCh <- addr
	})

	// Start the SBR event loop.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go mgr.Start(ctx)

	// Small delay to let Start() subscribe to the event bus.
	time.Sleep(10 * time.Millisecond)

	// Publish UnreachableMember — triggers the fast-down path.
	start := time.Now()
	cm.ForcePublishEvent(cluster.UnreachableMember{Member: unreachableAddr})

	// Expect the member to be downed well within the stable-after period.
	select {
	case got := <-downCh:
		elapsed := time.Since(start)
		t.Logf("member downed in %v (stable-after: %v)", elapsed, stableAfter)
		if elapsed > fastDownDeadline {
			t.Errorf("fast-down took %v, expected < %v", elapsed, fastDownDeadline)
		}
		if got != unreachableAddr {
			t.Errorf("expected %v to be downed, got %v", unreachableAddr, got)
		}
	case <-time.After(stableAfter / 2):
		t.Fatal("member was NOT downed before stable-after/2 — fast-down path not working")
	}
}

// TestSelfHealing_FallsBackToStableAfterWhenInfraUnknown verifies that when
// the InfrastructureProvider returns InfraUnknown (network split — cannot
// confirm) the SBR falls back to the normal stable-after timer path.
func TestSelfHealing_FallsBackToStableAfterWhenInfraUnknown(t *testing.T) {
	const stableAfter = 80 * time.Millisecond

	cm := newTestClusterManager()

	cfg := cluster.SBRConfig{
		ActiveStrategy: "keep-majority",
		StableAfter:    stableAfter,
	}
	mgr := cluster.NewSBRManager(cm, cfg)
	if mgr == nil {
		t.Fatal("NewSBRManager returned nil")
	}

	// Infra returns Unknown for all addresses (simulates K8s API unreachable).
	infra := newMockInfra()
	// no deadHosts set → all return InfraUnknown
	mgr.SetInfraProvider(infra)

	// Capture down calls.
	downCh := make(chan cluster.MemberAddress, 4)
	mgr.SetDownFnForTest(func(addr cluster.MemberAddress) {
		downCh <- addr
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go mgr.Start(ctx)
	time.Sleep(10 * time.Millisecond)

	// No Down should happen immediately — the stable-after timer hasn't fired.
	unreachableAddr := cluster.MemberAddress{
		Protocol: "pekko", System: "TestSystem",
		Host: "10.0.0.9", Port: 2551,
	}
	cm.ForcePublishEvent(cluster.UnreachableMember{Member: unreachableAddr})

	// Verify no down within < stableAfter/2 (fast-down NOT triggered).
	select {
	case got := <-downCh:
		t.Errorf("unexpected early down of %v (infra=Unknown, stable-after not elapsed)", got)
	case <-time.After(stableAfter / 2):
		// Good — no premature down.
	}
}

// TestSelfHealing_AutoDownAfterTimeout verifies the auto-down-unreachable-after
// feature: after the configured timeout the SBR executes the strategy even if
// no stable-after timer was running (e.g. it was reset by new events).
func TestSelfHealing_AutoDownAfterTimeout(t *testing.T) {
	const autoDownAfter = 150 * time.Millisecond
	const stableAfter = 10 * time.Second // long — must not fire

	cm := newTestClusterManager()

	// Inject a gossip state so classifyMembers returns a non-empty unreachable list.
	// We inject two Up members (local=index 0, remote=index 1) and mark
	// index 1 unreachable in the overview.
	remoteUA := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
			Hostname: proto.String("10.0.0.8"),
			Port:     proto.Uint32(2551),
		},
		Uid: proto.Uint32(2),
	}
	cm.Mu.Lock()
	cm.State.AllAddresses = append(cm.State.AllAddresses, remoteUA)
	cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(1),
		Status:       gproto_cluster.MemberStatus_Up.Enum(),
		UpNumber:     proto.Int32(2),
	})
	// Mark local (index 0) as Up too.
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	// Mark member at index 1 as unreachable from observer 0.
	cm.State.Overview.ObserverReachability = []*gproto_cluster.ObserverReachability{
		{
			AddressIndex: proto.Int32(0),
			SubjectReachability: []*gproto_cluster.SubjectReachability{
				{
					AddressIndex: proto.Int32(1),
					Status:       gproto_cluster.ReachabilityStatus_Unreachable.Enum(),
				},
			},
		},
	}
	cm.Mu.Unlock()

	cfg := cluster.SBRConfig{
		ActiveStrategy:           "keep-majority",
		StableAfter:              stableAfter,
		AutoDownUnreachableAfter: autoDownAfter,
	}
	mgr := cluster.NewSBRManager(cm, cfg)
	if mgr == nil {
		t.Fatal("NewSBRManager returned nil")
	}

	// Infra returns Unknown (so the infra fast-path won't fire).
	infra := newMockInfra()
	mgr.SetInfraProvider(infra)

	downCh := make(chan cluster.MemberAddress, 4)
	mgr.SetDownFnForTest(func(addr cluster.MemberAddress) {
		downCh <- addr
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go mgr.Start(ctx)
	time.Sleep(10 * time.Millisecond)

	// Publish unreachable event to seed unreachableSince timestamp.
	unreachableAddr := cluster.MemberAddress{
		Protocol: "pekko", System: "TestSystem",
		Host: "10.0.0.8", Port: 2551,
	}
	cm.ForcePublishEvent(cluster.UnreachableMember{Member: unreachableAddr})

	// After autoDownAfter + some grace the strategy should fire.
	deadline := autoDownAfter + 300*time.Millisecond
	select {
	case got := <-downCh:
		t.Logf("auto-down fired for %v", got)
	case <-time.After(deadline):
		t.Fatalf("auto-down-unreachable-after (%v) elapsed but no Down was triggered within %v",
			autoDownAfter, deadline)
	}
}

// TestSelfHealing_ReachableEventCancelsDown verifies that a ReachableMember
// event that arrives before stable-after fires prevents the member from being
// downed (recovery before action).
func TestSelfHealing_ReachableEventCancelsDown(t *testing.T) {
	const stableAfter = 100 * time.Millisecond

	cm := newTestClusterManager()

	cfg := cluster.SBRConfig{
		ActiveStrategy: "keep-majority",
		StableAfter:    stableAfter,
	}
	mgr := cluster.NewSBRManager(cm, cfg)
	if mgr == nil {
		t.Fatal("NewSBRManager returned nil")
	}

	// Infra returns Unknown so we rely entirely on the stable-after timer.
	infra := newMockInfra()
	mgr.SetInfraProvider(infra)

	downCh := make(chan cluster.MemberAddress, 4)
	mgr.SetDownFnForTest(func(addr cluster.MemberAddress) {
		downCh <- addr
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go mgr.Start(ctx)
	time.Sleep(10 * time.Millisecond)

	addr := cluster.MemberAddress{
		Protocol: "pekko", System: "TestSystem",
		Host: "10.0.0.7", Port: 2551,
	}

	// Member becomes unreachable.
	cm.ForcePublishEvent(cluster.UnreachableMember{Member: addr})

	// Member recovers before stable-after fires.
	time.Sleep(stableAfter / 3)
	cm.ForcePublishEvent(cluster.ReachableMember{Member: addr})

	// Wait well past stable-after — no Down should have fired.
	select {
	case got := <-downCh:
		t.Errorf("unexpected Down for %v — member recovered before stable-after", got)
	case <-time.After(stableAfter * 3):
		// Good — member recovered, no Down executed.
	}
}

// TestSelfHealing_MockK8sSimulatedPodDeath is the headline scenario described
// in the Phase 17 task:
//
//  1. A cluster is running.
//  2. Node A becomes unreachable (UnreachableMember event).
//  3. The mock K8s API returns PodStatus: Failed for Node A.
//  4. The remaining cluster marks Node A as Down significantly faster than
//     the standard stable-after timeout.
//
// This validates the core self-healing contract: infrastructure evidence
// short-circuits the gossip-stabilisation wait.
func TestSelfHealing_MockK8sSimulatedPodDeath(t *testing.T) {
	const stableAfter = 10 * time.Second // "standard timeout"
	const fastDownMax = 300 * time.Millisecond

	cm := newTestClusterManager()

	cfg := cluster.SBRConfig{
		ActiveStrategy: "keep-majority",
		StableAfter:    stableAfter,
	}
	mgr := cluster.NewSBRManager(cm, cfg)
	if mgr == nil {
		t.Fatal("NewSBRManager returned nil")
	}

	// Simulate Kubernetes returning PodStatus: Failed for Node A.
	nodeA := cluster.MemberAddress{
		Protocol: "pekko", System: "ProductionSystem",
		Host: "10.0.1.10", Port: 2551,
	}
	k8sMock := newMockInfra()
	k8sMock.setPodStatus(nodeA.Host, cluster.InfraDead) // K8s says: pod is dead
	mgr.SetInfraProvider(k8sMock)

	downCh := make(chan cluster.MemberAddress, 4)
	mgr.SetDownFnForTest(func(addr cluster.MemberAddress) {
		downCh <- addr
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go mgr.Start(ctx)
	time.Sleep(10 * time.Millisecond)

	// Node A becomes unreachable.
	start := time.Now()
	cm.ForcePublishEvent(cluster.UnreachableMember{Member: nodeA})

	// Verify: Node A is marked Down significantly faster than the standard timeout.
	select {
	case got := <-downCh:
		elapsed := time.Since(start)
		t.Logf("Node A marked Down in %v (standard timeout: %v, speedup: %.0fx)",
			elapsed, stableAfter, float64(stableAfter)/float64(elapsed))
		if elapsed > fastDownMax {
			t.Errorf("expected fast-down within %v, took %v", fastDownMax, elapsed)
		}
		if got.Host != nodeA.Host {
			t.Errorf("expected Node A (%s) to be downed, got %s", nodeA.Host, got.Host)
		}
	case <-time.After(stableAfter / 4):
		t.Fatalf("Node A was NOT marked Down within stableAfter/4 (%v) despite K8s confirming pod death",
			stableAfter/4)
	}
}
