//go:build integration

/*
 * operational_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package integration_test contains end-to-end integration tests that spin up
// real in-process cluster nodes.  Run with:
//
//	go test -tags integration -v ./test/integration/
package integration_test

import (
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	gekka "github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/internal/core"
	"github.com/sopranoworks/gekka/management/client"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
)

// freePort returns an available TCP port on localhost by briefly listening
// on :0 and immediately releasing the listener.  There is a small TOCTOU
// window, but it is acceptable in a test context.
func freePort(t *testing.T) uint32 {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return uint32(port)
}

// waitHTTP polls url until it returns HTTP 200 or the deadline is exceeded.
func waitHTTP(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url) //nolint:gosec // test-only URL
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("waitHTTP: %s did not return 200 within %s", url, timeout)
}

// waitMembers polls the management client until memberCount members are Up,
// or the timeout is exceeded.  Returns the member list on success.
func waitMembers(t *testing.T, c *client.Client, memberCount int, timeout time.Duration) []client.MemberInfo {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		members, err := c.Members()
		if err == nil {
			up := 0
			for _, m := range members {
				if m.Status == "Up" {
					up++
				}
			}
			if up >= memberCount {
				return members
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("waitMembers: cluster did not reach %d Up members within %s", memberCount, timeout)
	return nil
}

// TestMetricsNodeAndCLISynergy verifies the interaction between a gekka-metrics
// style monitoring node and the Management HTTP API used by gekka-cli.
//
// Topology:
//
//	Seed node  (artery: seedPort,  management HTTP: mgmtPort)
//	Metrics node (artery: metricsPort, role: "metrics-exporter")
//
// Assertions:
//  1. Both nodes reach Up status as observed via the Management API.
//  2. The seed node has no application roles (only the implicit dc-default).
//  3. The metrics node advertises the "metrics-exporter" role.
//  4. The Management API returns the expected JSON shape (used by gekka-cli).
func TestMetricsNodeAndCLISynergy(t *testing.T) {
	const systemName = "ClusterSystem"

	// ── Allocate free ports ────────────────────────────────────────────────
	seedPort := freePort(t)
	metricsPort := freePort(t)
	mgmtPort := freePort(t)

	seedAddr := actor.Address{
		Protocol: "pekko",
		System:   systemName,
		Host:     "127.0.0.1",
		Port:     int(seedPort),
	}

	// ── Start seed node with management HTTP ──────────────────────────────
	seedCfg := gekka.ClusterConfig{
		Address:    seedAddr,
		SystemName: systemName,
		SeedNodes:  []actor.Address{seedAddr}, // self as seed
		Management: core.ManagementConfig{
			Enabled:             true,
			Hostname:            "127.0.0.1",
			Port:                int(mgmtPort),
			HealthChecksEnabled: true,
		},
	}

	seed, err := gekka.NewCluster(seedCfg)
	if err != nil {
		t.Fatalf("seed NewCluster: %v", err)
	}
	t.Cleanup(func() { _ = seed.Shutdown() })

	if err := seed.Join("127.0.0.1", seedPort); err != nil {
		t.Fatalf("seed self-join: %v", err)
	}

	mgmtURL := fmt.Sprintf("http://127.0.0.1:%d", mgmtPort)
	waitHTTP(t, mgmtURL+"/health/alive", 10*time.Second)
	t.Logf("seed management API ready at %s", mgmtURL)

	// ── Start metrics node (mirrors gekka-metrics behaviour) ──────────────
	metricsAddr := actor.Address{
		Protocol: "pekko",
		System:   systemName,
		Host:     "127.0.0.1",
		Port:     int(metricsPort),
	}
	metricsCfg := gekka.ClusterConfig{
		Address:    metricsAddr,
		SystemName: systemName,
		SeedNodes:  []actor.Address{seedAddr},
		Roles:      []string{"metrics-exporter"},
	}

	metricsNode, err := gekka.NewCluster(metricsCfg)
	if err != nil {
		t.Fatalf("metrics NewCluster: %v", err)
	}
	t.Cleanup(func() { _ = metricsNode.Shutdown() })

	if err := metricsNode.JoinSeeds(); err != nil {
		t.Fatalf("metrics JoinSeeds: %v", err)
	}
	t.Logf("metrics node joining at port %d", metricsPort)

	// ── Wait for both nodes to be Up ──────────────────────────────────────
	c := client.New(mgmtURL)
	members := waitMembers(t, c, 2, 30*time.Second)
	t.Logf("cluster reached 2 Up members")

	// ── Assertions ────────────────────────────────────────────────────────

	// Assertion 1: exactly 2 members, both Up.
	if got := len(members); got != 2 {
		t.Errorf("member count = %d, want 2", got)
	}
	for _, m := range members {
		if m.Status != "Up" {
			t.Errorf("member %s status = %q, want Up", m.Address, m.Status)
		}
	}

	// Assertion 2 & 3: locate each node by port; verify roles.
	seedExpected := fmt.Sprintf("pekko://%s@127.0.0.1:%d", systemName, seedPort)
	metricsExpected := fmt.Sprintf("pekko://%s@127.0.0.1:%d", systemName, metricsPort)

	findMember := func(addr string) (client.MemberInfo, bool) {
		for _, m := range members {
			if m.Address == addr {
				return m, true
			}
		}
		return client.MemberInfo{}, false
	}

	seedMember, ok := findMember(seedExpected)
	if !ok {
		t.Errorf("seed member %q not found in member list", seedExpected)
	} else {
		for _, r := range seedMember.Roles {
			if r == "metrics-exporter" {
				t.Errorf("seed member unexpectedly has metrics-exporter role")
			}
		}
		t.Logf("seed member roles: %v", seedMember.Roles)
	}

	metricsMember, ok := findMember(metricsExpected)
	if !ok {
		t.Errorf("metrics member %q not found in member list", metricsExpected)
	} else {
		hasRole := false
		for _, r := range metricsMember.Roles {
			if r == "metrics-exporter" {
				hasRole = true
			}
		}
		if !hasRole {
			t.Errorf("metrics member roles %v do not contain metrics-exporter", metricsMember.Roles)
		}
		t.Logf("metrics member roles: %v", metricsMember.Roles)
	}

	// Assertion 4: management client returns the same JSON used by gekka-cli.
	// Verify reachability flag is set (both nodes should see each other as reachable).
	// Gossip reachability tables converge in the background; poll until all members
	// are reachable or the deadline is exceeded.
	var allReachable bool
	reachDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(reachDeadline) {
		members = waitMembers(t, c, 2, 5*time.Second)
		allReachable = true
		for _, m := range members {
			if !m.Reachable {
				allReachable = false
				break
			}
		}
		if allReachable {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	for _, m := range members {
		if !m.Reachable {
			t.Errorf("member %s is not reachable", m.Address)
		}
	}

	// Assertion 5: the gossip observed by the metrics node itself also shows
	// >= 2 Up members (verifies the direct gossip read path used by gekka-metrics).
	// The metrics node's gossip state lags  the seed by up to one gossip cycle;
	// poll until convergence rather than reading the state only once.
	mcm := metricsNode.ClusterManager()
	var upInGossip int
	gossipDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(gossipDeadline) {
		mcm.Mu.RLock()
		gossip := mcm.State
		upInGossip = 0
		if gossip != nil {
			for _, m := range gossip.GetMembers() {
				if m.GetStatus() == gproto_cluster.MemberStatus_Up {
					upInGossip++
				}
			}
		}
		mcm.Mu.RUnlock()
		if upInGossip >= 2 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if upInGossip < 2 {
		t.Errorf("metrics node gossip shows %d Up members, want >= 2", upInGossip)
	}
	t.Logf("metrics node gossip: %d Up members", upInGossip)
}
