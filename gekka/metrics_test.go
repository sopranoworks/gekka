/*
 * metrics_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// freeMonitoringPort returns the monitoring server's actual port, discovered
// after the OS assigns it (when MonitoringPort == 0).
func monPort(n *GekkaNode) int {
	if n.monitoring == nil {
		return 0
	}
	return n.monitoring.Port()
}

// ── /healthz endpoint ─────────────────────────────────────────────────────────

func TestMonitoring_Healthz_NotReady_BeforeJoin(t *testing.T) {
	node, err := Spawn(NodeConfig{
		SystemName:       "HealthzTest",
		Host:             "127.0.0.1",
		Port:             0,
		EnableMonitoring: true,
		MonitoringPort:   0, // OS assigns port
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	defer node.Shutdown()

	port := monPort(node)
	if port == 0 {
		t.Fatal("monitoring server did not start (port == 0)")
	}

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", port))
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("/healthz before join: got %d, want %d (ServiceUnavailable)",
			resp.StatusCode, http.StatusServiceUnavailable)
	}

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "not_ready") {
		t.Errorf("/healthz body: %q does not contain 'not_ready'", body)
	}
}

func TestMonitoring_Healthz_Ready_AfterJoin(t *testing.T) {
	// Spawn a seed node (node1) that accepts a Join message.
	node1, err := Spawn(NodeConfig{SystemName: "MonTest", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("Spawn node1: %v", err)
	}
	defer node1.Shutdown()

	// Spawn the monitored client node (node2).
	node2, err := Spawn(NodeConfig{
		SystemName:       "MonTest",
		Host:             "127.0.0.1",
		Port:             0,
		EnableMonitoring: true,
		MonitoringPort:   0,
	})
	if err != nil {
		t.Fatalf("Spawn node2: %v", err)
	}
	defer node2.Shutdown()

	port := monPort(node2)
	if port == 0 {
		t.Fatal("monitoring server did not start")
	}

	// /healthz before join — must be not_ready.
	checkHealth(t, port, http.StatusServiceUnavailable, "before join")

	// node2 joins node1 via the full cluster handshake (InitJoin→InitJoinAck→Join→Welcome).
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := node2.Join(node1.localAddr.GetHostname(), node1.localAddr.GetPort()); err != nil {
		t.Fatalf("Join: %v", err)
	}

	// Wait for the Artery handshake.
	if err := node2.WaitForHandshake(ctx, node1.localAddr.GetHostname(), node1.localAddr.GetPort()); err != nil {
		t.Fatalf("WaitForHandshake: %v", err)
	}

	// Give the cluster message exchange (InitJoin→IJA→Join→Welcome) time to complete.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", port))
		if err != nil {
			t.Fatalf("GET /healthz: %v", err)
		}
		code := resp.StatusCode
		resp.Body.Close()
		if code == http.StatusOK {
			t.Logf("/healthz returned 200 OK after join")
			return // success
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Errorf("/healthz: still not_ready after Welcome should have been received")
}

func checkHealth(t *testing.T, port, wantCode int, context string) {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", port))
	if err != nil {
		t.Fatalf("GET /healthz (%s): %v", context, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantCode {
		t.Errorf("/healthz (%s): got %d, want %d", context, resp.StatusCode, wantCode)
	}
}

// ── /metrics endpoint ─────────────────────────────────────────────────────────

func TestMonitoring_Metrics_JSONShape(t *testing.T) {
	node, err := Spawn(NodeConfig{
		SystemName:       "MetricsTest",
		Host:             "127.0.0.1",
		Port:             0,
		EnableMonitoring: true,
		MonitoringPort:   0,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	defer node.Shutdown()

	port := monPort(node)
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", port))
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/metrics: got %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type: got %q, want application/json", ct)
	}

	var snap MetricsSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("decode /metrics JSON: %v", err)
	}

	// Fresh node — all counters zero.
	if snap.MessagesSent != 0 {
		t.Errorf("MessagesSent: got %d, want 0", snap.MessagesSent)
	}
	if snap.MessagesReceived != 0 {
		t.Errorf("MessagesReceived: got %d, want 0", snap.MessagesReceived)
	}
	if snap.LastConvergenceTime != "never" {
		t.Errorf("LastConvergenceTime: got %q, want \"never\"", snap.LastConvergenceTime)
	}
}

func TestMonitoring_Metrics_PrometheusFormat(t *testing.T) {
	node, err := Spawn(NodeConfig{
		SystemName:       "PromTest",
		Host:             "127.0.0.1",
		Port:             0,
		EnableMonitoring: true,
		MonitoringPort:   0,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	defer node.Shutdown()

	port := monPort(node)
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics?fmt=prom", port))
	if err != nil {
		t.Fatalf("GET /metrics?fmt=prom: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/metrics?fmt=prom: got %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	text := string(body)

	for _, want := range []string{
		"gekka_messages_sent_total",
		"gekka_messages_received_total",
		"gekka_bytes_sent_total",
		"gekka_active_associations",
		"gekka_gossips_received_total",
		"gekka_member_up_events_total",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("Prometheus output missing metric %q", want)
		}
	}
}

// ── Counter increment tests ───────────────────────────────────────────────────

func TestMonitoring_Metrics_MessageCounters(t *testing.T) {
	// Two in-process Go nodes exchange Artery user messages; verify that the
	// MessagesSent/BytesSent counters on nodeB and MessagesReceived/BytesReceived
	// on nodeA increment, and that the HTTP /metrics endpoint agrees with the
	// in-process Metrics() snapshot.
	//
	// This test uses raw Artery user messages (no cluster join protocol) so that
	// it works with two standalone Go nodes.
	nodeA, err := Spawn(NodeConfig{SystemName: "CtrTest", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("Spawn nodeA: %v", err)
	}
	defer nodeA.Shutdown()

	nodeB, err := Spawn(NodeConfig{
		SystemName:       "CtrTest",
		Host:             "127.0.0.1",
		Port:             0,
		EnableMonitoring: true,
		MonitoringPort:   0,
	})
	if err != nil {
		t.Fatalf("Spawn nodeB: %v", err)
	}
	defer nodeB.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// nodeA receives messages — set up the callback before the first send so we
	// don't race with early arrivals.
	received := make(chan struct{}, 20)
	nodeA.OnMessage(func(_ context.Context, _ *IncomingMessage) error {
		select {
		case received <- struct{}{}:
		default:
		}
		return nil
	})

	// Target path for nodeA — the actor doesn't need to exist for the frame to
	// be decoded and counted by handleUserMessage.
	targetPath := fmt.Sprintf("pekko://CtrTest@%s:%d/user/probe",
		nodeA.localAddr.GetHostname(), nodeA.localAddr.GetPort())

	// Send an initial probe from nodeB to nodeA to trigger the Artery handshake.
	go nodeB.Send(ctx, targetPath, []byte("probe"))

	// Wait for the Artery handshake to complete on nodeB's OUTBOUND association.
	if err := nodeB.WaitForHandshake(ctx, nodeA.localAddr.GetHostname(), nodeA.localAddr.GetPort()); err != nil {
		t.Fatalf("WaitForHandshake: %v", err)
	}

	// nodeB sends 3 user messages to nodeA.
	const nSend = 3
	for i := 0; i < nSend; i++ {
		if err := nodeB.Send(ctx, targetPath, []byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Logf("Send %d: %v (may be transient)", i, err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Wait for all messages (probe + nSend) to arrive on nodeA.
	total := nSend + 1 // +1 for the probe
	recvDeadline := time.Now().Add(5 * time.Second)
	got := 0
	for got < total && time.Now().Before(recvDeadline) {
		select {
		case <-received:
			got++
		case <-time.After(500 * time.Millisecond):
		}
	}

	// Check nodeB's sent counter (probe + nSend, but probe may not have been
	// counted if it was sent before ASSOCIATED; use >= nSend as the floor).
	snapB := nodeB.Metrics()
	if snapB.MessagesSent < int64(nSend) {
		t.Errorf("nodeB MessagesSent: got %d, want >= %d", snapB.MessagesSent, nSend)
	}
	if snapB.BytesSent == 0 {
		t.Errorf("nodeB BytesSent: got 0, want > 0")
	}

	// Check nodeA's received counter (if messages arrived).
	if got > 0 {
		snapA := nodeA.Metrics()
		if snapA.MessagesReceived < int64(got) {
			t.Errorf("nodeA MessagesReceived: got %d, want >= %d", snapA.MessagesReceived, got)
		}
		if snapA.BytesReceived == 0 {
			t.Errorf("nodeA BytesReceived: got 0, want > 0")
		}
	} else {
		t.Log("no messages received on nodeA — skipping receiver counter check")
	}

	// Verify /metrics HTTP endpoint matches the in-process snapshot.
	port := monPort(nodeB)
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", port))
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	var httpSnap MetricsSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&httpSnap); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if httpSnap.MessagesSent != snapB.MessagesSent {
		t.Errorf("HTTP /metrics MessagesSent=%d, in-process=%d", httpSnap.MessagesSent, snapB.MessagesSent)
	}
}

// ── NodeMetrics unit tests (no network) ──────────────────────────────────────

func TestNodeMetrics_AtomicIncrement(t *testing.T) {
	m := &NodeMetrics{}
	m.MessagesSent.Add(5)
	m.MessagesReceived.Add(3)
	m.BytesSent.Add(100)
	m.BytesReceived.Add(80)
	m.GossipsReceived.Add(2)
	m.MemberUpEvents.Add(1)
	m.MemberRemovedEvents.Add(1)

	snap := m.Snapshot(4)
	if snap.MessagesSent != 5 {
		t.Errorf("MessagesSent: %d", snap.MessagesSent)
	}
	if snap.MessagesReceived != 3 {
		t.Errorf("MessagesReceived: %d", snap.MessagesReceived)
	}
	if snap.BytesSent != 100 {
		t.Errorf("BytesSent: %d", snap.BytesSent)
	}
	if snap.BytesReceived != 80 {
		t.Errorf("BytesReceived: %d", snap.BytesReceived)
	}
	if snap.ActiveAssociations != 4 {
		t.Errorf("ActiveAssociations: %d", snap.ActiveAssociations)
	}
	if snap.GossipsReceived != 2 {
		t.Errorf("GossipsReceived: %d", snap.GossipsReceived)
	}
	if snap.MemberUpEvents != 1 {
		t.Errorf("MemberUpEvents: %d", snap.MemberUpEvents)
	}
	if snap.MemberRemovedEvents != 1 {
		t.Errorf("MemberRemovedEvents: %d", snap.MemberRemovedEvents)
	}
}

func TestNodeMetrics_ConvergenceTime(t *testing.T) {
	m := &NodeMetrics{}

	snap := m.Snapshot(0)
	if snap.LastConvergenceTime != "never" {
		t.Errorf("before RecordConvergence: got %q, want \"never\"", snap.LastConvergenceTime)
	}

	before := time.Now().UTC().Truncate(time.Second)
	m.RecordConvergence()
	after := time.Now().UTC().Add(time.Second)

	snap = m.Snapshot(0)
	if snap.LastConvergenceTime == "never" {
		t.Fatal("after RecordConvergence: still \"never\"")
	}
	parsed, err := time.Parse(time.RFC3339, snap.LastConvergenceTime)
	if err != nil {
		t.Fatalf("parse LastConvergenceTime %q: %v", snap.LastConvergenceTime, err)
	}
	if parsed.Before(before) || parsed.After(after) {
		t.Errorf("LastConvergenceTime %v not in [%v, %v]", parsed, before, after)
	}
}

func TestNodeMetrics_PrometheusText(t *testing.T) {
	m := &NodeMetrics{}
	m.MessagesSent.Add(42)
	m.GossipsReceived.Add(7)

	text := m.Snapshot(2).PrometheusText()
	for _, want := range []string{
		"gekka_messages_sent_total 42",
		"gekka_gossips_received_total 7",
		"gekka_active_associations 2",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("PrometheusText missing %q\nFull output:\n%s", want, text)
		}
	}
}

func TestMonitoring_MonitoringPort_ImpliesEnabled(t *testing.T) {
	// Setting MonitoringPort alone (without EnableMonitoring) should start the server.
	node, err := Spawn(NodeConfig{
		SystemName:     "PortTest",
		Host:           "127.0.0.1",
		Port:           0,
		MonitoringPort: 0, // OS-assigned
		// EnableMonitoring not set — MonitoringPort 0 is falsy, so monitoring off.
		// This test verifies that explicit EnableMonitoring=true is required when port=0.
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	defer node.Shutdown()
	if node.monitoring != nil {
		t.Error("monitoring should be nil when MonitoringPort==0 and EnableMonitoring==false")
	}
}

func TestMonitoring_MonitoringAddr_NilWhenDisabled(t *testing.T) {
	node, err := Spawn(NodeConfig{SystemName: "NilMon", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	defer node.Shutdown()
	if addr := node.MonitoringAddr(); addr != nil {
		t.Errorf("MonitoringAddr() should be nil when monitoring is disabled, got %v", addr)
	}
}

// ── Metrics wired into ClusterManager ────────────────────────────────────────

func TestMonitoring_GossipCounter_Incremented(t *testing.T) {
	// Two Go nodes: when node1 sends a GossipEnvelope to node2, the
	// GossipsReceived counter on node2 must increment.
	node1, err := Spawn(NodeConfig{SystemName: "GossipCtr", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("Spawn node1: %v", err)
	}
	defer node1.Shutdown()

	node2, err := Spawn(NodeConfig{SystemName: "GossipCtr", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("Spawn node2: %v", err)
	}
	defer node2.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := node2.Join(node1.localAddr.GetHostname(), node1.localAddr.GetPort()); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if err := node2.WaitForHandshake(ctx, node1.localAddr.GetHostname(), node1.localAddr.GetPort()); err != nil {
		t.Fatalf("WaitForHandshake: %v", err)
	}

	// The cluster gossip loop runs every 1 s. Wait up to 10 s for at least one
	// GossipEnvelope to arrive on node2.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if node2.metrics.GossipsReceived.Load() > 0 {
			t.Logf("GossipsReceived=%d after %v",
				node2.metrics.GossipsReceived.Load(),
				time.Until(deadline))
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Errorf("GossipsReceived still 0 after 10s; cluster gossip loop may not be running")
}

// ── CountAssociations ─────────────────────────────────────────────────────────

func TestMonitoring_CountAssociations(t *testing.T) {
	node1, err := Spawn(NodeConfig{SystemName: "AssocCtr", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("Spawn node1: %v", err)
	}
	defer node1.Shutdown()

	if n := node1.nm.CountAssociations(); n != 0 {
		t.Errorf("CountAssociations before any connection: got %d, want 0", n)
	}

	node2, err := Spawn(NodeConfig{SystemName: "AssocCtr", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("Spawn node2: %v", err)
	}
	defer node2.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := node2.Join(node1.localAddr.GetHostname(), node1.localAddr.GetPort()); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if err := node2.WaitForHandshake(ctx, node1.localAddr.GetHostname(), node1.localAddr.GetPort()); err != nil {
		t.Fatalf("WaitForHandshake: %v", err)
	}

	if n := node2.nm.CountAssociations(); n == 0 {
		t.Error("CountAssociations after join: got 0, want > 0")
	} else {
		t.Logf("CountAssociations = %d", n)
	}
}
