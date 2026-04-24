/*
 * lifecycle_timers_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"testing"
	"time"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// lifecycleTestAddr builds a minimal gproto_remote.Address for lifecycle tests.
func lifecycleTestAddr(host string, port uint32, system string) *gproto_remote.Address {
	return &gproto_remote.Address{
		Protocol: proto.String("akka"),
		System:   proto.String(system),
		Hostname: proto.String(host),
		Port:     proto.Uint32(port),
	}
}

// TestLifecycleTimers_Defaults verifies the Effective*() accessors fall back
// to the Pekko reference defaults when the NodeManager field is zero-valued.
func TestLifecycleTimers_Defaults(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)

	if got := nm.EffectiveStopIdleOutboundAfter(); got != DefaultStopIdleOutboundAfter {
		t.Errorf("EffectiveStopIdleOutboundAfter = %v, want %v", got, DefaultStopIdleOutboundAfter)
	}
	if got := nm.EffectiveQuarantineIdleOutboundAfter(); got != DefaultQuarantineIdleOutboundAfter {
		t.Errorf("EffectiveQuarantineIdleOutboundAfter = %v, want %v", got, DefaultQuarantineIdleOutboundAfter)
	}
	if got := nm.EffectiveStopQuarantinedAfterIdle(); got != DefaultStopQuarantinedAfterIdle {
		t.Errorf("EffectiveStopQuarantinedAfterIdle = %v, want %v", got, DefaultStopQuarantinedAfterIdle)
	}
	if got := nm.EffectiveRemoveQuarantinedAssociationAfter(); got != DefaultRemoveQuarantinedAssociationAfter {
		t.Errorf("EffectiveRemoveQuarantinedAssociationAfter = %v, want %v", got, DefaultRemoveQuarantinedAssociationAfter)
	}
	if got := nm.EffectiveShutdownFlushTimeout(); got != DefaultShutdownFlushTimeout {
		t.Errorf("EffectiveShutdownFlushTimeout = %v, want %v", got, DefaultShutdownFlushTimeout)
	}
	if got := nm.EffectiveDeathWatchNotificationFlushTimeout(); got != DefaultDeathWatchNotificationFlushTimeout {
		t.Errorf("EffectiveDeathWatchNotificationFlushTimeout = %v, want %v", got, DefaultDeathWatchNotificationFlushTimeout)
	}
	if got := nm.EffectiveInboundRestartTimeout(); got != DefaultInboundRestartTimeout {
		t.Errorf("EffectiveInboundRestartTimeout = %v, want %v", got, DefaultInboundRestartTimeout)
	}
	if got := nm.EffectiveInboundMaxRestarts(); got != DefaultInboundMaxRestarts {
		t.Errorf("EffectiveInboundMaxRestarts = %d, want %d", got, DefaultInboundMaxRestarts)
	}
	if got := nm.EffectiveOutboundRestartBackoff(); got != DefaultOutboundRestartBackoff {
		t.Errorf("EffectiveOutboundRestartBackoff = %v, want %v", got, DefaultOutboundRestartBackoff)
	}
	if got := nm.EffectiveOutboundRestartTimeout(); got != DefaultOutboundRestartTimeout {
		t.Errorf("EffectiveOutboundRestartTimeout = %v, want %v", got, DefaultOutboundRestartTimeout)
	}
	if got := nm.EffectiveOutboundMaxRestarts(); got != DefaultOutboundMaxRestarts {
		t.Errorf("EffectiveOutboundMaxRestarts = %d, want %d", got, DefaultOutboundMaxRestarts)
	}
}

// TestLifecycleTimers_Overrides verifies each accessor honors the configured
// override value instead of the Pekko default.
func TestLifecycleTimers_Overrides(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)

	nm.StopIdleOutboundAfter = 11 * time.Minute
	nm.QuarantineIdleOutboundAfter = 42 * time.Second
	nm.StopQuarantinedAfterIdle = 7 * time.Second
	nm.RemoveQuarantinedAssociationAfter = 13 * time.Minute
	nm.ShutdownFlushTimeout = 8 * time.Second
	nm.DeathWatchNotificationFlushTimeout = 6 * time.Second
	nm.InboundRestartTimeout = 17 * time.Second
	nm.InboundMaxRestarts = 3
	nm.OutboundRestartBackoff = 750 * time.Millisecond
	nm.OutboundRestartTimeout = 19 * time.Second
	nm.OutboundMaxRestarts = 2

	if got := nm.EffectiveStopIdleOutboundAfter(); got != 11*time.Minute {
		t.Errorf("EffectiveStopIdleOutboundAfter = %v, want 11m", got)
	}
	if got := nm.EffectiveQuarantineIdleOutboundAfter(); got != 42*time.Second {
		t.Errorf("EffectiveQuarantineIdleOutboundAfter = %v, want 42s", got)
	}
	if got := nm.EffectiveStopQuarantinedAfterIdle(); got != 7*time.Second {
		t.Errorf("EffectiveStopQuarantinedAfterIdle = %v, want 7s", got)
	}
	if got := nm.EffectiveRemoveQuarantinedAssociationAfter(); got != 13*time.Minute {
		t.Errorf("EffectiveRemoveQuarantinedAssociationAfter = %v, want 13m", got)
	}
	if got := nm.EffectiveShutdownFlushTimeout(); got != 8*time.Second {
		t.Errorf("EffectiveShutdownFlushTimeout = %v, want 8s", got)
	}
	if got := nm.EffectiveDeathWatchNotificationFlushTimeout(); got != 6*time.Second {
		t.Errorf("EffectiveDeathWatchNotificationFlushTimeout = %v, want 6s", got)
	}
	if got := nm.EffectiveInboundRestartTimeout(); got != 17*time.Second {
		t.Errorf("EffectiveInboundRestartTimeout = %v, want 17s", got)
	}
	if got := nm.EffectiveInboundMaxRestarts(); got != 3 {
		t.Errorf("EffectiveInboundMaxRestarts = %d, want 3", got)
	}
	if got := nm.EffectiveOutboundRestartBackoff(); got != 750*time.Millisecond {
		t.Errorf("EffectiveOutboundRestartBackoff = %v, want 750ms", got)
	}
	if got := nm.EffectiveOutboundRestartTimeout(); got != 19*time.Second {
		t.Errorf("EffectiveOutboundRestartTimeout = %v, want 19s", got)
	}
	if got := nm.EffectiveOutboundMaxRestarts(); got != 2 {
		t.Errorf("EffectiveOutboundMaxRestarts = %d, want 2", got)
	}
}

// TestSweepIdleOutboundQuarantine_RemovesIdleAssociation is a behavior test
// for quarantine-idle-outbound-after: an OUTBOUND association whose lastSeen
// timestamp is older than the configured threshold must be transitioned to
// QUARANTINED by SweepIdleOutboundQuarantine and removed from the registry;
// its UID must also be registered in the permanent quarantine registry.
func TestSweepIdleOutboundQuarantine_RemovesIdleAssociation(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)
	nm.QuarantineIdleOutboundAfter = 50 * time.Millisecond

	remote := &gproto_remote.UniqueAddress{
		Address: lifecycleTestAddr("10.0.0.1", 2551, "Remote"),
		Uid:     proto.Uint64(777),
	}
	assoc := &GekkaAssociation{
		state:    ASSOCIATED,
		role:     OUTBOUND,
		nodeMgr:  nm,
		localUid: nm.localUid,
		outbox:   make(chan []byte, 1),
		remote:   remote,
		streamId: 1,
		lastSeen: time.Now().Add(-time.Second), // older than 50ms threshold
	}
	nm.RegisterAssociation(remote, assoc)

	// Pre-condition: the association is present.
	if _, ok := nm.GetAssociation(remote, 1); !ok {
		t.Fatal("association should be registered before sweep")
	}

	n := nm.SweepIdleOutboundQuarantine()
	if n != 1 {
		t.Errorf("SweepIdleOutboundQuarantine quarantined %d, want 1", n)
	}

	// Post-condition: association removed from the registry.
	if _, ok := nm.GetAssociation(remote, 1); ok {
		t.Fatal("expected association to be removed after idle sweep")
	}

	// Post-condition: association state transitioned to QUARANTINED.
	if assoc.GetState() != QUARANTINED {
		t.Errorf("assoc state = %v, want QUARANTINED", assoc.GetState())
	}

	// Post-condition: UID recorded in permanent quarantine registry.
	if !nm.IsQuarantined(777) {
		t.Errorf("expected UID 777 to be in permanent quarantine registry")
	}
}

// TestSweepIdleOutboundQuarantine_SkipsFreshAssociation verifies an OUTBOUND
// association with a recent lastSeen stays registered.
func TestSweepIdleOutboundQuarantine_SkipsFreshAssociation(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)
	nm.QuarantineIdleOutboundAfter = time.Hour

	remote := &gproto_remote.UniqueAddress{
		Address: lifecycleTestAddr("10.0.0.2", 2551, "Remote"),
		Uid:     proto.Uint64(888),
	}
	assoc := &GekkaAssociation{
		state:    ASSOCIATED,
		role:     OUTBOUND,
		nodeMgr:  nm,
		localUid: nm.localUid,
		outbox:   make(chan []byte, 1),
		remote:   remote,
		streamId: 1,
		lastSeen: time.Now(), // fresh — well within the 1h threshold
	}
	nm.RegisterAssociation(remote, assoc)

	if n := nm.SweepIdleOutboundQuarantine(); n != 0 {
		t.Errorf("SweepIdleOutboundQuarantine quarantined %d, want 0", n)
	}
	if _, ok := nm.GetAssociation(remote, 1); !ok {
		t.Fatal("fresh association must survive the sweep")
	}
	if nm.IsQuarantined(888) {
		t.Errorf("fresh UID 888 must not be in permanent quarantine registry")
	}
}

// TestSweepIdleOutboundQuarantine_SkipsInbound verifies that INBOUND
// associations are never quarantined by the idle sweeper — Pekko's
// quarantine-idle-outbound-after applies only to outbound streams.
func TestSweepIdleOutboundQuarantine_SkipsInbound(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)
	nm.QuarantineIdleOutboundAfter = 10 * time.Millisecond

	remote := &gproto_remote.UniqueAddress{
		Address: lifecycleTestAddr("10.0.0.3", 2551, "Remote"),
		Uid:     proto.Uint64(999),
	}
	assoc := &GekkaAssociation{
		state:    ASSOCIATED,
		role:     INBOUND, // key difference
		nodeMgr:  nm,
		localUid: nm.localUid,
		outbox:   make(chan []byte, 1),
		remote:   remote,
		streamId: 1,
		lastSeen: time.Now().Add(-time.Second),
	}
	nm.RegisterAssociation(remote, assoc)

	if n := nm.SweepIdleOutboundQuarantine(); n != 0 {
		t.Errorf("inbound associations must be skipped, got %d quarantined", n)
	}
	if _, ok := nm.GetAssociation(remote, 1); !ok {
		t.Fatal("inbound association must survive the sweep")
	}
}

// TestRestartCap_Outbound verifies outbound-max-restarts enforcement: the
// first N calls must return true, and the (N+1)th call within the rolling
// window must return false.
func TestRestartCap_Outbound(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)
	nm.OutboundMaxRestarts = 3
	nm.OutboundRestartTimeout = time.Hour // ensure none of our calls age out

	for i := 0; i < 3; i++ {
		if !nm.TryRecordOutboundRestart() {
			t.Fatalf("TryRecordOutboundRestart #%d: want true (within cap), got false", i+1)
		}
	}
	if nm.TryRecordOutboundRestart() {
		t.Error("TryRecordOutboundRestart #4: want false (cap exceeded), got true")
	}
}

// TestRestartCap_Inbound verifies inbound-max-restarts enforcement.
func TestRestartCap_Inbound(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)
	nm.InboundMaxRestarts = 2
	nm.InboundRestartTimeout = time.Hour

	if !nm.TryRecordInboundRestart() {
		t.Fatal("TryRecordInboundRestart #1: want true, got false")
	}
	if !nm.TryRecordInboundRestart() {
		t.Fatal("TryRecordInboundRestart #2: want true, got false")
	}
	if nm.TryRecordInboundRestart() {
		t.Error("TryRecordInboundRestart #3: want false (cap exceeded), got true")
	}
}

// TestRestartCap_WindowPruning verifies the restart cap uses a rolling
// window: restarts older than the configured timeout must fall out of the
// count and let fresh restarts succeed again.
func TestRestartCap_WindowPruning(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)
	nm.OutboundMaxRestarts = 2
	nm.OutboundRestartTimeout = 40 * time.Millisecond

	if !nm.TryRecordOutboundRestart() {
		t.Fatal("first restart should succeed")
	}
	if !nm.TryRecordOutboundRestart() {
		t.Fatal("second restart should succeed")
	}
	if nm.TryRecordOutboundRestart() {
		t.Fatal("third restart should be capped")
	}

	// Wait for the rolling window to fully elapse, then verify new restarts
	// succeed again because the earlier timestamps have been pruned.
	time.Sleep(80 * time.Millisecond)

	if !nm.TryRecordOutboundRestart() {
		t.Error("after window expiry, restart should succeed")
	}
}

// TestRestartCap_Reset verifies ResetRestartCounters clears both windows.
func TestRestartCap_Reset(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)
	nm.OutboundMaxRestarts = 1
	nm.OutboundRestartTimeout = time.Hour
	nm.InboundMaxRestarts = 1
	nm.InboundRestartTimeout = time.Hour

	_ = nm.TryRecordOutboundRestart()
	if nm.TryRecordOutboundRestart() {
		t.Fatal("second outbound restart should be capped")
	}
	_ = nm.TryRecordInboundRestart()
	if nm.TryRecordInboundRestart() {
		t.Fatal("second inbound restart should be capped")
	}

	nm.ResetRestartCounters()

	if !nm.TryRecordOutboundRestart() {
		t.Error("outbound restart after reset should succeed")
	}
	if !nm.TryRecordInboundRestart() {
		t.Error("inbound restart after reset should succeed")
	}
}
