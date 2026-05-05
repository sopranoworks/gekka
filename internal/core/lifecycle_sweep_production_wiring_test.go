/*
 * lifecycle_sweep_production_wiring_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"context"
	"net"
	"testing"
	"time"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// sweepTestAddr builds a minimal address for production-wiring tests.
func sweepTestAddr(host string, port uint32, system string) *gproto_remote.Address {
	return &gproto_remote.Address{
		Protocol: proto.String("akka"),
		System:   proto.String(system),
		Hostname: proto.String(host),
		Port:     proto.Uint32(port),
	}
}

// pollUntil retries fn at small intervals until it returns true or the
// deadline elapses.  Avoids time.Sleep flake when observing async ticker
// effects.
func pollUntil(t *testing.T, deadline time.Duration, fn func() bool) bool {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if fn() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fn()
}

// stubConn returns one half of net.Pipe(); the other half is closed
// immediately so writes on the returned conn do not block, and the
// caller can observe Close() taking effect.
func stubConn(t *testing.T) net.Conn {
	t.Helper()
	a, b := net.Pipe()
	_ = b.Close()
	return a
}

// TestStartLifecycleSweepers_StopIdleOutboundAfter_FiresFromTicker
// observes that pekko.remote.artery.advanced.stop-idle-outbound-after
// is honored by the production-wired ticker: an OUTBOUND ASSOCIATED
// association whose lastSeen is older than the threshold is gracefully
// stopped (conn closed, entry deleted, UID NOT permanently quarantined).
func TestStartLifecycleSweepers_StopIdleOutboundAfter_FiresFromTicker(t *testing.T) {
	nm := NewNodeManager(sweepTestAddr("127.0.0.1", 2552, "S"), 1)
	nm.StopIdleOutboundAfter = 80 * time.Millisecond
	nm.QuarantineIdleOutboundAfter = time.Hour          // skipped
	nm.StopQuarantinedAfterIdle = time.Hour             // skipped
	nm.RemoveQuarantinedAssociationAfter = time.Hour    // skipped

	remote := &gproto_remote.UniqueAddress{
		Address: sweepTestAddr("10.0.0.1", 2551, "Remote"),
		Uid:     proto.Uint64(2001),
	}
	conn := stubConn(t)
	assoc := &GekkaAssociation{
		state:    ASSOCIATED,
		role:     OUTBOUND,
		nodeMgr:  nm,
		localUid: nm.localUid,
		outbox:   make(chan []byte, 1),
		remote:   remote,
		streamId: 1,
		lastSeen: time.Now().Add(-time.Second),
		conn:     conn,
	}
	nm.RegisterAssociation(remote, assoc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartLifecycleSweepers(ctx, nm)

	if !pollUntil(t, time.Second, func() bool {
		_, ok := nm.GetAssociation(remote, 1)
		return !ok
	}) {
		t.Fatal("expected idle ASSOCIATED outbound to be removed by the ticker")
	}

	// Stop is a graceful tear-down — the UID must NOT have been added
	// to the permanent quarantine registry.
	if nm.IsQuarantined(2001) {
		t.Errorf("StopIdleOutbound must not register the UID as quarantined")
	}

	// The stub conn was closed by the sweep; further writes return an
	// error. Use a non-blocking write to avoid hanging the test.
	if _, err := conn.Write([]byte("x")); err == nil {
		t.Errorf("expected stub conn to be closed by the sweep")
	}
}

// TestStartLifecycleSweepers_QuarantineIdleOutboundAfter_FiresFromTicker
// observes the second sweep: an OUTBOUND ASSOCIATED association whose
// lastSeen is older than quarantine-idle-outbound-after is transitioned
// to QUARANTINED with its quarantinedSince timestamp recorded and its
// UID registered. The entry stays in the registry (split-phase).
func TestStartLifecycleSweepers_QuarantineIdleOutboundAfter_FiresFromTicker(t *testing.T) {
	nm := NewNodeManager(sweepTestAddr("127.0.0.1", 2552, "S"), 1)
	nm.StopIdleOutboundAfter = time.Hour                // skipped
	nm.QuarantineIdleOutboundAfter = 80 * time.Millisecond
	nm.StopQuarantinedAfterIdle = time.Hour             // skipped (no transitions yet)
	nm.RemoveQuarantinedAssociationAfter = time.Hour    // skipped

	remote := &gproto_remote.UniqueAddress{
		Address: sweepTestAddr("10.0.0.2", 2551, "Remote"),
		Uid:     proto.Uint64(2002),
	}
	assoc := &GekkaAssociation{
		state:    ASSOCIATED,
		role:     OUTBOUND,
		nodeMgr:  nm,
		localUid: nm.localUid,
		outbox:   make(chan []byte, 1),
		remote:   remote,
		streamId: 1,
		lastSeen: time.Now().Add(-time.Second),
	}
	nm.RegisterAssociation(remote, assoc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartLifecycleSweepers(ctx, nm)

	if !pollUntil(t, time.Second, func() bool {
		return assoc.GetState() == QUARANTINED && nm.IsQuarantined(2002)
	}) {
		t.Fatalf("expected QUARANTINED+UID-registered, got state=%v isQuarantined=%v", assoc.GetState(), nm.IsQuarantined(2002))
	}
	if assoc.QuarantinedSince().IsZero() {
		t.Errorf("expected QuarantinedSince to be non-zero after quarantine tick")
	}
	if _, ok := nm.GetAssociation(remote, 1); !ok {
		t.Errorf("split-phase: assoc must remain in registry after quarantine sweep")
	}
}

// TestStartLifecycleSweepers_StopQuarantinedAfterIdle_ClosesConn
// observes the third sweep: an OUTBOUND association already in
// QUARANTINED state with an open conn and old lastSeen has its conn
// closed by the ticker once stop-quarantined-after-idle elapses.
func TestStartLifecycleSweepers_StopQuarantinedAfterIdle_ClosesConn(t *testing.T) {
	nm := NewNodeManager(sweepTestAddr("127.0.0.1", 2552, "S"), 1)
	nm.StopIdleOutboundAfter = time.Hour                // skipped
	nm.QuarantineIdleOutboundAfter = time.Hour          // skipped (assoc is already QUARANTINED)
	nm.StopQuarantinedAfterIdle = 80 * time.Millisecond
	nm.RemoveQuarantinedAssociationAfter = time.Hour    // skipped

	remote := &gproto_remote.UniqueAddress{
		Address: sweepTestAddr("10.0.0.3", 2551, "Remote"),
		Uid:     proto.Uint64(2003),
	}
	conn := stubConn(t)
	assoc := &GekkaAssociation{
		state:            QUARANTINED,
		role:             OUTBOUND,
		nodeMgr:          nm,
		localUid:         nm.localUid,
		outbox:           make(chan []byte, 1),
		remote:           remote,
		streamId:         1,
		lastSeen:         time.Now().Add(-time.Second),
		quarantinedSince: time.Now(),
		conn:             conn,
	}
	nm.RegisterAssociation(remote, assoc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartLifecycleSweepers(ctx, nm)

	if !pollUntil(t, time.Second, func() bool {
		assoc.mu.RLock()
		closed := assoc.conn == nil
		assoc.mu.RUnlock()
		return closed
	}) {
		t.Fatal("expected conn to be closed by the stop-quarantined sweep")
	}
	// This sweep must not delete the entry; the remove-quarantined
	// sweep is responsible for that step.
	if _, ok := nm.GetAssociation(remote, 1); !ok {
		t.Errorf("stop-quarantined sweep must not delete the assoc entry")
	}
}

// TestStartLifecycleSweepers_RemoveQuarantinedAssociationAfter_ExpiresUID
// observes the fourth sweep: an OUTBOUND QUARANTINED association whose
// quarantinedSince is older than the threshold is fully removed from
// the registry, AND its UID is dropped from quarantinedUIDs so a fresh
// handshake from the same UID can succeed.
func TestStartLifecycleSweepers_RemoveQuarantinedAssociationAfter_ExpiresUID(t *testing.T) {
	nm := NewNodeManager(sweepTestAddr("127.0.0.1", 2552, "S"), 1)
	nm.StopIdleOutboundAfter = time.Hour                // skipped
	nm.QuarantineIdleOutboundAfter = time.Hour          // skipped (assoc is already QUARANTINED)
	nm.StopQuarantinedAfterIdle = time.Hour             // skipped (no open conn)
	nm.RemoveQuarantinedAssociationAfter = 80 * time.Millisecond

	remote := &gproto_remote.UniqueAddress{
		Address: sweepTestAddr("10.0.0.4", 2551, "Remote"),
		Uid:     proto.Uint64(2004),
	}
	old := time.Now().Add(-time.Second)
	assoc := &GekkaAssociation{
		state:            QUARANTINED,
		role:             OUTBOUND,
		nodeMgr:          nm,
		localUid:         nm.localUid,
		outbox:           make(chan []byte, 1),
		remote:           remote,
		streamId:         1,
		lastSeen:         old,
		quarantinedSince: old,
	}
	nm.RegisterAssociation(remote, assoc)
	nm.RegisterQuarantinedUIDAt(remote, old)

	if !nm.IsQuarantined(2004) {
		t.Fatal("pre-condition: UID 2004 should be quarantined before sweep")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartLifecycleSweepers(ctx, nm)

	if !pollUntil(t, time.Second, func() bool {
		return !nm.IsQuarantined(2004)
	}) {
		t.Fatal("expected UID 2004 to be expired from quarantinedUIDs by the ticker")
	}
	if _, ok := nm.GetAssociation(remote, 1); ok {
		t.Errorf("expected QUARANTINED association entry to be removed by the sweep")
	}
}

// TestStartLifecycleSweepers_StopsWhenContextCancelled verifies the
// ticker goroutine exits cleanly when its bound context is cancelled.
// After cancellation, fresh idle associations registered later must not
// be touched by any sweep.
func TestStartLifecycleSweepers_StopsWhenContextCancelled(t *testing.T) {
	nm := NewNodeManager(sweepTestAddr("127.0.0.1", 2552, "S"), 1)
	nm.StopIdleOutboundAfter = 50 * time.Millisecond
	nm.QuarantineIdleOutboundAfter = 50 * time.Millisecond
	nm.StopQuarantinedAfterIdle = 50 * time.Millisecond
	nm.RemoveQuarantinedAssociationAfter = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	StartLifecycleSweepers(ctx, nm)
	cancel()
	// Wait long enough for the in-flight tick (if any) to settle.
	time.Sleep(150 * time.Millisecond)

	remote := &gproto_remote.UniqueAddress{
		Address: sweepTestAddr("10.0.0.5", 2551, "Remote"),
		Uid:     proto.Uint64(2005),
	}
	assoc := &GekkaAssociation{
		state:    ASSOCIATED,
		role:     OUTBOUND,
		nodeMgr:  nm,
		localUid: nm.localUid,
		outbox:   make(chan []byte, 1),
		remote:   remote,
		streamId: 1,
		lastSeen: time.Now().Add(-time.Second),
	}
	nm.RegisterAssociation(remote, assoc)

	// Wait several tick periods worth; the cancelled goroutine must
	// not touch the association.
	time.Sleep(200 * time.Millisecond)

	if _, ok := nm.GetAssociation(remote, 1); !ok {
		t.Errorf("cancelled ticker must not have swept the registry")
	}
}

// TestLifecycleSweepInterval_ClampsToFloor verifies the interval picker
// returns the floor (50ms) for tight test thresholds, so behaviour
// tests reliably observe a tick within ~100ms.
func TestLifecycleSweepInterval_ClampsToFloor(t *testing.T) {
	nm := NewNodeManager(sweepTestAddr("127.0.0.1", 2552, "S"), 1)
	nm.StopIdleOutboundAfter = 80 * time.Millisecond
	got := lifecycleSweepInterval(nm)
	if got != minLifecycleSweepInterval {
		t.Errorf("lifecycleSweepInterval = %v, want %v (floor)", got, minLifecycleSweepInterval)
	}
}

// TestLifecycleSweepInterval_DefaultsAreReasonable verifies the picker
// returns a sub-second cadence when all thresholds are at production
// defaults. The smallest default is StopQuarantinedAfterIdle (3s), so
// smallest/4 = 750ms — comfortably within [50ms, 1s] and well below
// the 5m/6h/1h thresholds the other knobs care about.
func TestLifecycleSweepInterval_DefaultsAreReasonable(t *testing.T) {
	nm := NewNodeManager(sweepTestAddr("127.0.0.1", 2552, "S"), 1)
	// All zero — Effective getters return Pekko defaults.
	got := lifecycleSweepInterval(nm)
	if got < minLifecycleSweepInterval || got > maxLifecycleSweepInterval {
		t.Errorf("lifecycleSweepInterval (defaults) = %v, want within [%v, %v]", got, minLifecycleSweepInterval, maxLifecycleSweepInterval)
	}
	// The smallest production default is 3s (StopQuarantinedAfterIdle);
	// 3s/4 = 750ms.
	want := DefaultStopQuarantinedAfterIdle / 4
	if got != want {
		t.Errorf("lifecycleSweepInterval (defaults) = %v, want %v (DefaultStopQuarantinedAfterIdle/4)", got, want)
	}
}

// TestLifecycleSweepInterval_ClampsToCeiling verifies the picker
// returns the 1s ceiling when all thresholds are configured well above
// 4s (so smallest/4 > 1s).
func TestLifecycleSweepInterval_ClampsToCeiling(t *testing.T) {
	nm := NewNodeManager(sweepTestAddr("127.0.0.1", 2552, "S"), 1)
	nm.StopIdleOutboundAfter = 10 * time.Second
	nm.QuarantineIdleOutboundAfter = 10 * time.Second
	nm.StopQuarantinedAfterIdle = 10 * time.Second
	nm.RemoveQuarantinedAssociationAfter = 10 * time.Second
	got := lifecycleSweepInterval(nm)
	if got != maxLifecycleSweepInterval {
		t.Errorf("lifecycleSweepInterval = %v, want %v (ceiling)", got, maxLifecycleSweepInterval)
	}
}
