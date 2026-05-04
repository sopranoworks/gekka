/*
 * inbound_lanes_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// goroutineID returns the current goroutine's numeric ID parsed from the
// runtime stack header ("goroutine N [running]:"). Used to verify that
// inbound-lanes fan-out actually executes on distinct goroutines.
func goroutineID() string {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	s := string(buf[:n])
	const prefix = "goroutine "
	s = strings.TrimPrefix(s, prefix)
	if end := strings.Index(s, " "); end >= 0 {
		return s[:end]
	}
	return s
}

// newInboundLanesAssoc spins up an OUTBOUND association on streamId via
// ProcessConnection so the production lane-allocation code path runs.
// (OUTBOUND is used so we don't have to feed an Artery preamble.) Returns
// the association after registration. The flight recorder is always enabled.
func newInboundLanesAssoc(t *testing.T, inboundLanes int, laneCap int, streamId int32) (*GekkaAssociation, *NodeManager) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	nm := NewNodeManager(&gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Test"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(0),
	}, 1)
	nm.InboundLanes = inboundLanes
	if laneCap > 0 {
		nm.OutboundMessageQueueSize = laneCap
	}
	nm.FlightRec = NewFlightRecorder(true, LevelLifecycle)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	remote := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Peer"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(uint32(ln.Addr().(*net.TCPAddr).Port)),
	}

	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- c
		buf := make([]byte, 4096)
		for {
			if _, err := c.Read(buf); err != nil {
				return
			}
		}
	}()

	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	select {
	case server := <-accepted:
		t.Cleanup(func() { _ = server.Close() })
	case <-time.After(2 * time.Second):
		t.Fatalf("accept timeout")
	}

	go func() {
		_ = nm.ProcessConnection(ctx, client, OUTBOUND, remote, streamId)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if a, ok := nm.GetGekkaAssociationByHost(remote.GetHostname(), remote.GetPort()); ok {
			return a, nm
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("association not registered within deadline")
	return nil, nil
}

// TestInboundLanes_FansOutByRecipientHash verifies that dispatchSharded
// routes frames to N distinct goroutines hashed by recipient path, and
// that two messages with the same recipient hit the same goroutine.
func TestInboundLanes_FansOutByRecipientHash(t *testing.T) {
	assoc, nm := newInboundLanesAssoc(t, 4, 0, 2)
	if got := len(assoc.inboundLanes); got != 4 {
		t.Fatalf("inboundLanes len = %d, want 4", got)
	}

	type record struct {
		gid       string
		recipient string
	}
	var mu sync.Mutex
	records := make([]record, 0, 16)
	done := make(chan struct{}, 16)
	nm.UserMessageCallback = func(ctx context.Context, meta *ArteryMetadata) error {
		mu.Lock()
		records = append(records, record{
			gid:       goroutineID(),
			recipient: meta.Recipient.GetPath(),
		})
		mu.Unlock()
		done <- struct{}{}
		return nil
	}

	// 8 distinct recipient paths.
	paths := []string{
		"/user/a", "/user/b", "/user/c", "/user/d",
		"/user/e", "/user/f", "/user/g", "/user/h",
	}
	for _, p := range paths {
		meta := &ArteryMetadata{
			Recipient: &gproto_remote.ActorRefData{Path: proto.String(p)},
			// Skip serializer-driven decode by leaving SerializerId at 0
			// (default switch case → handleUserMessage which calls callback).
		}
		if err := assoc.dispatchSharded(context.Background(), meta); err != nil {
			t.Fatalf("dispatchSharded: %v", err)
		}
	}

	for i := 0; i < len(paths); i++ {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("callback did not fire %d times within deadline (got %d)", len(paths), i)
		}
	}

	// Send each path twice and verify second copy lands on same goroutine.
	prevGoroutine := make(map[string]string)
	mu.Lock()
	for _, r := range records {
		prevGoroutine[r.recipient] = r.gid
	}
	mu.Unlock()

	for _, p := range paths {
		meta := &ArteryMetadata{
			Recipient: &gproto_remote.ActorRefData{Path: proto.String(p)},
		}
		if err := assoc.dispatchSharded(context.Background(), meta); err != nil {
			t.Fatalf("dispatchSharded retry: %v", err)
		}
	}
	for i := 0; i < len(paths); i++ {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("retry callback did not fire %d times (got %d)", len(paths), i)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	gids := make(map[string]struct{})
	for _, r := range records {
		gids[r.gid] = struct{}{}
	}
	if len(gids) > 4 {
		t.Errorf("observed %d distinct goroutines; want ≤ 4 (one per lane)", len(gids))
	}
	if len(gids) < 2 {
		t.Errorf("observed %d distinct goroutine; want ≥ 2 (lanes are not fanning out)", len(gids))
	}
	// Per-recipient consistency: each recipient must always land on the
	// same goroutine. Build per-recipient sets and verify size 1.
	perRecipient := make(map[string]map[string]struct{})
	for _, r := range records {
		if perRecipient[r.recipient] == nil {
			perRecipient[r.recipient] = make(map[string]struct{})
		}
		perRecipient[r.recipient][r.gid] = struct{}{}
	}
	for p, gset := range perRecipient {
		if len(gset) != 1 {
			t.Errorf("recipient %s observed on %d goroutines; want 1 (consistent hashing)", p, len(gset))
		}
	}
}

// TestInboundLanes_DefaultIsFour verifies the production allocation path
// reads EffectiveInboundLanes() at ProcessConnection time and allocates
// exactly four lane channels by default.
func TestInboundLanes_DefaultIsFour(t *testing.T) {
	assoc, _ := newInboundLanesAssoc(t, 0, 0, 2) // 0 → default
	if got := len(assoc.inboundLanes); got != DefaultInboundLanes {
		t.Errorf("inboundLanes len = %d, want %d", got, DefaultInboundLanes)
	}
}

// TestInboundLanes_OneFallsBackToSerial verifies that with InboundLanes=1
// no fan-out goroutines are allocated; dispatchSharded falls back to the
// inline path on the read goroutine.
func TestInboundLanes_OneFallsBackToSerial(t *testing.T) {
	assoc, nm := newInboundLanesAssoc(t, 1, 0, 2)
	if assoc.inboundLanes != nil {
		t.Errorf("inboundLanes = %v; want nil for InboundLanes=1", assoc.inboundLanes)
	}

	got := make(chan string, 1)
	nm.UserMessageCallback = func(ctx context.Context, meta *ArteryMetadata) error {
		got <- goroutineID()
		return nil
	}

	caller := goroutineID()
	meta := &ArteryMetadata{
		Recipient: &gproto_remote.ActorRefData{Path: proto.String("/user/a")},
	}
	if err := assoc.dispatchSharded(context.Background(), meta); err != nil {
		t.Fatalf("dispatchSharded: %v", err)
	}
	select {
	case gid := <-got:
		if gid != caller {
			t.Errorf("inline-fallback callback ran on goroutine %s; want %s (caller)", gid, caller)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("callback did not fire")
	}
}

// TestInboundLanes_ControlStreamBypassesLanes verifies streamId=1
// associations skip lane allocation regardless of EffectiveInboundLanes(),
// preserving control-stream ordering for handshake and heartbeat traffic.
func TestInboundLanes_ControlStreamBypassesLanes(t *testing.T) {
	assoc, _ := newInboundLanesAssoc(t, 8, 0, 1)
	if assoc.inboundLanes != nil {
		t.Errorf("inboundLanes = %v; control-stream must bypass lanes", assoc.inboundLanes)
	}
}

// TestInboundLanes_RecipientlessFrameBypassesLanes verifies that frames
// without a recipient (cluster heartbeats, system messages) dispatch
// inline on the caller's goroutine even when lanes are configured.
func TestInboundLanes_RecipientlessFrameBypassesLanes(t *testing.T) {
	assoc, nm := newInboundLanesAssoc(t, 4, 0, 2)
	if got := len(assoc.inboundLanes); got != 4 {
		t.Fatalf("inboundLanes len = %d, want 4", got)
	}

	got := make(chan string, 1)
	nm.UserMessageCallback = func(ctx context.Context, meta *ArteryMetadata) error {
		got <- goroutineID()
		return nil
	}

	caller := goroutineID()
	meta := &ArteryMetadata{Recipient: nil} // recipientless
	if err := assoc.dispatchSharded(context.Background(), meta); err != nil {
		t.Fatalf("dispatchSharded: %v", err)
	}
	select {
	case gid := <-got:
		if gid != caller {
			t.Errorf("recipientless callback ran on goroutine %s; want %s (caller)", gid, caller)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("callback did not fire")
	}

	// Lanes must be empty (no message was queued).
	for i, lane := range assoc.inboundLanes {
		if len(lane) != 0 {
			t.Errorf("lane %d has %d messages; want 0", i, len(lane))
		}
	}
}

// TestInboundLanes_LaneFullFallsBackInline saturates a lane channel and
// verifies the dispatchSharded `default:` branch fires: the
// CatInboundLaneFull flight event is emitted and the message is still
// dispatched (inline on the caller's goroutine).
func TestInboundLanes_LaneFullFallsBackInline(t *testing.T) {
	assoc, nm := newInboundLanesAssoc(t, 2, 1, 2) // lane cap = 1
	if got := len(assoc.inboundLanes); got != 2 {
		t.Fatalf("inboundLanes len = %d, want 2", got)
	}
	for i, lane := range assoc.inboundLanes {
		if cap(lane) != 1 {
			t.Fatalf("lane %d cap = %d, want 1", i, cap(lane))
		}
	}

	var firstCallSeen atomic.Bool
	gate := make(chan struct{})
	firstCallSignal := make(chan struct{}, 1)
	receivedCount := atomic.Int32{}
	doneCh := make(chan struct{}, 8)

	nm.UserMessageCallback = func(ctx context.Context, meta *ArteryMetadata) error {
		if firstCallSeen.CompareAndSwap(false, true) {
			firstCallSignal <- struct{}{}
			<-gate
		}
		receivedCount.Add(1)
		doneCh <- struct{}{}
		return nil
	}

	// All three messages route to the same lane (same recipient → same hash).
	const recipient = "/user/saturated"
	meta := func() *ArteryMetadata {
		return &ArteryMetadata{
			Recipient: &gproto_remote.ActorRefData{Path: proto.String(recipient)},
		}
	}

	// Frame 1 — picked up by lane goroutine; callback blocks on gate.
	if err := assoc.dispatchSharded(context.Background(), meta()); err != nil {
		t.Fatalf("dispatchSharded msg1: %v", err)
	}
	select {
	case <-firstCallSignal:
	case <-time.After(2 * time.Second):
		t.Fatalf("first callback did not fire")
	}

	// Frame 2 — buffered in lane channel (cap=1, was empty, now full).
	if err := assoc.dispatchSharded(context.Background(), meta()); err != nil {
		t.Fatalf("dispatchSharded msg2: %v", err)
	}

	// Snapshot pre-saturation flight events.
	preEvents := nm.FlightRec.Snapshot(assoc.remoteKey())
	preLaneFullCount := countByCategory(preEvents, CatInboundLaneFull)

	// Frame 3 — lane channel full, must hit `default:` and dispatch inline.
	if err := assoc.dispatchSharded(context.Background(), meta()); err != nil {
		t.Fatalf("dispatchSharded msg3: %v", err)
	}

	// Inline dispatch ran the callback synchronously; receivedCount +1 already.
	if got := receivedCount.Load(); got != 1 {
		t.Errorf("after inline-fallback: receivedCount = %d, want 1 (the inline message ran but blocked-callback message is still gated)", got)
	}

	postEvents := nm.FlightRec.Snapshot(assoc.remoteKey())
	postLaneFullCount := countByCategory(postEvents, CatInboundLaneFull)
	if postLaneFullCount <= preLaneFullCount {
		t.Errorf("CatInboundLaneFull count %d → %d; want strict increase (saturation event missing)",
			preLaneFullCount, postLaneFullCount)
	}

	// Release the gate; lane goroutine drains the buffered frame 2.
	close(gate)
	for i := 0; i < 2; i++ {
		select {
		case <-doneCh:
		case <-time.After(2 * time.Second):
			t.Fatalf("post-gate drain did not complete (i=%d)", i)
		}
	}
	if got := receivedCount.Load(); got != 3 {
		t.Errorf("final receivedCount = %d, want 3 (no message lost)", got)
	}
}

func countByCategory(events []FlightEvent, cat EventCategory) int {
	n := 0
	for _, e := range events {
		if e.Category == cat {
			n++
		}
	}
	return n
}

// fmt is referenced indirectly via t.Fatalf format strings; explicit import
// is unused otherwise. Keep the linker happy without a blank-import hack.
var _ = fmt.Sprintf
