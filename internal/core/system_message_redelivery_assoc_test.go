/*
 * system_message_redelivery_assoc_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// newSystemRedeliveryAssoc constructs a minimal GekkaAssociation with a
// system-message outbox and a wire outbox of the given capacity. Skips the
// network setup of newOutboxAssocForTest so the test can drive SendSystem
// and runSystemRedelivery synchronously via the outbox channel.
func newSystemRedeliveryAssoc(t *testing.T, sysBufCap, outboxCap int, resendInterval time.Duration) (*GekkaAssociation, *NodeManager) {
	t.Helper()
	nm := NewNodeManager(&gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Test"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(0),
	}, 1)
	nm.SystemMessageBufferSize = sysBufCap
	nm.SystemMessageResendInterval = resendInterval

	assoc := &GekkaAssociation{
		state:        ASSOCIATED,
		role:         OUTBOUND,
		nodeMgr:      nm,
		Handshake:    make(chan struct{}),
		localUid:     1,
		outbox:       make(chan []byte, outboxCap),
		streamId:     1,
		systemOutbox: NewSystemMessageOutbox(nm.EffectiveSystemMessageBufferSize()),
	}
	assoc.remote.Store(&gproto_remote.UniqueAddress{
		Address: &gproto_remote.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("Peer"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(2552),
		},
		Uid: proto.Uint64(42),
	})
	close(assoc.Handshake)
	return assoc, nm
}

func TestSendSystem_BuffersAndWritesToWire(t *testing.T) {
	assoc, _ := newSystemRedeliveryAssoc(t, 16, 4, 100*time.Millisecond)

	frame := []byte{0x01, 0x02, 0x03}
	if err := assoc.SendSystem(7, frame); err != nil {
		t.Fatalf("SendSystem(7) = %v", err)
	}

	// Buffer recorded the entry.
	if got := assoc.SystemOutbox().Len(); got != 1 {
		t.Errorf("SystemOutbox().Len() = %d, want 1", got)
	}

	// Wire outbox received the frame (initial best-effort write).
	select {
	case got := <-assoc.outbox:
		if string(got) != string(frame) {
			t.Errorf("outbox frame = %v, want %v", got, frame)
		}
	default:
		t.Fatal("outbox did not receive the initial system-message frame")
	}
}

func TestSendSystem_BufferFullReturnsError(t *testing.T) {
	// Phase 2.4: SendSystem on a full buffer not only returns
	// ErrSystemOutboxFull, it also quarantines the association and
	// drains the buffer (Pekko semantics: buffer-full ⇒ give up).
	// Provide a dummy conn so the quarantine path's conn.Close() is
	// safe.
	assoc, _ := newSystemRedeliveryAssoc(t, 2, 16, 100*time.Millisecond)
	connA, connB := net.Pipe()
	t.Cleanup(func() { _ = connA.Close(); _ = connB.Close() })
	go func() { _, _ = io.Copy(io.Discard, connB) }()
	assoc.conn = connA

	if err := assoc.SendSystem(1, []byte("a")); err != nil {
		t.Fatalf("SendSystem(1): %v", err)
	}
	if err := assoc.SendSystem(2, []byte("b")); err != nil {
		t.Fatalf("SendSystem(2): %v", err)
	}
	err := assoc.SendSystem(3, []byte("c"))
	if !errors.Is(err, ErrSystemOutboxFull) {
		t.Errorf("SendSystem on full buffer = %v, want ErrSystemOutboxFull", err)
	}

	// Quarantine drained the buffer.
	if got := assoc.SystemOutbox().Len(); got != 0 {
		t.Errorf("SystemOutbox().Len() = %d, want 0 (quarantine drains)", got)
	}
	// Association is now QUARANTINED.
	assoc.mu.RLock()
	state := assoc.state
	assoc.mu.RUnlock()
	if state != QUARANTINED {
		t.Errorf("after buffer-full, state = %v, want QUARANTINED", state)
	}
}

func TestSendSystem_OutboxFullKeepsEntryForResend(t *testing.T) {
	// Wire outbox capacity 1, system buffer capacity 4. First SendSystem
	// fills the wire outbox; second SendSystem still buffers in
	// systemOutbox even though the wire write fails (non-blocking).
	assoc, _ := newSystemRedeliveryAssoc(t, 4, 1, 100*time.Millisecond)

	if err := assoc.SendSystem(1, []byte("a")); err != nil {
		t.Fatalf("SendSystem(1) = %v", err)
	}
	if err := assoc.SendSystem(2, []byte("b")); err != nil {
		t.Fatalf("SendSystem(2) = %v (should succeed; wire-full does not fail)", err)
	}

	// Both entries must be in the systemOutbox awaiting redelivery.
	if got := assoc.SystemOutbox().Len(); got != 2 {
		t.Errorf("SystemOutbox().Len() = %d, want 2", got)
	}

	// Wire outbox holds only the first frame; second was dropped on the
	// initial best-effort write and waits for the resend ticker.
	select {
	case got := <-assoc.outbox:
		if string(got) != "a" {
			t.Errorf("first wire frame = %q, want %q", got, "a")
		}
	default:
		t.Fatal("expected first frame in wire outbox")
	}
	select {
	case got := <-assoc.outbox:
		t.Errorf("unexpected extra frame on wire outbox: %v", got)
	default:
	}
}

func TestRunSystemRedelivery_ResendsUnackedEntries(t *testing.T) {
	// Short interval so the ticker fires quickly.
	assoc, _ := newSystemRedeliveryAssoc(t, 16, 16, 30*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Send two frames. Drain the wire outbox to simulate the write loop
	// having sent them — if we don't drain, the ticker's "lastAttempt
	// younger than interval" check would skip them.
	if err := assoc.SendSystem(10, []byte("frame-10")); err != nil {
		t.Fatalf("SendSystem(10) = %v", err)
	}
	if err := assoc.SendSystem(11, []byte("frame-11")); err != nil {
		t.Fatalf("SendSystem(11) = %v", err)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-assoc.outbox:
		case <-time.After(time.Second):
			t.Fatalf("initial frame %d not on outbox", i)
		}
	}

	// Start the redelivery loop. It must re-write both unacked frames.
	go assoc.runSystemRedelivery(ctx)

	got := map[string]int{}
	deadline := time.After(2 * time.Second)
collect:
	for len(got) < 2 {
		select {
		case f := <-assoc.outbox:
			got[string(f)]++
		case <-deadline:
			break collect
		}
	}
	if got["frame-10"] == 0 {
		t.Errorf("resend ticker did not re-emit frame-10; got=%v", got)
	}
	if got["frame-11"] == 0 {
		t.Errorf("resend ticker did not re-emit frame-11; got=%v", got)
	}
}

func TestRunSystemRedelivery_PrunedEntryIsNotResent(t *testing.T) {
	assoc, _ := newSystemRedeliveryAssoc(t, 16, 16, 30*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := assoc.SendSystem(5, []byte("keep")); err != nil {
		t.Fatalf("SendSystem(5) = %v", err)
	}
	if err := assoc.SendSystem(6, []byte("ack-this")); err != nil {
		t.Fatalf("SendSystem(6) = %v", err)
	}
	// Drain initial frames.
	for i := 0; i < 2; i++ {
		select {
		case <-assoc.outbox:
		case <-time.After(time.Second):
			t.Fatalf("initial frame %d missing", i)
		}
	}

	// Simulate an ack for seq=6 (cumulative — also covers seq=5 ⇒ both are
	// pruned). Actually we want to keep seq=5 unacked and only prune seq=6,
	// which a cumulative ack cannot do. To exercise the prune path, ack
	// the lower seq instead: ack of seq=5 prunes only seq=5.
	if pruned := assoc.SystemOutbox().PruneAcked(5); pruned != 1 {
		t.Fatalf("PruneAcked(5) = %d, want 1", pruned)
	}

	go assoc.runSystemRedelivery(ctx)

	// Only seq=6 ("ack-this") should be re-emitted; "keep" was pruned.
	deadline := time.After(2 * time.Second)
	resentSet := map[string]int{}
collect:
	for {
		select {
		case f := <-assoc.outbox:
			resentSet[string(f)]++
			if resentSet["ack-this"] >= 1 {
				break collect
			}
		case <-deadline:
			break collect
		}
	}
	if resentSet["ack-this"] == 0 {
		t.Errorf("expected at least one resend of ack-this, got %v", resentSet)
	}
	if resentSet["keep"] != 0 {
		t.Errorf("pruned frame-5 was resent (got=%v); should have been dropped from buffer", resentSet)
	}
}

func TestHandleSystemMessage_DedupesByLastDeliveredSeq(t *testing.T) {
	assoc, nm := newSystemRedeliveryAssoc(t, 16, 16, 100*time.Millisecond)

	// sendSystemAck writes back on assoc.conn; provide a net.Pipe so the
	// write succeeds without a real TCP listener. The peer end just drains.
	connA, connB := net.Pipe()
	t.Cleanup(func() { _ = connA.Close(); _ = connB.Close() })
	go func() { _, _ = io.Copy(io.Discard, connB) }()
	assoc.conn = connA

	dispatched := 0
	nm.SystemMessageCallback = func(_ *gproto_remote.UniqueAddress, _ *gproto_remote.SystemMessageEnvelope, _ *gproto_remote.SystemMessage) error {
		dispatched++
		return nil
	}

	makeFrame := func(seq uint64) *ArteryMetadata {
		inner := &gproto_remote.SystemMessage{
			Type: gproto_remote.SystemMessage_WATCH.Enum(),
		}
		innerBytes, err := proto.Marshal(inner)
		if err != nil {
			t.Fatal(err)
		}
		env := &gproto_remote.SystemMessageEnvelope{
			Message:         innerBytes,
			SerializerId:    proto.Int32(2),
			MessageManifest: []byte("SystemMessage"),
			SeqNo:           proto.Uint64(seq),
			// AckReplyTo is required by the proto schema; the test's wire
			// outbox is unbuffered for ack writes so sendSystemAck below
			// will fail silently — that's fine, the dedupe path under test
			// runs before the dispatch regardless of whether the ack hit.
			AckReplyTo: &gproto_remote.UniqueAddress{
				Address: assoc.remote.Load().GetAddress(),
				Uid:     proto.Uint64(assoc.remote.Load().GetUid()),
			},
		}
		envBytes, err := proto.Marshal(env)
		if err != nil {
			t.Fatal(err)
		}
		return &ArteryMetadata{
			SerializerId:    17,
			MessageManifest: []byte("SystemMessage"),
			Payload:         envBytes,
		}
	}

	// First delivery: seq=5 dispatches.
	if err := assoc.handleSystemMessage(makeFrame(5)); err != nil {
		t.Fatalf("handleSystemMessage(5): %v", err)
	}
	if dispatched != 1 {
		t.Errorf("after first delivery, dispatched = %d, want 1", dispatched)
	}

	// Duplicate with same seq: must be deduped (no callback).
	if err := assoc.handleSystemMessage(makeFrame(5)); err != nil {
		t.Fatalf("handleSystemMessage(5 dup): %v", err)
	}
	if dispatched != 1 {
		t.Errorf("after duplicate, dispatched = %d, want 1 (dedupe failed)", dispatched)
	}

	// Older seq: also deduped.
	if err := assoc.handleSystemMessage(makeFrame(3)); err != nil {
		t.Fatalf("handleSystemMessage(3): %v", err)
	}
	if dispatched != 1 {
		t.Errorf("after older seq, dispatched = %d, want 1 (older-seq dedupe failed)", dispatched)
	}

	// Strictly higher seq: dispatches.
	if err := assoc.handleSystemMessage(makeFrame(6)); err != nil {
		t.Fatalf("handleSystemMessage(6): %v", err)
	}
	if dispatched != 2 {
		t.Errorf("after new seq=6, dispatched = %d, want 2", dispatched)
	}
}

func TestHandleControlMessage_DeliveryAckPrunesSenderBuffer(t *testing.T) {
	// Set up two associations on the same NodeManager:
	//   - outAssoc: OUTBOUND streamId=1 to a peer; this is where we
	//     buffer outbound SystemMessages and where the ack must prune.
	//   - inAssoc:  INBOUND  streamId=1 from the same peer; this is the
	//     assoc that handleControlMessage runs on when the ack arrives.
	// GetGekkaAssociationByHost(peerHost, peerPort) preferentially
	// returns the OUTBOUND streamId=1 assoc, so the prune routes to
	// outAssoc.systemOutbox even though the ack is dispatched on inAssoc.
	outAssoc, nm := newSystemRedeliveryAssoc(t, 16, 16, 100*time.Millisecond)
	peerHost := outAssoc.remote.Load().GetAddress().GetHostname()
	peerPort := outAssoc.remote.Load().GetAddress().GetPort()

	// Register outAssoc so GetGekkaAssociationByHost can find it.
	nm.RegisterAssociation(outAssoc.remote.Load(), outAssoc)
	outAssoc.mu.Lock()
	outAssoc.state = ASSOCIATED
	outAssoc.mu.Unlock()

	// Pre-populate outAssoc.systemOutbox with three buffered entries.
	for _, seq := range []uint64{10, 11, 12} {
		if err := outAssoc.systemOutbox.Enqueue(seq, []byte{byte(seq)}, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if got := outAssoc.SystemOutbox().Len(); got != 3 {
		t.Fatalf("setup: outbox Len = %d, want 3", got)
	}

	// inAssoc is the INBOUND on which the ack arrives. Same peer.
	inAssoc := &GekkaAssociation{
		state:    ASSOCIATED,
		role:     INBOUND,
		nodeMgr:  nm,
		streamId: 1,
		outbox:   make(chan []byte, 4),
	}
	inAssoc.remote.Store(&gproto_remote.UniqueAddress{
		Address: &gproto_remote.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("Peer"),
			Hostname: proto.String(peerHost),
			Port:     proto.Uint32(peerPort),
		},
		Uid: proto.Uint64(99),
	})

	// Build an inbound SystemMessageDeliveryAck frame with cumulative
	// ack of seq=11. After dispatch, outAssoc.systemOutbox should drop
	// {10, 11} and keep {12}.
	ack := &gproto_remote.SystemMessageDeliveryAck{
		SeqNo: proto.Uint64(11),
		From: &gproto_remote.UniqueAddress{
			Address: &gproto_remote.Address{
				Protocol: proto.String("pekko"),
				System:   proto.String("Peer"),
				Hostname: proto.String(peerHost),
				Port:     proto.Uint32(peerPort),
			},
			Uid: proto.Uint64(99),
		},
	}
	ackBytes, err := proto.Marshal(ack)
	if err != nil {
		t.Fatal(err)
	}
	meta := &ArteryMetadata{
		SerializerId:    17,
		MessageManifest: []byte("k"),
		Payload:         ackBytes,
	}

	if err := inAssoc.handleControlMessage(context.Background(), meta); err != nil {
		t.Fatalf("handleControlMessage(ack): %v", err)
	}

	if got := outAssoc.SystemOutbox().Len(); got != 1 {
		t.Errorf("after cumulative ack(11), outbox Len = %d, want 1 (only seq=12 remains)", got)
	}
	snap := outAssoc.SystemOutbox().Snapshot()
	if len(snap) != 1 || snap[0].seqNo != 12 {
		t.Errorf("after cumulative ack(11), remaining entry = %+v, want seqNo=12", snap)
	}
}

func TestHandleControlMessage_DeliveryAckIgnoredForUnknownPeer(t *testing.T) {
	outAssoc, _ := newSystemRedeliveryAssoc(t, 16, 16, 100*time.Millisecond)
	// Note: outAssoc is NOT registered with the NodeManager, so
	// GetGekkaAssociationByHost will not find it for the ack's
	// From address. The handler must no-op without panicking.
	if err := outAssoc.systemOutbox.Enqueue(5, []byte("x"), time.Now()); err != nil {
		t.Fatal(err)
	}

	ack := &gproto_remote.SystemMessageDeliveryAck{
		SeqNo: proto.Uint64(5),
		From: &gproto_remote.UniqueAddress{
			Address: &gproto_remote.Address{
				Protocol: proto.String("pekko"),
				System:   proto.String("Stranger"),
				Hostname: proto.String("203.0.113.99"),
				Port:     proto.Uint32(2552),
			},
			Uid: proto.Uint64(99),
		},
	}
	ackBytes, err := proto.Marshal(ack)
	if err != nil {
		t.Fatal(err)
	}
	meta := &ArteryMetadata{
		SerializerId:    17,
		MessageManifest: []byte("k"),
		Payload:         ackBytes,
	}

	if err := outAssoc.handleControlMessage(context.Background(), meta); err != nil {
		t.Fatalf("handleControlMessage(ack from unknown peer): %v", err)
	}
	if got := outAssoc.SystemOutbox().Len(); got != 1 {
		t.Errorf("ack from unknown peer pruned the buffer: Len = %d, want 1 (no-op)", got)
	}
}

func TestRunSystemRedelivery_GiveUpQuarantines(t *testing.T) {
	// Resend ticker every 30ms, give-up after 60ms — so by the second
	// tick at most, the head entry has exceeded give-up-after and the
	// loop must quarantine.
	assoc, nm := newSystemRedeliveryAssoc(t, 16, 16, 30*time.Millisecond)
	nm.GiveUpSystemMessageAfter = 60 * time.Millisecond

	// Provide a dummy conn so quarantine's conn.Close() is safe.
	connA, connB := net.Pipe()
	t.Cleanup(func() { _ = connA.Close(); _ = connB.Close() })
	go func() { _, _ = io.Copy(io.Discard, connB) }()
	assoc.conn = connA

	// Pre-populate one entry whose firstAttempt is already past the
	// deadline. The first tick will observe oldest < now-giveUp and
	// trigger the quarantine path immediately.
	old := time.Now().Add(-200 * time.Millisecond)
	if err := assoc.systemOutbox.Enqueue(1, []byte("x"), old); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan struct{})
	go func() {
		assoc.runSystemRedelivery(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runSystemRedelivery did not return after give-up deadline")
	}

	// Quarantine side-effects.
	assoc.mu.RLock()
	state := assoc.state
	since := assoc.quarantinedSince
	assoc.mu.RUnlock()
	if state != QUARANTINED {
		t.Errorf("after give-up, state = %v, want QUARANTINED", state)
	}
	if since.IsZero() {
		t.Error("quarantinedSince was not set")
	}
	if got := assoc.SystemOutbox().Len(); got != 0 {
		t.Errorf("after give-up, outbox not drained: Len = %d, want 0", got)
	}
}

func TestSendSystem_BufferFullQuarantines(t *testing.T) {
	assoc, _ := newSystemRedeliveryAssoc(t, 1, 16, 30*time.Millisecond)
	connA, connB := net.Pipe()
	t.Cleanup(func() { _ = connA.Close(); _ = connB.Close() })
	go func() { _, _ = io.Copy(io.Discard, connB) }()
	assoc.conn = connA

	if err := assoc.SendSystem(1, []byte("a")); err != nil {
		t.Fatalf("SendSystem(1) on empty buffer: %v", err)
	}

	// Now SendSystem must return ErrSystemOutboxFull AND quarantine.
	err := assoc.SendSystem(2, []byte("b"))
	if !errors.Is(err, ErrSystemOutboxFull) {
		t.Fatalf("SendSystem on full buffer = %v, want ErrSystemOutboxFull", err)
	}

	assoc.mu.RLock()
	state := assoc.state
	assoc.mu.RUnlock()
	if state != QUARANTINED {
		t.Errorf("after buffer-full SendSystem, state = %v, want QUARANTINED", state)
	}
	// Drain side-effect: buffer is empty post-quarantine.
	if got := assoc.SystemOutbox().Len(); got != 0 {
		t.Errorf("after buffer-full quarantine, outbox not drained: Len = %d, want 0", got)
	}
}

func TestQuarantineForSystemRedelivery_Idempotent(t *testing.T) {
	assoc, _ := newSystemRedeliveryAssoc(t, 4, 4, 30*time.Millisecond)
	connA, connB := net.Pipe()
	t.Cleanup(func() { _ = connA.Close(); _ = connB.Close() })
	go func() { _, _ = io.Copy(io.Discard, connB) }()
	assoc.conn = connA

	assoc.quarantineForSystemRedelivery("first")
	first := func() time.Time {
		assoc.mu.RLock()
		defer assoc.mu.RUnlock()
		return assoc.quarantinedSince
	}()
	time.Sleep(10 * time.Millisecond)
	assoc.quarantineForSystemRedelivery("second")
	second := func() time.Time {
		assoc.mu.RLock()
		defer assoc.mu.RUnlock()
		return assoc.quarantinedSince
	}()
	if !first.Equal(second) {
		t.Errorf("quarantinedSince changed on second call: %v vs %v", first, second)
	}
}

func TestRunSystemRedelivery_StopsOnContextCancel(t *testing.T) {
	assoc, _ := newSystemRedeliveryAssoc(t, 16, 16, 20*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		assoc.runSystemRedelivery(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runSystemRedelivery did not exit on ctx.Done()")
	}
}
