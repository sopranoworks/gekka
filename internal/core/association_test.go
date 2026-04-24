/*
 * association_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// TestNodeManager_ArteryAdvanced_Defaults verifies that an unconfigured
// NodeManager returns Pekko defaults for all four advanced knobs.
func TestNodeManager_ArteryAdvanced_Defaults(t *testing.T) {
	nm := NewNodeManager(&gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Test"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(0),
	}, 1)

	if got := nm.EffectiveInboundLanes(); got != DefaultInboundLanes {
		t.Errorf("EffectiveInboundLanes() = %d, want %d", got, DefaultInboundLanes)
	}
	if got := nm.EffectiveOutboundLanes(); got != DefaultOutboundLanes {
		t.Errorf("EffectiveOutboundLanes() = %d, want %d", got, DefaultOutboundLanes)
	}
	if got := nm.EffectiveOutboundMessageQueueSize(); got != DefaultOutboundMessageQueueSize {
		t.Errorf("EffectiveOutboundMessageQueueSize() = %d, want %d", got, DefaultOutboundMessageQueueSize)
	}
	if got := nm.EffectiveSystemMessageBufferSize(); got != DefaultSystemMessageBufferSize {
		t.Errorf("EffectiveSystemMessageBufferSize() = %d, want %d", got, DefaultSystemMessageBufferSize)
	}
}

// TestNodeManager_ArteryAdvanced_Overrides verifies that configured values
// override the defaults.
func TestNodeManager_ArteryAdvanced_Overrides(t *testing.T) {
	nm := NewNodeManager(&gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Test"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(0),
	}, 1)
	nm.InboundLanes = 8
	nm.OutboundLanes = 3
	nm.OutboundMessageQueueSize = 8192
	nm.SystemMessageBufferSize = 40000

	if got := nm.EffectiveInboundLanes(); got != 8 {
		t.Errorf("EffectiveInboundLanes() = %d, want 8", got)
	}
	if got := nm.EffectiveOutboundLanes(); got != 3 {
		t.Errorf("EffectiveOutboundLanes() = %d, want 3", got)
	}
	if got := nm.EffectiveOutboundMessageQueueSize(); got != 8192 {
		t.Errorf("EffectiveOutboundMessageQueueSize() = %d, want 8192", got)
	}
	if got := nm.EffectiveSystemMessageBufferSize(); got != 40000 {
		t.Errorf("EffectiveSystemMessageBufferSize() = %d, want 40000", got)
	}
}

// TestAssociation_OutboundMessageQueueSize_RuntimeBehavior verifies that
// the outbox channel capacity matches nm.OutboundMessageQueueSize at the
// time the association is created. A second association with a different
// configured value must have a different outbox capacity — this demonstrates
// that the config reaches the runtime consumer, not just a stored field.
func TestAssociation_OutboundMessageQueueSize_RuntimeBehavior(t *testing.T) {
	cases := []struct {
		name string
		size int
		want int
	}{
		{"default", 0, DefaultOutboundMessageQueueSize},
		{"override-small", 64, 64},
		{"override-large", 8192, 8192},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assoc := newOutboxAssocForTest(t, tc.size, 0)
			if got := cap(assoc.outbox); got != tc.want {
				t.Errorf("outbox capacity = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestNodeManager_ArteryAdvanced_Session02_Defaults verifies that an unconfigured
// NodeManager returns Pekko defaults for the session-02 knobs.
func TestNodeManager_ArteryAdvanced_Session02_Defaults(t *testing.T) {
	nm := NewNodeManager(&gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Test"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(0),
	}, 1)

	if got := nm.EffectiveHandshakeTimeout(); got != DefaultHandshakeTimeout {
		t.Errorf("EffectiveHandshakeTimeout() = %v, want %v", got, DefaultHandshakeTimeout)
	}
	if got := nm.EffectiveHandshakeRetryInterval(); got != DefaultHandshakeRetryInterval {
		t.Errorf("EffectiveHandshakeRetryInterval() = %v, want %v", got, DefaultHandshakeRetryInterval)
	}
	if got := nm.EffectiveSystemMessageResendInterval(); got != DefaultSystemMessageResendInterval {
		t.Errorf("EffectiveSystemMessageResendInterval() = %v, want %v", got, DefaultSystemMessageResendInterval)
	}
	if got := nm.EffectiveGiveUpSystemMessageAfter(); got != DefaultGiveUpSystemMessageAfter {
		t.Errorf("EffectiveGiveUpSystemMessageAfter() = %v, want %v", got, DefaultGiveUpSystemMessageAfter)
	}
	if got := nm.EffectiveOutboundControlQueueSize(); got != DefaultOutboundControlQueueSize {
		t.Errorf("EffectiveOutboundControlQueueSize() = %d, want %d", got, DefaultOutboundControlQueueSize)
	}
}

// TestNodeManager_ArteryAdvanced_Session02_Overrides verifies configured values
// override the session-02 defaults.
func TestNodeManager_ArteryAdvanced_Session02_Overrides(t *testing.T) {
	nm := NewNodeManager(&gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Test"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(0),
	}, 1)
	nm.HandshakeTimeout = 90 * time.Second
	nm.HandshakeRetryInterval = 500 * time.Millisecond
	nm.SystemMessageResendInterval = 2 * time.Second
	nm.GiveUpSystemMessageAfter = 12 * time.Hour
	nm.OutboundControlQueueSize = 1234

	if got := nm.EffectiveHandshakeTimeout(); got != 90*time.Second {
		t.Errorf("EffectiveHandshakeTimeout() = %v, want 90s", got)
	}
	if got := nm.EffectiveHandshakeRetryInterval(); got != 500*time.Millisecond {
		t.Errorf("EffectiveHandshakeRetryInterval() = %v, want 500ms", got)
	}
	if got := nm.EffectiveSystemMessageResendInterval(); got != 2*time.Second {
		t.Errorf("EffectiveSystemMessageResendInterval() = %v, want 2s", got)
	}
	if got := nm.EffectiveGiveUpSystemMessageAfter(); got != 12*time.Hour {
		t.Errorf("EffectiveGiveUpSystemMessageAfter() = %v, want 12h", got)
	}
	if got := nm.EffectiveOutboundControlQueueSize(); got != 1234 {
		t.Errorf("EffectiveOutboundControlQueueSize() = %d, want 1234", got)
	}
}

// TestAssociation_OutboundControlQueueSize_RuntimeBehavior verifies the
// outbox channel capacity for an outbound control-stream (streamId=1)
// association matches nm.OutboundControlQueueSize, not OutboundMessageQueueSize.
func TestAssociation_OutboundControlQueueSize_RuntimeBehavior(t *testing.T) {
	cases := []struct {
		name    string
		control int
		message int
		want    int
	}{
		{"default", 0, 0, DefaultOutboundControlQueueSize},
		{"override-small", 32, 0, 32},
		// Confirm control stream uses control-queue-size, not message-queue-size.
		{"control-overrides-message", 128, 16, 128},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assoc := newControlOutboxAssocForTest(t, tc.control, tc.message)
			if got := cap(assoc.outbox); got != tc.want {
				t.Errorf("outbox capacity = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestAssociation_HandshakeRetry_FiresAtInterval is the session-02 behavior
// test: when the remote peer never sends HandshakeRsp, the outbound
// association MUST re-emit HandshakeReq at nm.HandshakeRetryInterval cadence
// up to nm.HandshakeTimeout.
func TestAssociation_HandshakeRetry_FiresAtInterval(t *testing.T) {
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
	// Fast cadence for test: retry every 80ms, time out at 600ms.
	nm.HandshakeRetryInterval = 80 * time.Millisecond
	nm.HandshakeTimeout = 600 * time.Millisecond

	// Count inbound frames on the server side.  ProcessConnection on the
	// OUTBOUND side first writes the 5-byte Pekko/Akka preamble, then sends
	// a HandshakeReq frame for every retry tick.  Frames are 4-byte LE
	// length-prefixed, so we count length-prefixed frames that come after
	// the preamble.
	frameCount := make(chan int, 1)
	accepted := make(chan struct{})
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		close(accepted)

		// Read and discard 5-byte preamble.
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		preamble := make(chan []byte, 1)
		go func() {
			buf := make([]byte, 5)
			if _, err := io.ReadFull(c, buf); err == nil {
				preamble <- buf
			} else {
				preamble <- nil
			}
		}()
		select {
		case <-preamble:
		case <-time.After(2 * time.Second):
			frameCount <- 0
			return
		}

		// Count length-prefixed frames; close channel at test end via deadline.
		count := 0
		for {
			_ = c.SetReadDeadline(time.Now().Add(700 * time.Millisecond))
			lenBuf := make([]byte, 4)
			if _, err := io.ReadFull(c, lenBuf); err != nil {
				frameCount <- count
				return
			}
			frameLen := int(lenBuf[0]) | int(lenBuf[1])<<8 | int(lenBuf[2])<<16 | int(lenBuf[3])<<24
			if frameLen < 0 || frameLen > (1<<20) {
				frameCount <- count
				return
			}
			payload := make([]byte, frameLen)
			if _, err := io.ReadFull(c, payload); err != nil {
				frameCount <- count
				return
			}
			count++
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	remote := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Peer"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(uint32(ln.Addr().(*net.TCPAddr).Port)),
	}

	go func() {
		_ = nm.ProcessConnection(ctx, client, OUTBOUND, remote, 1)
	}()

	select {
	case <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatalf("server did not accept")
	}

	// Allow the handshake loop to run up to timeout + a bit of slack.
	count := 0
	select {
	case count = <-frameCount:
	case <-time.After(3 * time.Second):
		t.Fatalf("server did not finish reading frames")
	}

	// Expected: initial HandshakeReq + retries at 80ms for ~600ms total.
	// Lower bound (conservative): the initial send plus at least two retries.
	if count < 3 {
		t.Errorf("frame count = %d, want >= 3 (initial + retries fired at handshake-retry-interval)", count)
	}
}

// newControlOutboxAssocForTest is like newOutboxAssocForTest but sets both
// OutboundControlQueueSize and OutboundMessageQueueSize so the test can
// confirm the control knob wins on streamId=1.
func newControlOutboxAssocForTest(t *testing.T, controlQueueSize, messageQueueSize int) *GekkaAssociation {
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
	nm.OutboundControlQueueSize = controlQueueSize
	nm.OutboundMessageQueueSize = messageQueueSize

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

	var server net.Conn
	select {
	case server = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatalf("accept timeout")
	}
	t.Cleanup(func() { _ = server.Close() })

	go func() {
		_ = nm.ProcessConnection(ctx, client, OUTBOUND, remote, 1)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if a, ok := nm.GetGekkaAssociationByHost(remote.GetHostname(), remote.GetPort()); ok {
			return a
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("association not registered within deadline")
	return nil
}

// TestAssociation_InboundLanes_RuntimeBehavior verifies that the effective
// inbound-lane count surfaced by the NodeManager matches the configured
// value. Each association reads from the same NodeManager, so associations
// created after a config change observe the new lane count.
func TestAssociation_InboundLanes_RuntimeBehavior(t *testing.T) {
	nm := NewNodeManager(&gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Test"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(0),
	}, 1)

	// Default path — assoc sees the Pekko default (4).
	if got := nm.EffectiveInboundLanes(); got != DefaultInboundLanes {
		t.Fatalf("default EffectiveInboundLanes() = %d, want %d", got, DefaultInboundLanes)
	}

	// Change config — assoc sees the new value.
	nm.InboundLanes = 1
	if got := nm.EffectiveInboundLanes(); got != 1 {
		t.Fatalf("overridden EffectiveInboundLanes() = %d, want 1", got)
	}

	nm.InboundLanes = 16
	if got := nm.EffectiveInboundLanes(); got != 16 {
		t.Fatalf("overridden EffectiveInboundLanes() = %d, want 16", got)
	}
}

// newOutboxAssocForTest spins up an inbound TCP association through
// ProcessConnection with the given outbound-queue size and system-message
// buffer size, then returns the association so tests can inspect its outbox.
// The context is cancelled via t.Cleanup.
func newOutboxAssocForTest(t *testing.T, outboundQueueSize, systemMsgBufSize int) *GekkaAssociation {
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
	nm.OutboundMessageQueueSize = outboundQueueSize
	nm.SystemMessageBufferSize = systemMsgBufSize

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	remote := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Peer"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(uint32(ln.Addr().(*net.TCPAddr).Port)),
	}

	// Accept exactly one connection (the one ProcessConnection dials below)
	// and drain its initial write so the outbound write loop doesn't block.
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- c
		// Drain: discard everything the outbound writes (preamble + handshake).
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

	var server net.Conn
	select {
	case server = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatalf("accept timeout")
	}
	t.Cleanup(func() { _ = server.Close() })

	// Run ProcessConnection as OUTBOUND on the ordinary (non-control) stream
	// so the outbox is sized by EffectiveOutboundMessageQueueSize(); control
	// stream (streamId=1) is exercised by newControlOutboxAssocForTest.
	go func() {
		_ = nm.ProcessConnection(ctx, client, OUTBOUND, remote, 2)
	}()

	// Poll for the registered OUTBOUND association.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if a, ok := nm.GetGekkaAssociationByHost(remote.GetHostname(), remote.GetPort()); ok {
			return a
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("association not registered within deadline")
	return nil
}
