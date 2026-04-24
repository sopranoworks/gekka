/*
 * compression_tcp_config_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// TestCompressionTcpConfig_Defaults verifies the Effective*() accessors for
// the new round2-session-04 knobs fall back to the Pekko reference defaults
// when the NodeManager field is zero-valued.
func TestCompressionTcpConfig_Defaults(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)

	if got := nm.EffectiveCompressionActorRefsMax(); got != DefaultCompressionActorRefsMax {
		t.Errorf("EffectiveCompressionActorRefsMax = %d, want %d", got, DefaultCompressionActorRefsMax)
	}
	if got := nm.EffectiveCompressionActorRefsAdvertisementInterval(); got != DefaultCompressionActorRefsAdvertisementInterval {
		t.Errorf("EffectiveCompressionActorRefsAdvertisementInterval = %v, want %v",
			got, DefaultCompressionActorRefsAdvertisementInterval)
	}
	if got := nm.EffectiveCompressionManifestsMax(); got != DefaultCompressionManifestsMax {
		t.Errorf("EffectiveCompressionManifestsMax = %d, want %d", got, DefaultCompressionManifestsMax)
	}
	if got := nm.EffectiveCompressionManifestsAdvertisementInterval(); got != DefaultCompressionManifestsAdvertisementInterval {
		t.Errorf("EffectiveCompressionManifestsAdvertisementInterval = %v, want %v",
			got, DefaultCompressionManifestsAdvertisementInterval)
	}
	if got := nm.EffectiveTcpConnectionTimeout(); got != DefaultTcpConnectionTimeout {
		t.Errorf("EffectiveTcpConnectionTimeout = %v, want %v", got, DefaultTcpConnectionTimeout)
	}
	if got := nm.EffectiveTcpOutboundClientHostname(); got != "" {
		t.Errorf("EffectiveTcpOutboundClientHostname = %q, want empty", got)
	}
	if got := nm.EffectiveBufferPoolSize(); got != DefaultBufferPoolSize {
		t.Errorf("EffectiveBufferPoolSize = %d, want %d", got, DefaultBufferPoolSize)
	}
	if got := nm.EffectiveMaximumLargeFrameSize(); got != DefaultMaximumLargeFrameSize {
		t.Errorf("EffectiveMaximumLargeFrameSize = %d, want %d", got, DefaultMaximumLargeFrameSize)
	}
	if got := nm.EffectiveLargeBufferPoolSize(); got != DefaultLargeBufferPoolSize {
		t.Errorf("EffectiveLargeBufferPoolSize = %d, want %d", got, DefaultLargeBufferPoolSize)
	}
	if got := nm.EffectiveOutboundLargeMessageQueueSize(); got != DefaultOutboundLargeMessageQueueSize {
		t.Errorf("EffectiveOutboundLargeMessageQueueSize = %d, want %d",
			got, DefaultOutboundLargeMessageQueueSize)
	}
}

// TestCompressionTcpConfig_Overrides verifies each accessor honors the
// configured override instead of the Pekko default.
func TestCompressionTcpConfig_Overrides(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)

	nm.CompressionActorRefsMax = 512
	nm.CompressionActorRefsAdvertisementInterval = 45 * time.Second
	nm.CompressionManifestsMax = 1024
	nm.CompressionManifestsAdvertisementInterval = 90 * time.Second
	nm.TcpConnectionTimeout = 150 * time.Millisecond
	nm.TcpOutboundClientHostname = "10.1.2.3"
	nm.BufferPoolSize = 64
	nm.MaximumLargeFrameSize = 4 * 1024 * 1024
	nm.LargeBufferPoolSize = 16
	nm.OutboundLargeMessageQueueSize = 1024

	if got := nm.EffectiveCompressionActorRefsMax(); got != 512 {
		t.Errorf("EffectiveCompressionActorRefsMax = %d, want 512", got)
	}
	if got := nm.EffectiveCompressionActorRefsAdvertisementInterval(); got != 45*time.Second {
		t.Errorf("EffectiveCompressionActorRefsAdvertisementInterval = %v, want 45s", got)
	}
	if got := nm.EffectiveCompressionManifestsMax(); got != 1024 {
		t.Errorf("EffectiveCompressionManifestsMax = %d, want 1024", got)
	}
	if got := nm.EffectiveCompressionManifestsAdvertisementInterval(); got != 90*time.Second {
		t.Errorf("EffectiveCompressionManifestsAdvertisementInterval = %v, want 90s", got)
	}
	if got := nm.EffectiveTcpConnectionTimeout(); got != 150*time.Millisecond {
		t.Errorf("EffectiveTcpConnectionTimeout = %v, want 150ms", got)
	}
	if got := nm.EffectiveTcpOutboundClientHostname(); got != "10.1.2.3" {
		t.Errorf("EffectiveTcpOutboundClientHostname = %q, want 10.1.2.3", got)
	}
	if got := nm.EffectiveBufferPoolSize(); got != 64 {
		t.Errorf("EffectiveBufferPoolSize = %d, want 64", got)
	}
	if got := nm.EffectiveMaximumLargeFrameSize(); got != 4*1024*1024 {
		t.Errorf("EffectiveMaximumLargeFrameSize = %d, want 4 MiB", got)
	}
	if got := nm.EffectiveLargeBufferPoolSize(); got != 16 {
		t.Errorf("EffectiveLargeBufferPoolSize = %d, want 16", got)
	}
	if got := nm.EffectiveOutboundLargeMessageQueueSize(); got != 1024 {
		t.Errorf("EffectiveOutboundLargeMessageQueueSize = %d, want 1024", got)
	}
}

// TestDialRemote_TcpConnectionTimeoutFires verifies the configured
// tcp.connection-timeout governs DialRemote's dial deadline — dialing a
// listener that accepts but never speaks Artery must complete within the
// configured window (here, 150ms), not the Pekko default 5s.
func TestDialRemote_TcpConnectionTimeoutFires(t *testing.T) {
	// A listener that accepts connections but never writes/reads — DialRemote
	// will connect, but never reach ASSOCIATED. The timeout fires and
	// DialRemote returns "dial timeout".
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			// Hold the connection open; never send a handshake.
			go func(c net.Conn) {
				buf := make([]byte, 1)
				_, _ = c.Read(buf)
				c.Close()
			}(c)
		}
	}()

	host, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	p, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse port %q: %v", portStr, err)
	}
	port := uint32(p)

	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "Local"), 1)
	nm.TcpConnectionTimeout = 150 * time.Millisecond

	target := &gproto_remote.Address{
		Protocol: proto.String("akka"),
		System:   proto.String("Remote"),
		Hostname: proto.String(host),
		Port:     proto.Uint32(port),
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = nm.DialRemote(ctx, target)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("DialRemote: expected timeout error, got nil")
	}
	// DialRemote's own "wait for association" poll deadline is
	// EffectiveTcpConnectionTimeout. Allow slack for scheduling, but it must
	// not approach the Pekko default of 5s.
	if elapsed > 2*time.Second {
		t.Errorf("DialRemote elapsed = %v; expected < 2s (configured timeout 150ms)", elapsed)
	}
}
