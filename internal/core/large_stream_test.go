/*
 * large_stream_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// TestAssociation_EffectiveStreamFrameSizeCap_StreamId3 verifies the
// Round-2 session 30 wiring: a stream-3 (Large) association uses
// maximum-large-frame-size, while streams 1 and 2 use maximum-frame-size.
func TestAssociation_EffectiveStreamFrameSizeCap_StreamId3(t *testing.T) {
	nm := NewNodeManager(&gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Test"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(0),
	}, 1)
	nm.MaxFrameSize = 64 * 1024              // 64 KiB ordinary cap
	nm.MaximumLargeFrameSize = 4 * 1024 * 1024 // 4 MiB large cap

	for _, tc := range []struct {
		name     string
		streamId int32
		want     int
	}{
		{"control stream uses ordinary cap", AeronStreamControl, 64 * 1024},
		{"ordinary stream uses ordinary cap", AeronStreamOrdinary, 64 * 1024},
		{"large stream uses large cap", AeronStreamLarge, 4 * 1024 * 1024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assoc := &GekkaAssociation{nodeMgr: nm, streamId: tc.streamId}
			if got := assoc.effectiveStreamFrameSizeCap(); got != tc.want {
				t.Errorf("effectiveStreamFrameSizeCap(streamId=%d) = %d, want %d", tc.streamId, got, tc.want)
			}
		})
	}
}

// TestTcpArteryReadLoop_LargeFrame_AcceptedUnderLargeCap verifies the
// underlying read loop honors a stream-3-sized cap: a frame between
// MaxFrameSize and MaximumLargeFrameSize is dispatched without rejection.
//
// This is the behavioral pair to TestAssociation_EffectiveStreamFrameSizeCap_StreamId3:
// together they prove (1) Process() picks the correct cap for streamId=3, and
// (2) the read loop actually accepts a payload sized for that cap.
func TestTcpArteryReadLoop_LargeFrame_AcceptedUnderLargeCap(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	const largeCap = 2 * 1024 * 1024 // 2 MiB
	const payloadSize = 600 * 1024   // 600 KiB — above 256 KiB ordinary, below 2 MiB large

	dispatched := make(chan int, 1)
	handler := func(ctx context.Context, meta *ArteryMetadata) error {
		dispatched <- len(meta.Payload)
		return nil
	}

	go func() {
		_ = tcpArteryReadLoop(ctx, server, handler, nil, 0, AeronStreamLarge, largeCap)
	}()

	// Build a real Artery frame whose payload happens to be ~600 KiB. We use a
	// HandshakeReq-shaped frame and shovel filler bytes into a manifest section
	// that downstream parsing tolerates, but for this read-loop test we only
	// need a valid 4-byte length prefix; the dispatch handler records the
	// payload size we actually received.
	bigPayload := bytes.Repeat([]byte("A"), payloadSize)
	frame, err := BuildArteryFrame(0, 4, "", "/user/large-1", "B", bigPayload, false)
	if err != nil {
		t.Fatalf("BuildArteryFrame: %v", err)
	}
	if err := WriteFrame(client, frame); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	select {
	case got := <-dispatched:
		if got != payloadSize {
			t.Errorf("dispatched payload size = %d, want %d", got, payloadSize)
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatalf("read loop did not dispatch the large frame")
	}
}

// TestTcpArteryReadLoop_LargeFrame_RejectedAboveCap verifies that a frame
// exceeding the configured large-frame cap is still rejected — the larger
// cap is a ceiling, not a bypass.
func TestTcpArteryReadLoop_LargeFrame_RejectedAboveCap(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	const largeCap = 1 * 1024 * 1024 // 1 MiB

	errCh := make(chan error, 1)
	go func() {
		errCh <- tcpArteryReadLoop(ctx, server, func(ctx context.Context, meta *ArteryMetadata) error {
			return nil
		}, nil, 0, AeronStreamLarge, largeCap)
	}()

	// Write a length header that claims a 2 MiB body (above the 1 MiB cap).
	// The read loop must abort on the length check before reading body bytes.
	header := make([]byte, 4)
	binary.LittleEndian.PutUint32(header, uint32(largeCap+1))
	if _, err := client.Write(header); err != nil {
		t.Fatalf("write header: %v", err)
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("expected read loop to fail on oversize frame, got nil")
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatalf("read loop did not reject the oversize frame")
	}
}

// TestProcessConnection_StreamId3_MagicAccepted verifies that an inbound
// TCP connection presenting the stream-3 magic header is accepted by
// ProcessConnection — i.e. the magic-parsing branch handles streamId=3
// without collision against streams 1/2. We assert acceptance by observing
// that ProcessConnection enters the read loop and blocks rather than
// returning an immediate "invalid artery magic" error.
func TestProcessConnection_StreamId3_MagicAccepted(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	localAddr := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Test"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	nm := NewNodeManager(localAddr, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	procDone := make(chan error, 1)
	go func() {
		procDone <- nm.ProcessConnection(ctx, server, INBOUND, nil, 0)
	}()

	// Send Akka stream-3 magic (AKKA + 0x03). Pekko nodes also accept this
	// preamble since the wire protocol is unchanged from Akka 2.6.
	if _, err := client.Write([]byte{'A', 'K', 'K', 'A', byte(AeronStreamLarge)}); err != nil {
		t.Fatalf("write magic: %v", err)
	}

	// If the magic were rejected, ProcessConnection would return immediately
	// with "invalid artery magic". Confirm it is still blocked in the read
	// loop after a short grace window.
	select {
	case err := <-procDone:
		t.Fatalf("ProcessConnection returned early — magic was rejected? err=%v", err)
	case <-time.After(150 * time.Millisecond):
		// Magic accepted, read loop is waiting for the next frame's length header.
	}

	// Cancel and clean up so the goroutine exits.
	cancel()
	_ = server.Close()
	select {
	case <-procDone:
	case <-time.After(500 * time.Millisecond):
		// ok — ProcessConnection may take a moment to unwind
	}
}
