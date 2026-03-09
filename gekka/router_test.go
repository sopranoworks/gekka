/*
 * router_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

func TestParseActorPath(t *testing.T) {
	raw := "pekko://mySystem@127.0.0.1:2552/user/echo"
	ap, err := ParseActorPath(raw)
	if err != nil {
		t.Fatalf("failed to parse path: %v", err)
	}

	if ap.Protocol != "pekko" {
		t.Errorf("expected protocol pekko, got %s", ap.Protocol)
	}
	if ap.System != "mySystem" {
		t.Errorf("expected system mySystem, got %s", ap.System)
	}
	if ap.Host != "127.0.0.1" {
		t.Errorf("expected host 127.0.0.1, got %s", ap.Host)
	}
	if ap.Port != 2552 {
		t.Errorf("expected port 2552, got %d", ap.Port)
	}
	if ap.Path != "/user/echo" {
		t.Errorf("expected path /user/echo, got %s", ap.Path)
	}
}

func TestRouter_Send_AutoDial(t *testing.T) {
	// 1. Setup Server (Target)
	serverAddr := "127.0.0.1:2553"
	ln, err := net.Listen("tcp", serverAddr)
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	remoteLocalAddr := &Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("remoteSystem"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2553),
	}
	remoteNM := NewNodeManager(remoteLocalAddr, 0)

	serverCtx, serverCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer serverCancel()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		_ = remoteNM.ProcessConnection(serverCtx, conn, INBOUND, nil, 0)
	}()

	// 2. Setup Local Router
	localAddr := &Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("localSystem"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	localNM := NewNodeManager(localAddr, 0)
	router := NewRouter(localNM)

	// 3. Send message (triggers dial + handshake)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	msg := &ArteryHeartbeatRsp{Uid: proto.Uint64(123)}
	if err := router.Send(ctx, "pekko://remoteSystem@127.0.0.1:2553/user/receiver", msg); err != nil {
		t.Fatalf("Router.Send failed: %v", err)
	}

	// 4. Verify association exists (dialRemote returns it as soon as it appears).
	assoc, ok := router.getAssociationByHost("127.0.0.1", 2553)
	if !ok {
		t.Fatal("association should have been created")
	}

	// 5. Wait up to 2 s for the handshake to complete (initiateHandshake sleeps 500 ms).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if assoc.GetState() == ASSOCIATED {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if assoc.GetState() != ASSOCIATED {
		t.Errorf("expected state ASSOCIATED, got %v", assoc.GetState())
	}
}

func TestRouter_Buffering(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	remoteAddr := &Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("remoteSystem"),
		Hostname: proto.String("10.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	nm := NewNodeManager(&Address{Hostname: proto.String("127.0.0.1")}, 0)

	// Build a fully-initialised outbound association that is registered in nm.
	assoc := &GekkaAssociation{
		state:     WAITING_FOR_HANDSHAKE,
		role:      OUTBOUND,
		conn:      server,
		nodeMgr:   nm,
		pending:   make([][]byte, 0),
		handshake: make(chan struct{}),
		outbox:    make(chan []byte, 100),
		remote: &UniqueAddress{
			Address: remoteAddr,
			Uid:     proto.Uint64(0),
		},
	}
	nm.mu.Lock()
	nm.associations["10.0.0.1:2552-0"] = assoc
	nm.mu.Unlock()

	// Start the outbox writer goroutine (normally started by ProcessConnection).
	outboxCtx, outboxCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer outboxCancel()
	go func() {
		for {
			select {
			case <-outboxCtx.Done():
				return
			case msg, ok := <-assoc.outbox:
				if !ok {
					return
				}
				_ = WriteFrame(assoc.conn, msg)
			}
		}
	}()

	// Send while waiting — message should be buffered, not written yet.
	payload := []byte("delayed-message")
	if err := assoc.Send("/user/test", payload, 4, ""); err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	if len(assoc.pending) != 1 {
		t.Fatal("message should be buffered in pending")
	}

	// Complete handshake: mwa.Address matches assoc.remote (host/port).
	mwa := &MessageWithAddress{
		Address: &UniqueAddress{
			Address: remoteAddr,
			Uid:     proto.Uint64(1),
		},
	}
	go func() {
		_ = assoc.handleHandshakeRsp(mwa)
	}()

	// The pending message should now be flushed through the outbox to `server`,
	// making it readable from `client` as a 4-byte length-prefixed Artery frame.
	header := make([]byte, 4)
	if _, err := io.ReadFull(client, header); err != nil {
		t.Fatalf("failed to read frame header after handshake: %v", err)
	}
	frameLen := binary.LittleEndian.Uint32(header)
	if frameLen == 0 {
		t.Fatal("expected non-zero frame length")
	}
	// The frame is a proper Artery binary frame (28-byte header + literals + payload).
	// Just verify the payload bytes contain our original message.
	frameBytes := make([]byte, frameLen)
	if _, err := io.ReadFull(client, frameBytes); err != nil {
		t.Fatalf("failed to read frame body: %v", err)
	}
	meta, err := ParseArteryFrame(frameBytes, nil, 0)
	if err != nil {
		t.Fatalf("ParseArteryFrame: %v", err)
	}
	if string(meta.Payload) != string(payload) {
		t.Errorf("expected payload %q, got %q", payload, meta.Payload)
	}
}
