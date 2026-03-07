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
	remoteNM := NewNodeManager(remoteLocalAddr)

	go func() {
		conn, _ := ln.Accept()
		remoteNM.ProcessConnection(context.Background(), conn, INBOUND, nil)
	}()

	// 2. Setup Local Router
	localAddr := &Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("localSystem"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	localNM := NewNodeManager(localAddr)
	router := NewRouter(localNM)

	// 3. Send message (triggers dial)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	msg := &ArteryHeartbeatRsp{Uid: proto.Uint64(123)}
	err = router.Send(ctx, "pekko://remoteSystem@127.0.0.1:2553/user/receiver", msg)
	if err != nil {
		t.Fatalf("Router.Send failed: %v", err)
	}

	// 4. Verify association exists
	assoc, ok := router.getAssociationByHost("127.0.0.1", 2553)
	if !ok {
		t.Fatal("association should have been created")
	}
	if assoc.GetState() != ASSOCIATED {
		t.Errorf("expected state ASSOCIATED, got %v", assoc.GetState())
	}
}

func TestRouter_Buffering(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	nm := NewNodeManager(&Address{Hostname: proto.String("127.0.0.1")})
	assoc := &GekkaAssociation{
		state:     WAITING_FOR_HANDSHAKE,
		role:      OUTBOUND,
		conn:      server,
		nodeMgr:   nm,
		pending:   make([][]byte, 0),
		handshake: make(chan struct{}),
	}

	// Send while waiting (unframed payload, Send adds frame)
	payload := []byte("delayed-message")
	if err := assoc.Send("/user/test", payload, 1, "test"); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	if len(assoc.pending) != 1 {
		t.Fatal("message should be buffered")
	}

	// Complete handshake
	mwa := &MessageWithAddress{
		Address: &UniqueAddress{
			Address: &Address{Hostname: proto.String("remote")},
			Uid:     proto.Uint64(1),
		},
	}
	go func() {
		assoc.handleHandshakeRsp(mwa)
	}()

	// Read from client
	header := make([]byte, 4)
	if _, err := io.ReadFull(client, header); err != nil {
		t.Fatalf("failed to read header: %v", err)
	}
	length := binary.LittleEndian.Uint32(header)
	if length != uint32(len(payload)) {
		t.Errorf("expected length %d, got %d", len(payload), length)
	}
}
