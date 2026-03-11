/*
 * handshake_test.go
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

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// sendMagic writes the Artery TCP connection magic header (AKKA + streamId).
// In Pekko Artery, the OUTBOUND (client) side writes this before any frames.
func sendMagic(t *testing.T, conn net.Conn, streamId byte) {
	t.Helper()
	if _, err := conn.Write([]byte{'A', 'K', 'K', 'A', streamId}); err != nil {
		t.Fatalf("sendMagic: %v", err)
	}
}

// sendArteryFrame builds and writes a proper Artery binary frame.
func sendArteryFrame(t *testing.T, conn net.Conn, manifest string, msg proto.Message) {
	t.Helper()
	payload, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	frame, err := BuildArteryFrame(0, ArteryInternalSerializerID, "", "", manifest, payload, true)
	if err != nil {
		t.Fatalf("BuildArteryFrame: %v", err)
	}
	if err := WriteFrame(conn, frame); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
}

// readArteryFrame reads one length-prefixed Artery frame and parses it.
func readArteryFrame(t *testing.T, conn net.Conn) *ArteryMetadata {
	t.Helper()
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		t.Fatalf("readArteryFrame header: %v", err)
	}
	length := binary.LittleEndian.Uint32(header)
	frameBytes := make([]byte, length)
	if _, err := io.ReadFull(conn, frameBytes); err != nil {
		t.Fatalf("readArteryFrame payload: %v", err)
	}
	meta, err := ParseArteryFrame(frameBytes, nil, 0)
	if err != nil {
		t.Fatalf("ParseArteryFrame: %v", err)
	}
	return meta
}

func TestArteryHandshake_Success(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	localAddr := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("localSystem"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	nm := NewNodeManager(localAddr, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	handler := TcpArteryHandlerWithNodeManager(nm)
	go func() {
		_ = handler(ctx, server)
	}()

	remoteAddr := &gproto_remote.UniqueAddress{
		Address: &gproto_remote.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("remoteSystem"),
			Hostname: proto.String("10.0.0.1"),
			Port:     proto.Uint32(2552),
		},
		Uid: proto.Uint64(12345),
	}

	// 1. Send Artery TCP magic header (OUTBOUND side sends this first).
	sendMagic(t, client, 1)

	// 2. Send HandshakeReq with correct 'To' address.
	req := &gproto_remote.HandshakeReq{From: remoteAddr, To: localAddr}
	sendArteryFrame(t, client, "d", req)

	// 3. Read HandshakeRsp (manifest "e", payload = MessageWithAddress).
	meta := readArteryFrame(t, client)
	if string(meta.MessageManifest) != "e" {
		t.Errorf("expected manifest %q, got %q", "e", string(meta.MessageManifest))
	}
	mwa := &gproto_remote.MessageWithAddress{}
	if err := proto.Unmarshal(meta.Payload, mwa); err != nil {
		t.Fatalf("unmarshal HandshakeRsp: %v", err)
	}
	if mwa.Address.Address.GetSystem() != "localSystem" {
		t.Errorf("expected system localSystem, got %s", mwa.Address.Address.GetSystem())
	}

	// 4. Verify association state.
	assoc, ok := nm.GetAssociation(remoteAddr)
	if !ok {
		t.Fatal("association not found")
	}
	if assoc.GetState() != ASSOCIATED {
		t.Errorf("expected state ASSOCIATED, got %v", assoc.GetState())
	}
}

func TestArteryHandshake_AddressMismatch(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	localAddr := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("localSystem"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	nm := NewNodeManager(localAddr, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	handler := TcpArteryHandlerWithNodeManager(nm)
	errChan := make(chan error, 1)
	go func() {
		errChan <- handler(ctx, server)
	}()

	// Send magic.
	sendMagic(t, client, 1)

	// Send HandshakeReq with WRONG 'To' system name.
	wrongTo := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("wrongSystem"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	req := &gproto_remote.HandshakeReq{
		From: &gproto_remote.UniqueAddress{
			Address: &gproto_remote.Address{
				Protocol: proto.String("pekko"),
				System:   proto.String("remoteSystem"),
				Hostname: proto.String("10.0.0.1"),
				Port:     proto.Uint32(2552),
			},
			Uid: proto.Uint64(123),
		},
		To: wrongTo,
	}
	sendArteryFrame(t, client, "d", req)

	// Handler should return an error due to address mismatch.
	select {
	case err := <-errChan:
		if err == nil {
			t.Error("expected error due to address mismatch, got nil")
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("timeout waiting for handshake failure")
	}
}

func TestArteryControl_HeartbeatAndQuarantine(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	localAddr := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("localSystem"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	nm := NewNodeManager(localAddr, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	handler := TcpArteryHandlerWithNodeManager(nm)
	go func() {
		_ = handler(ctx, server)
	}()

	remoteUA := &gproto_remote.UniqueAddress{
		Address: &gproto_remote.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("remoteSystem"),
			Hostname: proto.String("10.0.0.1"),
			Port:     proto.Uint32(2552),
		},
		Uid: proto.Uint64(123),
	}

	// 0. Handshake.
	sendMagic(t, client, 1)
	sendArteryFrame(t, client, "d", &gproto_remote.HandshakeReq{From: remoteUA, To: localAddr})

	// Drain HandshakeRsp.
	readArteryFrame(t, client)

	// 1. Send ArteryHeartbeatRsp (manifest "n").
	sendArteryFrame(t, client, "n", &gproto_remote.ArteryHeartbeatRsp{Uid: proto.Uint64(123)})

	// 2. Send Quarantined (manifest "Quarantined").
	quar := &gproto_remote.Quarantined{
		From: remoteUA,
		To: &gproto_remote.UniqueAddress{
			Address: localAddr,
			Uid:     proto.Uint64(1),
		},
	}
	sendArteryFrame(t, client, "Quarantined", quar)

	// 3. Wait a moment for the handler to process the Quarantined message.
	time.Sleep(100 * time.Millisecond)

	foundAssoc, ok := nm.GetAssociation(remoteUA)
	if !ok {
		t.Fatal("association not found")
	}
	if foundAssoc.GetState() != QUARANTINED {
		t.Errorf("expected state QUARANTINED, got %v", foundAssoc.GetState())
	}
}
