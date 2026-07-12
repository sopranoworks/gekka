/*
 * handshake_test.go
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

	"github.com/sopranoworks/gekka/actor"
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
	frame, err := BuildArteryFrame(0, actor.ArteryInternalSerializerID, "", "", manifest, payload, true)
	if err != nil {
		t.Fatalf("BuildArteryFrame: %v", err)
	}
	if err := WriteFrame(conn, frame); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
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

	// Pre-register a fake OUTBOUND control association.  As of the
	// dashboard self-down fix (2026-05-16), handleHandshakeReq routes
	// HandshakeRsp to the OUTBOUND-control outbox of the peer, never
	// back over the INBOUND TCP — Pekko's Artery treats INBOUND TCP as
	// write-only from its peer's side and rejects bytes with
	// "Unexpected incoming bytes" + quarantines the association.  The
	// test mirrors production by setting up a sibling OUTBOUND so the
	// rsp has a legitimate channel.  See association.go
	// `handleHandshakeReq` for the rationale.
	outbound := preregisterOutboundControl(t, nm, remoteAddr.Address, remoteAddr.GetUid())

	// 1. Send Artery TCP magic header (OUTBOUND side sends this first).
	sendMagic(t, client, 1)

	// 2. Send HandshakeReq with correct 'To' address.
	req := &gproto_remote.HandshakeReq{From: remoteAddr, To: localAddr}
	sendArteryFrame(t, client, "d", req)

	// 3. Read HandshakeRsp (manifest "e", payload = MessageWithAddress)
	// off the pre-registered OUTBOUND outbox.
	var frame []byte
	select {
	case frame = <-outbound.outbox:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for HandshakeRsp on OUTBOUND outbox")
	}
	meta, err := ParseArteryFrame(frame, nil, 0)
	if err != nil {
		t.Fatalf("ParseArteryFrame: %v", err)
	}
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
	assoc, ok := nm.GetAssociation(remoteAddr, 1)
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

	// 0. Handshake.  This test does NOT preregister an OUTBOUND because it
	// verifies state transitions on the INBOUND assoc (which is the one
	// the Quarantined control frame mutates).  Without a preregistered
	// OUTBOUND, handleHandshakeReq's post-2026-05-16 logic simply drops
	// the HandshakeRsp — that's correct for this test's purpose: we don't
	// need the rsp, and the dropped path leaves the INBOUND registered so
	// GetAssociation below returns the assoc whose state we want to check.
	sendMagic(t, client, 1)
	sendArteryFrame(t, client, "d", &gproto_remote.HandshakeReq{From: remoteUA, To: localAddr})

	// Give handleHandshakeReq a moment to complete its work (the test's
	// next sendArteryFrame writes synchronously to the pipe, so we must
	// not race with it).
	time.Sleep(50 * time.Millisecond)

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

	foundAssoc, ok := nm.GetAssociation(remoteUA, 1)
	if !ok {
		t.Fatal("association not found")
	}
	if foundAssoc.GetState() != QUARANTINED {
		t.Errorf("expected state QUARANTINED, got %v", foundAssoc.GetState())
	}
}
