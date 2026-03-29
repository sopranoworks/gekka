/*
 * quarantine_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// readArteryFrameNoFatal reads a length-prefixed Artery frame without calling t.Fatal.
func readArteryFrameNoFatal(conn net.Conn) (*ArteryMetadata, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}
	length := binary.LittleEndian.Uint32(header)
	frameBytes := make([]byte, length)
	if _, err := io.ReadFull(conn, frameBytes); err != nil {
		return nil, err
	}
	return ParseArteryFrame(frameBytes, nil, 0)
}

// localAddr builds a minimal gproto_remote.Address for use in tests.
func quarTestAddr(host string, port uint32, system string) *gproto_remote.Address {
	return &gproto_remote.Address{
		Protocol: proto.String("akka"),
		System:   proto.String(system),
		Hostname: proto.String(host),
		Port:     proto.Uint32(port),
	}
}

// TestQuarantine_IsQuarantined verifies that RegisterQuarantinedUID correctly
// populates the registry and IsQuarantined returns the right answer.
func TestQuarantine_IsQuarantined(t *testing.T) {
	nm := NewNodeManager(quarTestAddr("127.0.0.1", 2552, "TestSystem"), 42)

	uid := uint64(999)
	remote := &gproto_remote.UniqueAddress{
		Address: quarTestAddr("10.0.0.1", 2551, "TestSystem"),
		Uid:     proto.Uint64(uid),
	}

	if nm.IsQuarantined(uid) {
		t.Fatal("expected IsQuarantined(uid) = false before registration")
	}

	nm.RegisterQuarantinedUID(remote)

	if !nm.IsQuarantined(uid) {
		t.Fatal("expected IsQuarantined(uid) = true after registration")
	}

	// Zero UID must never be quarantined.
	if nm.IsQuarantined(0) {
		t.Fatal("UID 0 must never be reported as quarantined")
	}

	// Unrelated UID must not be quarantined.
	if nm.IsQuarantined(12345) {
		t.Fatal("unrelated UID 12345 must not be quarantined")
	}
}

// TestQuarantine_QuarantinedUIDs verifies the snapshot method.
func TestQuarantine_QuarantinedUIDs(t *testing.T) {
	nm := NewNodeManager(quarTestAddr("127.0.0.1", 2552, "TestSystem"), 42)

	if ids := nm.QuarantinedUIDs(); len(ids) != 0 {
		t.Fatalf("expected empty quarantine registry, got %v", ids)
	}

	for i := uint64(1); i <= 3; i++ {
		nm.RegisterQuarantinedUID(&gproto_remote.UniqueAddress{
			Address: quarTestAddr("10.0.0.1", uint32(2550+i), "TestSystem"),
			Uid:     proto.Uint64(i),
		})
	}

	ids := nm.QuarantinedUIDs()
	if len(ids) != 3 {
		t.Fatalf("expected 3 quarantined UIDs, got %d: %v", len(ids), ids)
	}
}

// TestQuarantine_SendFrameEnqueued verifies that SendQuarantined enqueues a
// properly encoded "Quarantined" Artery control frame on the association outbox.
func TestQuarantine_SendFrameEnqueued(t *testing.T) {
	nm := NewNodeManager(quarTestAddr("127.0.0.1", 2552, "TestSystem"), 99)

	assoc := &GekkaAssociation{
		state:    ASSOCIATED,
		localUid: nm.localUid,
		outbox:   make(chan []byte, 10),
		nodeMgr:  nm,
	}

	to := &gproto_remote.UniqueAddress{
		Address: quarTestAddr("10.0.0.2", 2551, "TestSystem"),
		Uid:     proto.Uint64(777),
	}

	assoc.SendQuarantined(to)

	select {
	case frame := <-assoc.outbox:
		// Parse the frame to confirm it contains a Quarantined message.
		meta, err := ParseArteryFrame(frame, nil, 0)
		if err != nil {
			t.Fatalf("ParseArteryFrame: %v", err)
		}
		if string(meta.MessageManifest) != "Quarantined" {
			t.Fatalf("expected manifest 'Quarantined', got %q", string(meta.MessageManifest))
		}
		if meta.SerializerId != actor.ArteryInternalSerializerID {
			t.Fatalf("expected serializer %d, got %d", actor.ArteryInternalSerializerID, meta.SerializerId)
		}
		var quar gproto_remote.Quarantined
		if err := proto.Unmarshal(meta.Payload, &quar); err != nil {
			t.Fatalf("proto.Unmarshal Quarantined: %v", err)
		}
		if quar.GetTo().GetUid() != 777 {
			t.Fatalf("expected To.UID=777, got %d", quar.GetTo().GetUid())
		}
		if quar.GetFrom().GetUid() != 99 {
			t.Fatalf("expected From.UID=99, got %d", quar.GetFrom().GetUid())
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Quarantined frame in outbox")
	}
}

// TestQuarantine_ReceiveQuarantinedFrame verifies that an association which
// receives a "Quarantined" control frame transitions to QUARANTINED state and
// registers the sender UID in the permanent registry.
func TestQuarantine_ReceiveQuarantinedFrame(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nm := NewNodeManager(quarTestAddr("127.0.0.1", 2552, "TestSystem"), 100)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	serverDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		serverDone <- nm.ProcessConnection(ctx, conn, INBOUND, nil, 1)
	}()

	clientConn, err := net.DialTimeout("tcp", ln.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer clientConn.Close()

	// Write Artery OUTBOUND magic: AKKA + streamId=1
	if _, err := clientConn.Write([]byte{'A', 'K', 'K', 'A', 1}); err != nil {
		t.Fatalf("write magic: %v", err)
	}

	remoteUID := uint64(555)

	// Step 1: Send HandshakeReq so the server creates a proper association.
	hsReq := &gproto_remote.HandshakeReq{
		From: &gproto_remote.UniqueAddress{
			Address: quarTestAddr("127.0.0.1", uint32(ln.Addr().(*net.TCPAddr).Port), "TestSystem"),
			Uid:     proto.Uint64(remoteUID),
		},
		To: quarTestAddr("127.0.0.1", 2552, "TestSystem"),
	}
	sendArteryFrame(t, clientConn, "d", hsReq)

	// Read HandshakeRsp from server.
	_ = clientConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	readArteryFrame(t, clientConn)
	_ = clientConn.SetReadDeadline(time.Time{})

	// Step 2: Send Quarantined control frame from the "remote" node to ourselves.
	quarMsg := &gproto_remote.Quarantined{
		From: &gproto_remote.UniqueAddress{
			Address: quarTestAddr("127.0.0.1", uint32(ln.Addr().(*net.TCPAddr).Port), "TestSystem"),
			Uid:     proto.Uint64(remoteUID),
		},
		To: &gproto_remote.UniqueAddress{
			Address: quarTestAddr("127.0.0.1", 2552, "TestSystem"),
			Uid:     proto.Uint64(100),
		},
	}
	sendArteryFrame(t, clientConn, "Quarantined", quarMsg)

	// Give the server goroutine time to process the frame.
	time.Sleep(200 * time.Millisecond)

	// Verify the UID is now in the permanent quarantine registry.
	if !nm.IsQuarantined(remoteUID) {
		t.Fatalf("expected UID %d to be quarantined after receiving Quarantined frame", remoteUID)
	}

	// Verify HasQuarantinedAssociation reflects the state.
	if !nm.HasQuarantinedAssociation() {
		t.Fatal("expected HasQuarantinedAssociation() = true after Quarantined frame")
	}

	clientConn.Close()
	cancel()

	select {
	case <-serverDone:
	case <-time.After(3 * time.Second):
		t.Fatal("server goroutine did not finish")
	}
}

// TestQuarantine_RejectReconnectFromQuarantinedUID verifies that a subsequent
// HandshakeReq from a permanently quarantined UID is rejected.
func TestQuarantine_RejectReconnectFromQuarantinedUID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nm := NewNodeManager(quarTestAddr("127.0.0.1", 2552, "TestSystem"), 200)

	quarantinedUID := uint64(888)
	nm.RegisterQuarantinedUID(&gproto_remote.UniqueAddress{
		Address: quarTestAddr("10.0.0.99", 2551, "TestSystem"),
		Uid:     proto.Uint64(quarantinedUID),
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	serverDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		// ProcessConnection returns an error when the handshake is rejected.
		serverDone <- nm.ProcessConnection(ctx, conn, INBOUND, nil, 1)
	}()

	clientConn, err := net.DialTimeout("tcp", ln.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer clientConn.Close()

	if _, err := clientConn.Write([]byte{'A', 'K', 'K', 'A', 1}); err != nil {
		t.Fatalf("write magic: %v", err)
	}

	// Send HandshakeReq with the quarantined UID.
	hsReq := &gproto_remote.HandshakeReq{
		From: &gproto_remote.UniqueAddress{
			Address: quarTestAddr("10.0.0.99", 2551, "TestSystem"),
			Uid:     proto.Uint64(quarantinedUID),
		},
		To: quarTestAddr("127.0.0.1", 2552, "TestSystem"),
	}
	sendArteryFrame(t, clientConn, "d", hsReq)

	// The server should close the connection after rejecting the handshake.
	// Try to read one frame — the server may either send a Quarantined frame or
	// immediately close the connection (both are acceptable protocol behaviour).
	clientConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	meta, frameErr := func() (*ArteryMetadata, error) {
		return readArteryFrameNoFatal(clientConn)
	}()
	clientConn.SetReadDeadline(time.Time{})

	if frameErr == nil && meta != nil {
		if string(meta.MessageManifest) == "Quarantined" {
			t.Log("server correctly replied with Quarantined frame before closing")
		} else {
			t.Logf("server sent frame with manifest=%q (not a HandshakeRsp — acceptable)", string(meta.MessageManifest))
		}
	}

	// Confirm the server goroutine completed with a quarantine rejection error.
	select {
	case err := <-serverDone:
		if err == nil {
			t.Fatal("expected ProcessConnection to return error for quarantined UID, got nil")
		}
		t.Logf("ProcessConnection correctly returned error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("server goroutine did not finish after rejecting quarantined handshake")
	}
}
