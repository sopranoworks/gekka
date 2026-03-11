/*
 * dispatcher_test.go
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

func TestNodeRegistry_UIDRestart(t *testing.T) {
	localAddr := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("localSystem"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	nm := NewNodeManager(localAddr, 0)

	remoteUA1 := &gproto_remote.UniqueAddress{
		Address: &gproto_remote.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("remoteSystem"),
			Hostname: proto.String("10.0.0.1"),
			Port:     proto.Uint32(2552),
		},
		Uid: proto.Uint64(1),
	}
	remoteUA2 := &gproto_remote.UniqueAddress{
		Address: &gproto_remote.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("remoteSystem"),
			Hostname: proto.String("10.0.0.1"),
			Port:     proto.Uint32(2552),
		},
		Uid: proto.Uint64(2), // New UID
	}

	conn1, _ := net.Pipe()
	assoc1 := &GekkaAssociation{state: ASSOCIATED, conn: conn1, nodeMgr: nm}
	nm.RegisterAssociation(remoteUA1, assoc1)

	// Verify UA1 is registered
	if _, ok := nm.GetAssociation(remoteUA1); !ok {
		t.Fatal("UA1 should be registered")
	}

	// Register UA2 (same host, different UID)
	conn2, _ := net.Pipe()
	assoc2 := &GekkaAssociation{state: ASSOCIATED, conn: conn2, nodeMgr: nm}
	nm.RegisterAssociation(remoteUA2, assoc2)

	// Verify UA1 is now quarantined and removed
	if assoc1.GetState() != QUARANTINED {
		t.Errorf("expected assoc1 to be QUARANTINED, got %v", assoc1.GetState())
	}
	if _, ok := nm.GetAssociation(remoteUA1); ok {
		t.Error("UA1 should have been removed from registry")
	}

	// Verify UA2 is registered
	if _, ok := nm.GetAssociation(remoteUA2); !ok {
		t.Fatal("UA2 should be registered")
	}
}

func TestDispatcher_AutoACK(t *testing.T) {
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

	remoteUA := &gproto_remote.UniqueAddress{
		Address: &gproto_remote.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("remoteSystem"),
			Hostname: proto.String("10.0.0.1"),
			Port:     proto.Uint32(2552),
		},
		Uid: proto.Uint64(123),
	}

	assoc := &GekkaAssociation{
		state:   ASSOCIATED,
		conn:    server,
		nodeMgr: nm,
		remote:  remoteUA,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		_ = assoc.Process(ctx)
	}()

	// Build a SystemMessageEnvelope (SeqNo=42 requires an ACK) and wrap it in
	// a proper Artery binary frame with manifest "SystemMessage".
	sm := &gproto_remote.SystemMessage{Type: gproto_remote.SystemMessage_WATCH.Enum()}
	smBytes, _ := proto.Marshal(sm)
	env := &gproto_remote.SystemMessageEnvelope{
		Message:         smBytes,
		SerializerId:    proto.Int32(ArteryInternalSerializerID),
		SeqNo:           proto.Uint64(42),
		AckReplyTo:      remoteUA,
		MessageManifest: []byte("SystemMessage"),
	}
	envPayload, _ := proto.Marshal(env)

	frame, err := BuildArteryFrame(0, ArteryInternalSerializerID, "", "", "SystemMessage", envPayload, true)
	if err != nil {
		t.Fatalf("BuildArteryFrame: %v", err)
	}
	if err := WriteFrame(client, frame); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	// Read the automatic ACK back (also an Artery frame).
	ackHeader := make([]byte, 4)
	if _, err := io.ReadFull(client, ackHeader); err != nil {
		t.Fatalf("failed to read ACK header: %v", err)
	}
	ackLen := binary.LittleEndian.Uint32(ackHeader)
	ackFrameBytes := make([]byte, ackLen)
	if _, err := io.ReadFull(client, ackFrameBytes); err != nil {
		t.Fatalf("failed to read ACK frame: %v", err)
	}

	ackMeta, err := ParseArteryFrame(ackFrameBytes, nil, 0)
	if err != nil {
		t.Fatalf("ParseArteryFrame on ACK: %v", err)
	}
	if string(ackMeta.MessageManifest) != "SystemMessageDeliveryAck" {
		t.Errorf("expected SystemMessageDeliveryAck manifest, got %q", string(ackMeta.MessageManifest))
	}

	ackBody := &gproto_remote.SystemMessageDeliveryAck{}
	if err := proto.Unmarshal(ackMeta.Payload, ackBody); err != nil {
		t.Fatalf("unmarshal SystemMessageDeliveryAck: %v", err)
	}
	if ackBody.GetSeqNo() != 42 {
		t.Errorf("expected ACK for seq 42, got %d", ackBody.GetSeqNo())
	}
}

func TestDispatcher_Heuristic(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	nm := NewNodeManager(&gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("localSystem"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}, 0)
	assoc := &GekkaAssociation{
		state:   ASSOCIATED,
		conn:    server,
		nodeMgr: nm,
	}

	ctx := context.Background()
	go func() {
		_ = assoc.Process(ctx)
	}()

	// Send HeartbeatRsp without manifest
	hb := &gproto_remote.ArteryHeartbeatRsp{Uid: proto.Uint64(999)}
	payload, _ := proto.Marshal(hb)
	env := &gproto_remote.SystemMessageEnvelope{
		Message:      payload,
		SerializerId: proto.Int32(ArteryInternalSerializerID),
		// No manifest
	}
	envPayload, _ := proto.Marshal(env)
	header := make([]byte, 4)
	binary.LittleEndian.PutUint32(header, uint32(len(envPayload)))
	_, _ = client.Write(header)
	_, _ = client.Write(envPayload)

	// If heuristic works, it should print "heuristic match: HeartbeatRsp" in logs (we can't easily verify logs here)
	// But we can verify it doesn't crash.
	time.Sleep(100 * time.Millisecond)
}
