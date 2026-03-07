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

	"google.golang.org/protobuf/proto"
)

func TestNodeRegistry_UIDRestart(t *testing.T) {
	localAddr := &Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("localSystem"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	nm := NewNodeManager(localAddr)

	remoteUA1 := &UniqueAddress{
		Address: &Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("remoteSystem"),
			Hostname: proto.String("10.0.0.1"),
			Port:     proto.Uint32(2552),
		},
		Uid: proto.Uint64(1),
	}
	remoteUA2 := &UniqueAddress{
		Address: &Address{
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

	localAddr := &Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("localSystem"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	nm := NewNodeManager(localAddr)

	remoteUA := &UniqueAddress{
		Address: &Address{
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

	// Send a SystemMessage that requires an ACK (SeqNo > 0)
	sm := &SystemMessage{Type: SystemMessage_WATCH.Enum()}
	payload, _ := proto.Marshal(sm)
	env := &SystemMessageEnvelope{
		Message:         payload,
		SerializerId:    proto.Int32(ArteryInternalSerializerID),
		SeqNo:           proto.Uint64(42),
		AckReplyTo:      remoteUA,
		MessageManifest: []byte("SystemMessage"),
	}
	envPayload, _ := proto.Marshal(env)

	header := make([]byte, 4)
	binary.LittleEndian.PutUint32(header, uint32(len(envPayload)))
	client.Write(header)
	client.Write(envPayload)

	// Read the automatic ACK back
	ackHeader := make([]byte, 4)
	if _, err := io.ReadFull(client, ackHeader); err != nil {
		t.Fatalf("failed to read ACK header: %v", err)
	}
	ackLen := binary.LittleEndian.Uint32(ackHeader)
	ackPayload := make([]byte, ackLen)
	if _, err := io.ReadFull(client, ackPayload); err != nil {
		t.Fatalf("failed to read ACK payload: %v", err)
	}

	ackEnv := &SystemMessageEnvelope{}
	proto.Unmarshal(ackPayload, ackEnv)
	if string(ackEnv.MessageManifest) != "SystemMessageDeliveryAck" {
		t.Errorf("expected SystemMessageDeliveryAck manifest, got %s", string(ackEnv.MessageManifest))
	}

	ackBody := &SystemMessageDeliveryAck{}
	proto.Unmarshal(ackEnv.Message, ackBody)
	if ackBody.GetSeqNo() != 42 {
		t.Errorf("expected ACK for seq 42, got %d", ackBody.GetSeqNo())
	}
}

func TestDispatcher_Heuristic(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	nm := NewNodeManager(&Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("localSystem"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	})
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
	hb := &ArteryHeartbeatRsp{Uid: proto.Uint64(999)}
	payload, _ := proto.Marshal(hb)
	env := &SystemMessageEnvelope{
		Message:      payload,
		SerializerId: proto.Int32(ArteryInternalSerializerID),
		// No manifest
	}
	envPayload, _ := proto.Marshal(env)
	header := make([]byte, 4)
	binary.LittleEndian.PutUint32(header, uint32(len(envPayload)))
	client.Write(header)
	client.Write(envPayload)

	// If heuristic works, it should print "heuristic match: HeartbeatRsp" in logs (we can't easily verify logs here)
	// But we can verify it doesn't crash.
	time.Sleep(100 * time.Millisecond)
}
