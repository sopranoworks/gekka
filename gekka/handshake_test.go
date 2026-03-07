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

	"google.golang.org/protobuf/proto"
)

func TestArteryHandshake_Success(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	handler := TcpArteryHandlerWithNodeManager(nm)
	errChan := make(chan error, 1)
	go func() {
		errChan <- handler(ctx, server)
	}()

	// Construct HandshakeReq
	remoteAddr := &UniqueAddress{
		Address: &Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("remoteSystem"),
			Hostname: proto.String("10.0.0.1"),
			Port:     proto.Uint32(2552),
		},
		Uid: proto.Uint64(12345),
	}
	req := &HandshakeReq{
		From: remoteAddr,
		To:   localAddr,
	}
	payload, _ := proto.Marshal(req)

	// Wrap in SystemMessageEnvelope
	env := &SystemMessageEnvelope{
		Message:         payload,
		SerializerId:    proto.Int32(ArteryInternalSerializerID),
		SeqNo:           proto.Uint64(0), // Set to 0 to avoid automatic ACK in this test
		AckReplyTo:      remoteAddr,
		MessageManifest: []byte("HandshakeReq"),
	}
	envPayload, _ := proto.Marshal(env)

	// Send length-prefixed frame
	header := make([]byte, 4)
	binary.LittleEndian.PutUint32(header, uint32(len(envPayload)))
	client.Write(header)
	client.Write(envPayload)

	// Read HandshakeRsp from server
	rspHeader := make([]byte, 4)
	_, err := io.ReadFull(client, rspHeader)
	if err != nil {
		t.Fatalf("failed to read response header: %v", err)
	}
	rspLen := binary.LittleEndian.Uint32(rspHeader)
	rspPayload := make([]byte, rspLen)
	_, err = io.ReadFull(client, rspPayload)
	if err != nil {
		t.Fatalf("failed to read response payload: %v", err)
	}

	// Decode response envelope
	rspEnv := &SystemMessageEnvelope{}
	err = proto.Unmarshal(rspPayload, rspEnv)
	if err != nil {
		t.Fatalf("failed to unmarshal response envelope: %v", err)
	}

	if rspEnv.GetSerializerId() != ArteryInternalSerializerID {
		t.Errorf("expected internal serializer ID 13, got %d", rspEnv.GetSerializerId())
	}

	// Decode HandshakeRsp (MessageWithAddress)
	mwa := &MessageWithAddress{}
	err = proto.Unmarshal(rspEnv.Message, mwa)
	if err != nil {
		t.Fatalf("failed to unmarshal HandshakeRsp: %v", err)
	}

	if mwa.Address.Address.GetSystem() != "localSystem" {
		t.Errorf("expected system localSystem, got %s", mwa.Address.Address.GetSystem())
	}

	// Verify Association state
	assoc, ok := nm.GetAssociation(remoteAddr)
	if !ok {
		t.Fatalf("association not found")
	}
	if assoc.GetState() != ASSOCIATED {
		t.Errorf("expected state ASSOCIATED, got %v", assoc.GetState())
	}
}

func TestArteryHandshake_AddressMismatch(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	handler := TcpArteryHandlerWithNodeManager(nm)
	errChan := make(chan error, 1)
	go func() {
		errChan <- handler(ctx, server)
	}()

	// Construct HandshakeReq with WRONG 'to' address
	wrongTo := &Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("wrongSystem"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	req := &HandshakeReq{
		From: &UniqueAddress{
			Address: &Address{
				Protocol: proto.String("pekko"),
				System:   proto.String("remoteSystem"),
				Hostname: proto.String("10.0.0.1"),
				Port:     proto.Uint32(2552),
			},
			Uid: proto.Uint64(123),
		},
		To: wrongTo,
	}
	payload, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("failed to marshal HandshakeReq: %v", err)
	}
	env := &SystemMessageEnvelope{
		Message:      payload,
		SerializerId: proto.Int32(ArteryInternalSerializerID),
		SeqNo:        proto.Uint64(0), // Set to 0 to avoid automatic ACK in this test
		AckReplyTo: &UniqueAddress{
			Address: &Address{
				Protocol: proto.String("pekko"),
				System:   proto.String("remoteSystem"),
				Hostname: proto.String("10.0.0.1"),
				Port:     proto.Uint32(2552),
			},
			Uid: proto.Uint64(123),
		},
		MessageManifest: []byte("HandshakeReq"),
	}
	envPayload, _ := proto.Marshal(env)

	header := make([]byte, 4)
	binary.LittleEndian.PutUint32(header, uint32(len(envPayload)))
	client.Write(header)
	client.Write(envPayload)

	// Expect an error from the handler
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

	localAddr := &Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("localSystem"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	nm := NewNodeManager(localAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	handler := TcpArteryHandlerWithNodeManager(nm)
	go func() {
		_ = handler(ctx, server)
	}()

	remoteUA := &UniqueAddress{
		Address: &Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("remoteSystem"),
			Hostname: proto.String("10.0.0.1"),
			Port:     proto.Uint32(2552),
		},
		Uid: proto.Uint64(123),
	}

	// 0. Handshake
	handReq := &HandshakeReq{
		From: remoteUA,
		To:   localAddr,
	}
	handPayload, _ := proto.Marshal(handReq)
	handEnv := &SystemMessageEnvelope{
		Message:         handPayload,
		SerializerId:    proto.Int32(ArteryInternalSerializerID),
		SeqNo:           proto.Uint64(0),
		AckReplyTo:      remoteUA,
		MessageManifest: []byte("HandshakeReq"),
	}
	handEnvPayload, _ := proto.Marshal(handEnv)
	header := make([]byte, 4)
	binary.LittleEndian.PutUint32(header, uint32(len(handEnvPayload)))
	client.Write(header)
	client.Write(handEnvPayload)

	// DRAIN HandshakeRsp from server to avoid blocking the server's write
	rspHeader := make([]byte, 4)
	io.ReadFull(client, rspHeader)
	rspLen := binary.LittleEndian.Uint32(rspHeader)
	rspPayload := make([]byte, rspLen)
	io.ReadFull(client, rspPayload)

	// 1. Send HeartbeatRsp
	hb := &ArteryHeartbeatRsp{
		Uid: proto.Uint64(123),
	}
	hbPayload, _ := proto.Marshal(hb)
	hbEnv := &SystemMessageEnvelope{
		Message:         hbPayload,
		SerializerId:    proto.Int32(ArteryInternalSerializerID),
		SeqNo:           proto.Uint64(0),
		AckReplyTo:      remoteUA,
		MessageManifest: []byte("HeartbeatRsp"),
	}
	hbEnvPayload, _ := proto.Marshal(hbEnv)
	binary.LittleEndian.PutUint32(header, uint32(len(hbEnvPayload)))
	client.Write(header)
	client.Write(hbEnvPayload)

	// 2. Send Quarantined
	quar := &Quarantined{
		From: remoteUA,
		To: &UniqueAddress{
			Address: localAddr,
			Uid:     proto.Uint64(1),
		},
	}
	quarPayload, _ := proto.Marshal(quar)
	quarEnv := &SystemMessageEnvelope{
		Message:         quarPayload,
		SerializerId:    proto.Int32(ArteryInternalSerializerID),
		SeqNo:           proto.Uint64(0),
		AckReplyTo:      remoteUA,
		MessageManifest: []byte("Quarantined"),
	}
	quarEnvPayload, _ := proto.Marshal(quarEnv)
	binary.LittleEndian.PutUint32(header, uint32(len(quarEnvPayload)))
	client.Write(header)
	client.Write(quarEnvPayload)

	// 3. Verify state is QUARANTINED
	// We need to wait a bit for the handler to process
	time.Sleep(100 * time.Millisecond)

	foundAssoc, ok := nm.GetAssociation(remoteUA)
	if !ok {
		t.Fatalf("association not found")
	}
	if foundAssoc.GetState() != QUARANTINED {
		t.Errorf("expected state QUARANTINED, got %v", foundAssoc.GetState())
	}
}
