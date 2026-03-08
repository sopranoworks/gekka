/*
 * tcp_artery_handler_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

func TestTcpArteryHandler_Framing(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	receivedMetas := make(chan *ArteryMetadata, 10)
	handler := func(ctx context.Context, meta *ArteryMetadata) error {
		receivedMetas <- meta
		return nil
	}

	go func() {
		_ = TcpArteryHandlerWithCallback(ctx, server, handler, nil, 0, 1)
	}()

	// Build a proper Artery binary frame carrying a user message.
	recipientPath := "pekko://system@127.0.0.1:2552/user/test"
	payload := []byte("hello artery")
	frame, err := BuildArteryFrame(42, 2, "", recipientPath, "MyMsg", payload, false)
	if err != nil {
		t.Fatalf("BuildArteryFrame: %v", err)
	}
	if err := WriteFrame(client, frame); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	select {
	case meta := <-receivedMetas:
		if meta.SerializerId != 2 {
			t.Errorf("expected serializerId 2, got %d", meta.SerializerId)
		}
		if string(meta.Payload) != string(payload) {
			t.Errorf("expected payload %q, got %q", payload, meta.Payload)
		}
		if meta.Recipient.GetPath() != recipientPath {
			t.Errorf("expected recipient %q, got %q", recipientPath, meta.Recipient.GetPath())
		}
		if string(meta.MessageManifest) != "MyMsg" {
			t.Errorf("expected manifest %q, got %q", "MyMsg", string(meta.MessageManifest))
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("timeout waiting for decoded frame")
	}

	// Close client to signal EOF; handler should exit cleanly.
	client.Close()
}

func TestTcpArteryHandler_InvalidLength(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	ctx := context.Background()
	errChan := make(chan error, 1)
	go func() {
		errChan <- TcpArteryHandlerWithCallback(ctx, server, func(context.Context, *ArteryMetadata) error { return nil }, nil, 0, 1)
	}()

	// 0xFFFFFFFF > MaxArteryPayloadLength — should trigger length check.
	header := make([]byte, 4)
	putUint32LE(header, 0xFFFFFFFF)
	client.Write(header)

	select {
	case err := <-errChan:
		if err == nil {
			t.Error("expected error for oversized length, got nil")
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("timeout waiting for error on invalid length")
	}
}

func TestTcpArteryHandler_Oversized(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	ctx := context.Background()
	errChan := make(chan error, 1)
	go func() {
		errChan <- TcpArteryHandlerWithCallback(ctx, server, func(context.Context, *ArteryMetadata) error { return nil }, nil, 0, 1)
	}()

	header := make([]byte, 4)
	putUint32LE(header, MaxArteryPayloadLength+1)
	client.Write(header)

	select {
	case err := <-errChan:
		if err == nil {
			t.Error("expected error for oversized payload, got nil")
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("timeout waiting for error on oversized payload")
	}
}

// putUint32LE writes a uint32 in little-endian to b.
func putUint32LE(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}

func TestDecodeArteryEnvelope_SystemMessage(t *testing.T) {
	// Build an Artery binary frame carrying a SystemMessageEnvelope as its payload.
	// This is how Pekko actually sends system messages over the wire.
	inner := &SystemMessage{Type: SystemMessage_WATCH.Enum()}
	innerBytes, _ := proto.Marshal(inner)

	sme := &SystemMessageEnvelope{
		Message:         innerBytes,
		SerializerId:    proto.Int32(ArteryInternalSerializerID),
		SeqNo:           proto.Uint64(456),
		MessageManifest: []byte("SystemMessage"),
		AckReplyTo: &UniqueAddress{
			Address: &Address{
				Protocol: proto.String("pekko"),
				System:   proto.String("system"),
				Hostname: proto.String("127.0.0.1"),
				Port:     proto.Uint32(2552),
			},
			Uid: proto.Uint64(999),
		},
	}
	smeBytes, _ := proto.Marshal(sme)

	// Wrap in an Artery frame with manifest "SystemMessage".
	frame, err := BuildArteryFrame(0, ArteryInternalSerializerID, "", "", "SystemMessage", smeBytes, true)
	if err != nil {
		t.Fatalf("BuildArteryFrame: %v", err)
	}

	meta, err := DecodeArteryEnvelope(frame, nil, 0)
	if err != nil {
		t.Fatalf("DecodeArteryEnvelope: %v", err)
	}

	if meta.SerializerId != ArteryInternalSerializerID {
		t.Errorf("expected serializerId %d, got %d", ArteryInternalSerializerID, meta.SerializerId)
	}
	if string(meta.MessageManifest) != "SystemMessage" {
		t.Errorf("expected manifest %q, got %q", "SystemMessage", string(meta.MessageManifest))
	}

	// The payload is the serialised SystemMessageEnvelope — verify it round-trips.
	var decoded SystemMessageEnvelope
	if err := proto.Unmarshal(meta.Payload, &decoded); err != nil {
		t.Fatalf("unmarshal SystemMessageEnvelope from payload: %v", err)
	}
	if decoded.GetSeqNo() != 456 {
		t.Errorf("expected SeqNo 456, got %d", decoded.GetSeqNo())
	}
	if decoded.AckReplyTo.GetUid() != 999 {
		t.Errorf("expected AckReplyTo uid 999, got %d", decoded.AckReplyTo.GetUid())
	}
}
