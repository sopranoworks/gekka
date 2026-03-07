/*
 * tcp_artery_handler_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"bytes"
	"context"
	"encoding/binary"
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

	errChan := make(chan error, 1)
	go func() {
		errChan <- TcpArteryHandlerWithCallback(ctx, server, handler, nil, 0)
	}()

	// Test Case 1: RemoteEnvelope
	recipientPath := "pekko://system@127.0.0.1:2552/user/test"
	payload := []byte("hello artery")
	remoteEnv := &RemoteEnvelope{
		Recipient: &ActorRefData{Path: &recipientPath},
		Message: &SerializedMessage{
			Message:      payload,
			SerializerId: proto.Int32(2),
		},
		Seq: proto.Uint64(123),
	}
	envPayload, _ := proto.Marshal(remoteEnv)

	header := make([]byte, 4)
	binary.LittleEndian.PutUint32(header, uint32(len(envPayload)))
	client.Write(header)
	client.Write(envPayload)

	select {
	case meta := <-receivedMetas:
		if meta.SerializerId != 2 {
			t.Errorf("expected serializerId 2, got %d", meta.SerializerId)
		}
		if !bytes.Equal(meta.Payload, payload) {
			t.Errorf("expected payload %s, got %s", payload, meta.Payload)
		}
		if meta.SeqNo != 123 {
			t.Errorf("expected seq 123, got %d", meta.SeqNo)
		}
		if meta.Recipient.GetPath() != recipientPath {
			t.Errorf("expected recipient %s, got %s", recipientPath, meta.Recipient.GetPath())
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("timeout waiting for remote envelope")
	}

	// Close client to signal EOF
	client.Close()

	select {
	case err := <-errChan:
		if err != nil {
			t.Errorf("expected nil error on EOF, got %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for handler to exit on EOF")
	}
}

func TestTcpArteryHandler_InvalidLength(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	ctx := context.Background()
	errChan := make(chan error, 1)
	go func() {
		errChan <- TcpArteryHandlerWithCallback(ctx, server, func(context.Context, *ArteryMetadata) error { return nil }, nil, 0)
	}()

	// Test Case: Negative Length (simulated by large uint32 interpreting as signed)
	header := make([]byte, 4)
	binary.LittleEndian.PutUint32(header, 0xFFFFFFFF) // -1 if signed
	client.Write(header)

	select {
	case err := <-errChan:
		if err == nil {
			t.Error("expected error for negative length, got nil")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for error on negative length")
	}
}

func TestTcpArteryHandler_Oversized(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	ctx := context.Background()
	errChan := make(chan error, 1)
	go func() {
		errChan <- TcpArteryHandlerWithCallback(ctx, server, func(context.Context, *ArteryMetadata) error { return nil }, nil, 0)
	}()

	header := make([]byte, 4)
	binary.LittleEndian.PutUint32(header, MaxArteryPayloadLength+1)
	client.Write(header)

	select {
	case err := <-errChan:
		if err == nil {
			t.Error("expected error for oversized payload, got nil")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for error on oversized payload")
	}
}

func TestDecodeArteryEnvelope_SystemMessage(t *testing.T) {
	payload := []byte("system message")
	systemEnv := &SystemMessageEnvelope{
		Message:      payload,
		SerializerId: proto.Int32(1),
		SeqNo:        proto.Uint64(456),
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
	envPayload, _ := proto.Marshal(systemEnv)

	meta, err := DecodeArteryEnvelope(envPayload, nil, 0)
	if err != nil {
		t.Fatalf("failed to decode system message: %v", err)
	}

	if meta.SerializerId != 1 {
		t.Errorf("expected serializerId 1, got %d", meta.SerializerId)
	}
	if !bytes.Equal(meta.Payload, payload) {
		t.Errorf("expected payload %s, got %s", payload, meta.Payload)
	}
	if meta.SeqNo != 456 {
		t.Errorf("expected seq 456, got %d", meta.SeqNo)
	}
	if meta.AckReplyTo.GetUid() != 999 {
		t.Errorf("expected uid 999, got %d", meta.AckReplyTo.GetUid())
	}
}
