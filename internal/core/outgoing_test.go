/*
 * outgoing_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"bytes"
	"encoding/binary"
	"testing"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

func TestBuildRemoteEnvelope_Nesting(t *testing.T) {
	recipient := "/user/testActor"
	payload := []byte("hello")
	serializerId := int32(2)
	manifest := "UserManifest"

	seq := uint64(42)
	sender := &gproto_remote.UniqueAddress{
		Address: &gproto_remote.Address{Hostname: proto.String("localhost")},
		Uid:     proto.Uint64(1),
	}
	envBytes, err := BuildRemoteEnvelope(recipient, payload, serializerId, manifest, seq, sender)
	if err != nil {
		t.Fatalf("failed to build remote envelope: %v", err)
	}

	env := &gproto_remote.RemoteEnvelope{}
	if err := proto.Unmarshal(envBytes, env); err != nil {
		t.Fatalf("failed to unmarshal RemoteEnvelope: %v", err)
	}

	if env.GetRecipient().GetPath() != recipient {
		t.Errorf("expected recipient %s, got %s", recipient, env.GetRecipient().GetPath())
	}

	msg := env.GetMessage()
	if msg.GetSerializerId() != serializerId {
		t.Errorf("expected serializerId %d, got %d", serializerId, msg.GetSerializerId())
	}

	if string(msg.GetMessageManifest()) != manifest {
		t.Errorf("expected manifest %s, got %s", manifest, string(msg.GetMessageManifest()))
	}

	if !bytes.Equal(msg.GetMessage(), payload) {
		t.Errorf("payload mismatch")
	}
}

func TestBuildSystemEnvelope_Reliability(t *testing.T) {
	payload := []byte("system-action")
	serializerId := int32(13)
	manifest := "SystemMessage"
	seqNo := uint64(12345)
	ackReplyTo := &gproto_remote.UniqueAddress{
		Address: &gproto_remote.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("sys"),
			Hostname: proto.String("localhost"),
			Port:     proto.Uint32(2552),
		},
		Uid: proto.Uint64(1),
	}

	envBytes, err := BuildSystemEnvelope(payload, serializerId, manifest, seqNo, ackReplyTo)
	if err != nil {
		t.Fatalf("failed to build system envelope: %v", err)
	}

	env := &gproto_remote.SystemMessageEnvelope{}
	if err := proto.Unmarshal(envBytes, env); err != nil {
		t.Fatalf("failed to unmarshal SystemMessageEnvelope: %v", err)
	}

	if env.GetSerializerId() != serializerId {
		t.Errorf("expected serializerId %d, got %d", serializerId, env.GetSerializerId())
	}

	if env.GetSeqNo() != seqNo {
		t.Errorf("expected seqNo %d, got %d", seqNo, env.GetSeqNo())
	}

	if env.GetAckReplyTo().GetUid() != 1 {
		t.Errorf("ackReplyTo UID mismatch")
	}
}

func TestWriteFrame_Correctness(t *testing.T) {
	payload := []byte("test-frame-payload")
	var buf bytes.Buffer

	if err := WriteFrame(&buf, payload); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	data := buf.Bytes()
	if len(data) != 4+len(payload) {
		t.Fatalf("expected length %d, got %d", 4+len(payload), len(data))
	}

	length := binary.LittleEndian.Uint32(data[:4])
	if int(length) != len(payload) {
		t.Errorf("length header mismatch: expected %d, got %d", len(payload), length)
	}

	if !bytes.Equal(data[4:], payload) {
		t.Errorf("payload mismatch in frame")
	}
}
