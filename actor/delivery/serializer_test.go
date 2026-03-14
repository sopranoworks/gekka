/*
 * actor/delivery/serializer_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package delivery

import (
	"bytes"
	"testing"
)

// ── Serializer identity ────────────────────────────────────────────────────

func TestSerializer_Identifier(t *testing.T) {
	s := NewSerializer()
	if got := s.Identifier(); got != 36 {
		t.Errorf("Identifier() = %d, want 36", got)
	}
}

// ── SequencedMessage roundtrip ─────────────────────────────────────────────

func TestSerializer_SequencedMessage_Roundtrip(t *testing.T) {
	s := NewSerializer()
	orig := &SequencedMessage{
		ProducerID:  "test-producer",
		SeqNr:       42,
		First:       true,
		Ack:         true,
		ProducerRef: "pekko://GekkaSystem@127.0.0.1:2553/user/producer",
		Message: Payload{
			EnclosedMessage: []byte("hello reliable delivery"),
			SerializerID:    4,
			Manifest:        "",
		},
	}

	data, err := s.ToBinary(orig)
	if err != nil {
		t.Fatalf("ToBinary: %v", err)
	}
	got, err := s.FromBinary(data, ManifestSequencedMessage)
	if err != nil {
		t.Fatalf("FromBinary: %v", err)
	}
	m, ok := got.(*SequencedMessage)
	if !ok {
		t.Fatalf("FromBinary returned %T, want *SequencedMessage", got)
	}
	if m.ProducerID != orig.ProducerID {
		t.Errorf("ProducerID = %q, want %q", m.ProducerID, orig.ProducerID)
	}
	if m.SeqNr != orig.SeqNr {
		t.Errorf("SeqNr = %d, want %d", m.SeqNr, orig.SeqNr)
	}
	if m.First != orig.First {
		t.Errorf("First = %v, want %v", m.First, orig.First)
	}
	if m.Ack != orig.Ack {
		t.Errorf("Ack = %v, want %v", m.Ack, orig.Ack)
	}
	if m.ProducerRef != orig.ProducerRef {
		t.Errorf("ProducerRef = %q, want %q", m.ProducerRef, orig.ProducerRef)
	}
	if !bytes.Equal(m.Message.EnclosedMessage, orig.Message.EnclosedMessage) {
		t.Errorf("Message.EnclosedMessage = %q, want %q", m.Message.EnclosedMessage, orig.Message.EnclosedMessage)
	}
	if m.Message.SerializerID != orig.Message.SerializerID {
		t.Errorf("Message.SerializerID = %d, want %d", m.Message.SerializerID, orig.Message.SerializerID)
	}
}

func TestSerializer_SequencedMessage_Chunked_Roundtrip(t *testing.T) {
	s := NewSerializer()
	orig := &SequencedMessage{
		ProducerID:  "chunked-producer",
		SeqNr:       1,
		First:       true,
		Ack:         false,
		ProducerRef: "pekko://GekkaSystem@127.0.0.1:2553/user/producer",
		Message: Payload{
			EnclosedMessage: bytes.Repeat([]byte{0xAB}, 64),
			SerializerID:    4,
			Manifest:        "",
		},
		HasChunk:   true,
		FirstChunk: true,
		LastChunk:  false,
	}

	data, err := s.ToBinary(orig)
	if err != nil {
		t.Fatalf("ToBinary: %v", err)
	}
	got, err := s.FromBinary(data, ManifestSequencedMessage)
	if err != nil {
		t.Fatalf("FromBinary: %v", err)
	}
	m := got.(*SequencedMessage)
	if !m.HasChunk {
		t.Error("HasChunk = false, want true")
	}
	if !m.FirstChunk {
		t.Error("FirstChunk = false, want true")
	}
	if m.LastChunk {
		t.Error("LastChunk = true, want false")
	}
}

// ── Ack roundtrip ──────────────────────────────────────────────────────────

func TestSerializer_Ack_Roundtrip(t *testing.T) {
	s := NewSerializer()
	orig := &AckMsg{ConfirmedSeqNr: 99}

	data, err := s.ToBinary(orig)
	if err != nil {
		t.Fatalf("ToBinary: %v", err)
	}
	got, err := s.FromBinary(data, ManifestAck)
	if err != nil {
		t.Fatalf("FromBinary: %v", err)
	}
	m, ok := got.(*AckMsg)
	if !ok {
		t.Fatalf("FromBinary returned %T, want *AckMsg", got)
	}
	if m.ConfirmedSeqNr != 99 {
		t.Errorf("ConfirmedSeqNr = %d, want 99", m.ConfirmedSeqNr)
	}
}

// ── Request roundtrip ──────────────────────────────────────────────────────

func TestSerializer_Request_Roundtrip(t *testing.T) {
	s := NewSerializer()
	orig := &Request{
		ConfirmedSeqNr:   5,
		RequestUpToSeqNr: 15,
		SupportResend:    true,
		ViaTimeout:       false,
	}

	data, err := s.ToBinary(orig)
	if err != nil {
		t.Fatalf("ToBinary: %v", err)
	}
	got, err := s.FromBinary(data, ManifestRequest)
	if err != nil {
		t.Fatalf("FromBinary: %v", err)
	}
	m := got.(*Request)
	if m.ConfirmedSeqNr != orig.ConfirmedSeqNr {
		t.Errorf("ConfirmedSeqNr = %d, want %d", m.ConfirmedSeqNr, orig.ConfirmedSeqNr)
	}
	if m.RequestUpToSeqNr != orig.RequestUpToSeqNr {
		t.Errorf("RequestUpToSeqNr = %d, want %d", m.RequestUpToSeqNr, orig.RequestUpToSeqNr)
	}
	if m.SupportResend != orig.SupportResend {
		t.Errorf("SupportResend = %v, want %v", m.SupportResend, orig.SupportResend)
	}
	if m.ViaTimeout != orig.ViaTimeout {
		t.Errorf("ViaTimeout = %v, want %v", m.ViaTimeout, orig.ViaTimeout)
	}
}

// ── Resend roundtrip ───────────────────────────────────────────────────────

func TestSerializer_Resend_Roundtrip(t *testing.T) {
	s := NewSerializer()
	orig := &Resend{FromSeqNr: 7}

	data, err := s.ToBinary(orig)
	if err != nil {
		t.Fatalf("ToBinary: %v", err)
	}
	got, err := s.FromBinary(data, ManifestResend)
	if err != nil {
		t.Fatalf("FromBinary: %v", err)
	}
	m := got.(*Resend)
	if m.FromSeqNr != 7 {
		t.Errorf("FromSeqNr = %d, want 7", m.FromSeqNr)
	}
}

// ── RegisterConsumer roundtrip ─────────────────────────────────────────────

func TestSerializer_RegisterConsumer_Roundtrip(t *testing.T) {
	s := NewSerializer()
	orig := &RegisterConsumer{
		ConsumerControllerRef: "pekko://GekkaSystem@127.0.0.1:2553/user/consumer",
	}

	data, err := s.ToBinary(orig)
	if err != nil {
		t.Fatalf("ToBinary: %v", err)
	}
	got, err := s.FromBinary(data, ManifestRegisterConsumer)
	if err != nil {
		t.Fatalf("FromBinary: %v", err)
	}
	m := got.(*RegisterConsumer)
	if m.ConsumerControllerRef != orig.ConsumerControllerRef {
		t.Errorf("ConsumerControllerRef = %q, want %q", m.ConsumerControllerRef, orig.ConsumerControllerRef)
	}
}

// ── ArterySerializerID / ArteryManifest ───────────────────────────────────

func TestMessages_ArteryMethods(t *testing.T) {
	cases := []struct {
		msg      RemoteSerializable
		wantID   int32
		wantMani string
	}{
		{&SequencedMessage{}, 36, "a"},
		{&AckMsg{}, 36, "b"},
		{&Request{}, 36, "c"},
		{&Resend{}, 36, "d"},
		{&RegisterConsumer{}, 36, "e"},
	}
	for _, tc := range cases {
		if got := tc.msg.ArterySerializerID(); got != tc.wantID {
			t.Errorf("%T.ArterySerializerID() = %d, want %d", tc.msg, got, tc.wantID)
		}
		if got := tc.msg.ArteryManifest(); got != tc.wantMani {
			t.Errorf("%T.ArteryManifest() = %q, want %q", tc.msg, got, tc.wantMani)
		}
	}
}

// ── RemoteSerializable interface check ────────────────────────────────────

type RemoteSerializable interface {
	ArterySerializerID() int32
	ArteryManifest() string
}

// ── Unknown manifest returns error ────────────────────────────────────────

func TestSerializer_FromBinary_UnknownManifest(t *testing.T) {
	s := NewSerializer()
	_, err := s.FromBinary([]byte{}, "z")
	if err == nil {
		t.Error("expected error for unknown manifest, got nil")
	}
}

// ── ToBinary unknown type returns error ───────────────────────────────────

func TestSerializer_ToBinary_UnknownType(t *testing.T) {
	s := NewSerializer()
	_, err := s.ToBinary("not a delivery message")
	if err == nil {
		t.Error("expected error for unknown type, got nil")
	}
}
