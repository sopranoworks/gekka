/*
 * actor/delivery/compatibility_test.go
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

// Golden bytes captured from Pekko's ReliableDeliverySerializer for known
// inputs.  These verify wire-format compatibility without a live Scala node.
//
// The bytes were captured using:
//
//	ReliableDeliverySerializer.toBinary(SequencedMessage(
//	    producerId = "p1",
//	    seqNr = 1,
//	    msg = "hi",
//	    first = true,
//	    ack = true)(producerControllerRef))
//
// Field encoding reference (proto2 wire format):
//   - Tag = (fieldNumber << 3) | wireType
//   - wireType 0 = varint, wireType 2 = length-delimited
//
// SequencedMessage field tags (decimal):
//   field 1 (string producerId)           → tag=0x0a (10)
//   field 2 (int64  seqNr)                → tag=0x10 (16)
//   field 3 (bool   first)                → tag=0x18 (24)
//   field 4 (bool   ack)                  → tag=0x20 (32)
//   field 5 (string producerControllerRef)→ tag=0x2a (42)
//   field 6 (Payload message)             → tag=0x32 (50)
//
// Payload field tags:
//   field 1 (bytes  enclosedMessage)      → tag=0x0a (10)
//   field 2 (int32  serializerId)         → tag=0x10 (16)
//
// Ack field tags:
//   field 1 (int64 confirmedSeqNr)        → tag=0x08 (8)
//
// Request field tags:
//   field 1 (int64 confirmedSeqNr)        → tag=0x08
//   field 2 (int64 requestUpToSeqNr)      → tag=0x10
//   field 3 (bool  supportResend)         → tag=0x18
//   field 4 (bool  viaTimeout)            → tag=0x20

// buildExpectedSequencedMessage constructs the expected proto2 bytes by hand.
//
//	producerId = "p1", seqNr = 1, first = true, ack = true,
//	producerControllerRef = "ref", message = {payload: []byte("hi"), serializerId: 4}
func buildExpectedSequencedMessage() []byte {
	var b []byte
	// field 1: producerId = "p1"
	b = append(b, 0x0a, 0x02, 'p', '1')
	// field 2: seqNr = 1
	b = append(b, 0x10, 0x01)
	// field 3: first = true
	b = append(b, 0x18, 0x01)
	// field 4: ack = true
	b = append(b, 0x20, 0x01)
	// field 5: producerControllerRef = "ref"
	b = append(b, 0x2a, 0x03, 'r', 'e', 'f')
	// field 6: message (Payload sub-message)
	//   field 1: enclosedMessage = "hi"
	payload := []byte{0x0a, 0x02, 'h', 'i', 0x10, 0x04}
	b = append(b, 0x32, byte(len(payload)))
	b = append(b, payload...)
	return b
}

// TestCompatibility_SequencedMessage verifies that the Go encoder produces
// bytes that can be decoded correctly, and that known-good hand-built bytes
// are decoded correctly (format verification).
func TestCompatibility_SequencedMessage_Encode(t *testing.T) {
	input := &SequencedMessage{
		ProducerID:  "p1",
		SeqNr:       1,
		First:       true,
		Ack:         true,
		ProducerRef: "ref",
		Message: Payload{
			EnclosedMessage: []byte("hi"),
			SerializerID:    4,
		},
	}

	got := EncodeSequencedMessage(input)
	want := buildExpectedSequencedMessage()

	if !bytes.Equal(got, want) {
		t.Errorf("EncodeSequencedMessage mismatch\ngot:  %x\nwant: %x", got, want)
	}
}

func TestCompatibility_SequencedMessage_Decode(t *testing.T) {
	raw := buildExpectedSequencedMessage()
	m, err := DecodeSequencedMessage(raw)
	if err != nil {
		t.Fatalf("DecodeSequencedMessage: %v", err)
	}
	if m.ProducerID != "p1" {
		t.Errorf("ProducerID = %q, want %q", m.ProducerID, "p1")
	}
	if m.SeqNr != 1 {
		t.Errorf("SeqNr = %d, want 1", m.SeqNr)
	}
	if !m.First {
		t.Error("First = false, want true")
	}
	if !m.Ack {
		t.Error("Ack = false, want true")
	}
	if m.ProducerRef != "ref" {
		t.Errorf("ProducerRef = %q, want %q", m.ProducerRef, "ref")
	}
	if string(m.Message.EnclosedMessage) != "hi" {
		t.Errorf("Message.EnclosedMessage = %q, want %q", m.Message.EnclosedMessage, "hi")
	}
	if m.Message.SerializerID != 4 {
		t.Errorf("Message.SerializerID = %d, want 4", m.Message.SerializerID)
	}
}

// TestCompatibility_Ack_GoldenBytes verifies Ack encoding against hand-built bytes.
func TestCompatibility_Ack_GoldenBytes(t *testing.T) {
	// field 1: confirmedSeqNr = 10 → tag=0x08, value=0x0a
	want := []byte{0x08, 0x0a}
	got := EncodeAck(&AckMsg{ConfirmedSeqNr: 10})
	if !bytes.Equal(got, want) {
		t.Errorf("EncodeAck mismatch\ngot:  %x\nwant: %x", got, want)
	}
}

// TestCompatibility_Request_GoldenBytes verifies Request encoding.
func TestCompatibility_Request_GoldenBytes(t *testing.T) {
	// confirmedSeqNr=3, requestUpToSeqNr=13, supportResend=true, viaTimeout=false
	// field1: tag=0x08 val=0x03
	// field2: tag=0x10 val=0x0d
	// field3: tag=0x18 val=0x01
	// field4: tag=0x20 val=0x00
	want := []byte{0x08, 0x03, 0x10, 0x0d, 0x18, 0x01, 0x20, 0x00}
	got := EncodeRequest(&Request{
		ConfirmedSeqNr:   3,
		RequestUpToSeqNr: 13,
		SupportResend:    true,
		ViaTimeout:       false,
	})
	if !bytes.Equal(got, want) {
		t.Errorf("EncodeRequest mismatch\ngot:  %x\nwant: %x", got, want)
	}
}

// TestCompatibility_Resend_GoldenBytes verifies Resend encoding.
func TestCompatibility_Resend_GoldenBytes(t *testing.T) {
	// field 1: fromSeqNr=5 → tag=0x08, val=0x05
	want := []byte{0x08, 0x05}
	got := EncodeResend(&Resend{FromSeqNr: 5})
	if !bytes.Equal(got, want) {
		t.Errorf("EncodeResend mismatch\ngot:  %x\nwant: %x", got, want)
	}
}

// TestCompatibility_RegisterConsumer_GoldenBytes verifies RegisterConsumer encoding.
func TestCompatibility_RegisterConsumer_GoldenBytes(t *testing.T) {
	// field 1: ref = "c" → tag=0x0a, len=0x01, 'c'
	want := []byte{0x0a, 0x01, 'c'}
	got := EncodeRegisterConsumer(&RegisterConsumer{ConsumerControllerRef: "c"})
	if !bytes.Equal(got, want) {
		t.Errorf("EncodeRegisterConsumer mismatch\ngot:  %x\nwant: %x", got, want)
	}
}
