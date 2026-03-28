/*
 * aeron_proto_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"bytes"
	"testing"
)

// ---------------------------------------------------------------------------
// AeronDataHeader round-trip
// ---------------------------------------------------------------------------

func TestAeronDataHeader_RoundTrip(t *testing.T) {
	orig := AeronDataHeader{
		FrameLength:   int32(32 + 128),
		Version:       AeronVersion,
		Flags:         AeronFlagComplete,
		FrameType:     AeronHdrTypeData,
		TermOffset:    int32(64),
		SessionId:     int32(-559038737), // 0xDEADBEEF
		StreamId:      AeronStreamOrdinary,
		TermId:        int32(42),
		ReservedValue: 0,
	}

	buf := make([]byte, AeronDataHeaderLen)
	orig.Encode(buf)

	var decoded AeronDataHeader
	decoded.Decode(buf)

	if orig != decoded {
		t.Errorf("AeronDataHeader round-trip mismatch:\n  orig    = %+v\n  decoded = %+v", orig, decoded)
	}
}

// Verify that the first 8 bytes encode frameType as Little-Endian uint16.
func TestAeronDataHeader_FrameTypeLittleEndian(t *testing.T) {
	hdr := AeronDataHeader{
		FrameLength: AeronDataHeaderLen,
		FrameType:   AeronHdrTypeData, // 0x0001
	}
	buf := make([]byte, AeronDataHeaderLen)
	hdr.Encode(buf)

	// bytes [6:8] must be 0x01, 0x00 for DATA (Little-Endian 0x0001)
	if buf[6] != 0x01 || buf[7] != 0x00 {
		t.Errorf("FrameType bytes[6:8] = [%02x,%02x], want [01,00]", buf[6], buf[7])
	}

	hdr.FrameType = AeronHdrTypeSm // 0x0003
	hdr.Encode(buf)
	// 0x0003 in LE = [0x03, 0x00]
	if buf[6] != 0x03 || buf[7] != 0x00 {
		t.Errorf("SM FrameType bytes[6:8] = [%02x,%02x], want [03,00]", buf[6], buf[7])
	}
}

// ---------------------------------------------------------------------------
// AeronSetupHeader round-trip
// ---------------------------------------------------------------------------

func TestAeronSetupHeader_RoundTrip(t *testing.T) {
	orig := AeronSetupHeader{
		FrameLength:   AeronSetupHeaderLen,
		Version:       AeronVersion,
		Flags:         0,
		FrameType:     AeronHdrTypeSetup,
		TermOffset:    0,
		SessionId:     int32(0x1234ABCD),
		StreamId:      AeronStreamControl,
		InitialTermId: int32(100),
		ActiveTermId:  int32(100),
		TermLength:    AeronDefaultTermLength,
		Mtu:           AeronDefaultMtu,
		Ttl:           0,
	}

	buf := make([]byte, AeronSetupHeaderLen)
	orig.Encode(buf)

	var decoded AeronSetupHeader
	decoded.Decode(buf)

	if orig != decoded {
		t.Errorf("AeronSetupHeader round-trip mismatch:\n  orig    = %+v\n  decoded = %+v", orig, decoded)
	}
}

// Ensure the setup frame type byte is correct (SETUP = 5 = 0x0005).
func TestAeronSetupHeader_TypeField(t *testing.T) {
	hdr := AeronSetupHeader{FrameLength: AeronSetupHeaderLen, FrameType: AeronHdrTypeSetup}
	buf := make([]byte, AeronSetupHeaderLen)
	hdr.Encode(buf)
	// 0x0005 in LE = [0x05, 0x00]
	if buf[6] != 0x05 || buf[7] != 0x00 {
		t.Errorf("SETUP type bytes[6:8] = [%02x,%02x], want [05,00]", buf[6], buf[7])
	}
}

// ---------------------------------------------------------------------------
// AeronStatusMessage round-trip
// ---------------------------------------------------------------------------

func TestAeronStatusMessage_RoundTrip(t *testing.T) {
	orig := AeronStatusMessage{
		FrameLength:           AeronSmHeaderLen,
		Version:               AeronVersion,
		Flags:                 0,
		FrameType:             AeronHdrTypeSm,
		SessionId:             int32(-889275714), // 0xCAFEBABE
		StreamId:              AeronStreamLarge,
		ConsumptionTermId:     int32(5),
		ConsumptionTermOffset: int32(4096),
		ReceiverWindowLength:  AeronDefaultWindowLen,
		ReceiverID:            int64(0x0102030405060708),
	}

	buf := make([]byte, AeronSmHeaderLen)
	orig.Encode(buf)

	var decoded AeronStatusMessage
	decoded.Decode(buf)

	if orig != decoded {
		t.Errorf("AeronStatusMessage round-trip mismatch:\n  orig    = %+v\n  decoded = %+v", orig, decoded)
	}
}

// Verify SM frame size matches Aeron 1.30.0 StatusMessageFlyweight.HEADER_LENGTH = 36
// (28 bytes fixed fields + 8-byte receiverId).
func TestAeronStatusMessage_Size(t *testing.T) {
	if AeronSmHeaderLen != 36 {
		t.Errorf("AeronSmHeaderLen = %d, want 36", AeronSmHeaderLen)
	}
}

// ---------------------------------------------------------------------------
// AeronNak round-trip
// ---------------------------------------------------------------------------

func TestAeronNak_RoundTrip(t *testing.T) {
	orig := AeronNak{
		FrameLength: AeronNakHeaderLen,
		Version:     AeronVersion,
		Flags:       0,
		FrameType:   AeronHdrTypeNak,
		SessionId:   int32(7),
		StreamId:    AeronStreamOrdinary,
		TermId:      int32(3),
		TermOffset:  int32(1024),
		Length:      int32(256),
	}

	buf := make([]byte, AeronNakHeaderLen)
	orig.Encode(buf)

	var decoded AeronNak
	decoded.Decode(buf)

	if orig != decoded {
		t.Errorf("AeronNak round-trip mismatch:\n  orig    = %+v\n  decoded = %+v", orig, decoded)
	}
}

// NAK type field: 0x0002 in LE = [0x02, 0x00]  (Aeron 1.30.0: HDR_TYPE_NAK = 2)
func TestAeronNak_TypeField(t *testing.T) {
	n := AeronNak{FrameLength: AeronNakHeaderLen, FrameType: AeronHdrTypeNak}
	buf := make([]byte, AeronNakHeaderLen)
	n.Encode(buf)
	if buf[6] != 0x02 || buf[7] != 0x00 {
		t.Errorf("NAK type bytes[6:8] = [%02x,%02x], want [02,00]", buf[6], buf[7])
	}
}

// ---------------------------------------------------------------------------
// aeronAlignedLen
// ---------------------------------------------------------------------------

func TestAeronAlignedLen(t *testing.T) {
	cases := [][2]int{
		{0, 0},
		{1, 32},
		{32, 32},
		{33, 64},
		{64, 64},
		{65, 96},
		{AeronDataHeaderLen, AeronDataHeaderLen},
		{AeronDataHeaderLen + 1, AeronDataHeaderLen + 32},
	}
	for _, c := range cases {
		got := aeronAlignedLen(c[0])
		if got != c[1] {
			t.Errorf("aeronAlignedLen(%d) = %d, want %d", c[0], got, c[1])
		}
	}
}

// ---------------------------------------------------------------------------
// DATA frame fragmentation helpers (flag logic)
// ---------------------------------------------------------------------------

func TestAeronFragmentFlags(t *testing.T) {
	// Single-frame message must have both BEGIN and END.
	if AeronFlagComplete != AeronFlagBeginFrag|AeronFlagEndFrag {
		t.Errorf("AeronFlagComplete (0x%02x) != BeginFrag|EndFrag (0x%02x)",
			AeronFlagComplete, AeronFlagBeginFrag|AeronFlagEndFrag)
	}

	// Individual flags must be distinct.
	if AeronFlagBeginFrag == AeronFlagEndFrag {
		t.Error("BeginFrag and EndFrag must not be equal")
	}

	// Middle fragment: neither BEGIN nor END.
	middleFlags := byte(0x00)
	if middleFlags&AeronFlagBeginFrag != 0 {
		t.Error("middle fragment must not have BEGIN flag")
	}
	if middleFlags&AeronFlagEndFrag != 0 {
		t.Error("middle fragment must not have END flag")
	}
}

// ---------------------------------------------------------------------------
// Verify canonical DATA frame binary layout (byte-level spec compliance)
// ---------------------------------------------------------------------------

// TestAeronDataHeader_BinaryLayout checks the exact byte positions mandated by
// the Aeron wire specification.
func TestAeronDataHeader_BinaryLayout(t *testing.T) {
	hdr := AeronDataHeader{
		FrameLength:   int32(0x00000120),   // 288
		Version:       0x00,
		Flags:         AeronFlagComplete,   // 0xC0
		FrameType:     AeronHdrTypeData,    // 0x0000
		TermOffset:    int32(0x00000040),   // 64
		SessionId:     int32(0x12345678),
		StreamId:      AeronStreamOrdinary, // 2
		TermId:        int32(0x0000002A),   // 42
		ReservedValue: 0,
	}
	buf := make([]byte, AeronDataHeaderLen)
	hdr.Encode(buf)

	want := []byte{
		// [0-3]  frameLength = 288 (0x120) LE
		0x20, 0x01, 0x00, 0x00,
		// [4]    version
		0x00,
		// [5]    flags = BEGIN|END = 0xC0
		0xC0,
		// [6-7]  type = DATA = 0x0001 LE  (Aeron 1.30.0: HDR_TYPE_DATA = 1)
		0x01, 0x00,
		// [8-11] termOffset = 64 (0x40) LE
		0x40, 0x00, 0x00, 0x00,
		// [12-15] sessionId = 0x12345678 LE
		0x78, 0x56, 0x34, 0x12,
		// [16-19] streamId = 2 LE
		0x02, 0x00, 0x00, 0x00,
		// [20-23] termId = 42 LE
		0x2A, 0x00, 0x00, 0x00,
		// [24-31] reservedValue = 0
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}

	if !bytes.Equal(buf, want) {
		t.Errorf("DATA header binary mismatch:\n  got  %02x\n  want %02x", buf, want)
	}
}
