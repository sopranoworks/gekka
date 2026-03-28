/*
 * aeron_proto.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package core provides the Aeron wire-protocol data structures used by the
// native UDP Artery transport.  All multi-byte fields are encoded in
// Little-Endian order, matching the canonical Aeron specification.
package core

import "encoding/binary"

// ---------------------------------------------------------------------------
// Aeron frame type codes (uint16, Little-Endian)
// ---------------------------------------------------------------------------

// Aeron 1.30.0 frame type constants (verified via javap -constants on
// aeron-client-1.30.0.jar io.aeron.protocol.HeaderFlyweight).
// NOTE: these differ from post-1.40 values; the mapping here is correct for
// the versions pinned in build.sbt (1.30.0 + agrona 1.9.0).
const (
	AeronHdrTypePad   = uint16(0x0000) // PAD frame (ignored by receiver)
	AeronHdrTypeData  = uint16(0x0001) // DATA frame
	AeronHdrTypeNak   = uint16(0x0002) // NAK  frame (subscriber → publisher)
	AeronHdrTypeSm    = uint16(0x0003) // Status Message (subscriber → publisher)
	AeronHdrTypeErr   = uint16(0x0004) // ERROR frame
	AeronHdrTypeSetup = uint16(0x0005) // SETUP frame (publisher → subscriber)
	AeronHdrTypeRtt   = uint16(0x0006) // RTT measurement
)

// Aeron protocol version embedded in every frame header.
const AeronVersion = byte(0)

// ---------------------------------------------------------------------------
// DATA frame flags (uint8)
// ---------------------------------------------------------------------------

const (
	AeronFlagBeginFrag = byte(0x80) // first (or only) fragment of a message
	AeronFlagEndFrag   = byte(0x40) // last  (or only) fragment of a message
	AeronFlagComplete  = byte(0xC0) // BEGIN|END — whole message fits in one frame
)

// ---------------------------------------------------------------------------
// Artery logical stream IDs carried over Aeron
// ---------------------------------------------------------------------------

const (
	AeronStreamControl  = int32(1) // Artery control messages  (handshake, heartbeat …)
	AeronStreamOrdinary = int32(2) // Artery ordinary user messages
	AeronStreamLarge    = int32(3) // Artery large messages (fragmented over multiple DATA frames)
)

// ---------------------------------------------------------------------------
// Canonical frame sizes (bytes)
// ---------------------------------------------------------------------------

const (
	AeronDataHeaderLen  = 32 // DATA frame header
	AeronSetupHeaderLen = 40 // SETUP frame
	AeronSmHeaderLen    = 36 // Status Message frame (28 fixed + 8-byte receiverId)
	AeronNakHeaderLen   = 28 // NAK frame
)

// ---------------------------------------------------------------------------
// Default Aeron transport parameters
// ---------------------------------------------------------------------------

const (
	// AeronDefaultTermLength is the default term-buffer size (16 MiB).
	// This must agree with the remote Aeron Media Driver configuration.
	AeronDefaultTermLength = int32(16 * 1024 * 1024)

	// AeronDefaultMtu is the maximum transmission unit for Aeron UDP frames,
	// chosen to fit within a standard 1500-byte Ethernet MTU after IP/UDP overhead.
	AeronDefaultMtu = int32(1408)

	// AeronDefaultWindowLen is the initial receiver window advertised in SM frames.
	AeronDefaultWindowLen = int32(256 * 1024)

	// aeronFrameAlignment is the alignment boundary for term-buffer offsets.
	aeronFrameAlignment = 32
)

// alignedLen rounds n up to the next aeronFrameAlignment boundary.
func aeronAlignedLen(n int) int {
	return (n + aeronFrameAlignment - 1) &^ (aeronFrameAlignment - 1)
}

// ---------------------------------------------------------------------------
// AeronDataHeader — 32-byte Aeron DATA frame header
// ---------------------------------------------------------------------------
//
// Byte layout (all Little-Endian):
//
//	[0–3]   frameLength   — total byte length of frame including this header
//	[4]     version       — AeronVersion = 0
//	[5]     flags         — AeronFlagBeginFrag | AeronFlagEndFrag
//	[6–7]   frameType     — AeronHdrTypeData = 0
//	[8–11]  termOffset    — byte offset inside the term buffer
//	[12–15] sessionId     — publisher session identifier
//	[16–19] streamId      — Artery logical stream (1/2/3)
//	[20–23] termId        — active term identifier
//	[24–31] reservedValue — must be 0
type AeronDataHeader struct {
	FrameLength   int32
	Version       uint8
	Flags         uint8
	FrameType     uint16
	TermOffset    int32
	SessionId     int32
	StreamId      int32
	TermId        int32
	ReservedValue int64
}

// Encode serialises h into buf.  buf must be at least AeronDataHeaderLen bytes.
func (h *AeronDataHeader) Encode(buf []byte) {
	binary.LittleEndian.PutUint32(buf[0:4], uint32(h.FrameLength))
	buf[4] = h.Version
	buf[5] = h.Flags
	binary.LittleEndian.PutUint16(buf[6:8], h.FrameType)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(h.TermOffset))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(h.SessionId))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(h.StreamId))
	binary.LittleEndian.PutUint32(buf[20:24], uint32(h.TermId))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(h.ReservedValue))
}

// Decode populates h from buf.  buf must be at least AeronDataHeaderLen bytes.
func (h *AeronDataHeader) Decode(buf []byte) {
	h.FrameLength = int32(binary.LittleEndian.Uint32(buf[0:4]))
	h.Version = buf[4]
	h.Flags = buf[5]
	h.FrameType = binary.LittleEndian.Uint16(buf[6:8])
	h.TermOffset = int32(binary.LittleEndian.Uint32(buf[8:12]))
	h.SessionId = int32(binary.LittleEndian.Uint32(buf[12:16]))
	h.StreamId = int32(binary.LittleEndian.Uint32(buf[16:20]))
	h.TermId = int32(binary.LittleEndian.Uint32(buf[20:24]))
	h.ReservedValue = int64(binary.LittleEndian.Uint64(buf[24:32]))
}

// ---------------------------------------------------------------------------
// AeronSetupHeader — 40-byte SETUP frame
// ---------------------------------------------------------------------------
//
// Byte layout:
//
//	[0–3]   frameLength   = AeronSetupHeaderLen
//	[4]     version
//	[5]     flags         (typically 0)
//	[6–7]   frameType     = AeronHdrTypeSetup
//	[8–11]  termOffset    (0 at session start)
//	[12–15] sessionId
//	[16–19] streamId
//	[20–23] initialTermId — term ID at which the publication started
//	[24–27] activeTermId  — current active term ID
//	[28–31] termLength    — size of the term buffer (e.g. 16 MiB)
//	[32–35] mtu           — max payload per DATA frame
//	[36–39] ttl           — multicast TTL (0 for unicast)
type AeronSetupHeader struct {
	FrameLength   int32
	Version       uint8
	Flags         uint8
	FrameType     uint16
	TermOffset    int32
	SessionId     int32
	StreamId      int32
	InitialTermId int32
	ActiveTermId  int32
	TermLength    int32
	Mtu           int32
	Ttl           int32
}

// Encode serialises h into buf.  buf must be at least AeronSetupHeaderLen bytes.
func (h *AeronSetupHeader) Encode(buf []byte) {
	binary.LittleEndian.PutUint32(buf[0:4], uint32(h.FrameLength))
	buf[4] = h.Version
	buf[5] = h.Flags
	binary.LittleEndian.PutUint16(buf[6:8], h.FrameType)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(h.TermOffset))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(h.SessionId))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(h.StreamId))
	binary.LittleEndian.PutUint32(buf[20:24], uint32(h.InitialTermId))
	binary.LittleEndian.PutUint32(buf[24:28], uint32(h.ActiveTermId))
	binary.LittleEndian.PutUint32(buf[28:32], uint32(h.TermLength))
	binary.LittleEndian.PutUint32(buf[32:36], uint32(h.Mtu))
	binary.LittleEndian.PutUint32(buf[36:40], uint32(h.Ttl))
}

// Decode populates h from buf.  buf must be at least AeronSetupHeaderLen bytes.
func (h *AeronSetupHeader) Decode(buf []byte) {
	h.FrameLength = int32(binary.LittleEndian.Uint32(buf[0:4]))
	h.Version = buf[4]
	h.Flags = buf[5]
	h.FrameType = binary.LittleEndian.Uint16(buf[6:8])
	h.TermOffset = int32(binary.LittleEndian.Uint32(buf[8:12]))
	h.SessionId = int32(binary.LittleEndian.Uint32(buf[12:16]))
	h.StreamId = int32(binary.LittleEndian.Uint32(buf[16:20]))
	h.InitialTermId = int32(binary.LittleEndian.Uint32(buf[20:24]))
	h.ActiveTermId = int32(binary.LittleEndian.Uint32(buf[24:28]))
	h.TermLength = int32(binary.LittleEndian.Uint32(buf[28:32]))
	h.Mtu = int32(binary.LittleEndian.Uint32(buf[32:36]))
	h.Ttl = int32(binary.LittleEndian.Uint32(buf[36:40]))
}

// ---------------------------------------------------------------------------
// AeronStatusMessage — 36-byte Status Message (SM) frame
// ---------------------------------------------------------------------------
//
// SM frames flow from subscriber back to publisher, advertising the receiver's
// current consumption position and window.  In Aeron 1.30.0 the frame
// includes an 8-byte receiverId at the end (StatusMessageFlyweight.HEADER_LENGTH = 36).
//
// Byte layout:
//
//	[0–3]   frameLength           = AeronSmHeaderLen (36)
//	[4]     version
//	[5]     flags                 (0)
//	[6–7]   frameType             = AeronHdrTypeSm (3)
//	[8–11]  sessionId
//	[12–15] streamId
//	[16–19] consumptionTermId     — term ID the receiver has consumed up to
//	[20–23] consumptionTermOffset — byte offset consumed within that term
//	[24–27] receiverWindowLength  — remaining receive buffer capacity
//	[28–35] receiverId            — unique receiver identifier (int64)
type AeronStatusMessage struct {
	FrameLength           int32
	Version               uint8
	Flags                 uint8
	FrameType             uint16
	SessionId             int32
	StreamId              int32
	ConsumptionTermId     int32
	ConsumptionTermOffset int32
	ReceiverWindowLength  int32
	ReceiverID            int64
}

// Encode serialises h into buf.  buf must be at least AeronSmHeaderLen (36) bytes.
func (h *AeronStatusMessage) Encode(buf []byte) {
	binary.LittleEndian.PutUint32(buf[0:4], uint32(h.FrameLength))
	buf[4] = h.Version
	buf[5] = h.Flags
	binary.LittleEndian.PutUint16(buf[6:8], h.FrameType)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(h.SessionId))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(h.StreamId))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(h.ConsumptionTermId))
	binary.LittleEndian.PutUint32(buf[20:24], uint32(h.ConsumptionTermOffset))
	binary.LittleEndian.PutUint32(buf[24:28], uint32(h.ReceiverWindowLength))
	binary.LittleEndian.PutUint64(buf[28:36], uint64(h.ReceiverID))
}

// Decode populates h from buf.  buf must be at least AeronSmHeaderLen (36) bytes.
func (h *AeronStatusMessage) Decode(buf []byte) {
	h.FrameLength = int32(binary.LittleEndian.Uint32(buf[0:4]))
	h.Version = buf[4]
	h.Flags = buf[5]
	h.FrameType = binary.LittleEndian.Uint16(buf[6:8])
	h.SessionId = int32(binary.LittleEndian.Uint32(buf[8:12]))
	h.StreamId = int32(binary.LittleEndian.Uint32(buf[12:16]))
	h.ConsumptionTermId = int32(binary.LittleEndian.Uint32(buf[16:20]))
	h.ConsumptionTermOffset = int32(binary.LittleEndian.Uint32(buf[20:24]))
	h.ReceiverWindowLength = int32(binary.LittleEndian.Uint32(buf[24:28]))
	if len(buf) >= 36 {
		h.ReceiverID = int64(binary.LittleEndian.Uint64(buf[28:36]))
	}
}

// ---------------------------------------------------------------------------
// AeronNak — 28-byte Negative-Acknowledgment frame
// ---------------------------------------------------------------------------
//
// NAK frames are sent from subscriber to publisher to request re-transmission
// of a specific byte range within a term.
//
// Byte layout:
//
//	[0–3]   frameLength = AeronNakHeaderLen
//	[4]     version
//	[5]     flags       (0)
//	[6–7]   frameType   = AeronHdrTypeNak
//	[8–11]  sessionId
//	[12–15] streamId
//	[16–19] termId      — term containing the missing data
//	[20–23] termOffset  — start of the gap
//	[24–27] length      — byte length of the missing range
type AeronNak struct {
	FrameLength int32
	Version     uint8
	Flags       uint8
	FrameType   uint16
	SessionId   int32
	StreamId    int32
	TermId      int32
	TermOffset  int32
	Length      int32
}

// Encode serialises h into buf.  buf must be at least AeronNakHeaderLen bytes.
func (h *AeronNak) Encode(buf []byte) {
	binary.LittleEndian.PutUint32(buf[0:4], uint32(h.FrameLength))
	buf[4] = h.Version
	buf[5] = h.Flags
	binary.LittleEndian.PutUint16(buf[6:8], h.FrameType)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(h.SessionId))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(h.StreamId))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(h.TermId))
	binary.LittleEndian.PutUint32(buf[20:24], uint32(h.TermOffset))
	binary.LittleEndian.PutUint32(buf[24:28], uint32(h.Length))
}

// Decode populates h from buf.  buf must be at least AeronNakHeaderLen bytes.
func (h *AeronNak) Decode(buf []byte) {
	h.FrameLength = int32(binary.LittleEndian.Uint32(buf[0:4]))
	h.Version = buf[4]
	h.Flags = buf[5]
	h.FrameType = binary.LittleEndian.Uint16(buf[6:8])
	h.SessionId = int32(binary.LittleEndian.Uint32(buf[8:12]))
	h.StreamId = int32(binary.LittleEndian.Uint32(buf[12:16]))
	h.TermId = int32(binary.LittleEndian.Uint32(buf[16:20]))
	h.TermOffset = int32(binary.LittleEndian.Uint32(buf[20:24]))
	h.Length = int32(binary.LittleEndian.Uint32(buf[24:28]))
}
