/*
 * artery_envelope_buffer.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
)

// Artery EnvelopeBuffer binary frame layout (all fields Little-Endian):
//   [0]       version  (byte)
//   [1]       flags    (byte)
//   [2]       actorRef compression table version (byte)
//   [3]       classManifest compression table version (byte)
//   [4-11]    uid (int64)
//   [12-15]   serializer ID (int32)
//   [16-19]   sender ActorRef tag (int32)
//   [20-23]   recipient ActorRef tag (int32)
//   [24-27]   classManifest tag (int32)
//   [28+]     literal section:
//               - for each "tag" offset above that is set to byteBuffer.position(): 2-byte length + US-ASCII string
//               - message payload bytes appended at end
//
// Tag encoding:
//   if (tag & 0xFF000000) != 0  => compressed idx (tag & 0x0000FFFF)
//   else                        => current position of literal in buffer (offset from start of frame)

const (
	arteryVersion         = byte(0) // Header version — must match ArteryTransport.HighestVersion = 0
	arteryFlags           = byte(0)
	arteryActorRefCompVer = byte(0)
	arteryManifestCompVer = byte(0)
	arteryUID             = int64(1) // simplified uid
	arteryHeaderSize      = 28
	DeadLettersCode       = -1
	TagTypeMask           = uint32(0xFF000000)
	TagValueMask          = uint32(0x00FFFFFF)
)

// BuildArteryFrame builds a proper Artery EnvelopeBuffer binary frame.
// sender and recipient are actor path strings (e.g. "pekko://Sys@host:port/user/foo").
// manifest is the message type manifest string (e.g. "HandshakeReq").
// serializerID is the serializer to use.
// uid is the local node uid.
// payload is the already-marshalled message bytes.
func BuildArteryFrame(
	uid int64,
	serializerID int32,
	senderPath string,
	recipientPath string,
	manifest string,
	payload []byte,
	control bool,
) ([]byte, error) {
	// Literal section: sender, recipient, manifest then payload
	// Each literal: position stored at the tag offset, then 2-byte short length + ASCII bytes
	//
	// HISTORY: gekka previously encoded an absent sender as DeadLettersCode = -1
	// to dodge a NoSuchElementException ("OptionVal.None.get") that fires when
	// Akka resolves an empty literal sender.  But -1 (= 0xFFFFFFFF) collides
	// with Akka's multi-DC Decoder, which reads the senderTag's high bits as
	// a "compressed" flag and the low 16 bits as a compression-table id.
	// -1 → flag set, id = 0xFFFF = 65535 → lookup in the empty per-origin
	// compression table → frame dropped → joiner stays in Joining forever
	// (the dashboard's reported failure).  See absentSenderLiteral for the
	// full rationale.
	//
	// The Akka-canonical absent-sender encoding is the system-only literal
	// "<proto>://<system>/deadLetters", which Akka resolves to the receiver's
	// own deadLetters singleton without invoking the compression decoder.
	litBuf := &bytes.Buffer{}

	// Resolve a (proto, system) hint for synthesising an absent sender.
	// Try recipientPath (most callers have it) — senderPath being empty
	// is what we're trying to handle, so it isn't useful here.
	hintProto, hintSystem := extractProtoAndSystem(recipientPath)

	// Sender encoding policy:
	//   - non-empty senderPath  -> literal at the next free position
	//   - empty AND we can synthesise a deadLetters path
	//     ("<localProto>://<localSystem>/deadLetters" derived from the
	//     recipient) -> literal deadLetters (Akka multi-DC compatible)
	//   - empty AND no usable hint (heartbeat-style frames where both
	//     sender and recipient are absent) -> legacy DeadLettersCode = -1
	//     sentinel.  This preserves the pre-multi-DC behaviour the
	//     existing tests and Akka non-multi-DC clusters depend on.
	var senderTag int32
	if senderPath == "" && hintSystem == "" {
		senderTag = DeadLettersCode
	} else {
		effectiveSender := senderPath
		if effectiveSender == "" {
			effectiveSender = absentSenderLiteral(hintProto, hintSystem)
		}
		senderTag = int32(arteryHeaderSize + litBuf.Len())
		writeLiteral(litBuf, effectiveSender)
	}

	// Recipient: always write as a literal (empty string is valid).
	recipientTag := int32(arteryHeaderSize + litBuf.Len())
	writeLiteral(litBuf, recipientPath)

	manifestTag := int32(arteryHeaderSize + litBuf.Len())
	writeLiteral(litBuf, manifest)

	// Payload follows literals
	litBuf.Write(payload)

	// Now build the final fixed header with LittleEndian
	var h [arteryHeaderSize]byte
	h[0] = arteryVersion
	// Flags byte must always be 0x00.
	// Bit 0 in Akka/Pekko Artery is MetadataPresentFlag — setting it causes
	// the remote decoder to try to read a metadata container at offset 28,
	// which reads our literal-length bytes as a huge buffer position and throws
	// IllegalArgumentException: newPosition > limit.
	// Control vs ordinary stream is signalled by the preamble streamId, not flags.
	_ = control
	h[1] = 0
	h[2] = arteryActorRefCompVer
	h[3] = arteryManifestCompVer
	binary.LittleEndian.PutUint64(h[4:12], uint64(uid))
	binary.LittleEndian.PutUint32(h[12:16], uint32(serializerID))
	binary.LittleEndian.PutUint32(h[16:20], uint32(senderTag))
	binary.LittleEndian.PutUint32(h[20:24], uint32(recipientTag))
	binary.LittleEndian.PutUint32(h[24:28], uint32(manifestTag))

	// Combine
	frame := make([]byte, arteryHeaderSize+litBuf.Len())
	copy(frame[0:arteryHeaderSize], h[:])
	copy(frame[arteryHeaderSize:], litBuf.Bytes())
	return frame, nil
}

func writeLiteral(buf *bytes.Buffer, s string) {
	l := len(s)
	var lenBytes [2]byte
	binary.LittleEndian.PutUint16(lenBytes[:], uint16(l))
	buf.Write(lenBytes[:])
	buf.WriteString(s)
}

// absentSenderLiteral returns the Akka-canonical deadLetters actor path
// "<proto>://<system>/deadLetters" used in place of an empty sender tag.
// Akka resolves system-only paths ending in /deadLetters to the receiver's
// own local deadLetters singleton, regardless of the system name in the
// path — so even if proto/system are best-guess fallbacks this still
// resolves cleanly and avoids the compression-decoder collision the
// legacy DeadLettersCode = -1 sentinel had in multi-DC mode.
func absentSenderLiteral(proto, system string) string {
	if proto == "" {
		proto = "akka"
	}
	if system == "" {
		system = "gekka"
	}
	return fmt.Sprintf("%s://%s/deadLetters", proto, system)
}

// extractProtoAndSystem pulls the protocol and system fields from the
// first non-empty Artery path it is given.  Recognised shape:
//
//	<proto>://<system>@<host>:<port>/...     (full Artery path)
//	<proto>://<system>/...                   (system-only path)
//
// Returns ("", "") when neither input parses successfully.
func extractProtoAndSystem(paths ...string) (proto, system string) {
	for _, p := range paths {
		if p == "" {
			continue
		}
		i := strings.Index(p, "://")
		if i <= 0 {
			continue
		}
		proto = p[:i]
		rest := p[i+3:]
		// rest is "<system>@<host>:<port>/..." or "<system>/...".
		if at := strings.IndexByte(rest, '@'); at > 0 {
			system = rest[:at]
		} else if slash := strings.IndexByte(rest, '/'); slash > 0 {
			system = rest[:slash]
		} else {
			system = rest
		}
		return proto, system
	}
	return "", ""
}

// ParseArteryFrame decodes an Artery binary frame back into its component fields.
func ParseArteryFrame(data []byte, ctm *CompressionTableManager, remoteUid uint64) (*ArteryMetadata, error) {
	if len(data) < arteryHeaderSize {
		return nil, fmt.Errorf("frame too short: %d < %d", len(data), arteryHeaderSize)
	}

	// version := data[0]
	// flags := data[1]
	uid := int64(binary.LittleEndian.Uint64(data[4:12]))
	serializerID := int32(binary.LittleEndian.Uint32(data[12:16]))
	senderTag := int32(binary.LittleEndian.Uint32(data[16:20]))
	recipientTag := int32(binary.LittleEndian.Uint32(data[20:24]))
	manifestTag := int32(binary.LittleEndian.Uint32(data[24:28]))

	_ = uid

	readLiteralAt := func(tagValue int32, isActorRef bool) (string, int, error) {
		if tagValue == DeadLettersCode {
			return "", 0, nil
		}

		isCompressed := (uint32(tagValue) & TagTypeMask) != 0
		if isCompressed {
			if ctm == nil {
				return "", 0, fmt.Errorf("ParseArteryFrame: received compressed tag %x but no CompressionTableManager provided", tagValue)
			}
			id := uint32(tagValue) & TagValueMask
			var str string
			var err error
			if isActorRef {
				str, err = ctm.LookupActorRef(remoteUid, id)
			} else {
				str, err = ctm.LookupManifest(remoteUid, id)
			}

			if err != nil {
				return "", 0, fmt.Errorf("ParseArteryFrame: failed to resolve compressed tag %d (isActorRef=%v) for uid %d: %w", id, isActorRef, remoteUid, err)
			}
			return str, 0, nil // Compressed tags don't consume literal buffer space
		}

		offset := int(tagValue)
		if offset < arteryHeaderSize || offset > len(data)-2 {
			return "", 0, fmt.Errorf("invalid literal offset %d (headerSize=%d, dataLen=%d)", tagValue, arteryHeaderSize, len(data))
		}
		r := bytes.NewReader(data[offset:])
		var lenBuf [2]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return "", 0, err
		}
		length := int(binary.LittleEndian.Uint16(lenBuf[:]))
		if offset+2+length > len(data) {
			return "", 0, fmt.Errorf("literal length %d at offset %d exceeds data length %d", length, offset, len(data))
		}
		strBytes := make([]byte, length)
		if _, err := io.ReadFull(r, strBytes); err != nil {
			return "", 0, err
		}
		return string(strBytes), offset + 2 + length, nil
	}

	senderPath, senderEnd, err := readLiteralAt(senderTag, true)
	if err != nil {
		return nil, fmt.Errorf("read sender literal: %w", err)
	}
	recipientPath, recipientEnd, err := readLiteralAt(recipientTag, true)
	if err != nil {
		return nil, fmt.Errorf("read recipient literal: %w", err)
	}
	manifestStr, manifestEnd, err := readLiteralAt(manifestTag, false)
	if err != nil {
		return nil, fmt.Errorf("read manifest literal: %w", err)
	}

	// Payload is everything after the maximum literal end
	maxEnd := arteryHeaderSize
	if senderEnd > maxEnd {
		maxEnd = senderEnd
	}
	if recipientEnd > maxEnd {
		maxEnd = recipientEnd
	}
	if manifestEnd > maxEnd {
		maxEnd = manifestEnd
	}

	// Copy the payload because `data` aliases the read loop's reusable
	// buffer (tcpArteryReadLoop reuses payloadBuf across iterations). With
	// inbound-lanes (sub-plan 8f) the metadata is sent through a channel
	// to a lane goroutine that processes it asynchronously; without this
	// copy the next frame's bytes would overwrite the previous frame's
	// payload while the lane goroutine is still reading it, corrupting
	// every async-dispatched message past the first.
	src := data[maxEnd:]
	payload := make([]byte, len(src))
	copy(payload, src)

	return &ArteryMetadata{
		Sender:          &gproto_remote.ActorRefData{Path: &senderPath},
		Recipient:       &gproto_remote.ActorRefData{Path: &recipientPath},
		SerializerId:    serializerID,
		MessageManifest: []byte(manifestStr),
		Payload:         payload,
	}, nil
}
