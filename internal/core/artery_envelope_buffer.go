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
	litBuf := &bytes.Buffer{}

	// Serialize literals
	senderTag := int32(arteryHeaderSize + litBuf.Len())
	writeLiteral(litBuf, senderPath)

	recipientTag := int32(arteryHeaderSize + litBuf.Len())
	writeLiteral(litBuf, recipientPath)

	manifestTag := int32(arteryHeaderSize + litBuf.Len())
	writeLiteral(litBuf, manifest)

	// Payload follows literals
	litBuf.Write(payload)

	// Now build the final fixed header with LittleEndian
	var h [arteryHeaderSize]byte
	h[0] = arteryVersion
	h[1] = arteryFlags
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

	payload := data[maxEnd:]

	return &ArteryMetadata{
		Sender:          &gproto_remote.ActorRefData{Path: &senderPath},
		Recipient:       &gproto_remote.ActorRefData{Path: &recipientPath},
		SerializerId:    serializerID,
		MessageManifest: []byte(manifestStr),
		Payload:         payload,
	}, nil
}
