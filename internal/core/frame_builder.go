/*
 * frame_builder.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"encoding/binary"
	"fmt"
	"net"
)

// ArteryFrameBuilder facilitates building Artery frames with minimal copying.
type ArteryFrameBuilder struct {
	header [arteryHeaderSize]byte
	lits   []byte
}

func NewArteryFrameBuilder() *ArteryFrameBuilder {
	return &ArteryFrameBuilder{}
}

// BuildToBuffers constructs an Artery frame as net.Buffers to avoid copying the payload.
func (b *ArteryFrameBuilder) BuildToBuffers(
	uid int64,
	serializerID int32,
	senderPath string,
	recipientPath string,
	manifest string,
	payload []byte,
) (net.Buffers, error) {
	// Reset literals
	b.lits = b.lits[:0]

	// Serialize literals and calculate tags
	senderTag := int32(arteryHeaderSize + len(b.lits))
	b.writeLiteral(senderPath)

	recipientTag := int32(arteryHeaderSize + len(b.lits))
	b.writeLiteral(recipientPath)

	manifestTag := int32(arteryHeaderSize + len(b.lits))
	b.writeLiteral(manifest)

	// Build fixed header
	b.header[0] = arteryVersion
	b.header[1] = arteryFlags
	b.header[2] = arteryActorRefCompVer
	b.header[3] = arteryManifestCompVer
	binary.LittleEndian.PutUint64(b.header[4:12], uint64(uid))
	binary.LittleEndian.PutUint32(b.header[12:16], uint32(serializerID))
	binary.LittleEndian.PutUint32(b.header[16:20], uint32(senderTag))
	binary.LittleEndian.PutUint32(b.header[20:24], uint32(recipientTag))
	binary.LittleEndian.PutUint32(b.header[24:28], uint32(manifestTag))

	// Resulting buffers: [header][literals][payload]
	// header and literals are copied into the builder's internal buffers,
	// but the payload remains as is.
	res := make(net.Buffers, 3)
	res[0] = b.header[:]

	// Create a copy of the literals to avoid subsequent overwrites
	litsCopy := make([]byte, len(b.lits))
	copy(litsCopy, b.lits)
	res[1] = litsCopy

	res[2] = payload

	return res, nil
}

func (b *ArteryFrameBuilder) writeLiteral(s string) {
	l := len(s)
	offset := len(b.lits)
	b.lits = append(b.lits, 0, 0)
	binary.LittleEndian.PutUint16(b.lits[offset:offset+2], uint16(l))
	b.lits = append(b.lits, s...)
}

// BuildWithZeroCopySerializer uses a ZeroCopySerializer to avoid even the payload allocation.
func (b *ArteryFrameBuilder) BuildWithZeroCopySerializer(
	uid int64,
	senderPath string,
	recipientPath string,
	manifest string,
	msg interface{},
	serializer ZeroCopySerializer,
) (net.Buffers, error) {
	size := serializer.Size(msg)
	payload := make([]byte, size)
	_, err := serializer.MarshalTo(payload, msg)
	if err != nil {
		return nil, fmt.Errorf("ZeroCopySerializer.MarshalTo failed: %w", err)
	}

	return b.BuildToBuffers(uid, serializer.Identifier(), senderPath, recipientPath, manifest, payload)
}
