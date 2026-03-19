/*
 * ddata_serializer.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package crdt provides CRDT implementations and, in this file, a Pekko
// Distributed Data (DData) wire-format serializer / deserializer.
//
// Pekko DData uses two serializers registered in reference.conf:
//
//	org.apache.pekko.cluster.ddata.protobuf.ReplicatedDataSerializer  = 11
//	org.apache.pekko.cluster.ddata.protobuf.ReplicatorMessageSerializer = 12
//
// The wire format is proto2-based but uses several non-obvious conventions:
//
//  1. UniqueAddress is encoded as:
//     - field 1 (len-delim): Address { hostname(f1 string), port(f2 varint) }
//     — NOTE: no system or protocol in the DData Address!
//     - field 2 (fixed32 LE): uid low 32 bits
//     - field 3 (fixed32 LE): uid high 32 bits
//
//  2. GCounter entry value is stored as bytes (big-endian, BigInteger format).
//
//  3. ORSet payload (manifest "C") is GZIP-compressed.
//
// Manifest codes (Pekko 1.0.x — different from older Akka 2.x codes):
//
//	"F"  GCounter               (serializerId=11)
//	"C"  ORSet (GZIP)           (serializerId=11)
//	"Q"  DeltaPropagation       (serializerId=12)
package ddata

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
)

// ---------------------------------------------------------------------------
// Serializer ID and manifest constants
// ---------------------------------------------------------------------------

const (
	// DDataReplicatedSerializerID is Pekko's ReplicatedDataSerializer ID.
	// Handles: GCounter, ORSet, GSet, PNCounter, LWWRegister, ORMap, etc.
	DDataReplicatedSerializerID = int32(11)

	// DDataReplicatorMsgSerializerID is Pekko's ReplicatorMessageSerializer ID.
	// Handles: DeltaPropagation, Gossip, Status, Write, Read, etc.
	DDataReplicatorMsgSerializerID = int32(12)

	// DData manifest codes (Pekko 1.0.x compact single/double-char format).
	GCounterManifest         = "F"
	ORSetManifest            = "C" // payload is GZIP-compressed
	DeltaPropagationManifest = "Q"
)

// ---------------------------------------------------------------------------
// Decoded types
// ---------------------------------------------------------------------------

// DDAddress is the DData wire-format address (hostname + port only; no system
// or protocol field — those are omitted by Pekko's DData serializer).
type DDAddress struct {
	Hostname string
	Port     uint32
}

// DDUniqueAddress is the Pekko DData wire-format unique address.
// The 64-bit UID is split into low/high fixed32 fields.
type DDUniqueAddress struct {
	Address DDAddress
	UIDLow  uint32 // field 2, wire type 5 (fixed32 LE)
	UIDHigh uint32 // field 3, wire type 5 (fixed32 LE)
}

// UID returns the reconstructed 64-bit UID.
func (a DDUniqueAddress) UID() uint64 {
	return uint64(a.UIDLow) | (uint64(a.UIDHigh) << 32)
}

// DDGCounterEntry is one node-value entry in a Pekko DData GCounter.
type DDGCounterEntry struct {
	Node  DDUniqueAddress
	Value uint64
}

// DDORSetDotEntry holds a (node, counter) pair from the ORSet version vector
// or per-element dot list.
type DDORSetDotEntry struct {
	Node    DDUniqueAddress
	Counter uint64
}

// DDORSet is the decoded form of a Pekko DData ORSet[String].
//
// Wire layout (inner proto, after GZIP decompress):
//   - field 1 (one len-delim blob): VVector sub-message containing repeated
//     DotEntries at its own field 1.
//   - field 2 (repeated len-delim): DotVector sub-messages, one per element.
//     Each DotVector contains repeated DotEntries at its field 1 (1 entry per
//     element in the common single-add case).
//   - field 3 (repeated string): element strings.
//
// DotVectors[i] holds the dots for Elements[i] (position-based mapping).
type DDORSet struct {
	VVector    []DDORSetDotEntry   // decoded from VVector sub-message (field 1 of ORSet)
	DotVectors [][]DDORSetDotEntry // decoded from DotVector sub-messages (field 2, repeated)
	Elements   []string            // string elements (field 3)
}

// DDDeltaEntry is one key→DataEnvelope pair inside a DeltaPropagation.
type DDDeltaEntry struct {
	Key          string
	SerializerID int32
	Manifest     string
	Data         []byte // raw serialized inner CRDT bytes
}

// DDDeltaPropagation is the decoded form of a Pekko DData DeltaPropagation.
type DDDeltaPropagation struct {
	FromNode DDUniqueAddress
	Deltas   []DDDeltaEntry
}

// ---------------------------------------------------------------------------
// DDataSerializer
// ---------------------------------------------------------------------------

// DDataSerializer encodes and decodes Pekko Distributed Data wire format.
type DDataSerializer struct{}

// ---------------------------------------------------------------------------
// GCounter
// ---------------------------------------------------------------------------

// EncodeGCounter encodes entries into a Pekko DData GCounter proto payload.
// The entries should be sorted by (hostname, port, uid) for deterministic output.
func (s *DDataSerializer) EncodeGCounter(entries []DDGCounterEntry) []byte {
	var out []byte
	for _, e := range entries {
		entryBytes := s.encodeGCounterEntry(e)
		out = appendLenDelimDD(out, 1, entryBytes)
	}
	return out
}

// DecodeGCounter decodes a Pekko DData GCounter payload into entries.
func (s *DDataSerializer) DecodeGCounter(data []byte) ([]DDGCounterEntry, error) {
	var entries []DDGCounterEntry
	i := 0
	for i < len(data) {
		tag, n := consumeVarintDD(data, i)
		if n <= 0 {
			return nil, fmt.Errorf("GCounter: bad varint at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch {
		case fieldNum == 1 && wireType == 2:
			entryBytes, nn := consumeBytesDD(data, i)
			if nn <= 0 {
				return nil, fmt.Errorf("GCounter: bad entry at %d", i)
			}
			i += nn
			entry, err := s.decodeGCounterEntry(entryBytes)
			if err != nil {
				return nil, err
			}
			entries = append(entries, entry)
		default:
			nn, err := skipProtoFieldDD(data, i, wireType)
			if err != nil {
				return nil, fmt.Errorf("GCounter: skip field %d: %w", fieldNum, err)
			}
			i += nn
		}
	}
	return entries, nil
}

func (s *DDataSerializer) encodeGCounterEntry(e DDGCounterEntry) []byte {
	var out []byte
	uaBytes := encodeUniqueAddressDD(e.Node)
	out = appendLenDelimDD(out, 1, uaBytes)
	valBytes := encodeBigIntBigEndian(e.Value)
	out = appendLenDelimDD(out, 2, valBytes)
	return out
}

func (s *DDataSerializer) decodeGCounterEntry(data []byte) (DDGCounterEntry, error) {
	var e DDGCounterEntry
	i := 0
	for i < len(data) {
		tag, n := consumeVarintDD(data, i)
		if n <= 0 {
			return e, fmt.Errorf("entry: bad varint at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch {
		case fieldNum == 1 && wireType == 2:
			uaBytes, nn := consumeBytesDD(data, i)
			if nn <= 0 {
				return e, fmt.Errorf("entry: bad UniqueAddress at %d", i)
			}
			i += nn
			ua, err := decodeUniqueAddressDD(uaBytes)
			if err != nil {
				return e, err
			}
			e.Node = ua
		case fieldNum == 2 && wireType == 2:
			valBytes, nn := consumeBytesDD(data, i)
			if nn <= 0 {
				return e, fmt.Errorf("entry: bad value bytes at %d", i)
			}
			i += nn
			e.Value = decodeBigIntBigEndian(valBytes)
		default:
			nn, err := skipProtoFieldDD(data, i, wireType)
			if err != nil {
				return e, fmt.Errorf("entry: skip field %d: %w", fieldNum, err)
			}
			i += nn
		}
	}
	return e, nil
}

// ---------------------------------------------------------------------------
// ORSet
// ---------------------------------------------------------------------------

// DecodeORSet decompresses a GZIP payload and decodes the inner ORSet proto.
func (s *DDataSerializer) DecodeORSet(compressed []byte) (*DDORSet, error) {
	inner, err := gunzipDD(compressed)
	if err != nil {
		return nil, fmt.Errorf("ORSet: decompress: %w", err)
	}
	return s.decodeORSetInner(inner)
}

// EncodeORSet serializes an ORSet and GZIP-compresses it.
func (s *DDataSerializer) EncodeORSet(orset *DDORSet) ([]byte, error) {
	inner := s.encodeORSetInner(orset)
	return gzipDD(inner)
}

func (s *DDataSerializer) decodeORSetInner(data []byte) (*DDORSet, error) {
	out := &DDORSet{}
	i := 0
	for i < len(data) {
		tag, n := consumeVarintDD(data, i)
		if n <= 0 {
			return nil, fmt.Errorf("ORSet: bad varint at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch {
		case fieldNum == 1 && wireType == 2:
			// One VVector sub-message blob; parse its repeated field-1 DotEntries.
			vvBytes, nn := consumeBytesDD(data, i)
			if nn <= 0 {
				return nil, fmt.Errorf("ORSet: bad vvector at %d", i)
			}
			i += nn
			entries, err := decodeDotEntryListDD(vvBytes)
			if err != nil {
				return nil, fmt.Errorf("ORSet vvector: %w", err)
			}
			out.VVector = entries
		case fieldNum == 2 && wireType == 2:
			// One DotVector sub-message blob per element; parse its repeated field-1 DotEntries.
			dvBytes, nn := consumeBytesDD(data, i)
			if nn <= 0 {
				return nil, fmt.Errorf("ORSet: bad dotvector at %d", i)
			}
			i += nn
			entries, err := decodeDotEntryListDD(dvBytes)
			if err != nil {
				return nil, fmt.Errorf("ORSet dotvector: %w", err)
			}
			out.DotVectors = append(out.DotVectors, entries)
		case fieldNum == 3 && wireType == 2: // string element
			elemBytes, nn := consumeBytesDD(data, i)
			if nn <= 0 {
				return nil, fmt.Errorf("ORSet: bad element at %d", i)
			}
			i += nn
			out.Elements = append(out.Elements, string(elemBytes))
		default:
			nn, err := skipProtoFieldDD(data, i, wireType)
			if err != nil {
				return nil, fmt.Errorf("ORSet: skip field %d: %w", fieldNum, err)
			}
			i += nn
		}
	}
	return out, nil
}

// decodeDotEntryListDD parses a sub-message blob that contains repeated DotEntries
// at field 1. Used for both VVector and DotVector sub-messages.
func decodeDotEntryListDD(data []byte) ([]DDORSetDotEntry, error) {
	var entries []DDORSetDotEntry
	i := 0
	for i < len(data) {
		tag, n := consumeVarintDD(data, i)
		if n <= 0 {
			return nil, fmt.Errorf("dotList: bad varint at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch {
		case fieldNum == 1 && wireType == 2:
			entryBytes, nn := consumeBytesDD(data, i)
			if nn <= 0 {
				return nil, fmt.Errorf("dotList: bad entry at %d", i)
			}
			i += nn
			de, err := decodeDotEntryDD(entryBytes)
			if err != nil {
				return nil, err
			}
			entries = append(entries, de)
		default:
			nn, err := skipProtoFieldDD(data, i, wireType)
			if err != nil {
				return nil, fmt.Errorf("dotList: skip field %d: %w", fieldNum, err)
			}
			i += nn
		}
	}
	return entries, nil
}

func (s *DDataSerializer) encodeORSetInner(orset *DDORSet) []byte {
	var out []byte

	// field 1: one VVector blob containing repeated DotEntries at field 1 inside
	var vvBytes []byte
	for _, e := range orset.VVector {
		vvBytes = appendLenDelimDD(vvBytes, 1, encodeDotEntryDD(e))
	}
	out = appendLenDelimDD(out, 1, vvBytes)

	// field 2 (repeated): one DotVector blob per element
	for _, dvEntries := range orset.DotVectors {
		var dvBytes []byte
		for _, e := range dvEntries {
			dvBytes = appendLenDelimDD(dvBytes, 1, encodeDotEntryDD(e))
		}
		out = appendLenDelimDD(out, 2, dvBytes)
	}

	// field 3 (repeated): element strings
	for _, elem := range orset.Elements {
		out = appendLenDelimDD(out, 3, []byte(elem))
	}
	return out
}

// ---------------------------------------------------------------------------
// DeltaPropagation
// ---------------------------------------------------------------------------

// DecodeDeltaPropagation decodes a Pekko DData DeltaPropagation payload.
func (s *DDataSerializer) DecodeDeltaPropagation(data []byte) (*DDDeltaPropagation, error) {
	out := &DDDeltaPropagation{}
	i := 0
	for i < len(data) {
		tag, n := consumeVarintDD(data, i)
		if n <= 0 {
			return nil, fmt.Errorf("DeltaProp: bad varint at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch {
		case fieldNum == 1 && wireType == 2: // fromNode UniqueAddress
			uaBytes, nn := consumeBytesDD(data, i)
			if nn <= 0 {
				return nil, fmt.Errorf("DeltaProp: bad fromNode at %d", i)
			}
			i += nn
			ua, err := decodeUniqueAddressDD(uaBytes)
			if err != nil {
				return nil, err
			}
			out.FromNode = ua
		case fieldNum == 2 && wireType == 2: // map entry (key → Delta)
			entryBytes, nn := consumeBytesDD(data, i)
			if nn <= 0 {
				return nil, fmt.Errorf("DeltaProp: bad entry at %d", i)
			}
			i += nn
			de, err := s.decodeDeltaEntry(entryBytes)
			if err != nil {
				return nil, err
			}
			out.Deltas = append(out.Deltas, de)
		default:
			nn, err := skipProtoFieldDD(data, i, wireType)
			if err != nil {
				return nil, fmt.Errorf("DeltaProp: skip field %d: %w", fieldNum, err)
			}
			i += nn
		}
	}
	return out, nil
}

func (s *DDataSerializer) decodeDeltaEntry(data []byte) (DDDeltaEntry, error) {
	var de DDDeltaEntry
	i := 0
	for i < len(data) {
		tag, n := consumeVarintDD(data, i)
		if n <= 0 {
			return de, fmt.Errorf("deltaEntry: bad varint at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch {
		case fieldNum == 1 && wireType == 2: // key string
			keyBytes, nn := consumeBytesDD(data, i)
			if nn <= 0 {
				return de, fmt.Errorf("deltaEntry: bad key at %d", i)
			}
			i += nn
			de.Key = string(keyBytes)
		case fieldNum == 2 && wireType == 2: // Delta (contains DataEnvelope)
			deltaBytes, nn := consumeBytesDD(data, i)
			if nn <= 0 {
				return de, fmt.Errorf("deltaEntry: bad delta at %d", i)
			}
			i += nn
			// Delta.field1 = DataEnvelope
			if err := s.extractDataEnvelope(deltaBytes, &de); err != nil {
				return de, err
			}
		default:
			nn, err := skipProtoFieldDD(data, i, wireType)
			if err != nil {
				return de, fmt.Errorf("deltaEntry: skip field %d: %w", fieldNum, err)
			}
			i += nn
		}
	}
	return de, nil
}

func (s *DDataSerializer) extractDataEnvelope(deltaBytes []byte, de *DDDeltaEntry) error {
	i := 0
	for i < len(deltaBytes) {
		tag, n := consumeVarintDD(deltaBytes, i)
		if n <= 0 {
			return fmt.Errorf("delta: bad varint at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch {
		case fieldNum == 1 && wireType == 2: // DataEnvelope
			envBytes, nn := consumeBytesDD(deltaBytes, i)
			if nn <= 0 {
				return fmt.Errorf("delta: bad envelope at %d", i)
			}
			_ = nn
			return s.decodeDataEnvelope(envBytes, de)
		default:
			nn, err := skipProtoFieldDD(deltaBytes, i, wireType)
			if err != nil {
				return fmt.Errorf("delta: skip field %d: %w", fieldNum, err)
			}
			i += nn
		}
	}
	return nil
}

func (s *DDataSerializer) decodeDataEnvelope(data []byte, de *DDDeltaEntry) error {
	i := 0
	for i < len(data) {
		tag, n := consumeVarintDD(data, i)
		if n <= 0 {
			return fmt.Errorf("envelope: bad varint at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch {
		case fieldNum == 1 && wireType == 2: // serialized data bytes
			raw, nn := consumeBytesDD(data, i)
			if nn <= 0 {
				return fmt.Errorf("envelope: bad data at %d", i)
			}
			i += nn
			de.Data = raw
		case fieldNum == 2 && wireType == 0: // serializerId
			val, nn := consumeVarintDD(data, i)
			if nn <= 0 {
				return fmt.Errorf("envelope: bad serializerId at %d", i)
			}
			i += nn
			de.SerializerID = int32(val)
		case fieldNum == 4 && wireType == 2: // manifest
			mfst, nn := consumeBytesDD(data, i)
			if nn <= 0 {
				return fmt.Errorf("envelope: bad manifest at %d", i)
			}
			i += nn
			de.Manifest = string(mfst)
		default:
			nn, err := skipProtoFieldDD(data, i, wireType)
			if err != nil {
				return fmt.Errorf("envelope: skip field %d: %w", fieldNum, err)
			}
			i += nn
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// UniqueAddress encode / decode
// ---------------------------------------------------------------------------

func encodeUniqueAddressDD(ua DDUniqueAddress) []byte {
	// Address: hostname(f1 string), port(f2 varint)
	var addrBytes []byte
	addrBytes = appendLenDelimDD(addrBytes, 1, []byte(ua.Address.Hostname))
	addrBytes = appendVarintDD(addrBytes, 2, uint64(ua.Address.Port))
	var out []byte
	out = appendLenDelimDD(out, 1, addrBytes)
	// uid_low as fixed32 (field 2, wire type 5)
	out = appendFixed32DD(out, 2, ua.UIDLow)
	// uid_high as fixed32 (field 3, wire type 5)
	out = appendFixed32DD(out, 3, ua.UIDHigh)
	return out
}

func decodeUniqueAddressDD(data []byte) (DDUniqueAddress, error) {
	var ua DDUniqueAddress
	i := 0
	for i < len(data) {
		tag, n := consumeVarintDD(data, i)
		if n <= 0 {
			return ua, fmt.Errorf("UniqueAddress: bad varint at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch {
		case fieldNum == 1 && wireType == 2:
			addrBytes, nn := consumeBytesDD(data, i)
			if nn <= 0 {
				return ua, fmt.Errorf("UniqueAddress: bad address at %d", i)
			}
			i += nn
			addr, err := decodeAddressDD(addrBytes)
			if err != nil {
				return ua, err
			}
			ua.Address = addr
		case wireType == 5: // fixed32
			if i+4 > len(data) {
				return ua, fmt.Errorf("UniqueAddress: truncated fixed32 at %d", i)
			}
			v := binary.LittleEndian.Uint32(data[i : i+4])
			i += 4
			if fieldNum == 2 {
				ua.UIDLow = v
			} else if fieldNum == 3 {
				ua.UIDHigh = v
			}
		default:
			nn, err := skipProtoFieldDD(data, i, wireType)
			if err != nil {
				return ua, fmt.Errorf("UniqueAddress: skip field %d: %w", fieldNum, err)
			}
			i += nn
		}
	}
	return ua, nil
}

func decodeAddressDD(data []byte) (DDAddress, error) {
	var addr DDAddress
	i := 0
	for i < len(data) {
		tag, n := consumeVarintDD(data, i)
		if n <= 0 {
			return addr, fmt.Errorf("Address: bad varint at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch {
		case fieldNum == 1 && wireType == 2:
			b, nn := consumeBytesDD(data, i)
			if nn <= 0 {
				return addr, fmt.Errorf("Address: bad hostname at %d", i)
			}
			i += nn
			addr.Hostname = string(b)
		case fieldNum == 2 && wireType == 0:
			v, nn := consumeVarintDD(data, i)
			if nn <= 0 {
				return addr, fmt.Errorf("Address: bad port at %d", i)
			}
			i += nn
			addr.Port = uint32(v)
		default:
			nn, err := skipProtoFieldDD(data, i, wireType)
			if err != nil {
				return addr, fmt.Errorf("Address: skip field %d: %w", fieldNum, err)
			}
			i += nn
		}
	}
	return addr, nil
}

// ---------------------------------------------------------------------------
// ORSet DotEntry encode / decode
// ---------------------------------------------------------------------------

func encodeDotEntryDD(e DDORSetDotEntry) []byte {
	var out []byte
	uaBytes := encodeUniqueAddressDD(e.Node)
	out = appendLenDelimDD(out, 1, uaBytes)
	out = appendVarintDD(out, 2, e.Counter)
	return out
}

func decodeDotEntryDD(data []byte) (DDORSetDotEntry, error) {
	var de DDORSetDotEntry
	i := 0
	for i < len(data) {
		tag, n := consumeVarintDD(data, i)
		if n <= 0 {
			return de, fmt.Errorf("DotEntry: bad varint at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch {
		case fieldNum == 1 && wireType == 2:
			uaBytes, nn := consumeBytesDD(data, i)
			if nn <= 0 {
				return de, fmt.Errorf("DotEntry: bad UA at %d", i)
			}
			i += nn
			ua, err := decodeUniqueAddressDD(uaBytes)
			if err != nil {
				return de, err
			}
			de.Node = ua
		case fieldNum == 2 && wireType == 0:
			v, nn := consumeVarintDD(data, i)
			if nn <= 0 {
				return de, fmt.Errorf("DotEntry: bad counter at %d", i)
			}
			i += nn
			de.Counter = v
		default:
			nn, err := skipProtoFieldDD(data, i, wireType)
			if err != nil {
				return de, fmt.Errorf("DotEntry: skip field %d: %w", fieldNum, err)
			}
			i += nn
		}
	}
	return de, nil
}

// ---------------------------------------------------------------------------
// Value encoding: big-endian bytes (Java BigInteger.toByteArray format)
// ---------------------------------------------------------------------------

// encodeBigIntBigEndian encodes a uint64 as a minimal big-endian byte slice,
// matching Java's BigInteger.toByteArray() for positive values.
func encodeBigIntBigEndian(v uint64) []byte {
	if v == 0 {
		return []byte{0}
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	// Strip leading zero bytes
	start := 0
	for start < 7 && buf[start] == 0 {
		start++
	}
	return buf[start:]
}

// decodeBigIntBigEndian decodes a big-endian byte slice as a uint64.
func decodeBigIntBigEndian(b []byte) uint64 {
	var v uint64
	for _, byt := range b {
		v = (v << 8) | uint64(byt)
	}
	return v
}

// ---------------------------------------------------------------------------
// GZIP helpers
// ---------------------------------------------------------------------------

func gunzipDD(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()
	return io.ReadAll(r)
}

func gzipDD(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ---------------------------------------------------------------------------
// Low-level proto wire helpers (DData-specific, not reusing sharding helpers)
// ---------------------------------------------------------------------------

func appendTagDD(dst []byte, fieldNum uint64, wireType uint64) []byte {
	return appendVarintRawDD(dst, (fieldNum<<3)|wireType)
}

func appendVarintRawDD(dst []byte, v uint64) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

func appendVarintDD(dst []byte, fieldNum uint64, v uint64) []byte {
	dst = appendTagDD(dst, fieldNum, 0)
	return appendVarintRawDD(dst, v)
}

func appendLenDelimDD(dst []byte, fieldNum uint64, data []byte) []byte {
	dst = appendTagDD(dst, fieldNum, 2)
	dst = appendVarintRawDD(dst, uint64(len(data)))
	return append(dst, data...)
}

func appendFixed32DD(dst []byte, fieldNum uint64, v uint32) []byte {
	dst = appendTagDD(dst, fieldNum, 5)
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	return append(dst, b[:]...)
}

func consumeVarintDD(data []byte, offset int) (uint64, int) {
	var v uint64
	shift := uint(0)
	for i := offset; i < len(data) && i < offset+10; i++ {
		b := data[i]
		v |= uint64(b&0x7f) << shift
		shift += 7
		if b < 0x80 {
			return v, i - offset + 1
		}
	}
	return 0, -1
}

func consumeBytesDD(data []byte, offset int) ([]byte, int) {
	length, n := consumeVarintDD(data, offset)
	if n <= 0 {
		return nil, -1
	}
	end := offset + n + int(length)
	if end > len(data) {
		return nil, -1
	}
	return data[offset+n : end], n + int(length)
}

func skipProtoFieldDD(data []byte, offset int, wireType uint64) (int, error) {
	switch wireType {
	case 0: // varint
		for i := offset; i < len(data); i++ {
			if data[i] < 0x80 {
				return i - offset + 1, nil
			}
		}
		return 0, fmt.Errorf("unterminated varint")
	case 1: // 64-bit
		if offset+8 > len(data) {
			return 0, fmt.Errorf("truncated 64-bit field")
		}
		return 8, nil
	case 2: // len-delim
		_, n := consumeBytesDD(data, offset)
		if n <= 0 {
			return 0, fmt.Errorf("bad len-delim at %d", offset)
		}
		return n, nil
	case 5: // 32-bit
		if offset+4 > len(data) {
			return 0, fmt.Errorf("truncated 32-bit field")
		}
		return 4, nil
	default:
		return 0, fmt.Errorf("unknown wire type %d", wireType)
	}
}
