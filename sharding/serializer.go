/*
 * sharding/serializer.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"fmt"
)

// ShardingSerializerID is the Artery serializer ID assigned by Pekko to
// ClusterShardingMessageSerializer.
//
// Source: cluster-sharding/src/main/resources/reference.conf
//
//	serialization-identifiers {
//	  "org.apache.pekko.cluster.sharding.protobuf.ClusterShardingMessageSerializer" = 13
//	}
const ShardingSerializerID = int32(13)

// Manifest codes from ClusterShardingMessageSerializer.scala.
// All messages are plain Protobuf (no GZIP) except CoordinatorState ("AA").
const (
	// Coordinator internal event manifests
	CoordinatorStateManifest    = "AA"
	ShardHomeAllocatedManifest  = "AF"
	ShardHomeDeallocatedManifest = "AG"

	// Coordinator protocol message manifests
	RegisterManifest        = "BA"
	RegisterAckManifest     = "BC"
	GetShardHomeManifest    = "BD"
	ShardHomeManifest       = "BE"
	HostShardManifest       = "BF"
	ShardStartedManifest    = "BG"
	BeginHandOffManifest    = "BH"
	BeginHandOffAckManifest = "BI"
	HandOffManifest         = "BJ"
	ShardStoppedManifest    = "BK"

	// Entity lifecycle manifests
	StartEntityManifest    = "EA"
	StartEntityAckManifest = "EB"
)

// ShardingSerializer encodes and decodes ClusterSharding protocol messages using
// the same binary wire format as Pekko's ClusterShardingMessageSerializer (ID 13).
//
// Proto schemas (from ClusterShardingMessages.proto):
//
//	StartEntity       { required string entityId = 1 }
//	StartEntityAck    { required string entityId = 1; required string shardId = 2 }
//	ShardIdMessage    { required string shard = 1 }   ← GetShardHome, BeginHandOff, etc.
//	ShardHomeAllocated{ required string shard = 1; required string region = 2 }
//	ShardHome         { required string shard = 1; required string region = 2 }
//	ActorRefMessage   { required string ref = 1 }     ← Register, RegisterAck, etc.
type ShardingSerializer struct{}

// ---------------------------------------------------------------------------
// StartEntity (manifest "EA")
//
// Proto: message StartEntity { required string entityId = 1; }
// ---------------------------------------------------------------------------

// EncodeStartEntity serializes a StartEntity message to plain Protobuf bytes.
func (s *ShardingSerializer) EncodeStartEntity(entityId string) []byte {
	return encodeStringField1(entityId)
}

// DecodeStartEntity deserializes a StartEntity message from plain Protobuf bytes.
func (s *ShardingSerializer) DecodeStartEntity(data []byte) (entityId string, err error) {
	return decodeFirstStringField(data, "StartEntity")
}

// ---------------------------------------------------------------------------
// StartEntityAck (manifest "EB")
//
// Proto: message StartEntityAck { required string entityId = 1; required string shardId = 2; }
// ---------------------------------------------------------------------------

// EncodeStartEntityAck serializes a StartEntityAck message to plain Protobuf bytes.
func (s *ShardingSerializer) EncodeStartEntityAck(entityId, shardId string) []byte {
	return encodeTwoStringFields(entityId, shardId)
}

// DecodeStartEntityAck deserializes a StartEntityAck message from plain Protobuf bytes.
func (s *ShardingSerializer) DecodeStartEntityAck(data []byte) (entityId, shardId string, err error) {
	return decodeTwoStringFields(data, "StartEntityAck")
}

// ---------------------------------------------------------------------------
// GetShardHome (manifest "BD") — uses ShardIdMessage proto
//
// Proto: message ShardIdMessage { required string shard = 1; }
// ---------------------------------------------------------------------------

// EncodeGetShardHome serializes a GetShardHome request to plain Protobuf bytes.
func (s *ShardingSerializer) EncodeGetShardHome(shardId string) []byte {
	return encodeStringField1(shardId)
}

// DecodeGetShardHome deserializes a GetShardHome request from plain Protobuf bytes.
func (s *ShardingSerializer) DecodeGetShardHome(data []byte) (shardId string, err error) {
	return decodeFirstStringField(data, "GetShardHome/ShardIdMessage")
}

// ---------------------------------------------------------------------------
// ShardHomeAllocated (manifest "AF")
//
// Proto: message ShardHomeAllocated { required string shard = 1; required string region = 2; }
//
// The region field holds the serialized actor path produced by
// Serialization.serializedActorPath(ref), which includes a UID suffix
// (e.g., "pekko://System@host:port/user/region#uid").
// ---------------------------------------------------------------------------

// EncodeShardHomeAllocated serializes a ShardHomeAllocated event to plain Protobuf bytes.
func (s *ShardingSerializer) EncodeShardHomeAllocated(shardId, regionPath string) []byte {
	return encodeTwoStringFields(shardId, regionPath)
}

// DecodeShardHomeAllocated deserializes a ShardHomeAllocated event from plain Protobuf bytes.
func (s *ShardingSerializer) DecodeShardHomeAllocated(data []byte) (shardId, regionPath string, err error) {
	return decodeTwoStringFields(data, "ShardHomeAllocated")
}

// ---------------------------------------------------------------------------
// BeginHandOff (manifest "BH") — uses ShardIdMessage proto
//
// Proto: message ShardIdMessage { required string shard = 1; }
// ---------------------------------------------------------------------------

// EncodeBeginHandOff serializes a BeginHandOff message to plain Protobuf bytes.
func (s *ShardingSerializer) EncodeBeginHandOff(shardId string) []byte {
	return encodeStringField1(shardId)
}

// DecodeBeginHandOff deserializes a BeginHandOff message from plain Protobuf bytes.
func (s *ShardingSerializer) DecodeBeginHandOff(data []byte) (shardId string, err error) {
	return decodeFirstStringField(data, "BeginHandOff/ShardIdMessage")
}

// ---------------------------------------------------------------------------
// ShardHome (manifest "BE")
//
// Proto: message ShardHome { required string shard = 1; required string region = 2; }
// Identical proto schema to ShardHomeAllocated.
// ---------------------------------------------------------------------------

// EncodeShardHome serializes a ShardHome response to plain Protobuf bytes.
func (s *ShardingSerializer) EncodeShardHome(shardId, regionPath string) []byte {
	return encodeTwoStringFields(shardId, regionPath)
}

// DecodeShardHome deserializes a ShardHome response from plain Protobuf bytes.
func (s *ShardingSerializer) DecodeShardHome(data []byte) (shardId, regionPath string, err error) {
	return decodeTwoStringFields(data, "ShardHome")
}

// ---------------------------------------------------------------------------
// ActorRefMessage — shared proto for Register, RegisterAck, etc.
//
// Proto: message ActorRefMessage { required string ref = 1; }
// ---------------------------------------------------------------------------

// EncodeActorRefMessage encodes a single actor path string (used by Register, RegisterAck, etc.).
func (s *ShardingSerializer) EncodeActorRefMessage(actorPath string) []byte {
	return encodeStringField1(actorPath)
}

// DecodeActorRefMessage decodes a single actor path string.
func (s *ShardingSerializer) DecodeActorRefMessage(data []byte) (actorPath string, err error) {
	return decodeFirstStringField(data, "ActorRefMessage")
}

// ---------------------------------------------------------------------------
// Proto2 wire encoding helpers
//
// Wire types:
//   0 = Varint
//   2 = Length-delimited (string, bytes, embedded messages)
//
// Field tag = (fieldNumber << 3) | wireType
//   field 1, string: (1<<3)|2 = 0x0a
//   field 2, string: (2<<3)|2 = 0x12
// ---------------------------------------------------------------------------

// encodeStringField1 encodes { required string field = 1 } — the most common pattern.
func encodeStringField1(s string) []byte {
	b := make([]byte, 0, 2+len(s))
	b = appendTag(b, 1, 2)
	b = appendLenDelim(b, []byte(s))
	return b
}

// encodeTwoStringFields encodes { required string f1 = 1; required string f2 = 2 }.
func encodeTwoStringFields(f1, f2 string) []byte {
	b := make([]byte, 0, 4+len(f1)+len(f2))
	b = appendTag(b, 1, 2)
	b = appendLenDelim(b, []byte(f1))
	b = appendTag(b, 2, 2)
	b = appendLenDelim(b, []byte(f2))
	return b
}

// decodeFirstStringField parses a message with a single string at field 1.
func decodeFirstStringField(data []byte, msgName string) (string, error) {
	var result string
	i := 0
	for i < len(data) {
		tag, n := consumeVarint(data[i:])
		if n == 0 {
			return "", fmt.Errorf("sharding: %s truncated tag at %d", msgName, i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		if fieldNum == 1 && wireType == 2 {
			v, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return "", fmt.Errorf("sharding: %s field 1 truncated", msgName)
			}
			i += n2
			result = string(v)
		} else {
			n2, err := skipProtoField(data[i:], wireType)
			if err != nil {
				return "", fmt.Errorf("sharding: %s: %w", msgName, err)
			}
			i += n2
		}
	}
	return result, nil
}

// decodeTwoStringFields parses a message with string fields at positions 1 and 2.
func decodeTwoStringFields(data []byte, msgName string) (f1, f2 string, err error) {
	i := 0
	for i < len(data) {
		tag, n := consumeVarint(data[i:])
		if n == 0 {
			return "", "", fmt.Errorf("sharding: %s truncated tag at %d", msgName, i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch {
		case fieldNum == 1 && wireType == 2:
			v, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return "", "", fmt.Errorf("sharding: %s field 1 truncated", msgName)
			}
			i += n2
			f1 = string(v)
		case fieldNum == 2 && wireType == 2:
			v, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return "", "", fmt.Errorf("sharding: %s field 2 truncated", msgName)
			}
			i += n2
			f2 = string(v)
		default:
			n2, err2 := skipProtoField(data[i:], wireType)
			if err2 != nil {
				return "", "", fmt.Errorf("sharding: %s: %w", msgName, err2)
			}
			i += n2
		}
	}
	return f1, f2, nil
}

// appendTag appends a proto field tag (fieldNum<<3)|wireType as a varint.
func appendTag(b []byte, fieldNum, wireType uint64) []byte {
	return appendVarint(b, (fieldNum<<3)|wireType)
}

// appendLenDelim appends length-prefixed bytes.
func appendLenDelim(b []byte, data []byte) []byte {
	b = appendVarint(b, uint64(len(data)))
	return append(b, data...)
}

// appendVarint appends a uint64 as a varint.
func appendVarint(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

// consumeVarint reads a varint from data; returns (value, bytesConsumed).
// Returns (0, 0) on truncation.
func consumeVarint(data []byte) (uint64, int) {
	var x uint64
	var s uint
	for i, b := range data {
		if i == 10 {
			return 0, 0
		}
		if b < 0x80 {
			return x | uint64(b)<<s, i + 1
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, 0
}

// consumeBytes reads a length-prefixed byte slice; returns (value, bytesConsumed).
// Returns (nil, 0) on truncation.
func consumeBytes(data []byte) ([]byte, int) {
	length, n := consumeVarint(data)
	if n == 0 {
		return nil, 0
	}
	end := n + int(length)
	if end > len(data) {
		return nil, 0
	}
	return data[n:end], end
}

// skipProtoField skips one field of the given wire type.
func skipProtoField(data []byte, wireType uint64) (int, error) {
	switch wireType {
	case 0: // varint
		_, n := consumeVarint(data)
		if n == 0 {
			return 0, fmt.Errorf("truncated varint field")
		}
		return n, nil
	case 1: // 64-bit
		if len(data) < 8 {
			return 0, fmt.Errorf("truncated 64-bit field")
		}
		return 8, nil
	case 2: // length-delimited
		_, n := consumeBytes(data)
		if n == 0 {
			return 0, fmt.Errorf("truncated length-delimited field")
		}
		return n, nil
	case 5: // 32-bit
		if len(data) < 4 {
			return 0, fmt.Errorf("truncated 32-bit field")
		}
		return 4, nil
	default:
		return 0, fmt.Errorf("unknown wire type %d", wireType)
	}
}
