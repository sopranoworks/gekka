/*
 * streamref.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// StreamRefSerializerID is the Artery serializer identifier for StreamRef
// protocol messages.  This matches Pekko's registration in reference.conf:
//
//	pekko.actor.serialization-identifiers {
//	    "org.apache.pekko.stream.serialization.StreamRefSerializer" = 36
//	}
const StreamRefSerializerID = 36

// Single-byte manifest codes for StreamRef wire frames.
// These match Pekko's StreamRefSerializer manifest strings exactly, enabling
// cross-JVM compatibility when frames are routed over an Artery association.
const (
	ManifestSequencedOnNext       = byte('A') // SequencedOnNext{seqNr, payload}
	ManifestCumulativeDemand      = byte('B') // CumulativeDemand{seqNr}
	ManifestRemoteStreamFailure   = byte('C') // RemoteStreamFailure{cause}
	ManifestRemoteStreamCompleted = byte('D') // RemoteStreamCompleted{seqNr}
	ManifestSourceRef             = byte('E') // SourceRef{originRef}
	ManifestSinkRef               = byte('F') // SinkRef{targetRef}
	ManifestOnSubscribeHandshake  = byte('G') // OnSubscribeHandshake{targetRef}
	ManifestAck                   = byte('H') // Ack (empty)
)

// SinkRef is an actor-level message that carries the address of a [TcpListener].
// Send it via actor Tell or Ask so the remote node can connect with [TcpOut].
//
// Wire format (when sent as an Artery message): serializer ID 36, manifest "F",
// protobuf-encoded ActorRef path string.
type SinkRef struct {
	// TargetPath is the Artery actor path of the TcpListener stage actor,
	// e.g. "tcp://go@127.0.0.1:5000/stream/sink".
	TargetPath string
	Host       string
	Port       int
}

// SourceRef is an actor-level message that carries the address of a remote
// stream origin.  The receiver uses it to subscribe and receive data.
//
// Wire format: serializer ID 36, manifest "E", protobuf ActorRef path.
type SourceRef struct {
	// OriginPath is the Artery actor path of the stream origin actor.
	OriginPath string
	Host       string
	Port       int
}

// ─── Proto encoding helpers ───────────────────────────────────────────────
//
// Messages follow the protobuf3 binary format.  We hand-encode them using
// google.golang.org/protobuf/encoding/protowire to avoid a protoc dependency
// for these small message types.

// encodeCumulativeDemand encodes a CumulativeDemand message.
//
//	message CumulativeDemand { required sint64 seqNr = 1; }
//
// seqNr is a monotonically-increasing total demand counter, not a delta.
func encodeCumulativeDemand(seqNr int64) []byte {
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.VarintType)
	b = protowire.AppendVarint(b, uint64(seqNr))
	return b
}

// decodeCumulativeDemand decodes a CumulativeDemand message.
func decodeCumulativeDemand(b []byte) (int64, error) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return 0, fmt.Errorf("streamref: CumulativeDemand bad tag: %w", protowire.ParseError(n))
		}
		b = b[n:]
		if num == 1 && typ == protowire.VarintType {
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return 0, fmt.Errorf("streamref: CumulativeDemand bad varint: %w", protowire.ParseError(n))
			}
			return int64(v), nil
		}
		n = protowire.ConsumeFieldValue(num, typ, b)
		if n < 0 {
			return 0, fmt.Errorf("streamref: CumulativeDemand skip field: %w", protowire.ParseError(n))
		}
		b = b[n:]
	}
	return 0, fmt.Errorf("streamref: CumulativeDemand missing seqNr field")
}

// encodeRemoteStreamCompleted encodes a RemoteStreamCompleted message.
// Identical proto layout to CumulativeDemand — seqNr = total elements sent.
func encodeRemoteStreamCompleted(seqNr int64) []byte { return encodeCumulativeDemand(seqNr) }

// decodeRemoteStreamCompleted decodes a RemoteStreamCompleted message.
// func decodeRemoteStreamCompleted(b []byte) (int64, error) { return decodeCumulativeDemand(b) }

// encodeRemoteStreamFailure encodes a RemoteStreamFailure message.
//
//	message RemoteStreamFailure { optional bytes cause = 1; }
func encodeRemoteStreamFailure(cause string) []byte {
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendBytes(b, []byte(cause))
	return b
}

// decodeRemoteStreamFailure decodes a RemoteStreamFailure message and returns
// the cause string (empty if absent).
func decodeRemoteStreamFailure(b []byte) string {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return ""
		}
		b = b[n:]
		if num == 1 && typ == protowire.BytesType {
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return ""
			}
			return string(v)
		}
		n = protowire.ConsumeFieldValue(num, typ, b)
		if n < 0 {
			return ""
		}
		b = b[n:]
	}
	return ""
}

// encodeOnSubscribeHandshake encodes an OnSubscribeHandshake message.
//
//	message OnSubscribeHandshake { required ActorRef targetRef = 1; }
//	message ActorRef              { required string  path      = 1; }
func encodeOnSubscribeHandshake(actorPath string) []byte {
	// Inner ActorRef
	var ref []byte
	ref = protowire.AppendTag(ref, 1, protowire.BytesType)
	ref = protowire.AppendBytes(ref, []byte(actorPath))
	// Outer OnSubscribeHandshake
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendBytes(b, ref)
	return b
}

// decodeOnSubscribeHandshake returns the actor path from an
// OnSubscribeHandshake frame (empty string if the field is absent).
func decodeOnSubscribeHandshake(b []byte) (string, error) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return "", fmt.Errorf("streamref: OnSubscribeHandshake bad tag: %w", protowire.ParseError(n))
		}
		b = b[n:]
		if num == 1 && typ == protowire.BytesType {
			refBytes, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return "", fmt.Errorf("streamref: OnSubscribeHandshake bad bytes: %w", protowire.ParseError(n))
			}
			// Decode inner ActorRef
			for len(refBytes) > 0 {
				fn, ft, m := protowire.ConsumeTag(refBytes)
				if m < 0 {
					break
				}
				refBytes = refBytes[m:]
				if fn == 1 && ft == protowire.BytesType {
					path, m := protowire.ConsumeBytes(refBytes)
					if m >= 0 {
						return string(path), nil
					}
				}
				m = protowire.ConsumeFieldValue(fn, ft, refBytes)
				if m < 0 {
					break
				}
				refBytes = refBytes[m:]
			}
			return "", nil
		}
		n = protowire.ConsumeFieldValue(num, typ, b)
		if n < 0 {
			return "", fmt.Errorf("streamref: OnSubscribeHandshake skip field: %w", protowire.ParseError(n))
		}
		b = b[n:]
	}
	return "", nil
}

// encodeSequencedOnNext encodes a SequencedOnNext message.
//
//	message SequencedOnNext { required int64   seqNr   = 1;
//	                          required Payload payload = 2; }
//	message Payload         { required bytes   enclosedMessage  = 1;
//	                          required int32   serializerId     = 2;
//	                          optional bytes   messageManifest  = 3; }
func encodeSequencedOnNext(seqNr int64, msgBytes []byte, serializerID int32, msgManifest []byte) []byte {
	// Payload
	var payload []byte
	payload = protowire.AppendTag(payload, 1, protowire.BytesType)
	payload = protowire.AppendBytes(payload, msgBytes)
	payload = protowire.AppendTag(payload, 2, protowire.VarintType)
	payload = protowire.AppendVarint(payload, uint64(uint32(serializerID)))
	if len(msgManifest) > 0 {
		payload = protowire.AppendTag(payload, 3, protowire.BytesType)
		payload = protowire.AppendBytes(payload, msgManifest)
	}
	// SequencedOnNext
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.VarintType)
	b = protowire.AppendVarint(b, uint64(seqNr))
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	b = protowire.AppendBytes(b, payload)
	return b
}

// decodeSequencedOnNext decodes a SequencedOnNext message.
func decodeSequencedOnNext(b []byte) (seqNr int64, msgBytes []byte, serializerID int32, msgManifest []byte, err error) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			err = fmt.Errorf("streamref: SequencedOnNext bad tag: %w", protowire.ParseError(n))
			return
		}
		b = b[n:]
		switch {
		case num == 1 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				err = fmt.Errorf("streamref: SequencedOnNext bad seqNr: %w", protowire.ParseError(n))
				return
			}
			seqNr = int64(v)
			b = b[n:]
		case num == 2 && typ == protowire.BytesType:
			payloadBytes, n := protowire.ConsumeBytes(b)
			if n < 0 {
				err = fmt.Errorf("streamref: SequencedOnNext bad payload: %w", protowire.ParseError(n))
				return
			}
			b = b[n:]
			msgBytes, serializerID, msgManifest, err = decodePayload(payloadBytes)
			if err != nil {
				return
			}
		default:
			n = protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				err = fmt.Errorf("streamref: SequencedOnNext skip field: %w", protowire.ParseError(n))
				return
			}
			b = b[n:]
		}
	}
	return
}

func decodePayload(b []byte) (msgBytes []byte, serializerID int32, msgManifest []byte, err error) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			err = fmt.Errorf("streamref: Payload bad tag: %w", protowire.ParseError(n))
			return
		}
		b = b[n:]
		switch {
		case num == 1 && typ == protowire.BytesType:
			raw, n := protowire.ConsumeBytes(b)
			if n < 0 {
				err = fmt.Errorf("streamref: Payload bad enclosedMessage: %w", protowire.ParseError(n))
				return
			}
			msgBytes = append([]byte(nil), raw...) // defensive copy
			b = b[n:]
		case num == 2 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				err = fmt.Errorf("streamref: Payload bad serializerId: %w", protowire.ParseError(n))
				return
			}
			serializerID = int32(v)
			b = b[n:]
		case num == 3 && typ == protowire.BytesType:
			raw, n := protowire.ConsumeBytes(b)
			if n < 0 {
				err = fmt.Errorf("streamref: Payload bad messageManifest: %w", protowire.ParseError(n))
				return
			}
			msgManifest = append([]byte(nil), raw...)
			b = b[n:]
		default:
			n = protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				err = fmt.Errorf("streamref: Payload skip field: %w", protowire.ParseError(n))
				return
			}
			b = b[n:]
		}
	}
	return
}
