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
// protocol messages (SequencedOnNext, CumulativeDemand, etc.).
// Matches Pekko's ID 36.
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

// SinkRef is an actor-level message that carries the address of a remote sink.
// Send it via actor Tell or Ask so the remote node can connect and push data.
//
// Wire format: serializer ID 36, manifest "F", protobuf ActorRef path.
type SinkRef struct {
	// TargetPath is the Artery actor path of the SinkRef stage actor.
	TargetPath string
	Host       string
	Port       int
}

func (m *SinkRef) ArterySerializerID() int32 { return StreamRefSerializerID }
func (m *SinkRef) ArteryManifest() string     { return "F" }

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

func (m *SourceRef) ArterySerializerID() int32 { return StreamRefSerializerID }
func (m *SourceRef) ArteryManifest() string     { return "E" }

// StreamRefSerializer implements the core.Serializer interface for the
// StreamRef protocol (serializer ID 36).
type StreamRefSerializer struct{}

func (s *StreamRefSerializer) Identifier() int32 { return StreamRefSerializerID }

func (s *StreamRefSerializer) ToBinary(msg any) ([]byte, error) {
	switch m := msg.(type) {
	case *SequencedOnNext:
		return encodeSequencedOnNext(m.SeqNr, m.Payload, m.SerializerID, m.Manifest), nil
	case *CumulativeDemand:
		return encodeCumulativeDemand(m.SeqNr), nil
	case *RemoteStreamFailure:
		return encodeRemoteStreamFailure(m.Cause), nil
	case *RemoteStreamCompleted:
		return encodeRemoteStreamCompleted(m.SeqNr), nil
	case *SourceRef:
		return encodeSourceRef(m.OriginPath), nil
	case *SinkRef:
		return encodeSinkRef(m.TargetPath), nil
	case *OnSubscribeHandshake:
		return encodeOnSubscribeHandshake(m.TargetRef), nil
	default:
		return nil, fmt.Errorf("streamref: unknown message type %T", msg)
	}
}

func (s *StreamRefSerializer) FromBinary(data []byte, manifest string) (any, error) {
	if len(manifest) != 1 {
		return nil, fmt.Errorf("streamref: invalid manifest %q", manifest)
	}
	switch manifest[0] {
	case ManifestSequencedOnNext:
		seqNr, payload, sid, mani, err := decodeSequencedOnNext(data)
		if err != nil {
			return nil, err
		}
		return &SequencedOnNext{SeqNr: seqNr, Payload: payload, SerializerID: sid, Manifest: mani}, nil
	case ManifestCumulativeDemand:
		seqNr, err := decodeCumulativeDemand(data)
		if err != nil {
			return nil, err
		}
		return &CumulativeDemand{SeqNr: seqNr}, nil
	case ManifestRemoteStreamFailure:
		cause := decodeRemoteStreamFailure(data)
		return &RemoteStreamFailure{Cause: cause}, nil
	case ManifestRemoteStreamCompleted:
		seqNr, err := decodeCumulativeDemand(data) // same format
		if err != nil {
			return nil, err
		}
		return &RemoteStreamCompleted{SeqNr: seqNr}, nil
	case ManifestSourceRef:
		path, err := decodeSourceRef(data)
		if err != nil {
			return nil, err
		}
		return &SourceRef{OriginPath: path}, nil
	case ManifestSinkRef:
		path, err := decodeSinkRef(data)
		if err != nil {
			return nil, err
		}
		return &SinkRef{TargetPath: path}, nil
	case ManifestOnSubscribeHandshake:
		path, err := decodeOnSubscribeHandshake(data)
		if err != nil {
			return nil, err
		}
		return &OnSubscribeHandshake{TargetRef: path}, nil
	default:
		return nil, fmt.Errorf("streamref: unknown manifest %q", manifest)
	}
}

// ─── Protocol Message Types ───────────────────────────────────────────────

type SequencedOnNext struct {
	SeqNr        int64
	Payload      []byte
	SerializerID int32
	Manifest     []byte
}

func (m *SequencedOnNext) ArterySerializerID() int32 { return StreamRefSerializerID }
func (m *SequencedOnNext) ArteryManifest() string     { return "A" }

type CumulativeDemand struct {
	SeqNr int64
}

func (m *CumulativeDemand) ArterySerializerID() int32 { return StreamRefSerializerID }
func (m *CumulativeDemand) ArteryManifest() string     { return "B" }

type RemoteStreamFailure struct {
	Cause string
}

func (m *RemoteStreamFailure) ArterySerializerID() int32 { return StreamRefSerializerID }
func (m *RemoteStreamFailure) ArteryManifest() string     { return "C" }

type RemoteStreamCompleted struct {
	SeqNr int64
}

func (m *RemoteStreamCompleted) ArterySerializerID() int32 { return StreamRefSerializerID }
func (m *RemoteStreamCompleted) ArteryManifest() string     { return "D" }

type OnSubscribeHandshake struct {
	TargetRef string
}

func (m *OnSubscribeHandshake) ArterySerializerID() int32 { return StreamRefSerializerID }
func (m *OnSubscribeHandshake) ArteryManifest() string     { return "G" }

// ─── Proto encoding helpers ───────────────────────────────────────────────
//
// Messages follow the protobuf3 binary format.  We hand-encode them using
// google.golang.org/protobuf/encoding/protowire to avoid a protoc dependency
// for these small message types.

// encodeSourceRef encodes a SourceRef message.
//
//	message SourceRef { required ActorRef originRef = 1; }
func encodeSourceRef(path string) []byte {
	return encodeActorRefWrapper(path)
}

func decodeSourceRef(b []byte) (string, error) {
	return decodeActorRefWrapper(b)
}

// encodeSinkRef encodes a SinkRef message.
//
//	message SinkRef { required ActorRef targetRef = 1; }
func encodeSinkRef(path string) []byte {
	return encodeActorRefWrapper(path)
}

func decodeSinkRef(b []byte) (string, error) {
	return decodeActorRefWrapper(b)
}

func encodeActorRefWrapper(path string) []byte {
	// Inner ActorRef
	var ref []byte
	ref = protowire.AppendTag(ref, 1, protowire.BytesType)
	ref = protowire.AppendBytes(ref, []byte(path))
	// Outer wrapper
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendBytes(b, ref)
	return b
}

func decodeActorRefWrapper(b []byte) (string, error) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return "", fmt.Errorf("streamref: bad tag: %w", protowire.ParseError(n))
		}
		b = b[n:]
		if num == 1 && typ == protowire.BytesType {
			refBytes, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return "", fmt.Errorf("streamref: bad bytes: %w", protowire.ParseError(n))
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
			return "", fmt.Errorf("streamref: skip field: %w", protowire.ParseError(n))
		}
		b = b[n:]
	}
	return "", nil
}

// encodeCumulativeDemand encodes a CumulativeDemand message.
//
//	message CumulativeDemand { required sint64 seqNr = 1; }
//
// seqNr is a monotonically-increasing total demand counter, not a delta.
func encodeCumulativeDemand(seqNr int64) []byte {
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.VarintType)
	b = protowire.AppendVarint(b, protowire.EncodeZigZag(seqNr))
	return b
}

func decodeCumulativeDemand(b []byte) (seqNr int64, err error) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			err = fmt.Errorf("streamref: CumulativeDemand bad tag: %w", protowire.ParseError(n))
			return
		}
		b = b[n:]
		if num == 1 && typ == protowire.VarintType {
			var v uint64
			v, n = protowire.ConsumeVarint(b)
			if n < 0 {
				err = fmt.Errorf("streamref: CumulativeDemand bad varint: %w", protowire.ParseError(n))
				return
			}
			seqNr = protowire.DecodeZigZag(v)
			return
		}
		n = protowire.ConsumeFieldValue(num, typ, b)
		if n < 0 {
			err = fmt.Errorf("streamref: CumulativeDemand skip field: %w", protowire.ParseError(n))
			return
		}
		b = b[n:]
	}
	return
}

// encodeRemoteStreamFailure encodes a RemoteStreamFailure message.
//
//	message RemoteStreamFailure { required string cause = 1; }
func encodeRemoteStreamFailure(cause string) []byte {
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendBytes(b, []byte(cause))
	return b
}

func decodeRemoteStreamFailure(b []byte) (cause string) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return
		}
		b = b[n:]
		if num == 1 && typ == protowire.BytesType {
			v, n := protowire.ConsumeBytes(b)
			if n >= 0 {
				cause = string(v)
				return
			}
		}
		n = protowire.ConsumeFieldValue(num, typ, b)
		if n < 0 {
			return
		}
		b = b[n:]
	}
	return
}

// encodeRemoteStreamCompleted encodes a RemoteStreamCompleted message.
//
//	message RemoteStreamCompleted { required sint64 seqNr = 1; }
func encodeRemoteStreamCompleted(seqNr int64) []byte {
	return encodeCumulativeDemand(seqNr) // same layout
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
//
// Payload is a nested message containing the actual serialized element.
func encodeSequencedOnNext(seqNr int64, payload []byte, serID int32, manifest []byte) []byte {
	// Nested Payload message
	var p []byte
	p = protowire.AppendTag(p, 1, protowire.VarintType)
	p = protowire.AppendVarint(p, uint64(serID))
	p = protowire.AppendTag(p, 2, protowire.BytesType)
	p = protowire.AppendBytes(p, manifest)
	p = protowire.AppendTag(p, 3, protowire.BytesType)
	p = protowire.AppendBytes(p, payload)

	// Outer SequencedOnNext
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.VarintType)
	b = protowire.AppendVarint(b, uint64(seqNr))
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	b = protowire.AppendBytes(b, p)
	return b
}

func decodeSequencedOnNext(b []byte) (seqNr int64, payload []byte, serID int32, manifest []byte, err error) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			err = fmt.Errorf("streamref: SequencedOnNext bad tag: %w", protowire.ParseError(n))
			return
		}
		b = b[n:]
		switch num {
		case 1:
			var v uint64
			v, n = protowire.ConsumeVarint(b)
			if n < 0 {
				err = fmt.Errorf("streamref: SequencedOnNext bad seqNr: %w", protowire.ParseError(n))
				return
			}
			seqNr = int64(v)
			b = b[n:]
		case 2:
			var p []byte
			p, n = protowire.ConsumeBytes(b)
			if n < 0 {
				err = fmt.Errorf("streamref: SequencedOnNext bad payload: %w", protowire.ParseError(n))
				return
			}
			// Decode inner Payload
			for len(p) > 0 {
				fn, ft, m := protowire.ConsumeTag(p)
				if m < 0 {
					break
				}
				p = p[m:]
				switch fn {
				case 1:
					v, m := protowire.ConsumeVarint(p)
					if m >= 0 {
						serID = int32(v)
					}
					p = p[m:]
				case 2:
					v, m := protowire.ConsumeBytes(p)
					if m >= 0 {
						manifest = v
					}
					p = p[m:]
				case 3:
					v, m := protowire.ConsumeBytes(p)
					if m >= 0 {
						payload = v
					}
					p = p[m:]
				default:
					m = protowire.ConsumeFieldValue(fn, ft, p)
					if m < 0 {
						break
					}
					p = p[m:]
				}
			}
			b = b[n:]
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
