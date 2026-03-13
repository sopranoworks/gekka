/*
 * cluster/pubsub/serializer.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package pubsub

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"math/bits"
)

// PubSubSerializer encodes and decodes DistributedPubSub control messages using
// the same binary format as Pekko's DistributedPubSubMessageSerializer.
//
// Encoding rules (mirroring Pekko's implementation):
//   - Status  (manifest "A"): GZIP-compressed Proto2 binary
//   - Delta   (manifest "B"): GZIP-compressed Proto2 binary
//   - Send    (manifest "C"): plain Proto2 binary
//   - SendToAll (manifest "D"): plain Proto2 binary
//   - Publish (manifest "E"): plain Proto2 binary
//   - SendToOneSubscriber (manifest "F"): plain Proto2 binary
//
// Payload fields inside Send/SendToAll/Publish/SendToOneSubscriber are encoded
// as a nested Payload message: { enclosedMessage bytes, serializerId int32,
// messageManifest bytes (optional) } matching the Pekko Payload proto schema.
type PubSubSerializer struct{}

// EncodeStatus serializes a Status message to GZIP-compressed Proto2 bytes.
func (s *PubSubSerializer) EncodeStatus(st Status) ([]byte, error) {
	raw := encodeStatusProto(st)
	return gzipCompress(raw)
}

// DecodeStatus deserializes a Status message from GZIP-compressed Proto2 bytes.
func (s *PubSubSerializer) DecodeStatus(data []byte) (Status, error) {
	raw, err := gzipDecompress(data)
	if err != nil {
		return Status{}, fmt.Errorf("pubsub: DecodeStatus decompress: %w", err)
	}
	return decodeStatusProto(raw)
}

// EncodeDelta serializes a Delta message to GZIP-compressed Proto2 bytes.
func (s *PubSubSerializer) EncodeDelta(d Delta) ([]byte, error) {
	raw := encodeDeltaProto(d)
	return gzipCompress(raw)
}

// DecodeDelta deserializes a Delta message from GZIP-compressed Proto2 bytes.
func (s *PubSubSerializer) DecodeDelta(data []byte) (Delta, error) {
	raw, err := gzipDecompress(data)
	if err != nil {
		return Delta{}, fmt.Errorf("pubsub: DecodeDelta decompress: %w", err)
	}
	return decodeDeltaProto(raw)
}

// EncodePublish serializes a Publish envelope to plain Proto2 bytes.
// The enclosed message payload must already be serialized (enclosedMsg) with
// its serializer ID and manifest provided separately.
func (s *PubSubSerializer) EncodePublish(topic string, enclosedMsg []byte, serializerID int32, manifest string) ([]byte, error) {
	payload := encodePayloadProto(enclosedMsg, serializerID, manifest)
	return encodePublishProto(topic, payload), nil
}

// DecodePublish deserializes a Publish envelope from plain Proto2 bytes.
// Returns the topic, enclosed raw bytes, serializer ID, and manifest.
func (s *PubSubSerializer) DecodePublish(data []byte) (topic string, enclosedMsg []byte, serializerID int32, manifest string, err error) {
	return decodePublishProto(data)
}

// EncodeSend serializes a Send envelope to plain Proto2 bytes.
func (s *PubSubSerializer) EncodeSend(path string, localAffinity bool, enclosedMsg []byte, serializerID int32, manifest string) ([]byte, error) {
	payload := encodePayloadProto(enclosedMsg, serializerID, manifest)
	return encodeSendProto(path, localAffinity, payload), nil
}

// DecodeSend deserializes a Send envelope from plain Proto2 bytes.
func (s *PubSubSerializer) DecodeSend(data []byte) (path string, localAffinity bool, enclosedMsg []byte, serializerID int32, manifest string, err error) {
	return decodeSendProto(data)
}

// EncodeSendToAll serializes a SendToAll envelope to plain Proto2 bytes.
func (s *PubSubSerializer) EncodeSendToAll(path string, allButSelf bool, enclosedMsg []byte, serializerID int32, manifest string) ([]byte, error) {
	payload := encodePayloadProto(enclosedMsg, serializerID, manifest)
	return encodeSendToAllProto(path, allButSelf, payload), nil
}

// DecodeSendToAll deserializes a SendToAll envelope from plain Proto2 bytes.
func (s *PubSubSerializer) DecodeSendToAll(data []byte) (path string, allButSelf bool, enclosedMsg []byte, serializerID int32, manifest string, err error) {
	return decodeSendToAllProto(data)
}

// EncodeSendToOneSubscriber serializes a SendToOneSubscriber envelope to plain Proto2 bytes.
func (s *PubSubSerializer) EncodeSendToOneSubscriber(enclosedMsg []byte, serializerID int32, manifest string) ([]byte, error) {
	payload := encodePayloadProto(enclosedMsg, serializerID, manifest)
	return encodeSendToOneSubscriberProto(payload), nil
}

// DecodeSendToOneSubscriber deserializes a SendToOneSubscriber envelope from plain Proto2 bytes.
func (s *PubSubSerializer) DecodeSendToOneSubscriber(data []byte) (enclosedMsg []byte, serializerID int32, manifest string, err error) {
	return decodeSendToOneSubscriberProto(data)
}

// ---------------------------------------------------------------------------
// Proto2 wire encoding helpers
//
// Proto2 wire types:
//   0 = Varint (int32, int64, uint32, uint64, bool, enum)
//   1 = 64-bit (fixed64, sfixed64, double)
//   2 = Length-delimited (string, bytes, embedded messages, repeated fields)
//   5 = 32-bit (fixed32, sfixed32, float)
//
// Field tags: (fieldNumber << 3) | wireType
// ---------------------------------------------------------------------------

const (
	wireVarint = 0
	wireBytes  = 2
)

func appendTag(b []byte, fieldNum uint64, wireType uint64) []byte {
	return appendVarint(b, (fieldNum<<3)|wireType)
}

func appendVarint(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

func appendBytes(b []byte, data []byte) []byte {
	b = appendVarint(b, uint64(len(data)))
	return append(b, data...)
}

func appendString(b []byte, s string) []byte {
	return appendBytes(b, []byte(s))
}

func appendBool(b []byte, v bool) []byte {
	if v {
		return append(b, 1)
	}
	return append(b, 0)
}

// ---------------------------------------------------------------------------
// Address proto encoding (shared by Status and Delta)
//
// message Address {
//   required string system   = 1;
//   required string hostname = 2;
//   required uint32 port     = 3;
//   optional string protocol = 4;
// }
// ---------------------------------------------------------------------------

func encodeAddressProto(addr Address) []byte {
	var b []byte
	b = appendTag(b, 1, wireBytes)
	b = appendString(b, addr.System)
	b = appendTag(b, 2, wireBytes)
	b = appendString(b, addr.Hostname)
	b = appendTag(b, 3, wireVarint)
	b = appendVarint(b, uint64(addr.Port))
	if addr.Protocol != "" {
		b = appendTag(b, 4, wireBytes)
		b = appendString(b, addr.Protocol)
	}
	return b
}

func decodeAddressProto(data []byte) (Address, int, error) {
	var addr Address
	i := 0
	for i < len(data) {
		tag, n := consumeVarint(data[i:])
		if n == 0 {
			return addr, i, fmt.Errorf("pubsub: truncated tag at offset %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch fieldNum {
		case 1: // system (string)
			if wireType != wireBytes {
				return addr, i, fmt.Errorf("pubsub: Address.system bad wiretype %d", wireType)
			}
			v, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return addr, i, fmt.Errorf("pubsub: truncated Address.system")
			}
			addr.System = string(v)
			i += n2
		case 2: // hostname (string)
			if wireType != wireBytes {
				return addr, i, fmt.Errorf("pubsub: Address.hostname bad wiretype %d", wireType)
			}
			v, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return addr, i, fmt.Errorf("pubsub: truncated Address.hostname")
			}
			addr.Hostname = string(v)
			i += n2
		case 3: // port (uint32)
			if wireType != wireVarint {
				return addr, i, fmt.Errorf("pubsub: Address.port bad wiretype %d", wireType)
			}
			v, n2 := consumeVarint(data[i:])
			if n2 == 0 {
				return addr, i, fmt.Errorf("pubsub: truncated Address.port")
			}
			addr.Port = uint32(v)
			i += n2
		case 4: // protocol (string, optional)
			if wireType != wireBytes {
				return addr, i, fmt.Errorf("pubsub: Address.protocol bad wiretype %d", wireType)
			}
			v, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return addr, i, fmt.Errorf("pubsub: truncated Address.protocol")
			}
			addr.Protocol = string(v)
			i += n2
		default:
			// skip unknown field
			n2, err := skipField(data[i:], wireType)
			if err != nil {
				return addr, i, err
			}
			i += n2
		}
	}
	return addr, i, nil
}

// ---------------------------------------------------------------------------
// Status proto encoding
//
// message Status {
//   repeated Version versions = 1;
//   optional bool replyToStatus = 2;
//   message Version {
//     required Address address   = 1;
//     required int64 timestamp   = 2;
//   }
// }
// ---------------------------------------------------------------------------

func encodeStatusProto(st Status) []byte {
	var b []byte
	for addr, ts := range st.Versions {
		var vb []byte
		vb = appendTag(vb, 1, wireBytes)
		addrBytes := encodeAddressProto(addr)
		vb = appendBytes(vb, addrBytes)
		vb = appendTag(vb, 2, wireVarint)
		vb = appendVarint(vb, uint64(ts))

		b = appendTag(b, 1, wireBytes)
		b = appendBytes(b, vb)
	}
	if st.IsReplyToStatus {
		b = appendTag(b, 2, wireVarint)
		b = appendBool(b, true)
	}
	return b
}

func decodeStatusProto(data []byte) (Status, error) {
	st := Status{Versions: make(map[Address]int64)}
	i := 0
	for i < len(data) {
		tag, n := consumeVarint(data[i:])
		if n == 0 {
			return st, fmt.Errorf("pubsub: Status truncated tag at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch fieldNum {
		case 1: // Version sub-message
			if wireType != wireBytes {
				return st, fmt.Errorf("pubsub: Status.versions bad wiretype %d", wireType)
			}
			raw, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return st, fmt.Errorf("pubsub: Status.versions truncated")
			}
			i += n2
			addr, ts, err := decodeStatusVersionProto(raw)
			if err != nil {
				return st, err
			}
			st.Versions[addr] = ts
		case 2: // replyToStatus (bool)
			if wireType != wireVarint {
				return st, fmt.Errorf("pubsub: Status.replyToStatus bad wiretype %d", wireType)
			}
			v, n2 := consumeVarint(data[i:])
			if n2 == 0 {
				return st, fmt.Errorf("pubsub: Status.replyToStatus truncated")
			}
			i += n2
			st.IsReplyToStatus = v != 0
		default:
			n2, err := skipField(data[i:], wireType)
			if err != nil {
				return st, err
			}
			i += n2
		}
	}
	return st, nil
}

func decodeStatusVersionProto(data []byte) (Address, int64, error) {
	var addr Address
	var ts int64
	i := 0
	for i < len(data) {
		tag, n := consumeVarint(data[i:])
		if n == 0 {
			return addr, 0, fmt.Errorf("pubsub: Status.Version truncated tag")
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch fieldNum {
		case 1: // address
			if wireType != wireBytes {
				return addr, 0, fmt.Errorf("pubsub: Status.Version.address bad wiretype %d", wireType)
			}
			raw, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return addr, 0, fmt.Errorf("pubsub: Status.Version.address truncated")
			}
			i += n2
			var err error
			addr, _, err = decodeAddressProto(raw)
			if err != nil {
				return addr, 0, err
			}
		case 2: // timestamp (int64)
			if wireType != wireVarint {
				return addr, 0, fmt.Errorf("pubsub: Status.Version.timestamp bad wiretype %d", wireType)
			}
			v, n2 := consumeVarint(data[i:])
			if n2 == 0 {
				return addr, 0, fmt.Errorf("pubsub: Status.Version.timestamp truncated")
			}
			i += n2
			ts = int64(v)
		default:
			n2, err := skipField(data[i:], wireType)
			if err != nil {
				return addr, 0, err
			}
			i += n2
		}
	}
	return addr, ts, nil
}

// ---------------------------------------------------------------------------
// Delta proto encoding
//
// message Delta {
//   repeated Bucket buckets = 1;
//   message Entry {
//     required string key     = 1;
//     required int64 version  = 2;
//     optional string ref     = 3;
//   }
//   message Bucket {
//     required Address owner  = 1;
//     required int64 version  = 2;
//     repeated Entry content  = 3;
//   }
// }
// ---------------------------------------------------------------------------

func encodeDeltaProto(d Delta) []byte {
	var b []byte
	for _, bucket := range d.Buckets {
		bb := encodeBucketProto(bucket)
		b = appendTag(b, 1, wireBytes)
		b = appendBytes(b, bb)
	}
	return b
}

func encodeBucketProto(bucket Bucket) []byte {
	var b []byte
	b = appendTag(b, 1, wireBytes)
	b = appendBytes(b, encodeAddressProto(bucket.Owner))
	b = appendTag(b, 2, wireVarint)
	b = appendVarint(b, uint64(bucket.Version))
	for key, vh := range bucket.Content {
		eb := encodeEntryProto(key, vh)
		b = appendTag(b, 3, wireBytes)
		b = appendBytes(b, eb)
	}
	return b
}

func encodeEntryProto(key string, vh ValueHolder) []byte {
	var b []byte
	b = appendTag(b, 1, wireBytes)
	b = appendString(b, key)
	b = appendTag(b, 2, wireVarint)
	b = appendVarint(b, uint64(vh.Version))
	if vh.Ref != "" {
		b = appendTag(b, 3, wireBytes)
		b = appendString(b, vh.Ref)
	}
	return b
}

func decodeDeltaProto(data []byte) (Delta, error) {
	var d Delta
	i := 0
	for i < len(data) {
		tag, n := consumeVarint(data[i:])
		if n == 0 {
			return d, fmt.Errorf("pubsub: Delta truncated tag at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		if fieldNum == 1 { // buckets
			if wireType != wireBytes {
				return d, fmt.Errorf("pubsub: Delta.buckets bad wiretype %d", wireType)
			}
			raw, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return d, fmt.Errorf("pubsub: Delta.buckets truncated")
			}
			i += n2
			bucket, err := decodeBucketProto(raw)
			if err != nil {
				return d, err
			}
			d.Buckets = append(d.Buckets, bucket)
		} else {
			n2, err := skipField(data[i:], wireType)
			if err != nil {
				return d, err
			}
			i += n2
		}
	}
	return d, nil
}

func decodeBucketProto(data []byte) (Bucket, error) {
	var b Bucket
	b.Content = make(map[string]ValueHolder)
	i := 0
	for i < len(data) {
		tag, n := consumeVarint(data[i:])
		if n == 0 {
			return b, fmt.Errorf("pubsub: Bucket truncated tag")
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch fieldNum {
		case 1: // owner address
			if wireType != wireBytes {
				return b, fmt.Errorf("pubsub: Bucket.owner bad wiretype %d", wireType)
			}
			raw, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return b, fmt.Errorf("pubsub: Bucket.owner truncated")
			}
			i += n2
			var err error
			b.Owner, _, err = decodeAddressProto(raw)
			if err != nil {
				return b, err
			}
		case 2: // version
			if wireType != wireVarint {
				return b, fmt.Errorf("pubsub: Bucket.version bad wiretype %d", wireType)
			}
			v, n2 := consumeVarint(data[i:])
			if n2 == 0 {
				return b, fmt.Errorf("pubsub: Bucket.version truncated")
			}
			i += n2
			b.Version = int64(v)
		case 3: // content entries
			if wireType != wireBytes {
				return b, fmt.Errorf("pubsub: Bucket.content bad wiretype %d", wireType)
			}
			raw, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return b, fmt.Errorf("pubsub: Bucket.content truncated")
			}
			i += n2
			key, vh, err := decodeEntryProto(raw)
			if err != nil {
				return b, err
			}
			b.Content[key] = vh
		default:
			n2, err := skipField(data[i:], wireType)
			if err != nil {
				return b, err
			}
			i += n2
		}
	}
	return b, nil
}

func decodeEntryProto(data []byte) (string, ValueHolder, error) {
	var key string
	var vh ValueHolder
	i := 0
	for i < len(data) {
		tag, n := consumeVarint(data[i:])
		if n == 0 {
			return "", vh, fmt.Errorf("pubsub: Entry truncated tag")
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch fieldNum {
		case 1: // key
			if wireType != wireBytes {
				return "", vh, fmt.Errorf("pubsub: Entry.key bad wiretype %d", wireType)
			}
			raw, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return "", vh, fmt.Errorf("pubsub: Entry.key truncated")
			}
			i += n2
			key = string(raw)
		case 2: // version
			if wireType != wireVarint {
				return "", vh, fmt.Errorf("pubsub: Entry.version bad wiretype %d", wireType)
			}
			v, n2 := consumeVarint(data[i:])
			if n2 == 0 {
				return "", vh, fmt.Errorf("pubsub: Entry.version truncated")
			}
			i += n2
			vh.Version = int64(v)
		case 3: // ref (optional)
			if wireType != wireBytes {
				return "", vh, fmt.Errorf("pubsub: Entry.ref bad wiretype %d", wireType)
			}
			raw, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return "", vh, fmt.Errorf("pubsub: Entry.ref truncated")
			}
			i += n2
			vh.Ref = string(raw)
		default:
			n2, err := skipField(data[i:], wireType)
			if err != nil {
				return "", vh, err
			}
			i += n2
		}
	}
	return key, vh, nil
}

// ---------------------------------------------------------------------------
// Payload proto encoding (nested inside Send/SendToAll/Publish/SendToOneSubscriber)
//
// message Payload {
//   required bytes enclosedMessage = 1;
//   required int32 serializerId    = 2;
//   optional bytes messageManifest = 4;
// }
// ---------------------------------------------------------------------------

func encodePayloadProto(msg []byte, serializerID int32, manifest string) []byte {
	var b []byte
	b = appendTag(b, 1, wireBytes)
	b = appendBytes(b, msg)
	b = appendTag(b, 2, wireVarint)
	b = appendVarint(b, uint64(serializerID))
	if manifest != "" {
		b = appendTag(b, 4, wireBytes)
		b = appendBytes(b, []byte(manifest))
	}
	return b
}

func decodePayloadProto(data []byte) (enclosedMsg []byte, serializerID int32, manifest string, err error) {
	i := 0
	for i < len(data) {
		tag, n := consumeVarint(data[i:])
		if n == 0 {
			return nil, 0, "", fmt.Errorf("pubsub: Payload truncated tag at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch fieldNum {
		case 1: // enclosedMessage
			if wireType != wireBytes {
				return nil, 0, "", fmt.Errorf("pubsub: Payload.enclosedMessage bad wiretype %d", wireType)
			}
			v, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return nil, 0, "", fmt.Errorf("pubsub: Payload.enclosedMessage truncated")
			}
			i += n2
			enclosedMsg = v
		case 2: // serializerId
			if wireType != wireVarint {
				return nil, 0, "", fmt.Errorf("pubsub: Payload.serializerId bad wiretype %d", wireType)
			}
			v, n2 := consumeVarint(data[i:])
			if n2 == 0 {
				return nil, 0, "", fmt.Errorf("pubsub: Payload.serializerId truncated")
			}
			i += n2
			serializerID = int32(v)
		case 4: // messageManifest (optional)
			if wireType != wireBytes {
				return nil, 0, "", fmt.Errorf("pubsub: Payload.messageManifest bad wiretype %d", wireType)
			}
			v, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return nil, 0, "", fmt.Errorf("pubsub: Payload.messageManifest truncated")
			}
			i += n2
			manifest = string(v)
		default:
			n2, err2 := skipField(data[i:], wireType)
			if err2 != nil {
				return nil, 0, "", err2
			}
			i += n2
		}
	}
	return enclosedMsg, serializerID, manifest, nil
}

// ---------------------------------------------------------------------------
// Publish proto encoding
//
// message Publish {
//   required string topic   = 1;
//   required Payload payload = 3;   // NOTE: field number 3, not 2
// }
// ---------------------------------------------------------------------------

func encodePublishProto(topic string, payload []byte) []byte {
	var b []byte
	b = appendTag(b, 1, wireBytes)
	b = appendString(b, topic)
	b = appendTag(b, 3, wireBytes) // field 3, matches Pekko proto
	b = appendBytes(b, payload)
	return b
}

func decodePublishProto(data []byte) (topic string, enclosedMsg []byte, serializerID int32, manifest string, err error) {
	i := 0
	for i < len(data) {
		tag, n := consumeVarint(data[i:])
		if n == 0 {
			return "", nil, 0, "", fmt.Errorf("pubsub: Publish truncated tag at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch fieldNum {
		case 1: // topic
			if wireType != wireBytes {
				return "", nil, 0, "", fmt.Errorf("pubsub: Publish.topic bad wiretype %d", wireType)
			}
			v, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return "", nil, 0, "", fmt.Errorf("pubsub: Publish.topic truncated")
			}
			i += n2
			topic = string(v)
		case 3: // payload (field 3 in Pekko's proto)
			if wireType != wireBytes {
				return "", nil, 0, "", fmt.Errorf("pubsub: Publish.payload bad wiretype %d", wireType)
			}
			raw, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return "", nil, 0, "", fmt.Errorf("pubsub: Publish.payload truncated")
			}
			i += n2
			enclosedMsg, serializerID, manifest, err = decodePayloadProto(raw)
			if err != nil {
				return "", nil, 0, "", err
			}
		default:
			n2, err2 := skipField(data[i:], wireType)
			if err2 != nil {
				return "", nil, 0, "", err2
			}
			i += n2
		}
	}
	return topic, enclosedMsg, serializerID, manifest, nil
}

// ---------------------------------------------------------------------------
// Send proto encoding
//
// message Send {
//   required string path           = 1;
//   required bool localAffinity    = 2;
//   required Payload payload       = 3;
// }
// ---------------------------------------------------------------------------

func encodeSendProto(path string, localAffinity bool, payload []byte) []byte {
	var b []byte
	b = appendTag(b, 1, wireBytes)
	b = appendString(b, path)
	b = appendTag(b, 2, wireVarint)
	b = appendBool(b, localAffinity)
	b = appendTag(b, 3, wireBytes)
	b = appendBytes(b, payload)
	return b
}

func decodeSendProto(data []byte) (path string, localAffinity bool, enclosedMsg []byte, serializerID int32, manifest string, err error) {
	i := 0
	for i < len(data) {
		tag, n := consumeVarint(data[i:])
		if n == 0 {
			return "", false, nil, 0, "", fmt.Errorf("pubsub: Send truncated tag at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch fieldNum {
		case 1: // path
			if wireType != wireBytes {
				return "", false, nil, 0, "", fmt.Errorf("pubsub: Send.path bad wiretype %d", wireType)
			}
			v, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return "", false, nil, 0, "", fmt.Errorf("pubsub: Send.path truncated")
			}
			i += n2
			path = string(v)
		case 2: // localAffinity
			if wireType != wireVarint {
				return "", false, nil, 0, "", fmt.Errorf("pubsub: Send.localAffinity bad wiretype %d", wireType)
			}
			v, n2 := consumeVarint(data[i:])
			if n2 == 0 {
				return "", false, nil, 0, "", fmt.Errorf("pubsub: Send.localAffinity truncated")
			}
			i += n2
			localAffinity = v != 0
		case 3: // payload
			if wireType != wireBytes {
				return "", false, nil, 0, "", fmt.Errorf("pubsub: Send.payload bad wiretype %d", wireType)
			}
			raw, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return "", false, nil, 0, "", fmt.Errorf("pubsub: Send.payload truncated")
			}
			i += n2
			enclosedMsg, serializerID, manifest, err = decodePayloadProto(raw)
			if err != nil {
				return "", false, nil, 0, "", err
			}
		default:
			n2, err2 := skipField(data[i:], wireType)
			if err2 != nil {
				return "", false, nil, 0, "", err2
			}
			i += n2
		}
	}
	return path, localAffinity, enclosedMsg, serializerID, manifest, nil
}

// ---------------------------------------------------------------------------
// SendToAll proto encoding
//
// message SendToAll {
//   required string path        = 1;
//   required bool allButSelf    = 2;
//   required Payload payload    = 3;
// }
// ---------------------------------------------------------------------------

func encodeSendToAllProto(path string, allButSelf bool, payload []byte) []byte {
	var b []byte
	b = appendTag(b, 1, wireBytes)
	b = appendString(b, path)
	b = appendTag(b, 2, wireVarint)
	b = appendBool(b, allButSelf)
	b = appendTag(b, 3, wireBytes)
	b = appendBytes(b, payload)
	return b
}

func decodeSendToAllProto(data []byte) (path string, allButSelf bool, enclosedMsg []byte, serializerID int32, manifest string, err error) {
	i := 0
	for i < len(data) {
		tag, n := consumeVarint(data[i:])
		if n == 0 {
			return "", false, nil, 0, "", fmt.Errorf("pubsub: SendToAll truncated tag at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch fieldNum {
		case 1: // path
			if wireType != wireBytes {
				return "", false, nil, 0, "", fmt.Errorf("pubsub: SendToAll.path bad wiretype %d", wireType)
			}
			v, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return "", false, nil, 0, "", fmt.Errorf("pubsub: SendToAll.path truncated")
			}
			i += n2
			path = string(v)
		case 2: // allButSelf
			if wireType != wireVarint {
				return "", false, nil, 0, "", fmt.Errorf("pubsub: SendToAll.allButSelf bad wiretype %d", wireType)
			}
			v, n2 := consumeVarint(data[i:])
			if n2 == 0 {
				return "", false, nil, 0, "", fmt.Errorf("pubsub: SendToAll.allButSelf truncated")
			}
			i += n2
			allButSelf = v != 0
		case 3: // payload
			if wireType != wireBytes {
				return "", false, nil, 0, "", fmt.Errorf("pubsub: SendToAll.payload bad wiretype %d", wireType)
			}
			raw, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return "", false, nil, 0, "", fmt.Errorf("pubsub: SendToAll.payload truncated")
			}
			i += n2
			enclosedMsg, serializerID, manifest, err = decodePayloadProto(raw)
			if err != nil {
				return "", false, nil, 0, "", err
			}
		default:
			n2, err2 := skipField(data[i:], wireType)
			if err2 != nil {
				return "", false, nil, 0, "", err2
			}
			i += n2
		}
	}
	return path, allButSelf, enclosedMsg, serializerID, manifest, nil
}

// ---------------------------------------------------------------------------
// SendToOneSubscriber proto encoding
//
// message SendToOneSubscriber {
//   required Payload payload = 1;
// }
// ---------------------------------------------------------------------------

func encodeSendToOneSubscriberProto(payload []byte) []byte {
	var b []byte
	b = appendTag(b, 1, wireBytes)
	b = appendBytes(b, payload)
	return b
}

func decodeSendToOneSubscriberProto(data []byte) (enclosedMsg []byte, serializerID int32, manifest string, err error) {
	i := 0
	for i < len(data) {
		tag, n := consumeVarint(data[i:])
		if n == 0 {
			return nil, 0, "", fmt.Errorf("pubsub: SendToOneSubscriber truncated tag at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		if fieldNum == 1 { // payload
			if wireType != wireBytes {
				return nil, 0, "", fmt.Errorf("pubsub: SendToOneSubscriber.payload bad wiretype %d", wireType)
			}
			raw, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return nil, 0, "", fmt.Errorf("pubsub: SendToOneSubscriber.payload truncated")
			}
			i += n2
			enclosedMsg, serializerID, manifest, err = decodePayloadProto(raw)
			if err != nil {
				return nil, 0, "", err
			}
		} else {
			n2, err2 := skipField(data[i:], wireType)
			if err2 != nil {
				return nil, 0, "", err2
			}
			i += n2
		}
	}
	return enclosedMsg, serializerID, manifest, nil
}

// ---------------------------------------------------------------------------
// Varint / bytes consumer helpers (no external dependency)
// ---------------------------------------------------------------------------

// consumeVarint reads a varint from data and returns (value, bytesConsumed).
// Returns (0, 0) on truncation.
func consumeVarint(data []byte) (uint64, int) {
	var x uint64
	var s uint
	for i, b := range data {
		if i == 10 {
			return 0, 0 // overflow
		}
		if b < 0x80 {
			if i == 9 && b > 1 {
				return 0, 0 // overflow
			}
			return x | uint64(b)<<s, i + 1
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, 0 // truncated
}

// consumeBytes reads a length-delimited field from data.
// Returns (value, bytesConsumed). Returns (nil, 0) on truncation.
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

// skipField skips one field of the given wire type. Returns bytes consumed or error.
func skipField(data []byte, wireType uint64) (int, error) {
	switch wireType {
	case wireVarint: // varint
		_, n := consumeVarint(data)
		if n == 0 {
			return 0, fmt.Errorf("pubsub: skipField varint truncated")
		}
		return n, nil
	case 1: // 64-bit
		if len(data) < 8 {
			return 0, fmt.Errorf("pubsub: skipField 64-bit truncated")
		}
		return 8, nil
	case wireBytes: // length-delimited
		_, n := consumeBytes(data)
		if n == 0 {
			return 0, fmt.Errorf("pubsub: skipField bytes truncated")
		}
		return n, nil
	case 5: // 32-bit
		if len(data) < 4 {
			return 0, fmt.Errorf("pubsub: skipField 32-bit truncated")
		}
		return 4, nil
	default:
		return 0, fmt.Errorf("pubsub: skipField unknown wiretype %d", wireType)
	}
}

// ---------------------------------------------------------------------------
// GZIP helpers
// ---------------------------------------------------------------------------

func gzipCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, fmt.Errorf("pubsub: gzip write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("pubsub: gzip close: %w", err)
	}
	return buf.Bytes(), nil
}

func gzipDecompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("pubsub: gzip reader: %w", err)
	}
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("pubsub: gzip read: %w", err)
	}
	return out, nil
}

// varIntLen returns the number of bytes needed to encode v as a varint.
func varIntLen(v uint64) int {
	return (bits.Len64(v|1) + 6) / 7
}

// ensure varIntLen is referenced to avoid unused-function lint warnings.
var _ = varIntLen
