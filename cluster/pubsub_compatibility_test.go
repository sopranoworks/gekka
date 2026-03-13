/*
 * cluster/pubsub_compatibility_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package cluster_test verifies that the binary wire format produced by
// cluster/pubsub matches Pekko's DistributedPubSubMessageSerializer exactly.
//
// Wire format reference (from Pekko source):
//   cluster-tools/src/main/scala/org/apache/pekko/cluster/pubsub/protobuf/DistributedPubSubMessageSerializer.scala
//   cluster-tools/src/main/protobuf/DistributedPubSubMessages.proto
//
// Pekko serializer ID: 9
// Manifests: "A"=Status, "B"=Delta, "C"=Send, "D"=SendToAll, "E"=Publish, "F"=SendToOneSubscriber
// Compression: Status and Delta are GZIP-compressed; others are plain Protobuf.
//
// IMPORTANT: Pekko assigns serializer ID 9 to DistributedPubSubMessageSerializer.
// Gekka's internal JSONSerializer also occupies ID 9 (used for pure-Go CRDT gossip).
// When enabling Pekko pub-sub interop, register PubSubSerializer at ID 9 in the
// SerializationRegistry, which supersedes the internal JSON serializer for that node.
package cluster_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/sopranoworks/gekka/cluster/pubsub"
)

// ---------------------------------------------------------------------------
// Serializer ID and manifest constants
// ---------------------------------------------------------------------------

// TestPubSubSerializerID verifies the serializer ID matches Pekko's assignment.
// Source: cluster-tools/src/main/resources/reference.conf
//
//	serialization-identifiers {
//	  "org.apache.pekko.cluster.pubsub.protobuf.DistributedPubSubMessageSerializer" = 9
//	}
func TestPubSubSerializerID(t *testing.T) {
	const expectedID = int32(9)
	if pubsub.PubSubSerializerID != expectedID {
		t.Errorf("PubSubSerializerID = %d, want %d (must match Pekko's assignment)", pubsub.PubSubSerializerID, expectedID)
	}
}

// TestPubSubManifests verifies all manifest codes match Pekko's implementation.
// Source: DistributedPubSubMessageSerializer.scala
//
//	private val StatusManifest              = "A"
//	private val DeltaManifest               = "B"
//	private val SendManifest                = "C"
//	private val SendToAllManifest           = "D"
//	private val PublishManifest             = "E"
//	private val SendToOneSubscriberManifest = "F"
func TestPubSubManifests(t *testing.T) {
	tests := []struct {
		name     string
		got      string
		expected string
	}{
		{"Status", pubsub.StatusManifest, "A"},
		{"Delta", pubsub.DeltaManifest, "B"},
		{"Send", pubsub.SendManifest, "C"},
		{"SendToAll", pubsub.SendToAllManifest, "D"},
		{"Publish", pubsub.PublishManifest, "E"},
		{"SendToOneSubscriber", pubsub.SendToOneSubscriberManifest, "F"},
	}
	for _, tc := range tests {
		if tc.got != tc.expected {
			t.Errorf("%s manifest = %q, want %q", tc.name, tc.got, tc.expected)
		}
	}
}

// ---------------------------------------------------------------------------
// Publish wire format
// ---------------------------------------------------------------------------

// TestPublish_BinaryRoundTrip verifies that EncodePublish / DecodePublish are
// inverses and that the output is plain Protobuf (not GZIP-compressed).
//
// Proto schema (DistributedPubSubMessages.proto):
//
//	message Publish {
//	  required string topic   = 1;
//	  required Payload payload = 3;   // NOTE: field 3, not 2
//	}
//	message Payload {
//	  required bytes  enclosedMessage = 1;
//	  required int32  serializerId    = 2;
//	  optional bytes  messageManifest = 4;
//	}
func TestPublish_BinaryRoundTrip(t *testing.T) {
	s := &pubsub.PubSubSerializer{}
	topic := "news"
	payload := []byte("hello world")
	serializerID := int32(4) // ByteArray
	manifest := ""

	encoded, err := s.EncodePublish(topic, payload, serializerID, manifest)
	if err != nil {
		t.Fatalf("EncodePublish: %v", err)
	}

	// Must NOT be GZIP-compressed (Pekko sends Publish as plain proto).
	if isGzip(encoded) {
		t.Error("Publish message must NOT be GZIP-compressed (Pekko sends plain Protobuf for Publish)")
	}

	gotTopic, gotMsg, gotID, gotManifest, err := s.DecodePublish(encoded)
	if err != nil {
		t.Fatalf("DecodePublish: %v", err)
	}
	if gotTopic != topic {
		t.Errorf("topic = %q, want %q", gotTopic, topic)
	}
	if !bytes.Equal(gotMsg, payload) {
		t.Errorf("payload = %v, want %v", gotMsg, payload)
	}
	if gotID != serializerID {
		t.Errorf("serializerID = %d, want %d", gotID, serializerID)
	}
	if gotManifest != manifest {
		t.Errorf("manifest = %q, want %q", gotManifest, manifest)
	}
}

// TestPublish_GoldenBytes verifies the exact binary encoding against a known-correct
// byte sequence computed from the proto2 spec.
//
// Message being encoded:
//
//	Publish { topic: "test", payload: Payload { enclosedMessage: [1,2,3], serializerId: 4 } }
//
// Expected encoding (hand-computed from proto2 wire format):
//
//	Field 1 (topic, wireBytes):    0x0a 0x04 "test"
//	Field 3 (payload, wireBytes):  0x1a <len> [Payload bytes]
//	  Payload.field1 (enclosedMessage, wireBytes): 0x0a 0x03 [1 2 3]
//	  Payload.field2 (serializerId, wireVarint):   0x10 0x04
func TestPublish_GoldenBytes(t *testing.T) {
	s := &pubsub.PubSubSerializer{}
	encoded, err := s.EncodePublish("test", []byte{1, 2, 3}, 4, "")
	if err != nil {
		t.Fatalf("EncodePublish: %v", err)
	}

	// Hand-computed expected bytes:
	// Topic field (1, wireBytes=2): tag=0x0a, len=4, "test"=74 65 73 74
	// Payload field (3, wireBytes=2): tag=0x1a, len=7 (payload bytes below)
	//   Payload: enclosedMessage (1, wireBytes): 0x0a 0x03 01 02 03
	//            serializerId (2, wireVarint): 0x10 0x04
	expected := []byte{
		0x0a, 0x04, 't', 'e', 's', 't', // field 1: topic
		0x1a, 0x07, // field 3: payload, len=7
		0x0a, 0x03, 0x01, 0x02, 0x03, // payload.field1: enclosedMessage
		0x10, 0x04, // payload.field2: serializerId = 4
	}
	if !bytes.Equal(encoded, expected) {
		t.Errorf("golden bytes mismatch:\n  got:  %v\n  want: %v", encoded, expected)
	}
}

// TestPublish_WithManifest verifies that a non-empty messageManifest is encoded at
// field 4 (optional field in the Payload proto).
func TestPublish_WithManifest(t *testing.T) {
	s := &pubsub.PubSubSerializer{}
	encoded, err := s.EncodePublish("events", []byte{0xde, 0xad}, 2, "MyEvent")
	if err != nil {
		t.Fatalf("EncodePublish with manifest: %v", err)
	}

	gotTopic, gotMsg, gotID, gotManifest, err := s.DecodePublish(encoded)
	if err != nil {
		t.Fatalf("DecodePublish: %v", err)
	}
	if gotTopic != "events" {
		t.Errorf("topic = %q, want %q", gotTopic, "events")
	}
	if !bytes.Equal(gotMsg, []byte{0xde, 0xad}) {
		t.Errorf("enclosedMessage mismatch")
	}
	if gotID != 2 {
		t.Errorf("serializerId = %d, want 2", gotID)
	}
	if gotManifest != "MyEvent" {
		t.Errorf("manifest = %q, want %q", gotManifest, "MyEvent")
	}
}

// ---------------------------------------------------------------------------
// Send wire format
// ---------------------------------------------------------------------------

// TestSend_BinaryRoundTrip verifies Send encode/decode and that the result is plain Protobuf.
func TestSend_BinaryRoundTrip(t *testing.T) {
	s := &pubsub.PubSubSerializer{}
	path := "/user/worker"
	payload := []byte("task-data")
	localAffinity := true

	encoded, err := s.EncodeSend(path, localAffinity, payload, 4, "")
	if err != nil {
		t.Fatalf("EncodeSend: %v", err)
	}

	if isGzip(encoded) {
		t.Error("Send message must NOT be GZIP-compressed")
	}

	gotPath, gotAffinity, gotMsg, gotID, gotManifest, err := s.DecodeSend(encoded)
	if err != nil {
		t.Fatalf("DecodeSend: %v", err)
	}
	if gotPath != path {
		t.Errorf("path = %q, want %q", gotPath, path)
	}
	if gotAffinity != localAffinity {
		t.Errorf("localAffinity = %v, want %v", gotAffinity, localAffinity)
	}
	if !bytes.Equal(gotMsg, payload) {
		t.Errorf("payload mismatch")
	}
	if gotID != 4 {
		t.Errorf("serializerId = %d, want 4", gotID)
	}
	if gotManifest != "" {
		t.Errorf("manifest = %q, want empty", gotManifest)
	}
}

// ---------------------------------------------------------------------------
// SendToAll wire format
// ---------------------------------------------------------------------------

// TestSendToAll_BinaryRoundTrip verifies SendToAll encode/decode.
func TestSendToAll_BinaryRoundTrip(t *testing.T) {
	s := &pubsub.PubSubSerializer{}
	encoded, err := s.EncodeSendToAll("/user/handler", true, []byte("broadcast"), 4, "")
	if err != nil {
		t.Fatalf("EncodeSendToAll: %v", err)
	}

	if isGzip(encoded) {
		t.Error("SendToAll must NOT be GZIP-compressed")
	}

	path, allButSelf, msg, id, manifest, err := s.DecodeSendToAll(encoded)
	if err != nil {
		t.Fatalf("DecodeSendToAll: %v", err)
	}
	if path != "/user/handler" {
		t.Errorf("path = %q, want /user/handler", path)
	}
	if !allButSelf {
		t.Error("allButSelf should be true")
	}
	if !bytes.Equal(msg, []byte("broadcast")) {
		t.Errorf("payload mismatch")
	}
	if id != 4 {
		t.Errorf("serializerId = %d, want 4", id)
	}
	if manifest != "" {
		t.Errorf("manifest = %q, want empty", manifest)
	}
}

// ---------------------------------------------------------------------------
// SendToOneSubscriber wire format
// ---------------------------------------------------------------------------

// TestSendToOneSubscriber_BinaryRoundTrip verifies SendToOneSubscriber encode/decode.
func TestSendToOneSubscriber_BinaryRoundTrip(t *testing.T) {
	s := &pubsub.PubSubSerializer{}
	encoded, err := s.EncodeSendToOneSubscriber([]byte("exclusive"), 4, "")
	if err != nil {
		t.Fatalf("EncodeSendToOneSubscriber: %v", err)
	}

	if isGzip(encoded) {
		t.Error("SendToOneSubscriber must NOT be GZIP-compressed")
	}

	msg, id, manifest, err := s.DecodeSendToOneSubscriber(encoded)
	if err != nil {
		t.Fatalf("DecodeSendToOneSubscriber: %v", err)
	}
	if !bytes.Equal(msg, []byte("exclusive")) {
		t.Errorf("payload mismatch")
	}
	if id != 4 {
		t.Errorf("serializerId = %d, want 4", id)
	}
	if manifest != "" {
		t.Errorf("manifest = %q, want empty", manifest)
	}
}

// ---------------------------------------------------------------------------
// Status wire format (GZIP-compressed)
// ---------------------------------------------------------------------------

// TestStatus_BinaryRoundTrip verifies Status encode/decode and GZIP compression.
// Pekko source: `compress(statusToProto(m))` for Status.
func TestStatus_BinaryRoundTrip(t *testing.T) {
	s := &pubsub.PubSubSerializer{}
	st := pubsub.Status{
		Versions: map[pubsub.Address]int64{
			{System: "ShardingTest", Hostname: "127.0.0.1", Port: 2551, Protocol: "pekko"}: 42,
		},
		IsReplyToStatus: true,
	}

	encoded, err := s.EncodeStatus(st)
	if err != nil {
		t.Fatalf("EncodeStatus: %v", err)
	}

	// Status MUST be GZIP-compressed (Pekko always compresses Status and Delta).
	if !isGzip(encoded) {
		t.Error("Status message MUST be GZIP-compressed (matches Pekko's compress() call)")
	}

	got, err := s.DecodeStatus(encoded)
	if err != nil {
		t.Fatalf("DecodeStatus: %v", err)
	}
	if got.IsReplyToStatus != st.IsReplyToStatus {
		t.Errorf("IsReplyToStatus = %v, want %v", got.IsReplyToStatus, st.IsReplyToStatus)
	}
	if len(got.Versions) != 1 {
		t.Fatalf("Versions len = %d, want 1", len(got.Versions))
	}
	for addr, ts := range got.Versions {
		if addr.System != "ShardingTest" {
			t.Errorf("addr.System = %q, want ShardingTest", addr.System)
		}
		if addr.Hostname != "127.0.0.1" {
			t.Errorf("addr.Hostname = %q, want 127.0.0.1", addr.Hostname)
		}
		if addr.Port != 2551 {
			t.Errorf("addr.Port = %d, want 2551", addr.Port)
		}
		if addr.Protocol != "pekko" {
			t.Errorf("addr.Protocol = %q, want pekko", addr.Protocol)
		}
		if ts != 42 {
			t.Errorf("timestamp = %d, want 42", ts)
		}
	}
}

// TestStatus_MultipleVersions verifies that all entries in the version map survive round-trip.
func TestStatus_MultipleVersions(t *testing.T) {
	s := &pubsub.PubSubSerializer{}
	st := pubsub.Status{
		Versions: map[pubsub.Address]int64{
			{System: "sys", Hostname: "host1", Port: 2551, Protocol: "pekko"}: 3,
			{System: "sys", Hostname: "host2", Port: 2551, Protocol: "pekko"}: 17,
			{System: "sys", Hostname: "host3", Port: 2552, Protocol: "pekko"}: 5,
		},
	}

	encoded, err := s.EncodeStatus(st)
	if err != nil {
		t.Fatalf("EncodeStatus: %v", err)
	}

	got, err := s.DecodeStatus(encoded)
	if err != nil {
		t.Fatalf("DecodeStatus: %v", err)
	}
	if len(got.Versions) != 3 {
		t.Fatalf("Versions len = %d, want 3", len(got.Versions))
	}
}

// ---------------------------------------------------------------------------
// Delta wire format (GZIP-compressed)
// ---------------------------------------------------------------------------

// TestDelta_BinaryRoundTrip verifies Delta encode/decode and GZIP compression.
// Pekko source: `compress(deltaToProto(m))` for Delta.
func TestDelta_BinaryRoundTrip(t *testing.T) {
	s := &pubsub.PubSubSerializer{}
	d := pubsub.Delta{
		Buckets: []pubsub.Bucket{
			{
				Owner:   pubsub.Address{System: "sys", Hostname: "node1", Port: 2551, Protocol: "pekko"},
				Version: 3,
				Content: map[string]pubsub.ValueHolder{
					"/user/u1": {Version: 2, Ref: "pekko://sys@node1:2551/user/u1"},
					"/user/u2": {Version: 3, Ref: ""},
				},
			},
		},
	}

	encoded, err := s.EncodeDelta(d)
	if err != nil {
		t.Fatalf("EncodeDelta: %v", err)
	}

	// Delta MUST be GZIP-compressed.
	if !isGzip(encoded) {
		t.Error("Delta message MUST be GZIP-compressed (matches Pekko's compress() call)")
	}

	got, err := s.DecodeDelta(encoded)
	if err != nil {
		t.Fatalf("DecodeDelta: %v", err)
	}
	if len(got.Buckets) != 1 {
		t.Fatalf("Buckets len = %d, want 1", len(got.Buckets))
	}
	b := got.Buckets[0]
	if b.Owner.Hostname != "node1" {
		t.Errorf("Bucket.Owner.Hostname = %q, want node1", b.Owner.Hostname)
	}
	if b.Version != 3 {
		t.Errorf("Bucket.Version = %d, want 3", b.Version)
	}
	if len(b.Content) != 2 {
		t.Errorf("Bucket.Content len = %d, want 2", len(b.Content))
	}
	if vh, ok := b.Content["/user/u1"]; !ok {
		t.Error("missing /user/u1 entry")
	} else {
		if vh.Version != 2 {
			t.Errorf("/user/u1 version = %d, want 2", vh.Version)
		}
		if vh.Ref != "pekko://sys@node1:2551/user/u1" {
			t.Errorf("/user/u1 ref = %q, want actor path", vh.Ref)
		}
	}
	if vh, ok := b.Content["/user/u2"]; !ok {
		t.Error("missing /user/u2 entry")
	} else if vh.Ref != "" {
		t.Errorf("/user/u2 ref should be empty (absent subscriber), got %q", vh.Ref)
	}
}

// TestDelta_EmptyContent verifies that a bucket with no content entries encodes correctly.
func TestDelta_EmptyContent(t *testing.T) {
	s := &pubsub.PubSubSerializer{}
	d := pubsub.Delta{
		Buckets: []pubsub.Bucket{
			{
				Owner:   pubsub.Address{System: "sys", Hostname: "empty-node", Port: 2551},
				Version: 0,
				Content: map[string]pubsub.ValueHolder{},
			},
		},
	}

	encoded, err := s.EncodeDelta(d)
	if err != nil {
		t.Fatalf("EncodeDelta: %v", err)
	}

	got, err := s.DecodeDelta(encoded)
	if err != nil {
		t.Fatalf("DecodeDelta: %v", err)
	}
	if len(got.Buckets) != 1 {
		t.Fatalf("Buckets len = %d, want 1", len(got.Buckets))
	}
	if got.Buckets[0].Owner.Hostname != "empty-node" {
		t.Errorf("Owner.Hostname = %q, want empty-node", got.Buckets[0].Owner.Hostname)
	}
}

// ---------------------------------------------------------------------------
// Compression boundary: exactly which messages are GZIP'd
// ---------------------------------------------------------------------------

// TestCompressionBoundary verifies the GZIP rule that mirrors Pekko's toBinary:
//
//	Status  → compress(statusToProto(m))    ← GZIP
//	Delta   → compress(deltaToProto(m))     ← GZIP
//	Send    → sendToProto(m).toByteArray    ← plain
//	SendToAll  → sendToAllToProto(...).toByteArray ← plain
//	Publish → publishToProto(m).toByteArray ← plain
//	SendToOneSubscriber → ...toByteArray    ← plain
func TestCompressionBoundary(t *testing.T) {
	s := &pubsub.PubSubSerializer{}

	statusBytes, err := s.EncodeStatus(pubsub.Status{
		Versions:        map[pubsub.Address]int64{{System: "s", Hostname: "h", Port: 1}: 1},
		IsReplyToStatus: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !isGzip(statusBytes) {
		t.Error("Status: expected GZIP (manifest A)")
	}

	deltaBytes, err := s.EncodeDelta(pubsub.Delta{
		Buckets: []pubsub.Bucket{{
			Owner:   pubsub.Address{System: "s", Hostname: "h", Port: 1},
			Version: 1,
			Content: map[string]pubsub.ValueHolder{},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !isGzip(deltaBytes) {
		t.Error("Delta: expected GZIP (manifest B)")
	}

	sendBytes, _ := s.EncodeSend("/p", false, []byte("x"), 4, "")
	if isGzip(sendBytes) {
		t.Error("Send: must NOT be GZIP (manifest C)")
	}

	sendToAllBytes, _ := s.EncodeSendToAll("/p", false, []byte("x"), 4, "")
	if isGzip(sendToAllBytes) {
		t.Error("SendToAll: must NOT be GZIP (manifest D)")
	}

	publishBytes, _ := s.EncodePublish("t", []byte("x"), 4, "")
	if isGzip(publishBytes) {
		t.Error("Publish: must NOT be GZIP (manifest E)")
	}

	sendToOneBytes, _ := s.EncodeSendToOneSubscriber([]byte("x"), 4, "")
	if isGzip(sendToOneBytes) {
		t.Error("SendToOneSubscriber: must NOT be GZIP (manifest F)")
	}
}

// ---------------------------------------------------------------------------
// Proto field ordering: Publish uses field 3 for payload, not field 2
// ---------------------------------------------------------------------------

// TestPublish_PayloadIsFieldThree verifies that the Publish payload is encoded at
// proto field number 3 (not 2), matching the DistributedPubSubMessages.proto schema:
//
//	message Publish {
//	  required string topic   = 1;
//	  required Payload payload = 3;   // ← field 3!
//	}
func TestPublish_PayloadIsFieldThree(t *testing.T) {
	s := &pubsub.PubSubSerializer{}
	// Encode a minimal Publish message
	encoded, err := s.EncodePublish("t", []byte{0xff}, 4, "")
	if err != nil {
		t.Fatalf("EncodePublish: %v", err)
	}

	// Find the payload field tag in the encoded bytes.
	// After topic (field 1, wireBytes): 0x0a <len> "t"
	// The next tag must be for field 3, wireBytes = (3<<3)|2 = 0x1a
	if len(encoded) < 4 {
		t.Fatalf("encoded too short: %v", encoded)
	}
	// Skip topic: tag(0x0a) + len(0x01) + 't'
	topicEnd := 3
	payloadTag := encoded[topicEnd]
	const expectedPayloadTag = byte(0x1a) // (3 << 3) | 2 = 0x18 | 0x02 = 0x1a
	if payloadTag != expectedPayloadTag {
		t.Errorf("payload tag byte = 0x%02x, want 0x1a (field 3, wireBytes); "+
			"Pekko's Publish proto defines payload at field number 3, not 2", payloadTag)
	}
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

// isGzip returns true if data begins with the GZIP magic bytes (1f 8b).
func isGzip(data []byte) bool {
	if len(data) < 2 {
		return false
	}
	return data[0] == 0x1f && data[1] == 0x8b
}

// gzipDecompress is a test helper to verify GZIP-compressed content is valid.
func gzipDecompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// Ensure gzipDecompress is referenced.
var _ = gzipDecompress
