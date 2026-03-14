/*
 * remoting_compatibility_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package actor_test provides black-box tests for Pekko binary interoperability.
// These tests verify that gekka can correctly encode and decode the binary
// wire formats used by Apache Pekko / Akka remoting, without relying on a
// live Scala server.
package actor_test

import (
	"reflect"
	"testing"

	"github.com/sopranoworks/gekka"
	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// ── Serializer ID constants (mirrored from the gekka package for clarity) ─────

const (
	serializerIDProtobuf = int32(2)  // ProtobufSerializer — proto.Message types
	serializerIDRawBytes = int32(4)  // ByteArraySerializer — raw []byte, empty manifest
	serializerIDCluster  = int32(5)  // ClusterMessageSerializer — short manifest codes
	serializerIDJSON     = int32(9)  // JSONSerializer — Jackson-compatible JSON
	serializerIDArtery   = int32(17) // ArteryMessageSerializer — Artery control frames
)

// ── System Message binary round-trip ─────────────────────────────────────────

// TestSystemMessage_Watch_BinaryRoundTrip verifies that a Watch system message
// can be encoded to the Pekko wire format and decoded back without data loss.
//
// On the Pekko side the payload of a SystemMessageEnvelope.message field is
// a serialized SystemMessage proto with Type=WATCH and a populated WatchData.
func TestSystemMessage_Watch_BinaryRoundTrip(t *testing.T) {
	watchee := &gproto_remote.ProtoActorRef{
		Path: proto.String("pekko://MySystem@10.0.0.2:2552/user/worker"),
	}
	watcher := &gproto_remote.ProtoActorRef{
		Path: proto.String("pekko://MySystem@10.0.0.1:2552/user/supervisor"),
	}

	original := &gproto_remote.SystemMessage{
		Type: gproto_remote.SystemMessage_WATCH.Enum(),
		WatchData: &gproto_remote.WatchData{
			Watchee: watchee,
			Watcher: watcher,
		},
	}

	// Encode — same path as gekka's artery handler when forwarding to Pekko.
	wire, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	if len(wire) == 0 {
		t.Fatal("marshaled SystemMessage is empty")
	}

	// Decode — same path as Pekko's inbound handler.
	decoded := &gproto_remote.SystemMessage{}
	if err := proto.Unmarshal(wire, decoded); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}

	if decoded.GetType() != gproto_remote.SystemMessage_WATCH {
		t.Errorf("type mismatch: got %v, want WATCH", decoded.GetType())
	}
	if decoded.GetWatchData() == nil {
		t.Fatal("WatchData is nil after decode")
	}
	if got := decoded.GetWatchData().GetWatchee().GetPath(); got != watchee.GetPath() {
		t.Errorf("watchee path mismatch: got %q, want %q", got, watchee.GetPath())
	}
	if got := decoded.GetWatchData().GetWatcher().GetPath(); got != watcher.GetPath() {
		t.Errorf("watcher path mismatch: got %q, want %q", got, watcher.GetPath())
	}
}

// TestSystemMessage_Terminated_BinaryRoundTrip verifies that a
// DeathWatchNotification (Terminated) system message encodes and decodes
// correctly. Pekko sends this when a watched actor stops.
func TestSystemMessage_Terminated_BinaryRoundTrip(t *testing.T) {
	actor := &gproto_remote.ProtoActorRef{
		Path: proto.String("pekko://MySystem@10.0.0.2:2552/user/worker"),
	}

	original := &gproto_remote.SystemMessage{
		Type: gproto_remote.SystemMessage_DEATHWATCH_NOTIFICATION.Enum(),
		DwNotificationData: &gproto_remote.DeathWatchNotificationData{
			Actor:              actor,
			ExistenceConfirmed: proto.Bool(true),
			AddressTerminated:  proto.Bool(false),
		},
	}

	wire, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}

	decoded := &gproto_remote.SystemMessage{}
	if err := proto.Unmarshal(wire, decoded); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}

	if decoded.GetType() != gproto_remote.SystemMessage_DEATHWATCH_NOTIFICATION {
		t.Errorf("type mismatch: got %v, want DEATHWATCH_NOTIFICATION", decoded.GetType())
	}
	dw := decoded.GetDwNotificationData()
	if dw == nil {
		t.Fatal("DwNotificationData is nil after decode")
	}
	if got := dw.GetActor().GetPath(); got != actor.GetPath() {
		t.Errorf("actor path mismatch: got %q, want %q", got, actor.GetPath())
	}
	if !dw.GetExistenceConfirmed() {
		t.Error("ExistenceConfirmed should be true")
	}
	if dw.GetAddressTerminated() {
		t.Error("AddressTerminated should be false")
	}
}

// TestSystemMessage_Envelope_BinaryRoundTrip verifies the SystemMessageEnvelope
// outer wrapper that Pekko wraps around every SystemMessage on the wire.
func TestSystemMessage_Envelope_BinaryRoundTrip(t *testing.T) {
	inner := &gproto_remote.SystemMessage{
		Type: gproto_remote.SystemMessage_TERMINATE.Enum(),
	}
	innerBytes, err := proto.Marshal(inner)
	if err != nil {
		t.Fatalf("marshal inner: %v", err)
	}

	envelope := &gproto_remote.SystemMessageEnvelope{
		Message:      innerBytes,
		SerializerId: proto.Int32(2), // ProtobufSerializer — required field
		SeqNo:        proto.Uint64(42),
		AckReplyTo: &gproto_remote.UniqueAddress{
			Address: &gproto_remote.Address{
				Protocol: proto.String("pekko"),
				System:   proto.String("MySystem"),
				Hostname: proto.String("10.0.0.1"),
				Port:     proto.Uint32(2552),
			},
			Uid: proto.Uint64(12345),
		},
	}

	wire, err := proto.Marshal(envelope)
	if err != nil {
		t.Fatalf("proto.Marshal envelope: %v", err)
	}

	decoded := &gproto_remote.SystemMessageEnvelope{}
	if err := proto.Unmarshal(wire, decoded); err != nil {
		t.Fatalf("proto.Unmarshal envelope: %v", err)
	}

	if decoded.GetSeqNo() != 42 {
		t.Errorf("SeqNo mismatch: got %d, want 42", decoded.GetSeqNo())
	}
	if decoded.GetAckReplyTo().GetUid() != 12345 {
		t.Errorf("UID mismatch: got %d, want 12345", decoded.GetAckReplyTo().GetUid())
	}
	if len(decoded.GetMessage()) == 0 {
		t.Error("inner Message bytes are empty after decode")
	}

	// Decode the inner message as well.
	decodedInner := &gproto_remote.SystemMessage{}
	if err := proto.Unmarshal(decoded.GetMessage(), decodedInner); err != nil {
		t.Fatalf("unmarshal inner from envelope: %v", err)
	}
	if decodedInner.GetType() != gproto_remote.SystemMessage_TERMINATE {
		t.Errorf("inner type mismatch: got %v, want TERMINATE", decodedInner.GetType())
	}
}

// ── Manifest mapping ──────────────────────────────────────────────────────────

// TestManifestMapping_JavaLangString verifies that callers can map the Pekko
// manifest "java.lang.String" to the Go string type, enabling JSON payloads
// from Scala actors to be deserialized correctly.
func TestManifestMapping_JavaLangString(t *testing.T) {
	reg := gekka.NewSerializationRegistry()

	// Register Pekko's "java.lang.String" manifest → Go string type.
	reg.RegisterManifest("java.lang.String", reflect.TypeOf(""))

	typ, ok := reg.GetTypeByManifest("java.lang.String")
	if !ok {
		t.Fatal("manifest 'java.lang.String' not found after registration")
	}
	if typ != reflect.TypeOf("") {
		t.Errorf("type mismatch: got %v, want string", typ)
	}
}

// TestManifestMapping_RoundTrip checks that reverse lookup (type → manifest)
// also works, which is needed when serializing Go messages to send to Pekko.
func TestManifestMapping_RoundTrip(t *testing.T) {
	type OrderPlaced struct {
		OrderID string `json:"orderId"`
		Amount  int    `json:"amount"`
	}

	reg := gekka.NewSerializationRegistry()
	reg.RegisterManifest("com.example.OrderPlaced", reflect.TypeOf(OrderPlaced{}))

	// Forward: manifest → type.
	typ, ok := reg.GetTypeByManifest("com.example.OrderPlaced")
	if !ok {
		t.Fatal("manifest not found")
	}
	if typ != reflect.TypeOf(OrderPlaced{}) {
		t.Errorf("type mismatch: got %v", typ)
	}

	// Reverse: type → manifest.
	manifest, ok := reg.GetManifestByType(reflect.TypeOf(OrderPlaced{}))
	if !ok {
		t.Fatal("reverse manifest lookup failed")
	}
	if manifest != "com.example.OrderPlaced" {
		t.Errorf("manifest mismatch: got %q", manifest)
	}
}

// TestManifestMapping_UnknownManifest confirms that looking up an unregistered
// manifest returns false — callers must handle unknown types gracefully.
func TestManifestMapping_UnknownManifest(t *testing.T) {
	reg := gekka.NewSerializationRegistry()
	_, ok := reg.GetTypeByManifest("com.example.NoSuchType")
	if ok {
		t.Fatal("expected false for unregistered manifest, got true")
	}
}

// ── SerializerID verification ─────────────────────────────────────────────────

// TestSerializerID_Protobuf_IsRegistered checks that the built-in Protobuf
// serializer (ID 2) is available in a freshly created registry.
func TestSerializerID_Protobuf_IsRegistered(t *testing.T) {
	reg := gekka.NewSerializationRegistry()
	s, err := reg.GetSerializer(serializerIDProtobuf)
	if err != nil {
		t.Fatalf("ID 2 (Protobuf) not registered: %v", err)
	}
	if s.Identifier() != serializerIDProtobuf {
		t.Errorf("Identifier() = %d, want 2", s.Identifier())
	}
}

// TestSerializerID_RawBytes_RoundTrip verifies the ByteArray serializer (ID 4)
// passes raw bytes through unchanged — no encoding, no manifest needed.
func TestSerializerID_RawBytes_RoundTrip(t *testing.T) {
	reg := gekka.NewSerializationRegistry()
	s, err := reg.GetSerializer(serializerIDRawBytes)
	if err != nil {
		t.Fatalf("ID 4 (RawBytes) not registered: %v", err)
	}
	if s.Identifier() != serializerIDRawBytes {
		t.Errorf("Identifier() = %d, want 4", s.Identifier())
	}

	payload := []byte("hello pekko")

	encoded, err := s.ToBinary(payload)
	if err != nil {
		t.Fatalf("ToBinary: %v", err)
	}
	if string(encoded) != string(payload) {
		t.Errorf("ToBinary mutated data: got %q, want %q", encoded, payload)
	}

	decoded, err := s.FromBinary(encoded, "")
	if err != nil {
		t.Fatalf("FromBinary: %v", err)
	}
	if string(decoded.([]byte)) != string(payload) {
		t.Errorf("FromBinary result mismatch: got %q, want %q", decoded, payload)
	}
}

// TestSerializerID_JSON_RoundTrip verifies the JSON serializer (ID 9) encodes
// and decodes a registered struct correctly.
func TestSerializerID_JSON_RoundTrip(t *testing.T) {
	type Greeting struct {
		Hello string `json:"hello"`
	}

	reg := gekka.NewSerializationRegistry()
	reg.RegisterManifest("Greeting", reflect.TypeOf(Greeting{}))

	s, err := reg.GetSerializer(serializerIDJSON)
	if err != nil {
		t.Fatalf("ID 9 (JSON) not registered: %v", err)
	}
	if s.Identifier() != serializerIDJSON {
		t.Errorf("Identifier() = %d, want 9", s.Identifier())
	}

	original := Greeting{Hello: "world"}
	data, err := s.ToBinary(original)
	if err != nil {
		t.Fatalf("ToBinary: %v", err)
	}

	result, err := s.FromBinary(data, "Greeting")
	if err != nil {
		t.Fatalf("FromBinary: %v", err)
	}
	got, ok := result.(Greeting)
	if !ok {
		t.Fatalf("FromBinary returned %T, want Greeting", result)
	}
	if got.Hello != original.Hello {
		t.Errorf("Hello mismatch: got %q, want %q", got.Hello, original.Hello)
	}
}

// TestSerializerID_Cluster_NotInDefaultRegistry verifies that the Cluster
// serializer (ID 5) is NOT registered in the default SerializationRegistry.
// Cluster messages are handled by a dedicated codec in the TCP artery handler
// and are never routed through the general-purpose registry.
func TestSerializerID_Cluster_NotInDefaultRegistry(t *testing.T) {
	reg := gekka.NewSerializationRegistry()
	_, err := reg.GetSerializer(serializerIDCluster)
	if err == nil {
		t.Fatal("ID 5 (Cluster) should not be in the default registry — it is handled by the artery TCP layer")
	}
}

// TestSerializerID_Artery_NotInDefaultRegistry verifies that the Artery
// control-frame serializer (ID 17) is NOT registered in the default registry.
// It is used exclusively by the Artery handshake machinery.
func TestSerializerID_Artery_NotInDefaultRegistry(t *testing.T) {
	reg := gekka.NewSerializationRegistry()
	_, err := reg.GetSerializer(serializerIDArtery)
	if err == nil {
		t.Fatal("ID 17 (Artery) should not be in the default registry — it is handled by the artery TCP layer")
	}
}

// TestSerializerID_AllBuiltins checks that all expected built-in IDs are
// present and have the correct Identifier() return value.
func TestSerializerID_AllBuiltins(t *testing.T) {
	reg := gekka.NewSerializationRegistry()

	builtins := []int32{
		serializerIDProtobuf, // 2
		serializerIDRawBytes, // 4
		serializerIDJSON,     // 9
	}

	for _, id := range builtins {
		s, err := reg.GetSerializer(id)
		if err != nil {
			t.Errorf("serializer ID %d: expected registered, got error: %v", id, err)
			continue
		}
		if s.Identifier() != id {
			t.Errorf("serializer ID %d: Identifier() returned %d", id, s.Identifier())
		}
	}
}
