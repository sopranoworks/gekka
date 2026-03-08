/*
 * serialization_registry.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/golang/protobuf/proto"
)

// Serializer defines the contract for encoding and decoding Artery messages.
//
// Register custom implementations with SerializationRegistry.RegisterSerializer
// to support non-Protobuf formats (Jackson JSON, Kryo, Avro, …) used by
// existing Pekko / Akka clusters.
//
//	type MyJSONSerializer struct{}
//
//	func (s *MyJSONSerializer) Identifier() int32 { return 200 }
//	func (s *MyJSONSerializer) ToBinary(obj interface{}) ([]byte, error) {
//	    return json.Marshal(obj)
//	}
//	func (s *MyJSONSerializer) FromBinary(data []byte, manifest string) (interface{}, error) {
//	    // use manifest to pick the target Go type
//	    switch manifest {
//	    case "com.example.MyEvent":
//	        var e MyEvent
//	        return e, json.Unmarshal(data, &e)
//	    }
//	    return nil, fmt.Errorf("unknown manifest: %s", manifest)
//	}
//
//	node.Serialization().RegisterSerializer(200, &MyJSONSerializer{})
type Serializer interface {
	// Identifier returns the serializer ID used in Artery frames.
	// This value is embedded in every outgoing message so the remote side
	// can select the correct deserializer.
	Identifier() int32

	// ToBinary serializes obj to its wire representation.
	ToBinary(obj interface{}) ([]byte, error)

	// FromBinary deserializes data back to an application object.
	// manifest carries the type tag embedded in the Artery envelope
	// (e.g. "com.example.MyEvent" for Jackson, "" for raw bytes).
	FromBinary(data []byte, manifest string) (interface{}, error)
}

// GekkaSerializer is a backward-compatible alias for Serializer.
// Prefer Serializer in new code.
type GekkaSerializer = Serializer

// NoSerializerFoundException is returned when no Serializer is registered for
// a given ID.
type NoSerializerFoundException struct {
	SerializerId int32
}

func (e *NoSerializerFoundException) Error() string {
	return fmt.Sprintf("no serializer found for ID: %d", e.SerializerId)
}

// SerializationRegistry manages Serializer implementations and manifest-to-type
// bindings.  Obtain the shared instance via GekkaNode.Serialization().
type SerializationRegistry struct {
	mu          sync.RWMutex
	serializers map[int32]Serializer
	manifests   map[string]reflect.Type
}

// Well-known serializer IDs used in Artery frames.
const (
	// JSONSerializerID is the Artery serializer ID for the built-in
	// JSONSerializer.  This is a Gekka-internal ID (not a Pekko standard);
	// choose a value that does not collide with your cluster peers.
	//
	// Pekko occupies IDs 1–31; IDs ≥ 100 are safe for application use.
	JSONSerializerID int32 = 9
)

// NewSerializationRegistry creates a registry pre-populated with three
// built-in serializers:
//
//	ID 2 — ProtobufSerializer  (google.golang.org/protobuf/proto)
//	ID 4 — ByteArraySerializer (raw []byte passthrough, Pekko ByteArraySerializer)
//	ID 9 — JSONSerializer      (encoding/json + manifest-to-type registry)
func NewSerializationRegistry() *SerializationRegistry {
	reg := &SerializationRegistry{
		serializers: make(map[int32]Serializer),
		manifests:   make(map[string]reflect.Type),
	}
	reg.RegisterSerializer(2, &ProtobufSerializer{registry: reg})
	reg.RegisterSerializer(4, &ByteArraySerializer{})
	reg.RegisterSerializer(JSONSerializerID, &JSONSerializer{registry: reg})
	return reg
}

// RegisterSerializer associates serializer s with the given Artery serializer
// ID.  The explicit id takes precedence over s.Identifier(), allowing the same
// implementation to serve multiple IDs.
//
//	reg.RegisterSerializer(200, &MyJSONSerializer{})
func (sr *SerializationRegistry) RegisterSerializer(id int32, s Serializer) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.serializers[id] = s
}

// RegisterManifest binds a manifest string to a Go reflect.Type so that
// ProtobufSerializer and other type-aware serializers can instantiate the
// correct struct during deserialization.
//
//	reg.RegisterManifest("com.example.MyProto", reflect.TypeOf((*pb.MyProto)(nil)))
func (sr *SerializationRegistry) RegisterManifest(manifest string, typ reflect.Type) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.manifests[manifest] = typ
}

// GetSerializer retrieves the Serializer registered for id.
// Returns *NoSerializerFoundException when no entry exists.
func (sr *SerializationRegistry) GetSerializer(id int32) (Serializer, error) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	if s, ok := sr.serializers[id]; ok {
		return s, nil
	}
	return nil, &NoSerializerFoundException{SerializerId: id}
}

// GetTypeByManifest retrieves the reflect.Type previously registered for manifest.
func (sr *SerializationRegistry) GetTypeByManifest(manifest string) (reflect.Type, bool) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	typ, ok := sr.manifests[manifest]
	return typ, ok
}

// DeserializePayload selects the Serializer for serializerId and calls
// FromBinary(payload, manifest).
func (sr *SerializationRegistry) DeserializePayload(serializerId int32, manifest string, payload []byte) (interface{}, error) {
	s, err := sr.GetSerializer(serializerId)
	if err != nil {
		return nil, err
	}
	return s.FromBinary(payload, manifest)
}

// ── Built-in serializers ──────────────────────────────────────────────────────

// ByteArraySerializer is the default serializer for ID 4.
// It passes raw []byte through unchanged — equivalent to Pekko's
// ByteArraySerializer.  The manifest is ignored on both encode and decode.
type ByteArraySerializer struct{}

func (b *ByteArraySerializer) Identifier() int32 { return 4 }

func (b *ByteArraySerializer) ToBinary(obj interface{}) ([]byte, error) {
	if data, ok := obj.([]byte); ok {
		return data, nil
	}
	return nil, fmt.Errorf("ByteArraySerializer: expected []byte, got %T", obj)
}

func (b *ByteArraySerializer) FromBinary(data []byte, _ string) (interface{}, error) {
	return data, nil
}

// ProtobufSerializer handles google.golang.org/protobuf messages (ID 2).
// Register Go types with SerializationRegistry.RegisterManifest so that
// FromBinary can instantiate the correct struct.
type ProtobufSerializer struct {
	registry *SerializationRegistry
}

func (p *ProtobufSerializer) Identifier() int32 { return 2 }

func (p *ProtobufSerializer) ToBinary(obj interface{}) ([]byte, error) {
	if msg, ok := obj.(proto.Message); ok {
		return proto.Marshal(msg)
	}
	return nil, fmt.Errorf("ProtobufSerializer: expected proto.Message, got %T", obj)
}

func (p *ProtobufSerializer) FromBinary(data []byte, manifest string) (interface{}, error) {
	typ, ok := p.registry.GetTypeByManifest(manifest)
	if !ok {
		return nil, fmt.Errorf("ProtobufSerializer: no type registered for manifest %q", manifest)
	}
	val := reflect.New(typ.Elem()).Interface()
	msg, ok := val.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("ProtobufSerializer: type for manifest %q does not implement proto.Message", manifest)
	}
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

// JSONSerializer is the built-in serializer for ID 9. It encodes arbitrary Go
// values as JSON (encoding/json) and uses the manifest string together with the
// registry's type table to instantiate the correct target type on decode.
//
// Register a manifest-to-type mapping before receiving messages:
//
//	node.RegisterType("com.example.OrderPlaced", reflect.TypeOf(OrderPlaced{}))
//
// Then send any struct via Tell; the router will automatically select ID 9 and
// use the reflect type name as the manifest.
type JSONSerializer struct {
	registry *SerializationRegistry
}

func (j *JSONSerializer) Identifier() int32 { return JSONSerializerID }

// ToBinary encodes obj as JSON. Any value supported by encoding/json is accepted.
func (j *JSONSerializer) ToBinary(obj interface{}) ([]byte, error) {
	return json.Marshal(obj)
}

// FromBinary decodes data as JSON into a new instance of the type registered for
// manifest.  Returns an error when no type is registered for the manifest or the
// JSON is malformed.
//
// The returned value has the same type that was passed to RegisterManifest:
// if a pointer type (*T) was registered the result is *T; if a value type (T)
// was registered the result is T.
func (j *JSONSerializer) FromBinary(data []byte, manifest string) (interface{}, error) {
	typ, ok := j.registry.GetTypeByManifest(manifest)
	if !ok {
		return nil, fmt.Errorf("JSONSerializer: no type registered for manifest %q", manifest)
	}
	// Allocate a pointer to the registered type so json.Unmarshal can populate it.
	var ptr reflect.Value
	if typ.Kind() == reflect.Ptr {
		ptr = reflect.New(typ.Elem()) // *T → **T, elem = *T
	} else {
		ptr = reflect.New(typ) // T → *T
	}
	if err := json.Unmarshal(data, ptr.Interface()); err != nil {
		return nil, fmt.Errorf("JSONSerializer: unmarshal into %v: %w", typ, err)
	}
	if typ.Kind() == reflect.Ptr {
		return ptr.Interface(), nil // return *T
	}
	return ptr.Elem().Interface(), nil // return T
}
