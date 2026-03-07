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

// GekkaSerializer defines the contract for serializing and deserializing messages.
type GekkaSerializer interface {
	Identifier() int32
	ToBinary(obj interface{}) ([]byte, error)
	FromBinary(data []byte, manifest string) (interface{}, error)
}

// NoSerializerFoundException represents an error when a serializer cannot be found.
type NoSerializerFoundException struct {
	SerializerId int32
}

func (e *NoSerializerFoundException) Error() string {
	return fmt.Sprintf("no serializer found for ID: %d", e.SerializerId)
}

// SerializationRegistry manages custom serializers and message manifest bindings.
type SerializationRegistry struct {
	mu          sync.RWMutex
	serializers map[int32]GekkaSerializer
	manifests   map[string]reflect.Type
}

// NewSerializationRegistry creates a new populated registry.
func NewSerializationRegistry() *SerializationRegistry {
	reg := &SerializationRegistry{
		serializers: make(map[int32]GekkaSerializer),
		manifests:   make(map[string]reflect.Type),
	}

	protoSerializer := &ProtobufSerializer{registry: reg}
	jsonSerializer := &JSONSerializer{registry: reg}

	reg.RegisterSerializer(protoSerializer)
	reg.RegisterSerializer(jsonSerializer)

	return reg
}

// RegisterSerializer registers a serializer by its Identifier.
func (sr *SerializationRegistry) RegisterSerializer(s GekkaSerializer) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.serializers[s.Identifier()] = s
}

// RegisterManifest binds a string manifest to a Go type.
func (sr *SerializationRegistry) RegisterManifest(manifest string, typ reflect.Type) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.manifests[manifest] = typ
}

// GetSerializer retrieves a serializer by its ID.
func (sr *SerializationRegistry) GetSerializer(id int32) (GekkaSerializer, error) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	if s, ok := sr.serializers[id]; ok {
		return s, nil
	}
	return nil, &NoSerializerFoundException{SerializerId: id}
}

// GetTypeByManifest retrieves a registered Go type.
func (sr *SerializationRegistry) GetTypeByManifest(manifest string) (reflect.Type, bool) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	typ, ok := sr.manifests[manifest]
	return typ, ok
}

// Deserialize Payload finds the right serializer and decodes.
func (sr *SerializationRegistry) DeserializePayload(serializerId int32, manifest string, payload []byte) (interface{}, error) {
	s, err := sr.GetSerializer(serializerId)
	if err != nil {
		return nil, err
	}
	return s.FromBinary(payload, manifest)
}

// --- Built-in Serializers ---

// ProtobufSerializer handles standard Google Protobufs (ID=2)
type ProtobufSerializer struct {
	registry *SerializationRegistry
}

func (p *ProtobufSerializer) Identifier() int32 {
	return 2
}

func (p *ProtobufSerializer) ToBinary(obj interface{}) ([]byte, error) {
	if msg, ok := obj.(proto.Message); ok {
		return proto.Marshal(msg)
	}
	return nil, fmt.Errorf("object is not a proto.Message")
}

func (p *ProtobufSerializer) FromBinary(data []byte, manifest string) (interface{}, error) {
	typ, ok := p.registry.GetTypeByManifest(manifest)
	if !ok {
		return nil, fmt.Errorf("no registered Protobuf type for manifest: %s", manifest)
	}

	// Create a new ptr to the struct
	val := reflect.New(typ.Elem()).Interface()
	msg, ok := val.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("registered type for manifest %s is not proto.Message", manifest)
	}

	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

// JSONSerializer provides fallback/convenience JSON encoding (ID=4)
type JSONSerializer struct {
	registry *SerializationRegistry
}

func (j *JSONSerializer) Identifier() int32 {
	return 4 // using 4 to avoid conflict with standard Java/Kryo usually around 1,2,3
}

func (j *JSONSerializer) ToBinary(obj interface{}) ([]byte, error) {
	return json.Marshal(obj)
}

func (j *JSONSerializer) FromBinary(data []byte, manifest string) (interface{}, error) {
	typ, ok := j.registry.GetTypeByManifest(manifest)
	if !ok {
		// Log warning, return payload as map or string?
		// For now, return dynamic map.
		var dynamic map[string]interface{}
		if err := json.Unmarshal(data, &dynamic); err != nil {
			return nil, err
		}
		return dynamic, nil
	}

	val := reflect.New(typ.Elem()).Interface()
	if err := json.Unmarshal(data, val); err != nil {
		return nil, err
	}
	return val, nil
}
