/*
 * serialization_registry.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"google.golang.org/protobuf/proto"
)

// Artery Control Serializer ID
const (
	ProtobufSerializerID = 2
	RawSerializerID      = 4
	ClusterSerializerID  = 5
	JSONSerializerID     = 9
)

// Serializer is an interface for serializing messages.
type Serializer interface {
	Identifier() int32
	ToBinary(msg interface{}) ([]byte, error)
	FromBinary(data []byte, manifest string) (interface{}, error)
}

// SerializationRegistry manages the mapping between message types and manifest strings.
// It is used by Artery handlers to serialize and deserialize messages.
type SerializationRegistry struct {
	mu                 sync.RWMutex
	manifestsToType    map[string]reflect.Type
	typeToManifests    map[reflect.Type]string
	serializers        map[int32]Serializer
	jsonSerializer     *JSONSerializer
	rawSerializer      *RawSerializer
	protobufSerializer *ProtobufSerializer
}

func NewSerializationRegistry() *SerializationRegistry {
	r := &SerializationRegistry{
		manifestsToType: make(map[string]reflect.Type),
		typeToManifests: make(map[reflect.Type]string),
		serializers:     make(map[int32]Serializer),
	}
	r.jsonSerializer = &JSONSerializer{registry: r}
	r.rawSerializer = &RawSerializer{}
	r.protobufSerializer = &ProtobufSerializer{}
	r.serializers[JSONSerializerID] = r.jsonSerializer
	r.serializers[RawSerializerID] = r.rawSerializer
	r.serializers[ProtobufSerializerID] = r.protobufSerializer
	return r
}

func (r *SerializationRegistry) RegisterManifest(manifest string, typ reflect.Type) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.manifestsToType[manifest] = typ
	r.typeToManifests[typ] = manifest
}

func (r *SerializationRegistry) GetTypeByManifest(manifest string) (reflect.Type, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	typ, ok := r.manifestsToType[manifest]
	return typ, ok
}

func (r *SerializationRegistry) GetManifestByType(typ reflect.Type) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	manifest, ok := r.typeToManifests[typ]
	return manifest, ok
}

func (r *SerializationRegistry) RegisterSerializer(id int32, s Serializer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.serializers[id] = s
}

func (r *SerializationRegistry) GetSerializer(id int32) (Serializer, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if s, ok := r.serializers[id]; ok {
		return s, nil
	}
	return nil, fmt.Errorf("SerializationRegistry: unknown serializer ID %d", id)
}

// DeserializePayload chooses the correct serializer based on ID and manifest.
func (r *SerializationRegistry) DeserializePayload(serializerId int32, manifest string, data []byte) (interface{}, error) {
	s, err := r.GetSerializer(serializerId)
	if err != nil {
		return nil, err
	}
	return s.FromBinary(data, manifest)
}

// JSONSerializer handles JSON serialization for types registered in the registry.
type JSONSerializer struct {
	registry *SerializationRegistry
}

func (j *JSONSerializer) Identifier() int32 {
	return JSONSerializerID
}

func (j *JSONSerializer) ToBinary(msg interface{}) ([]byte, error) {
	return json.Marshal(msg)
}

// FromBinary deserializes JSON bytes into the type registered for manifest.
func (j *JSONSerializer) FromBinary(data []byte, manifest string) (interface{}, error) {
	typ, ok := j.registry.GetTypeByManifest(manifest)
	if !ok {
		return nil, fmt.Errorf("JSONSerializer: no type registered for manifest %q", manifest)
	}
	var ptr reflect.Value
	if typ.Kind() == reflect.Ptr {
		ptr = reflect.New(typ.Elem())
	} else {
		ptr = reflect.New(typ)
	}
	if err := json.Unmarshal(data, ptr.Interface()); err != nil {
		return nil, fmt.Errorf("JSONSerializer: unmarshal into %v: %w", typ, err)
	}
	if typ.Kind() == reflect.Ptr {
		return ptr.Interface(), nil
	}
	return ptr.Elem().Interface(), nil
}

// RawSerializer handles raw byte slice messages.
type RawSerializer struct{}

func (s *RawSerializer) Identifier() int32 {
	return RawSerializerID
}

func (s *RawSerializer) ToBinary(msg interface{}) ([]byte, error) {
	if data, ok := msg.([]byte); ok {
		return data, nil
	}
	return nil, fmt.Errorf("RawSerializer: msg is not []byte")
}

func (s *RawSerializer) FromBinary(data []byte, manifest string) (interface{}, error) {
	return data, nil
}

// ProtobufSerializer handles Protobuf serialization.
type ProtobufSerializer struct{}

func (s *ProtobufSerializer) Identifier() int32 {
	return ProtobufSerializerID
}

func (s *ProtobufSerializer) ToBinary(msg interface{}) ([]byte, error) {
	if pmsg, ok := msg.(proto.Message); ok {
		return proto.Marshal(pmsg)
	}
	return nil, fmt.Errorf("ProtobufSerializer: msg is not proto.Message")
}

func (s *ProtobufSerializer) FromBinary(data []byte, manifest string) (interface{}, error) {
	return nil, fmt.Errorf("ProtobufSerializer: FromBinary not implemented (requires type registry for Protobuf types)")
}
