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
	"io"
	"reflect"
	"sync"

	"github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// Artery Control Serializer ID
const (
        ProtobufSerializerID = 2
        RawSerializerID      = 4
        ClusterSerializerID  = 5
        JSONSerializerID     = 9
        StringSerializerID   = 20

        // Pekko Distributed Data Serializers

	DDataReplicatedSerializerID    = 11
	DDataReplicatorMsgSerializerID = 12
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
	r.protobufSerializer = &ProtobufSerializer{registry: r}
	r.serializers[JSONSerializerID] = r.jsonSerializer
	r.serializers[RawSerializerID] = r.rawSerializer
	r.serializers[ProtobufSerializerID] = r.protobufSerializer
	r.serializers[StringSerializerID] = &StringSerializer{}

	// Registration of Artery control types (v0.14.x alignment)
	r.RegisterManifest("d", reflect.TypeOf((*remote.HandshakeReq)(nil)))
	r.RegisterManifest("e", reflect.TypeOf((*remote.MessageWithAddress)(nil)))
	r.RegisterManifest("n", reflect.TypeOf((*remote.ArteryHeartbeatRsp)(nil)))
	r.RegisterManifest("q", reflect.TypeOf((*remote.Quarantined)(nil)))
	r.RegisterManifest("ct", reflect.TypeOf((*remote.CompressionTableAdvertisement)(nil)))
	r.RegisterManifest("cta", reflect.TypeOf((*remote.CompressionTableAdvertisementAck)(nil)))
	r.RegisterManifest("SystemMessage", reflect.TypeOf((*remote.SystemMessageEnvelope)(nil)))
	r.RegisterManifest("h", reflect.TypeOf((*remote.SystemMessageDeliveryAck)(nil)))

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

// StringSerializer handles plain string messages.
type StringSerializer struct{}

func (s *StringSerializer) Identifier() int32 {
        return StringSerializerID
}

func (s *StringSerializer) ToBinary(msg interface{}) ([]byte, error) {
        if str, ok := msg.(string); ok {
                return []byte(str), nil
        }
        return nil, fmt.Errorf("StringSerializer: msg is not string")
}

func (s *StringSerializer) FromBinary(data []byte, manifest string) (interface{}, error) {
        return string(data), nil
}

func (s *StringSerializer) WriteTo(w io.Writer, msg interface{}) (int64, error) {
        if str, ok := msg.(string); ok {
                n, err := w.Write([]byte(str))
                return int64(n), err
        }
        return 0, fmt.Errorf("StringSerializer: msg is not string")
}

func (s *StringSerializer) MarshalTo(buf []byte, msg interface{}) (int, error) {
        if str, ok := msg.(string); ok {
                if len(buf) < len(str) {
                        return 0, io.ErrShortBuffer
                }
                n := copy(buf, []byte(str))
                return n, nil
        }
        return 0, fmt.Errorf("StringSerializer: msg is not string")
}

func (s *StringSerializer) Size(msg interface{}) int {
        if str, ok := msg.(string); ok {
                return len(str)
        }
        return 0
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

func (s *RawSerializer) WriteTo(w io.Writer, msg interface{}) (int64, error) {
	if data, ok := msg.([]byte); ok {
		n, err := w.Write(data)
		return int64(n), err
	}
	return 0, fmt.Errorf("RawSerializer: msg is not []byte")
}

func (s *RawSerializer) MarshalTo(buf []byte, msg interface{}) (int, error) {
	if data, ok := msg.([]byte); ok {
		if len(buf) < len(data) {
			return 0, io.ErrShortBuffer
		}
		n := copy(buf, data)
		return n, nil
	}
	return 0, fmt.Errorf("RawSerializer: msg is not []byte")
}

func (s *RawSerializer) Size(msg interface{}) int {
	if data, ok := msg.([]byte); ok {
		return len(data)
	}
	return 0
}

// ProtobufSerializer handles Protobuf serialization.
type ProtobufSerializer struct {
	registry *SerializationRegistry
}

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
	typ, ok := s.registry.GetTypeByManifest(manifest)
	if !ok {
		return nil, fmt.Errorf("ProtobufSerializer: no type registered for manifest %q", manifest)
	}
	var ptr reflect.Value
	if typ.Kind() == reflect.Ptr {
		ptr = reflect.New(typ.Elem())
	} else {
		ptr = reflect.New(typ)
	}
	m, ok := ptr.Interface().(proto.Message)
	if !ok {
		return nil, fmt.Errorf("ProtobufSerializer: %v is not proto.Message", typ)
	}
	if err := proto.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("ProtobufSerializer: unmarshal into %v: %w", typ, err)
	}
	if typ.Kind() == reflect.Ptr {
		return m, nil
	}
	return ptr.Elem().Interface(), nil
}

func (s *ProtobufSerializer) WriteTo(w io.Writer, msg interface{}) (int64, error) {
	if pmsg, ok := msg.(proto.Message); ok {
		data, err := proto.Marshal(pmsg)
		if err != nil {
			return 0, err
		}
		n, err := w.Write(data)
		return int64(n), err
	}
	return 0, fmt.Errorf("ProtobufSerializer: msg is not proto.Message")
}

func (s *ProtobufSerializer) MarshalTo(buf []byte, msg interface{}) (int, error) {
	if pmsg, ok := msg.(proto.Message); ok {
		options := proto.MarshalOptions{}
		data, err := options.MarshalAppend(buf[:0], pmsg)
		if err != nil {
			return 0, err
		}
		return len(data), nil
	}
	return 0, fmt.Errorf("ProtobufSerializer: msg is not proto.Message")
}

func (s *ProtobufSerializer) Size(msg interface{}) int {
	if pmsg, ok := msg.(proto.Message); ok {
		return proto.Size(pmsg)
	}
	return 0
}
