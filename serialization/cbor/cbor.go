/*
 * cbor.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package cbor provides a CBOR serializer for gekka, offering compact binary
// serialization as an alternative to JSON. It uses the fxamacker/cbor/v2
// library and supports manifest-based type resolution.
//
// Register the serializer by importing this package (blank import) or by
// calling Register explicitly:
//
//	import _ "github.com/sopranoworks/gekka/serialization/cbor"
//
// The default serializer ID is 101. Override via HOCON configuration.
package cbor

import (
	"fmt"
	"reflect"
	"sync"

	fxcbor "github.com/fxamacker/cbor/v2"

	"github.com/sopranoworks/gekka/internal/serialization"
)

const (
	// DefaultID is the default serializer identifier for CBOR.
	// Values below 100 are reserved for built-in serializers.
	DefaultID int32 = 101
)

func init() {
	serialization.Register("cbor", New(DefaultID))
}

// CborSerializer implements the serialization.Serializer interface using CBOR
// encoding. It supports manifest-based type resolution for deserializing into
// concrete Go types.
type CborSerializer struct {
	id int32

	mu       sync.RWMutex
	byManifest map[string]reflect.Type
	byType     map[reflect.Type]string

	encMode fxcbor.EncMode
	decMode fxcbor.DecMode
}

// New creates a CborSerializer with the given numeric ID.
func New(id int32) *CborSerializer {
	em, _ := fxcbor.EncOptions{
		Sort: fxcbor.SortCanonical,
	}.EncMode()

	dm, _ := fxcbor.DecOptions{}.DecMode()

	return &CborSerializer{
		id:         id,
		byManifest: make(map[string]reflect.Type),
		byType:     make(map[reflect.Type]string),
		encMode:    em,
		decMode:    dm,
	}
}

// RegisterManifest associates a manifest string with a Go type, enabling
// deserialization into the correct concrete type. The manifest is typically the
// fully-qualified type name.
//
//	s.RegisterManifest("com.example.UserCreated", reflect.TypeOf(UserCreated{}))
func (s *CborSerializer) RegisterManifest(manifest string, typ reflect.Type) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byManifest[manifest] = typ
	s.byType[typ] = manifest
}

// ManifestFor returns the manifest string for the given value's type.
// Returns ("", false) if no manifest was registered.
func (s *CborSerializer) ManifestFor(v any) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.byType[reflect.TypeOf(v)]
	return m, ok
}

// Marshal encodes v into CBOR bytes.
func (s *CborSerializer) Marshal(v any) ([]byte, error) {
	return s.encMode.Marshal(v)
}

// Unmarshal decodes CBOR data into v. v must be a non-nil pointer.
func (s *CborSerializer) Unmarshal(data []byte, v any) error {
	return s.decMode.Unmarshal(data, v)
}

// UnmarshalWithManifest decodes CBOR data into a new instance of the type
// registered for the given manifest. Returns the decoded value and any error.
func (s *CborSerializer) UnmarshalWithManifest(data []byte, manifest string) (any, error) {
	s.mu.RLock()
	typ, ok := s.byManifest[manifest]
	s.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("cbor: no type registered for manifest %q", manifest)
	}

	ptr := reflect.New(typ)
	if err := s.decMode.Unmarshal(data, ptr.Interface()); err != nil {
		return nil, err
	}
	return ptr.Elem().Interface(), nil
}

// Identifier returns the numeric ID of this serializer.
func (s *CborSerializer) Identifier() int32 {
	return s.id
}
