/*
 * cbor_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cbor

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/sopranoworks/gekka/internal/serialization"
)

type UserCreated struct {
	Name  string `cbor:"1,keyasint" json:"name"`
	Email string `cbor:"2,keyasint" json:"email"`
	Age   int    `cbor:"3,keyasint" json:"age"`
}

type OrderPlaced struct {
	OrderID string  `cbor:"1,keyasint" json:"orderId"`
	Amount  float64 `cbor:"2,keyasint" json:"amount"`
	Items   int     `cbor:"3,keyasint" json:"items"`
}

func TestCborSerializer_RoundTrip(t *testing.T) {
	s := New(101)

	original := UserCreated{Name: "Alice", Email: "alice@example.com", Age: 30}
	data, err := s.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded UserCreated
	if err := s.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded != original {
		t.Errorf("round-trip mismatch: got %+v, want %+v", decoded, original)
	}
}

func TestCborSerializer_ManifestBasedDeserialization(t *testing.T) {
	s := New(101)
	s.RegisterManifest("com.example.UserCreated", reflect.TypeOf(UserCreated{}))
	s.RegisterManifest("com.example.OrderPlaced", reflect.TypeOf(OrderPlaced{}))

	original := OrderPlaced{OrderID: "ORD-123", Amount: 99.99, Items: 3}
	data, err := s.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	decoded, err := s.UnmarshalWithManifest(data, "com.example.OrderPlaced")
	if err != nil {
		t.Fatalf("unmarshal with manifest: %v", err)
	}

	order, ok := decoded.(OrderPlaced)
	if !ok {
		t.Fatalf("expected OrderPlaced, got %T", decoded)
	}
	if order != original {
		t.Errorf("mismatch: got %+v, want %+v", order, original)
	}
}

func TestCborSerializer_ManifestFor(t *testing.T) {
	s := New(101)
	s.RegisterManifest("com.example.UserCreated", reflect.TypeOf(UserCreated{}))

	m, ok := s.ManifestFor(UserCreated{})
	if !ok || m != "com.example.UserCreated" {
		t.Errorf("expected manifest, got %q, ok=%v", m, ok)
	}

	_, ok = s.ManifestFor(OrderPlaced{})
	if ok {
		t.Error("should not have manifest for unregistered type")
	}
}

func TestCborSerializer_UnknownManifest(t *testing.T) {
	s := New(101)

	_, err := s.UnmarshalWithManifest([]byte{0xa0}, "unknown.Type")
	if err == nil {
		t.Fatal("expected error for unknown manifest")
	}
}

func TestCborSerializer_SmallerThanJSON(t *testing.T) {
	s := New(101)

	original := UserCreated{Name: "Alice", Email: "alice@example.com", Age: 30}

	cborData, err := s.Marshal(original)
	if err != nil {
		t.Fatalf("cbor marshal: %v", err)
	}

	jsonData, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json marshal: %v", err)
	}

	t.Logf("CBOR size: %d bytes, JSON size: %d bytes (%.0f%% reduction)",
		len(cborData), len(jsonData),
		100*(1-float64(len(cborData))/float64(len(jsonData))))

	if len(cborData) >= len(jsonData) {
		t.Errorf("CBOR (%d bytes) should be smaller than JSON (%d bytes)",
			len(cborData), len(jsonData))
	}
}

func TestCborSerializer_Identifier(t *testing.T) {
	s := New(101)
	if s.Identifier() != 101 {
		t.Errorf("expected ID 101, got %d", s.Identifier())
	}
}

func TestCborSerializer_RegisteredInGlobalRegistry(t *testing.T) {
	s, ok := serialization.Get("cbor")
	if !ok {
		t.Fatal("cbor serializer should be registered globally via init()")
	}
	if s.Identifier() != DefaultID {
		t.Errorf("expected default ID %d, got %d", DefaultID, s.Identifier())
	}
}

func TestCborSerializer_ComplexTypes(t *testing.T) {
	s := New(102) // different ID to avoid registry conflict

	type Nested struct {
		Tags   []string       `cbor:"1,keyasint"`
		Meta   map[string]int `cbor:"2,keyasint"`
		Active bool           `cbor:"3,keyasint"`
	}

	original := Nested{
		Tags:   []string{"go", "crdt", "actor"},
		Meta:   map[string]int{"retries": 3, "timeout": 30},
		Active: true,
	}

	data, err := s.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded Nested
	if err := s.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !reflect.DeepEqual(decoded, original) {
		t.Errorf("mismatch: got %+v, want %+v", decoded, original)
	}
}
