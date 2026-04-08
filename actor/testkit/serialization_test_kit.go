/*
 * serialization_test_kit.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package testkit

import (
	"fmt"
	"reflect"
	"testing"
)

// Serializer is the interface that serialization-capable components must
// implement for use with [SerializationTestKit].
type Serializer interface {
	Identifier() int32
	Serialize(obj any) ([]byte, error)
	Deserialize(data []byte, manifest string) (any, error)
	Manifest(obj any) string
}

// SerializerRegistry maps message types to their serializer.
type SerializerRegistry interface {
	SerializerFor(obj any) (Serializer, error)
}

// SerializationTestKit provides utilities for testing message serialization
// round-trips.  It verifies that messages can be serialized and deserialized
// without loss.
type SerializationTestKit struct {
	registry SerializerRegistry
}

// NewSerializationTestKit creates a [SerializationTestKit] backed by the given
// registry.
func NewSerializationTestKit(registry SerializerRegistry) *SerializationTestKit {
	return &SerializationTestKit{registry: registry}
}

// VerifySerialization performs a round-trip (serialize → deserialize) on msg and
// asserts the result is deeply equal to the original.
func (stk *SerializationTestKit) VerifySerialization(t *testing.T, msg any) {
	t.Helper()
	ser, err := stk.registry.SerializerFor(msg)
	if err != nil {
		t.Fatalf("no serializer found for %T: %v", msg, err)
	}
	stk.roundTrip(t, ser, msg)
}

// VerifySerializationOf performs a round-trip using a specific serializer ID.
func (stk *SerializationTestKit) VerifySerializationOf(t *testing.T, msg any, ser Serializer) {
	t.Helper()
	stk.roundTrip(t, ser, msg)
}

func (stk *SerializationTestKit) roundTrip(t *testing.T, ser Serializer, msg any) {
	t.Helper()
	manifest := ser.Manifest(msg)
	data, err := ser.Serialize(msg)
	if err != nil {
		t.Fatalf("serialize %T failed: %v", msg, err)
	}

	result, err := ser.Deserialize(data, manifest)
	if err != nil {
		t.Fatalf("deserialize %T failed: %v", msg, err)
	}

	if !reflect.DeepEqual(msg, result) {
		t.Errorf("round-trip mismatch for %T:\n  original:     %+v\n  deserialized: %+v", msg, msg, result)
	}
}

// simpleSerializerRegistry is a basic implementation for testing.
type simpleSerializerRegistry struct {
	serializers map[reflect.Type]Serializer
}

// NewSimpleSerializerRegistry creates a registry that maps types to serializers.
func NewSimpleSerializerRegistry() *simpleSerializerRegistry {
	return &simpleSerializerRegistry{serializers: make(map[reflect.Type]Serializer)}
}

// Register associates a serializer with the type of obj.
func (r *simpleSerializerRegistry) Register(obj any, ser Serializer) {
	r.serializers[reflect.TypeOf(obj)] = ser
}

func (r *simpleSerializerRegistry) SerializerFor(obj any) (Serializer, error) {
	t := reflect.TypeOf(obj)
	if ser, ok := r.serializers[t]; ok {
		return ser, nil
	}
	return nil, fmt.Errorf("no serializer registered for %T", obj)
}
