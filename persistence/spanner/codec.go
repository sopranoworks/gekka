/*
 * codec.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package spannerstore

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
)

// PayloadCodec handles serialisation of event and snapshot payloads.
//
// The manifest string is stored alongside the serialised bytes so the correct
// Go type can be reconstructed during replay.
type PayloadCodec interface {
	Encode(payload any) (manifest string, data []byte, err error)
	Decode(manifest string, data []byte) (any, error)
}

// JSONCodec is a PayloadCodec that serialises payloads as JSON.
//
// Each concrete Go type that may appear as a payload must be registered before
// the journal or snapshot store can decode it.
//
//	codec := spannerstore.NewJSONCodec()
//	codec.Register(MyEvent{})
//	codec.RegisterManifest("my.Event", MyEvent{})
type JSONCodec struct {
	mu    sync.RWMutex
	types map[string]reflect.Type
}

// NewJSONCodec creates an empty JSONCodec.
func NewJSONCodec() *JSONCodec {
	return &JSONCodec{types: make(map[string]reflect.Type)}
}

// Register adds zero's concrete type using its reflect type string as manifest.
func (c *JSONCodec) Register(zero any) {
	t := reflect.TypeOf(zero)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	c.mu.Lock()
	c.types[t.String()] = t
	c.mu.Unlock()
}

// RegisterManifest adds zero's concrete type under an explicit manifest string.
func (c *JSONCodec) RegisterManifest(manifest string, zero any) {
	t := reflect.TypeOf(zero)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	c.mu.Lock()
	c.types[manifest] = t
	c.mu.Unlock()
}

// Encode marshals payload to JSON and returns its reflect type name as manifest.
func (c *JSONCodec) Encode(payload any) (string, []byte, error) {
	if payload == nil {
		return "nil", []byte("null"), nil
	}
	t := reflect.TypeOf(payload)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	manifest := t.String()
	data, err := json.Marshal(payload)
	if err != nil {
		return "", nil, fmt.Errorf("spannerstore: JSON encode %q: %w", manifest, err)
	}
	return manifest, data, nil
}

// Decode unmarshals data into the Go type registered under manifest.
func (c *JSONCodec) Decode(manifest string, data []byte) (any, error) {
	if manifest == "nil" {
		return nil, nil
	}
	c.mu.RLock()
	t, ok := c.types[manifest]
	c.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("spannerstore: no type registered for manifest %q", manifest)
	}
	ptr := reflect.New(t)
	if err := json.Unmarshal(data, ptr.Interface()); err != nil {
		return nil, fmt.Errorf("spannerstore: JSON decode %q: %w", manifest, err)
	}
	return ptr.Elem().Interface(), nil
}
