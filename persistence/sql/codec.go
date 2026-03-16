/*
 * codec.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sqlstore

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
)

// PayloadCodec handles serialization of event and snapshot payloads to and
// from the raw bytes stored in the database.
//
// The manifest string is stored alongside the payload so the correct Go type
// can be reconstructed during replay.
type PayloadCodec interface {
	// Encode serializes payload into bytes.  It also returns a manifest
	// string (typically the Go type name) that is stored in the database
	// and passed back to Decode during replay.
	Encode(payload any) (manifest string, data []byte, err error)

	// Decode deserializes data into the Go value described by manifest.
	// It must return a value of the same concrete type that was encoded.
	Decode(manifest string, data []byte) (any, error)
}

// ── JSONCodec ─────────────────────────────────────────────────────────────────

// JSONCodec is a PayloadCodec that serializes payloads as JSON.
//
// Each Go type that may appear as a payload must be registered before the
// journal or snapshot store can decode it.  Registration maps a manifest
// string (defaults to the Go type's reflect name) to a zero value of that
// type.
//
//	codec := sqlstore.NewJSONCodec()
//	codec.Register(MyEvent{})              // manifest = "sqlstore_test.MyEvent"
//	codec.RegisterManifest("my.Event", MyEvent{})  // explicit manifest
type JSONCodec struct {
	mu    sync.RWMutex
	types map[string]reflect.Type // manifest → concrete type
}

// NewJSONCodec creates a JSONCodec with an empty type registry.
func NewJSONCodec() *JSONCodec {
	return &JSONCodec{types: make(map[string]reflect.Type)}
}

// Register adds zero's concrete type to the registry using the Go reflect
// type name (e.g. "mypkg.MyEvent") as the manifest.
//
// This is the most common registration path.  Use RegisterManifest to
// control the manifest string explicitly (e.g. to match a Pekko class name).
func (c *JSONCodec) Register(zero any) {
	t := reflect.TypeOf(zero)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	c.mu.Lock()
	c.types[t.String()] = t
	c.mu.Unlock()
}

// RegisterManifest adds zero's concrete type to the registry under an
// explicit manifest string.  Use this when you need the manifest to match a
// Pekko class name or a stable identifier that doesn't change with refactoring.
func (c *JSONCodec) RegisterManifest(manifest string, zero any) {
	t := reflect.TypeOf(zero)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	c.mu.Lock()
	c.types[manifest] = t
	c.mu.Unlock()
}

// Encode marshals payload to JSON.  The manifest is the reflect type name of
// the concrete payload value.
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
		return "", nil, fmt.Errorf("sqlstore: JSON encode %q: %w", manifest, err)
	}
	return manifest, data, nil
}

// Decode unmarshals data into the Go type registered under manifest.
// It returns an error if the manifest is unknown.
func (c *JSONCodec) Decode(manifest string, data []byte) (any, error) {
	if manifest == "nil" {
		return nil, nil
	}
	c.mu.RLock()
	t, ok := c.types[manifest]
	c.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("sqlstore: no type registered for manifest %q (call codec.Register or codec.RegisterManifest)", manifest)
	}
	ptr := reflect.New(t)
	if err := json.Unmarshal(data, ptr.Interface()); err != nil {
		return nil, fmt.Errorf("sqlstore: JSON decode %q: %w", manifest, err)
	}
	return ptr.Elem().Interface(), nil
}
