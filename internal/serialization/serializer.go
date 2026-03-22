/*
 * serializer.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package serialization provides a protocol-agnostic abstraction for encoding
// and decoding application-level messages.
//
// It is intentionally free of wire-protocol concepts (manifests, serializer IDs)
// so that persistence backends, codecs, and future extension modules can depend
// on this package without pulling in Artery or Protobuf.
//
// The Artery-specific Serializer (ToBinary/FromBinary/Identifier) lives in
// internal/core and is kept separate to prepare for extracting the Protobuf
// implementation into an extension module in a future sprint.
package serialization

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Serializer is the abstraction for encoding and decoding application-level messages.
// Implementations must be safe for concurrent use.
type Serializer interface {
	// Marshal encodes v into bytes. v may be any application-defined type.
	Marshal(v any) ([]byte, error)

	// Unmarshal decodes data into v. v must be a non-nil pointer.
	Unmarshal(data []byte, v any) error

	// Identifier returns the numeric ID of this serializer.
	// IDs below 100 are reserved for built-in implementations.
	Identifier() int32
}

// ── Registry ──────────────────────────────────────────────────────────────────

var (
	registryMu  sync.RWMutex
	byName      = make(map[string]Serializer)
	byID        = make(map[int32]Serializer)
)

// Register adds s to the default registry under name.
// Panics if name or Identifier() is already registered.
func Register(name string, s Serializer) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dup := byName[name]; dup {
		panic(fmt.Sprintf("serialization: Register called twice for name %q", name))
	}
	if _, dup := byID[s.Identifier()]; dup {
		panic(fmt.Sprintf("serialization: Register called twice for identifier %d (name %q)", s.Identifier(), name))
	}
	byName[name] = s
	byID[s.Identifier()] = s
}

// Get returns the Serializer registered under name.
func Get(name string) (Serializer, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	s, ok := byName[name]
	return s, ok
}

// GetByID returns the Serializer with the given numeric ID.
func GetByID(id int32) (Serializer, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	s, ok := byID[id]
	return s, ok
}

// Registered returns the sorted names of all registered serializers.
func Registered() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(byName))
	for n := range byName {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// MustGet looks up name in the registry and panics if it is not registered.
// Intended for use in init() or startup code where a missing serializer is
// always a programming error.
func MustGet(name string) Serializer {
	s, ok := Get(name)
	if !ok {
		available := Registered()
		panic(fmt.Sprintf("serialization: %q not found — available: [%s]",
			name, strings.Join(available, ", ")))
	}
	return s
}
