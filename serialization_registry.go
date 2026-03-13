/*
 * serialization_registry.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"github.com/sopranoworks/gekka/internal/core"
)

// SerializationRegistry manages the mapping between message types and manifest strings.
// It is used by Artery handlers to serialize and deserialize messages.
type SerializationRegistry = core.SerializationRegistry

// Serializer is an interface for serializing messages.
type Serializer = core.Serializer

// NewSerializationRegistry creates a new SerializationRegistry.
func NewSerializationRegistry() *SerializationRegistry {
	return core.NewSerializationRegistry()
}

// Artery Control Serializer IDs
const (
	ProtobufSerializerID = core.ProtobufSerializerID
	RawSerializerID      = core.RawSerializerID
	ClusterSerializerID  = core.ClusterSerializerID
	JSONSerializerID     = core.JSONSerializerID
)
