/*
 * debug_types.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package management

// CRDTEntry is one row in the /cluster/debug/crdt list response.
type CRDTEntry struct {
	Key  string `json:"key"`
	Type string `json:"type"` // "gcounter"|"orset"|"lwwmap"|"pncounter"|"orflag"|"lwwregister"|"unknown"
}

// CRDTValue is the /cluster/debug/crdt/{key} detail response.
// Value is type-specific; see the spec table for shapes.
type CRDTValue struct {
	Key   string      `json:"key"`
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

// ActorEntry is one row in the /cluster/debug/actors response.
type ActorEntry struct {
	Path string `json:"path"`
	Kind string `json:"kind"` // "user"|"system"
}

// DebugProvider gives the debug handlers read-only access to introspection
// data without pulling cluster/replicator/actor-system types into the
// management package.
type DebugProvider interface {
	// CRDTList returns every CRDT known to this node's Replicator.
	CRDTList() []CRDTEntry

	// CRDT returns the current value snapshot for key, or nil when key is
	// not registered on this node.
	CRDT(key string) (*CRDTValue, error)

	// Actors returns every actor registered on this node.  When
	// includeSystem is false, /system/* paths are filtered out.
	Actors(includeSystem bool) []ActorEntry
}
