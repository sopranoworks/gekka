/*
 * debug_crdt.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package management

import (
	"net/http"
	"sort"
	"strings"
)

// handleDebugCRDTListImpl serves GET /cluster/debug/crdt — returns every CRDT
// registered on this node's Replicator, sorted alphabetically by key.
func (ms *ManagementServer) handleDebugCRDTListImpl(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeDebugError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	entries := ms.debug.CRDTList()
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})
	writeDebugEnvelope(w, "crdt-list", entries, nil)
}

// handleDebugCRDTImpl serves GET /cluster/debug/crdt/{key}.  Stub for now —
// Task 7 provides the real implementation.
func (ms *ManagementServer) handleDebugCRDTImpl(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeDebugError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/cluster/debug/crdt/")
	if key == "" {
		writeDebugError(w, http.StatusBadRequest, "key is required")
		return
	}
	val, err := ms.debug.CRDT(key)
	if err != nil {
		writeDebugError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if val == nil {
		writeDebugError(w, http.StatusNotFound, "key "+key+" not found")
		return
	}
	writeDebugEnvelope(w, "crdt", val, nil)
}
