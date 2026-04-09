/*
 * debug_actors.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package management

import (
	"net/http"
)

// handleDebugActorsImpl serves GET /cluster/debug/actors[?system=true|false].
func (ms *ManagementServer) handleDebugActorsImpl(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeDebugError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	includeSystem := false
	if raw := r.URL.Query().Get("system"); raw != "" {
		switch raw {
		case "true":
			includeSystem = true
		case "false":
			includeSystem = false
		default:
			writeDebugError(w, http.StatusBadRequest, "system must be true or false")
			return
		}
	}
	entries := ms.debug.Actors(includeSystem)
	writeDebugEnvelope(w, "actors", entries, nil)
}
