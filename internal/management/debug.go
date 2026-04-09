/*
 * debug.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package management

import (
	"encoding/json"
	"net/http"
)

// debugEnvelope is the shared shape of every /cluster/debug/* success response.
type debugEnvelope struct {
	Kind     string      `json:"kind"`
	Data     interface{} `json:"data"`
	Warnings []string    `json:"warnings,omitempty"`
}

// writeDebugEnvelope writes a 200 JSON response with the standard envelope.
// Warnings may be nil; when non-nil it is emitted verbatim.
func writeDebugEnvelope(w http.ResponseWriter, kind string, data interface{}, warnings []string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(debugEnvelope{
		Kind:     kind,
		Data:     data,
		Warnings: warnings,
	})
}

// writeDebugError writes a plain JSON error body with the given status.
// Debug errors do NOT use the envelope — operators grep for "error" directly.
func writeDebugError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"error":` + jsonString(msg) + `}` + "\n"))
}

// jsonString marshals s as a JSON string, returning the quoted form
// (e.g. hello → "hello").  Falls back to an empty string on marshal error.
func jsonString(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		return `""`
	}
	return string(b)
}
