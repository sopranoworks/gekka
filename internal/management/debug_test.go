/*
 * debug_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package management

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWriteDebugEnvelope_Basic(t *testing.T) {
	rec := httptest.NewRecorder()
	writeDebugEnvelope(rec, "actors", []ActorEntry{
		{Path: "/user/foo", Kind: "user"},
	}, nil)

	if rec.Code != 200 {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("content-type = %q, want application/json", ct)
	}

	var got struct {
		Kind     string       `json:"kind"`
		Data     []ActorEntry `json:"data"`
		Warnings []string     `json:"warnings"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v; body=%q", err, rec.Body.String())
	}
	if got.Kind != "actors" {
		t.Errorf("kind = %q, want actors", got.Kind)
	}
	if len(got.Data) != 1 || got.Data[0].Path != "/user/foo" {
		t.Errorf("unexpected data: %+v", got.Data)
	}
}

func TestWriteDebugEnvelope_WithWarnings(t *testing.T) {
	rec := httptest.NewRecorder()
	writeDebugEnvelope(rec, "crdt", map[string]any{"x": 1},
		[]string{"unknown type encountered"})

	var got map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	warnings, _ := got["warnings"].([]any)
	if len(warnings) != 1 || warnings[0] != "unknown type encountered" {
		t.Errorf("warnings not preserved: %+v", got)
	}
}
