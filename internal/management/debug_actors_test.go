/*
 * debug_actors_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// fakeActorProvider is a DebugProvider that returns configurable actor data.
// CRDT methods return nil/nil so it can stand in wherever a provider is needed
// but only actor handlers are exercised.
type fakeActorProvider struct {
	entries []ActorEntry
}

func (f *fakeActorProvider) CRDTList() []CRDTEntry             { return nil }
func (f *fakeActorProvider) CRDT(k string) (*CRDTValue, error) { return nil, nil }
func (f *fakeActorProvider) Actors(includeSystem bool) []ActorEntry {
	if includeSystem {
		return f.entries
	}
	out := make([]ActorEntry, 0, len(f.entries))
	for _, e := range f.entries {
		if e.Kind != "system" {
			out = append(out, e)
		}
	}
	return out
}

func TestHandleDebugActors_UserOnlyByDefault(t *testing.T) {
	ms, err := NewManagementServer(newMockClusterProvider(), "127.0.0.1", 0)
	if err != nil {
		t.Fatalf("NewManagementServer: %v", err)
	}
	t.Cleanup(func() { _ = ms.listener.Close() })
	ms.EnableDebug(&fakeActorProvider{
		entries: []ActorEntry{
			{Path: "/user/alpha", Kind: "user"},
			{Path: "/system/cluster/core/daemon", Kind: "system"},
		},
	})

	req := httptest.NewRequest("GET", "/cluster/debug/actors", nil)
	rec := httptest.NewRecorder()
	ms.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var got struct {
		Kind string       `json:"kind"`
		Data []ActorEntry `json:"data"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	if got.Kind != "actors" {
		t.Errorf("kind = %q, want actors", got.Kind)
	}
	if len(got.Data) != 1 || got.Data[0].Path != "/user/alpha" {
		t.Errorf("data = %+v", got.Data)
	}
}

func TestHandleDebugActors_IncludeSystem(t *testing.T) {
	ms, err := NewManagementServer(newMockClusterProvider(), "127.0.0.1", 0)
	if err != nil {
		t.Fatalf("NewManagementServer: %v", err)
	}
	t.Cleanup(func() { _ = ms.listener.Close() })
	ms.EnableDebug(&fakeActorProvider{
		entries: []ActorEntry{
			{Path: "/user/alpha", Kind: "user"},
			{Path: "/system/cluster/core/daemon", Kind: "system"},
		},
	})

	req := httptest.NewRequest("GET", "/cluster/debug/actors?system=true", nil)
	rec := httptest.NewRecorder()
	ms.ServeHTTP(rec, req)

	var got struct {
		Data []ActorEntry `json:"data"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	if len(got.Data) != 2 {
		t.Errorf("expected 2 entries with system=true, got %d", len(got.Data))
	}
}

func TestHandleDebugActors_BadSystemParam(t *testing.T) {
	ms, err := NewManagementServer(newMockClusterProvider(), "127.0.0.1", 0)
	if err != nil {
		t.Fatalf("NewManagementServer: %v", err)
	}
	t.Cleanup(func() { _ = ms.listener.Close() })
	ms.EnableDebug(&fakeActorProvider{})

	req := httptest.NewRequest("GET", "/cluster/debug/actors?system=maybe", nil)
	rec := httptest.NewRecorder()
	ms.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}
