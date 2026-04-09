/*
 * debug_crdt_test.go
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

type fakeDebugProvider struct {
	list []CRDTEntry
	vals map[string]*CRDTValue
}

func (f *fakeDebugProvider) CRDTList() []CRDTEntry { return f.list }
func (f *fakeDebugProvider) CRDT(key string) (*CRDTValue, error) {
	v, ok := f.vals[key]
	if !ok {
		return nil, nil
	}
	return v, nil
}
func (f *fakeDebugProvider) Actors(includeSystem bool) []ActorEntry { return nil }

func TestHandleDebugCRDTList_OK(t *testing.T) {
	ms, err := NewManagementServer(newMockClusterProvider(), "127.0.0.1", 0)
	if err != nil {
		t.Fatalf("NewManagementServer: %v", err)
	}
	t.Cleanup(func() { _ = ms.listener.Close() })
	ms.EnableDebug(&fakeDebugProvider{
		list: []CRDTEntry{
			{Key: "alpha", Type: "gcounter"},
			{Key: "beta", Type: "orset"},
		},
	})

	req := httptest.NewRequest("GET", "/cluster/debug/crdt", nil)
	rec := httptest.NewRecorder()
	ms.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	var got struct {
		Kind string      `json:"kind"`
		Data []CRDTEntry `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Kind != "crdt-list" {
		t.Errorf("kind = %q, want crdt-list", got.Kind)
	}
	if len(got.Data) != 2 {
		t.Errorf("data len = %d, want 2", len(got.Data))
	}
}

func TestHandleDebugCRDTList_SortedAlphabetically(t *testing.T) {
	ms, err := NewManagementServer(newMockClusterProvider(), "127.0.0.1", 0)
	if err != nil {
		t.Fatalf("NewManagementServer: %v", err)
	}
	t.Cleanup(func() { _ = ms.listener.Close() })
	ms.EnableDebug(&fakeDebugProvider{
		list: []CRDTEntry{
			{Key: "zebra", Type: "orset"},
			{Key: "alpha", Type: "gcounter"},
			{Key: "mango", Type: "lwwmap"},
		},
	})

	req := httptest.NewRequest("GET", "/cluster/debug/crdt", nil)
	rec := httptest.NewRecorder()
	ms.ServeHTTP(rec, req)

	var got struct {
		Data []CRDTEntry `json:"data"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	if len(got.Data) != 3 {
		t.Fatalf("len = %d", len(got.Data))
	}
	if got.Data[0].Key != "alpha" || got.Data[1].Key != "mango" || got.Data[2].Key != "zebra" {
		t.Errorf("not sorted: %+v", got.Data)
	}
}
