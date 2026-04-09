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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sopranoworks/gekka/cluster"
	"github.com/sopranoworks/gekka/internal/core"
	"github.com/sopranoworks/gekka/persistence"
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

// ── Helpers for route-registration tests ─────────────────────────────────────

// minimalClusterProvider satisfies ClusterStateProvider with no-op methods.
// It is used by tests that only care about HTTP routing, not cluster state.
type minimalClusterProvider struct{}

func (minimalClusterProvider) ClusterManager() *cluster.ClusterManager        { return nil }
func (minimalClusterProvider) NodeManager() *core.NodeManager                  { return nil }
func (minimalClusterProvider) LeaveMember(_ string) error                      { return nil }
func (minimalClusterProvider) DownMember(_ string) error                       { return nil }
func (minimalClusterProvider) HasQuarantinedAssociation() bool                 { return false }
func (minimalClusterProvider) Services() map[string][]string                   { return nil }
func (minimalClusterProvider) ConfigEntries() map[string]any                   { return nil }
func (minimalClusterProvider) UpdateConfigEntry(_ string, _ any)               {}
func (minimalClusterProvider) ShardDistribution(_ string) (map[string]string, bool) {
	return nil, false
}
func (minimalClusterProvider) RebalanceShard(_, _, _ string) error { return nil }
func (minimalClusterProvider) DurableStateStore() persistence.DurableStateStore {
	return nil
}

// newMockClusterProvider returns a minimal ClusterStateProvider for tests that
// only care about the HTTP surface, not cluster state.
func newMockClusterProvider() ClusterStateProvider {
	return minimalClusterProvider{}
}

// stubDebugProvider satisfies DebugProvider for route-registration tests.
type stubDebugProvider struct{}

func (stubDebugProvider) CRDTList() []CRDTEntry                  { return nil }
func (stubDebugProvider) CRDT(_ string) (*CRDTValue, error)      { return nil, nil }
func (stubDebugProvider) Actors(_ bool) []ActorEntry             { return nil }

func TestDebugDisabled_Returns404(t *testing.T) {
	ms, err := NewManagementServer(newMockClusterProvider(), "127.0.0.1", 0)
	if err != nil {
		t.Fatalf("NewManagementServer: %v", err)
	}
	t.Cleanup(func() { _ = ms.listener.Close() })

	req := httptest.NewRequest("GET", "/cluster/debug/crdt", nil)
	rec := httptest.NewRecorder()
	ms.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("disabled: got %d, want 404", rec.Code)
	}
}

func TestDebugEnabled_RoutesExist(t *testing.T) {
	ms, err := NewManagementServer(newMockClusterProvider(), "127.0.0.1", 0)
	if err != nil {
		t.Fatalf("NewManagementServer: %v", err)
	}
	t.Cleanup(func() { _ = ms.listener.Close() })

	ms.EnableDebug(stubDebugProvider{})

	req := httptest.NewRequest("GET", "/cluster/debug/crdt", nil)
	rec := httptest.NewRecorder()
	ms.ServeHTTP(rec, req)

	// 200 — route is registered and the real CRDT-list handler is now live
	// (Task 6 replaced the stub).
	if rec.Code != http.StatusOK {
		t.Errorf("enabled: got %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
}
