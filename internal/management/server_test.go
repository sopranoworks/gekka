/*
 * server_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package management_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/sopranoworks/gekka/cluster"
	"github.com/sopranoworks/gekka/internal/core"
	"github.com/sopranoworks/gekka/internal/management"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// ── Mock provider ─────────────────────────────────────────────────────────────

// mockProvider satisfies management.ClusterStateProvider using an in-memory
// gossip state.  Write operations record which method was called and with which
// address so that tests can assert on them.
type mockProvider struct {
	cm *cluster.ClusterManager

	mu            sync.Mutex
	leaveCalledOn []string
	downCalledOn  []string
	leaveErr      error // if set, LeaveMember returns this error
	downErr       error // if set, DownMember returns this error
	quarantined   bool  // simulates a quarantined Artery association
}

func (p *mockProvider) ClusterManager() *cluster.ClusterManager { return p.cm }
func (p *mockProvider) NodeManager() *core.NodeManager          { return nil }
func (p *mockProvider) HasQuarantinedAssociation() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.quarantined
}

func (p *mockProvider) LeaveMember(address string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.leaveErr != nil {
		return p.leaveErr
	}
	p.leaveCalledOn = append(p.leaveCalledOn, address)
	return nil
}

func (p *mockProvider) DownMember(address string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.downErr != nil {
		return p.downErr
	}
	p.downCalledOn = append(p.downCalledOn, address)
	return nil
}

// ── Helpers ───────────────────────────────────────────────────────────────────

type memberSpec struct {
	host   string
	port   uint32
	system string
	status gproto_cluster.MemberStatus
	roles  []string
}

// buildGossip returns a ClusterManager whose State contains the provided
// members so that handler tests have a predictable member list.
func buildGossip(members []memberSpec) *cluster.ClusterManager {
	remoteUA := &gproto_remote.UniqueAddress{
		Address: &gproto_remote.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String(members[0].system),
			Hostname: proto.String(members[0].host),
			Port:     proto.Uint32(members[0].port),
		},
		Uid: proto.Uint64(1),
	}
	cm := cluster.NewClusterManager(
		core.ToClusterUniqueAddress(remoteUA),
		func(_ context.Context, _ string, _ any) error { return nil },
	)

	allAddresses := make([]*gproto_cluster.UniqueAddress, 0, len(members))
	allRoles := []string{}
	roleIndex := map[string]int32{}
	protoMembers := make([]*gproto_cluster.Member, 0, len(members))

	for i, mb := range members {
		ua := &gproto_cluster.UniqueAddress{
			Address: &gproto_cluster.Address{
				Protocol: proto.String("pekko"),
				System:   proto.String(mb.system),
				Hostname: proto.String(mb.host),
				Port:     proto.Uint32(mb.port),
			},
			Uid: proto.Uint32(uint32(i + 1)),
		}
		allAddresses = append(allAddresses, ua)

		var roleIdxs []int32
		for _, r := range mb.roles {
			idx, ok := roleIndex[r]
			if !ok {
				idx = int32(len(allRoles))
				allRoles = append(allRoles, r)
				roleIndex[r] = idx
			}
			roleIdxs = append(roleIdxs, idx)
		}

		st := mb.status
		protoMembers = append(protoMembers, &gproto_cluster.Member{
			AddressIndex: proto.Int32(int32(i)),
			Status:       &st,
			UpNumber:     proto.Int32(int32(i + 1)),
			RolesIndexes: roleIdxs,
		})
	}

	cm.Mu.Lock()
	cm.State.AllAddresses = allAddresses
	cm.State.AllRoles = allRoles
	cm.State.Members = protoMembers
	cm.State.Overview = &gproto_cluster.GossipOverview{}
	cm.Mu.Unlock()

	return cm
}

// markUnreachable sets addressIndex as unreachable from the local observer
// (index 0) in the gossip state.  Used to simulate SBR instability.
func markUnreachable(cm *cluster.ClusterManager, addressIndex int32) {
	unreachable := gproto_cluster.ReachabilityStatus_Unreachable
	cm.Mu.Lock()
	cm.State.Overview = &gproto_cluster.GossipOverview{
		ObserverReachability: []*gproto_cluster.ObserverReachability{
			{
				AddressIndex: proto.Int32(0), // local node is observer
				Version:      proto.Int64(1),
				SubjectReachability: []*gproto_cluster.SubjectReachability{
					{
						AddressIndex: proto.Int32(addressIndex),
						Status:       &unreachable,
						Version:      proto.Int64(1),
					},
				},
			},
		},
	}
	cm.Mu.Unlock()
}

// newHandler builds a ManagementServer and returns it as an http.Handler.
// The server binds a real listener on port 0 (immediately freed after the
// test) but all requests are exercised via httptest.NewRecorder so no network
// I/O is needed.
func newHandler(t *testing.T, p *mockProvider) http.Handler {
	t.Helper()
	ms, err := management.NewManagementServer(p, "127.0.0.1", 0)
	if err != nil {
		t.Fatalf("NewManagementServer: %v", err)
	}
	return ms
}

// memberURL returns the /cluster/members/{encoded-address} path for addr.
// Artery addresses contain "://" and "@" which must be percent-encoded so that
// Go's ServeMux does not clean the double-slash into a single slash (301).
func memberURL(addr string) string {
	return "/cluster/members/" + url.PathEscape(addr)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestGetMembers_ReturnsList(t *testing.T) {
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{"backend"}},
			{"127.0.0.2", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
	}
	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodGet, "/cluster/members", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var members []management.MemberInfo
	if err := json.Unmarshal(w.Body.Bytes(), &members); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if len(members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(members))
	}
	if len(members[0].Roles) != 1 || members[0].Roles[0] != "backend" {
		t.Errorf("expected roles [backend], got %v", members[0].Roles)
	}
	for _, m := range members {
		if !m.Reachable {
			t.Errorf("member %s should be reachable", m.Address)
		}
		if m.Status != "Up" {
			t.Errorf("member %s: expected status Up, got %s", m.Address, m.Status)
		}
	}
}

func TestGetMember_Found(t *testing.T) {
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
	}
	h := newHandler(t, p)

	target := "pekko://GekkaSystem@127.0.0.1:2552"
	req := httptest.NewRequest(http.MethodGet, memberURL(target), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var info management.MemberInfo
	if err := json.Unmarshal(w.Body.Bytes(), &info); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if info.Address != target {
		t.Errorf("expected address %q, got %q", target, info.Address)
	}
}

func TestGetMember_NotFound(t *testing.T) {
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
	}
	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodGet, memberURL("pekko://GekkaSystem@9.9.9.9:2552"), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestPutMember_LeaveCalled(t *testing.T) {
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
	}
	h := newHandler(t, p)

	target := "pekko://GekkaSystem@127.0.0.1:2552"
	req := httptest.NewRequest(http.MethodPut, memberURL(target), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.leaveCalledOn) != 1 || p.leaveCalledOn[0] != target {
		t.Errorf("LeaveMember not called with %q: %v", target, p.leaveCalledOn)
	}
	if len(p.downCalledOn) != 0 {
		t.Errorf("DownMember should not be called: %v", p.downCalledOn)
	}
}

func TestDeleteMember_DownCalled(t *testing.T) {
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
	}
	h := newHandler(t, p)

	target := "pekko://GekkaSystem@127.0.0.1:2552"
	req := httptest.NewRequest(http.MethodDelete, memberURL(target), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.downCalledOn) != 1 || p.downCalledOn[0] != target {
		t.Errorf("DownMember not called with %q: %v", target, p.downCalledOn)
	}
	if len(p.leaveCalledOn) != 0 {
		t.Errorf("LeaveMember should not be called: %v", p.leaveCalledOn)
	}
}

func TestPutMember_NotFound(t *testing.T) {
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
		leaveErr: fmt.Errorf("management: member %q not found in cluster", "pekko://GekkaSystem@9.9.9.9:2552"),
	}
	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodPut, memberURL("pekko://GekkaSystem@9.9.9.9:2552"), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestDeleteMember_NotFound(t *testing.T) {
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
		downErr: fmt.Errorf("management: member %q not found in cluster", "pekko://GekkaSystem@9.9.9.9:2552"),
	}
	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodDelete, memberURL("pekko://GekkaSystem@9.9.9.9:2552"), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestPutMember_InternalError(t *testing.T) {
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
		leaveErr: fmt.Errorf("management: send Leave to %q: connection refused", "pekko://GekkaSystem@127.0.0.1:2552"),
	}
	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodPut, memberURL("pekko://GekkaSystem@127.0.0.1:2552"), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", w.Code, w.Body.String())
	}
}

func TestMethodNotAllowed_Members(t *testing.T) {
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
	}
	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodPost, "/cluster/members", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestMemberInfo_DCRole(t *testing.T) {
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{"dc-us-east", "worker"}},
		}),
	}
	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodGet, "/cluster/members", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	var members []management.MemberInfo
	if err := json.Unmarshal(w.Body.Bytes(), &members); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(members))
	}
	if members[0].DataCenter != "us-east" {
		t.Errorf("expected dc=us-east, got %q", members[0].DataCenter)
	}
	if len(members[0].Roles) != 1 || members[0].Roles[0] != "worker" {
		t.Errorf("expected roles=[worker], got %v", members[0].Roles)
	}
}

// ── Health check tests ────────────────────────────────────────────────────────

func TestHealthAlive_AlwaysOK(t *testing.T) {
	// Liveness probe must return 200 regardless of cluster state.
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Joining, []string{}},
		}),
	}
	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodGet, "/health/alive", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "alive") {
		t.Errorf("expected body to contain 'alive', got: %s", w.Body.String())
	}
}

func TestHealthReady_NodeUp_NoUnreachable(t *testing.T) {
	// Node is Up with no unreachable members — should be ready.
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
	}
	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "ready") {
		t.Errorf("expected body to contain 'ready', got: %s", w.Body.String())
	}
}

func TestHealthReady_NodeNotUp(t *testing.T) {
	// Node is still Joining — not ready yet.
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Joining, []string{}},
		}),
	}
	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "not_up") {
		t.Errorf("expected reason 'not_up', got: %s", w.Body.String())
	}
}

func TestHealthReady_UnreachableMembers(t *testing.T) {
	// Node is Up but there is an unreachable member — SBR instability.
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
			{"127.0.0.2", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
	}
	// Mark the second member (index 1) as unreachable.
	markUnreachable(p.cm, 1)

	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "unreachable_members") {
		t.Errorf("expected reason 'unreachable_members', got: %s", w.Body.String())
	}
}

func TestHealthReady_Quarantined(t *testing.T) {
	// Node is Up but has a quarantined Artery association — not safe.
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
		quarantined: true,
	}
	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "quarantined") {
		t.Errorf("expected reason 'quarantined', got: %s", w.Body.String())
	}
}

func TestHealthReady_QuarantinedTakesPriorityOverUnreachable(t *testing.T) {
	// Both quarantined AND unreachable — quarantined reason wins (more severe).
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
			{"127.0.0.2", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
		quarantined: true,
	}
	markUnreachable(p.cm, 1)

	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "quarantined") {
		t.Errorf("expected reason 'quarantined', got: %s", w.Body.String())
	}
}

func TestHealthChecks_DisabledByDefault_WhenExplicitlyDisabled(t *testing.T) {
	// When healthChecks=false, /health/* should return 404.
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
	}
	ms, err := management.NewManagementServer(p, "127.0.0.1", 0, false)
	if err != nil {
		t.Fatalf("NewManagementServer: %v", err)
	}

	for _, path := range []string{"/health/alive", "/health/ready"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		ms.ServeHTTP(w, req)
		if w.Code != http.StatusNotFound {
			t.Errorf("%s with health disabled: expected 404, got %d", path, w.Code)
		}
	}
}

func TestHealthAlive_MethodNotAllowed(t *testing.T) {
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
	}
	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodPost, "/health/alive", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}
