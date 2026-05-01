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
	"github.com/sopranoworks/gekka/persistence"
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

	// Phase-13 extensions
	services       map[string][]string
	configEntries  map[string]any
	configUpdated  map[string]any // captures UpdateConfigEntry calls
	shardDist      map[string]string
	shardDistFound bool

	// Phase-15 extensions
	rebalanceCalled []rebalanceCall
	rebalanceErr    error

	// Sub-plan 8d extensions — sharding healthcheck readiness probe.
	// Negative sense so the zero value yields ready=true and existing
	// /health/ready tests are not affected.
	shardingNotReady bool
	shardingReason   string
}

type rebalanceCall struct {
	typeName, shardID, targetRegion string
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

func (p *mockProvider) Services() map[string][]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.services == nil {
		return map[string][]string{}
	}
	return p.services
}

func (p *mockProvider) ConfigEntries() map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.configEntries == nil {
		return map[string]any{}
	}
	return p.configEntries
}

func (p *mockProvider) UpdateConfigEntry(configKey string, value any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.configUpdated == nil {
		p.configUpdated = make(map[string]any)
	}
	p.configUpdated[configKey] = value
	if p.configEntries == nil {
		p.configEntries = make(map[string]any)
	}
	p.configEntries[configKey] = value
}

func (p *mockProvider) ShardDistribution(typeName string) (map[string]string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.shardDist, p.shardDistFound
}

func (p *mockProvider) RebalanceShard(typeName, shardID, targetRegion string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.rebalanceErr != nil {
		return p.rebalanceErr
	}
	p.rebalanceCalled = append(p.rebalanceCalled, rebalanceCall{typeName, shardID, targetRegion})
	return nil
}

func (p *mockProvider) DurableStateStore() persistence.DurableStateStore { return nil }

func (p *mockProvider) ShardingHealthCheckReady() (bool, string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.shardingNotReady {
		return false, p.shardingReason
	}
	return true, ""
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

// ── Phase 13: services, config, sharding, dashboard ──────────────────────────

func TestGetServices_ReturnsMap(t *testing.T) {
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
		services: map[string][]string{
			"order-processor": {"10.0.0.1:8080", "10.0.0.2:8080"},
			"auth-service":    {"10.0.0.3:9090"},
		},
	}
	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodGet, "/cluster/services", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var result map[string][]string
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 services, got %d", len(result))
	}
	if len(result["order-processor"]) != 2 {
		t.Errorf("order-processor: expected 2 addresses, got %v", result["order-processor"])
	}
	if len(result["auth-service"]) != 1 || result["auth-service"][0] != "10.0.0.3:9090" {
		t.Errorf("auth-service: expected [10.0.0.3:9090], got %v", result["auth-service"])
	}
}

func TestGetServices_MethodNotAllowed(t *testing.T) {
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
	}
	h := newHandler(t, p)
	req := httptest.NewRequest(http.MethodPost, "/cluster/services", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestGetConfig_ReturnsEntries(t *testing.T) {
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
		configEntries: map[string]any{
			"log-level":    "DEBUG",
			"max-replicas": float64(3),
		},
	}
	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodGet, "/cluster/config", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var result map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if result["log-level"] != "DEBUG" {
		t.Errorf("expected log-level=DEBUG, got %v", result["log-level"])
	}
	if result["max-replicas"] != float64(3) {
		t.Errorf("expected max-replicas=3, got %v", result["max-replicas"])
	}
}

func TestPostConfig_UpdatesEntry(t *testing.T) {
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
	}
	h := newHandler(t, p)

	body := strings.NewReader(`{"key":"log-level","value":"WARN"}`)
	req := httptest.NewRequest(http.MethodPost, "/cluster/config", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.configUpdated["log-level"] != "WARN" {
		t.Errorf("expected UpdateConfigEntry called with log-level=WARN, got %v", p.configUpdated)
	}
}

func TestPostConfig_MissingKey(t *testing.T) {
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
	}
	h := newHandler(t, p)
	body := strings.NewReader(`{"value":"WARN"}`)
	req := httptest.NewRequest(http.MethodPost, "/cluster/config", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestGetSharding_Found(t *testing.T) {
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
		shardDist:      map[string]string{"0": "/user/shardRegion-Cart", "1": "/user/shardRegion-Cart"},
		shardDistFound: true,
	}
	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodGet, "/cluster/sharding/Cart", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var result map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 shards, got %d", len(result))
	}
	if result["0"] != "/user/shardRegion-Cart" {
		t.Errorf("shard 0: expected /user/shardRegion-Cart, got %q", result["0"])
	}
}

func TestGetSharding_NotFound(t *testing.T) {
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
		shardDistFound: false,
	}
	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodGet, "/cluster/sharding/Unknown", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetSharding_MethodNotAllowed(t *testing.T) {
	p := &mockProvider{
		cm:             buildGossip([]memberSpec{{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}}}),
		shardDistFound: false,
	}
	h := newHandler(t, p)
	req := httptest.NewRequest(http.MethodPost, "/cluster/sharding/Cart", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestDashboard_ReturnsHTML(t *testing.T) {
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
	}
	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Errorf("expected Content-Type text/html, got %q", ct)
	}
	if !strings.Contains(w.Body.String(), "Gekka Cluster Dashboard") {
		t.Errorf("dashboard body does not contain expected title")
	}
}

// ── Phase-15: POST /cluster/sharding/{typeName}/rebalance ─────────────────────

func TestPostShardingRebalance_Accepted(t *testing.T) {
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
		shardDistFound: true,
		shardDist:      map[string]string{"s1": "/user/region-A"},
	}
	h := newHandler(t, p)

	body := `{"shard_id":"s1","target_region":"/user/region-C"}`
	req := httptest.NewRequest(http.MethodPost, "/cluster/sharding/Cart/rebalance",
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
	p.mu.Lock()
	calls := p.rebalanceCalled
	p.mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("expected RebalanceShard called once, got %d", len(calls))
	}
	if calls[0].typeName != "Cart" || calls[0].shardID != "s1" || calls[0].targetRegion != "/user/region-C" {
		t.Errorf("unexpected call args: %+v", calls[0])
	}
}

func TestPostShardingRebalance_MissingFields(t *testing.T) {
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
	}
	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodPost, "/cluster/sharding/Cart/rebalance",
		strings.NewReader(`{"shard_id":"s1"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestPostShardingRebalance_MethodNotAllowed(t *testing.T) {
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
	}
	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodDelete, "/cluster/sharding/Cart/rebalance", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}


func TestHealthReady_FailsWhenShardingNotReady(t *testing.T) {
	// Node is Up, no unreachable, no quarantine, but the sharding
	// healthcheck (pekko.cluster.sharding.healthcheck.names) reports a
	// missing coordinator. /health/ready must surface the
	// provider-supplied reason verbatim.
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
		shardingNotReady: true,
		shardingReason:   `sharding_not_ready: sharding healthcheck: type "Cart" has no registered coordinator`,
	}
	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "sharding_not_ready") {
		t.Errorf("expected body to contain 'sharding_not_ready', got: %s", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `Cart`) {
		t.Errorf("expected body to surface provider-supplied detail, got: %s", w.Body.String())
	}
}

func TestHealthReady_PassesWhenShardingReady(t *testing.T) {
	// Node is Up, no unreachable, no quarantine, sharding healthcheck
	// reports ready. /health/ready must return 200 OK.
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
		// shardingNotReady defaults to false — provider returns ready=true.
	}
	h := newHandler(t, p)

	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"status":"ready"`) {
		t.Errorf(`expected body to contain `+"`"+`"status":"ready"`+"`"+`, got: %s`, w.Body.String())
	}
}

func TestHealthReady_QuarantinedTakesPriorityOverShardingNotReady(t *testing.T) {
	// Both quarantined and sharding-not-ready: the quarantined reason
	// must win because the sharding check is the lowest-priority gate.
	// This test guards the ordering of readinessReason checks: the
	// sharding gate runs LAST, so any earlier reason masks it.
	p := &mockProvider{
		cm: buildGossip([]memberSpec{
			{"127.0.0.1", 2552, "GekkaSystem", gproto_cluster.MemberStatus_Up, []string{}},
		}),
		quarantined:      true,
		shardingNotReady: true,
		shardingReason:   "sharding_not_ready: this should not appear",
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
	if strings.Contains(w.Body.String(), "sharding_not_ready") {
		t.Errorf("sharding reason leaked past higher-priority gate: %s", w.Body.String())
	}
}
