/*
 * server.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package management implements the Cluster HTTP Management API.
//
// Additional endpoints added in Phase 13:
//
//	GET /cluster/services
//	    Returns a JSON object mapping service names to their registered address
//	    slices, sourced from the Distributed Data ServiceDiscovery ORSets.
//
//	GET /cluster/config
//	    Returns the current cluster-wide key/value configuration as a JSON
//	    object, sourced from the Distributed Data ConfigRegistry LWWMap.
//
//	POST /cluster/config
//	    Updates a single configuration key.  Body: {"key":"<k>","value":<v>}.
//
//	GET /cluster/sharding/{typeName}
//	    Returns the current shard→region allocation map for the given entity
//	    type, sourced from the registered ShardCoordinator snapshot.
//	    Responds 404 when no coordinator has been registered for typeName.
//
//	GET /dashboard
//	    A minimal browser dashboard that renders all of the above in real time.
//
// Package management implements the Cluster HTTP Management API.
//
// It exposes REST endpoints that allow operators and tools to inspect and
// manage the live cluster state without having to connect over the Artery
// protocol.  The endpoints follow Pekko Management HTTP API conventions.
//
// Read endpoints:
//
//	GET /cluster/members
//	    Returns a JSON array of all known cluster members with their current
//	    status, roles, data-center label, and reachability.
//
//	GET /cluster/members/{address}
//	    Returns detailed JSON for the member identified by the URL-encoded
//	    Artery address (e.g. "pekko%3A%2F%2FSystem%40127.0.0.1%3A2552").
//	    Responds 404 when no matching member is found.
//
// Write endpoints:
//
//	PUT /cluster/members/{address}
//	    Initiates a graceful leave for the named member.
//	    Returns 200 on success, 404 when the address is unknown, 500 on failure.
//
//	DELETE /cluster/members/{address}
//	    Marks the named member as Down immediately.
//	    Returns 200 on success, 404 when the address is unknown, 500 on failure.
package management

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sopranoworks/gekka/cluster"
	"github.com/sopranoworks/gekka/internal/core"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
)

//go:embed dashboard.html
var dashboardHTML []byte

// ClusterStateProvider is the subset of the Cluster that the ManagementServer
// needs.  Keeping this narrow allows the server to be tested with a lightweight
// stub rather than a full Cluster.
type ClusterStateProvider interface {
	ClusterManager() *cluster.ClusterManager
	NodeManager() *core.NodeManager

	// LeaveMember initiates a graceful leave for the member at the given
	// Artery address string (e.g. "pekko://System@127.0.0.1:2552").
	// Returns an error when the address is unknown or delivery fails.
	LeaveMember(address string) error

	// DownMember marks the member at the given Artery address as Down.
	// Returns an error when the address is unknown.
	DownMember(address string) error

	// HasQuarantinedAssociation reports whether any Artery association is in
	// QUARANTINED state — a sign of a UID conflict caused by a node restart
	// or network split.  Used by the /health/ready probe.
	HasQuarantinedAssociation() bool

	// Services returns all currently registered service names and their
	// address slices, sourced from the Distributed Data ORSets.
	Services() map[string][]string

	// ConfigEntries returns the current cluster-wide key/value configuration
	// sourced from the Distributed Data LWWMap.
	ConfigEntries() map[string]any

	// UpdateConfigEntry sets configKey to value in the cluster configuration.
	UpdateConfigEntry(configKey string, value any)

	// ShardDistribution returns the shard→region allocation map for the given
	// entity type.  Returns nil, false when no coordinator is registered for
	// typeName (i.e. this node is not the coordinator for that type, or
	// StartSharding has not been called).
	ShardDistribution(typeName string) (map[string]string, bool)

	// RebalanceShard sends a manual rebalance request to the coordinator for
	// typeName, instructing it to move shardId to targetRegion.
	// Returns an error when typeName has no registered coordinator, or when
	// shardId / targetRegion validation fails inside the coordinator.
	RebalanceShard(typeName, shardId, targetRegion string) error
}

// MemberInfo is the JSON representation of a single cluster member returned by
// both list and detail endpoints.
type MemberInfo struct {
	Address    string   `json:"address"`
	Status     string   `json:"status"`
	Roles      []string `json:"roles"`
	DataCenter string   `json:"dc"`
	UpNumber   int32    `json:"upNumber"`
	Reachable  bool     `json:"reachable"`
	LatencyMs  int64    `json:"latency_ms"`
	Self       bool     `json:"self"`
}

// ManagementServer hosts the Cluster HTTP Management API.
type ManagementServer struct {
	provider     ClusterStateProvider
	srv          *http.Server
	listener     net.Listener
	healthChecks bool // whether /health/* endpoints are registered

	// shuttingDown is set to 1 atomically when the coordinated-shutdown
	// sequence enters PhaseServiceUnbind.  Once set, /health/ready returns
	// 503 with reason "shutting_down" immediately, signalling Kubernetes to
	// stop routing traffic to this node before the cluster Leave is issued.
	shuttingDown atomic.Bool
}

// SetShuttingDown marks the server as entering the shutdown sequence.
// After this call every subsequent /health/ready request returns 503 with
// reason "shutting_down", regardless of actual cluster membership state.
//
// This should be called at the start of PhaseServiceUnbind, before
// PhaseShardingShutdownRegion and PhaseClusterLeave execute, so that the
// load-balancer drains connections well before the node stops processing
// cluster traffic.
func (ms *ManagementServer) SetShuttingDown() {
	ms.shuttingDown.Store(true)
}

// NewManagementServer creates a ManagementServer that will listen on
// hostname:port.  When healthChecks is true the /health/alive and
// /health/ready Kubernetes probe endpoints are also registered.
func NewManagementServer(provider ClusterStateProvider, hostname string, port int, healthChecks ...bool) (*ManagementServer, error) {
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", hostname, port))
	if err != nil {
		return nil, fmt.Errorf("management: listen on %s:%d: %w", hostname, port, err)
	}
	slog.Info("gekka: management server listening", "address", ln.Addr().String())

	enableHealth := len(healthChecks) == 0 || healthChecks[0] // default true
	ms := &ManagementServer{
		provider:     provider,
		listener:     ln,
		healthChecks: enableHealth,
	}

	mux := http.NewServeMux()
	// NOTE: Go's default mux matches the longest prefix, so registering both
	// "/cluster/members/" (trailing slash) and "/cluster/members" covers both
	// the list endpoint and any sub-path (address lookup / write operations).
	mux.HandleFunc("/cluster/members/", ms.handleMemberByAddress)
	mux.HandleFunc("/cluster/members", ms.handleMembers)
	mux.HandleFunc("/cluster/services", ms.handleServices)
	mux.HandleFunc("/cluster/config", ms.handleConfig)
	mux.HandleFunc("/cluster/sharding/", ms.handleSharding)
	mux.HandleFunc("/dashboard", ms.handleDashboard)

	if enableHealth {
		mux.HandleFunc("/health/alive", ms.handleAlive)
		mux.HandleFunc("/health/ready", ms.handleReady)
	}

	ms.srv = &http.Server{
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	return ms, nil
}

// Addr returns the TCP address the server is bound to.
func (ms *ManagementServer) Addr() net.Addr {
	return ms.listener.Addr()
}

// Start begins serving HTTP requests in the background and arranges graceful
// shutdown when ctx is cancelled. It returns immediately.
func (ms *ManagementServer) Start(ctx context.Context) {
	addr := ms.listener.Addr().String()

	// Start the server in a background goroutine immediately.
	go func() {
		if err := ms.srv.Serve(ms.listener); err != nil && err != http.ErrServerClosed {
			slog.Error("gekka: management server serve error", "error", err)
		}
	}()

	// Background goroutine for graceful shutdown.
	go func() {
		<-ctx.Done()
		slog.Info("gekka: management server shutting down", "address", addr)
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := ms.srv.Shutdown(shutCtx); err != nil {
			slog.Error("gekka: management server shutdown error", "error", err)
		}
	}()
}

// Stop gracefully shuts down the HTTP server, waiting at most until ctx expires.
func (ms *ManagementServer) Stop(ctx context.Context) error {
	return ms.srv.Shutdown(ctx)
}

// ServeHTTP implements http.Handler, delegating to the underlying mux.
// This allows ManagementServer to be used directly with httptest.NewServer in tests.
func (ms *ManagementServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ms.srv.Handler.ServeHTTP(w, r)
}

// ── Handlers ──────────────────────────────────────────────────────────────────

// handleMembers serves GET /cluster/members.
func (ms *ManagementServer) handleMembers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	members := ms.buildMemberList()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(members) //nolint:errcheck
}

// handleMemberByAddress routes requests to /cluster/members/{address} based
// on the HTTP method:
//
//	GET    — return the member's current status (or 404)
//	PUT    — initiate graceful leave for that member
//	DELETE — mark that member as Down immediately
func (ms *ManagementServer) handleMemberByAddress(w http.ResponseWriter, r *http.Request) {
	// Strip the "/cluster/members/" prefix to get the address token.
	// Use RawPath when available (preserves percent-encoding before mux cleans it),
	// then percent-decode to obtain the actual Artery address string.
	pathSrc := r.URL.RawPath
	if pathSrc == "" {
		pathSrc = r.URL.Path
	}
	encoded := strings.TrimPrefix(pathSrc, "/cluster/members/")
	decoded, err := url.PathUnescape(encoded)
	if err != nil {
		http.Error(w, "bad request: invalid address encoding", http.StatusBadRequest)
		return
	}
	raw := strings.TrimSpace(decoded)
	if raw == "" {
		// Treat bare /cluster/members/ as a list request (GET only).
		ms.handleMembers(w, r)
		return
	}

	switch r.Method {
	case http.MethodGet:
		ms.handleGetMember(w, raw)
	case http.MethodPut:
		ms.handleLeaveMember(w, raw)
	case http.MethodDelete:
		ms.handleDownMember(w, raw)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleGetMember returns detailed JSON for the named member, or 404.
func (ms *ManagementServer) handleGetMember(w http.ResponseWriter, address string) {
	members := ms.buildMemberList()
	for i := range members {
		if members[i].Address == address {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			enc := json.NewEncoder(w)
			enc.SetIndent("", "  ")
			enc.Encode(members[i]) //nolint:errcheck
			return
		}
	}
	writeNotFound(w, address)
}

// handleLeaveMember serves PUT /cluster/members/{address} — initiates a
// graceful leave for the named member.
func (ms *ManagementServer) handleLeaveMember(w http.ResponseWriter, address string) {
	if err := ms.provider.LeaveMember(address); err != nil {
		if isNotFound(err) {
			writeNotFound(w, address)
			return
		}
		http.Error(w,
			fmt.Sprintf(`{"error":%q}`+"\n", err.Error()),
			http.StatusInternalServerError,
		)
		return
	}
	writeOK(w, fmt.Sprintf("leave initiated for %s", address))
}

// handleDownMember serves DELETE /cluster/members/{address} — marks the named
// member as Down immediately.
func (ms *ManagementServer) handleDownMember(w http.ResponseWriter, address string) {
	if err := ms.provider.DownMember(address); err != nil {
		if isNotFound(err) {
			writeNotFound(w, address)
			return
		}
		http.Error(w,
			fmt.Sprintf(`{"error":%q}`+"\n", err.Error()),
			http.StatusInternalServerError,
		)
		return
	}
	writeOK(w, fmt.Sprintf("member %s marked as Down", address))
}

// ── Service-discovery handler ─────────────────────────────────────────────────

// handleServices serves GET /cluster/services — returns all registered service
// names and their address lists from the Distributed Data ORSets.
func (ms *ManagementServer) handleServices(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	svc := ms.provider.Services()
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(svc) //nolint:errcheck
}

// ── Config handler ────────────────────────────────────────────────────────────

// handleConfig serves GET and POST /cluster/config.
//
//   - GET  — returns the full cluster configuration map.
//   - POST — updates a single key; body must be {"key":"<k>","value":<v>}.
func (ms *ManagementServer) handleConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		entries := ms.provider.ConfigEntries()
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(entries) //nolint:errcheck

	case http.MethodPost:
		var body struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		if body.Key == "" {
			http.Error(w, `bad request: "key" is required`, http.StatusBadRequest)
			return
		}
		ms.provider.UpdateConfigEntry(body.Key, body.Value)
		writeOK(w, fmt.Sprintf("config[%s] updated", body.Key))

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// ── Sharding handler ──────────────────────────────────────────────────────────

// handleSharding dispatches requests under /cluster/sharding/:
//
//	GET  /cluster/sharding/{typeName}           → shard distribution map
//	POST /cluster/sharding/{typeName}/rebalance → manual shard rebalance
func (ms *ManagementServer) handleSharding(w http.ResponseWriter, r *http.Request) {
	tail := strings.TrimPrefix(r.URL.Path, "/cluster/sharding/")

	// POST .../rebalance
	if r.Method == http.MethodPost && strings.HasSuffix(tail, "/rebalance") {
		typeName := strings.TrimSuffix(tail, "/rebalance")
		if typeName == "" {
			http.Error(w, "bad request: typeName is required", http.StatusBadRequest)
			return
		}
		ms.handleShardingRebalance(w, r, typeName)
		return
	}

	// GET .../typeName
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	typeName := tail
	if typeName == "" {
		http.Error(w, `bad request: typeName is required`, http.StatusBadRequest)
		return
	}
	dist, ok := ms.provider.ShardDistribution(typeName)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, `{"error":"no coordinator registered for type %q"}`+"\n", typeName) //nolint:errcheck
		return
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(dist) //nolint:errcheck
}

// handleShardingRebalance serves POST /cluster/sharding/{typeName}/rebalance.
// Body: {"shard_id":"<id>","target_region":"<actorPath>"}
func (ms *ManagementServer) handleShardingRebalance(w http.ResponseWriter, r *http.Request, typeName string) {
	var body struct {
		ShardID      string `json:"shard_id"`
		TargetRegion string `json:"target_region"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if body.ShardID == "" || body.TargetRegion == "" {
		http.Error(w, "bad request: shard_id and target_region are required", http.StatusBadRequest)
		return
	}
	if err := ms.provider.RebalanceShard(typeName, body.ShardID, body.TargetRegion); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, `{"status":"rebalance initiated","shard_id":%q,"target_region":%q}`+"\n", //nolint:errcheck
		body.ShardID, body.TargetRegion)
}

// ── Dashboard handler ─────────────────────────────────────────────────────────

// handleDashboard serves GET /dashboard — returns the embedded HTML dashboard.
func (ms *ManagementServer) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write(dashboardHTML) //nolint:errcheck
}

// ── Health-check handlers ─────────────────────────────────────────────────────

// handleAlive serves GET /health/alive — the Kubernetes liveness probe.
// Always returns 200 OK as long as the HTTP server is responsive.
func (ms *ManagementServer) handleAlive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, `{"status":"alive"}`+"\n") //nolint:errcheck
}

// handleReady serves GET /health/ready — the Kubernetes readiness probe.
// Returns 200 OK only when the local node is healthy for cluster operations:
//
//   - The local node's membership status is Up.
//   - No cluster members are marked as unreachable (SBR unstable state).
//   - No Artery associations are in QUARANTINED state.
//
// Returns 503 with a JSON body describing the reason otherwise.
func (ms *ManagementServer) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	reason := ms.readinessReason()
	w.Header().Set("Content-Type", "application/json")
	if reason == "" {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"ready"}`+"\n") //nolint:errcheck
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	fmt.Fprintf(w, `{"status":"not_ready","reason":%q}`+"\n", reason) //nolint:errcheck
}

// readinessReason returns a non-empty string describing why the node is not
// ready, or "" when the node is fully ready.  The checks are ordered from most
// to least severe so the most actionable reason is always reported.
func (ms *ManagementServer) readinessReason() string {
	cm := ms.provider.ClusterManager()

	// 0. Coordinated-shutdown in progress: the node is intentionally leaving
	//    the cluster.  Return immediately so the load-balancer stops routing
	//    before shards are handed off and the Leave message is sent.
	if ms.shuttingDown.Load() {
		return "shutting_down"
	}

	// 1. Quarantine takes highest priority: a quarantined association means a
	//    UID conflict was detected — the node should be restarted.
	if ms.provider.HasQuarantinedAssociation() {
		return "quarantined"
	}

	// 2. SBR instability: unreachable members mean a network partition may be
	//    in progress.  The node should not accept traffic until the partition
	//    is resolved.
	cm.Mu.RLock()
	unreachable := buildUnreachableSet(cm.State)
	cm.Mu.RUnlock()
	if len(unreachable) > 0 {
		return "unreachable_members"
	}

	// 3. Node not yet Up in the cluster.
	if !cm.IsUp() {
		return "not_up"
	}

	return ""
}

// ── Response helpers ──────────────────────────────────────────────────────────

func writeOK(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"message":%q}`+"\n", message) //nolint:errcheck
}

func writeNotFound(w http.ResponseWriter, address string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	fmt.Fprintf(w, `{"error":"member not found","address":%q}`+"\n", address) //nolint:errcheck
}

// isNotFound reports whether err indicates a "member not found" condition,
// as returned by LeaveMember and DownMember when the address is unknown.
func isNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), "not found")
}

// ── Internal helpers ──────────────────────────────────────────────────────────

// buildMemberList reads the current gossip state under the ClusterManager's
// read lock and returns one MemberInfo per member.
func (ms *ManagementServer) buildMemberList() []MemberInfo {
	cm := ms.provider.ClusterManager()
	nm := ms.provider.NodeManager()
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()

	gossip := cm.State
	if gossip == nil {
		return []MemberInfo{}
	}

	unreachable := buildUnreachableSet(gossip)

	result := make([]MemberInfo, 0, len(gossip.GetMembers()))
	for _, m := range gossip.GetMembers() {
		result = append(result, memberInfoFromProto(gossip, m, unreachable, nm, cm))
	}
	return result
}

// buildUnreachableSet returns the set of address indices that the local node
// (observer index 0) considers unreachable.
func buildUnreachableSet(gossip *gproto_cluster.Gossip) map[int32]struct{} {
	unreachable := make(map[int32]struct{})
	if gossip.GetOverview() == nil {
		return unreachable
	}
	for _, obs := range gossip.GetOverview().GetObserverReachability() {
		if obs.GetAddressIndex() != 0 {
			continue
		}
		for _, subj := range obs.GetSubjectReachability() {
			if subj.GetStatus() == gproto_cluster.ReachabilityStatus_Unreachable {
				unreachable[subj.GetAddressIndex()] = struct{}{}
			}
		}
	}
	return unreachable
}

// memberInfoFromProto converts a proto Member into a MemberInfo.
// Caller must hold cm.Mu.RLock.
func memberInfoFromProto(
	gossip *gproto_cluster.Gossip,
	m *gproto_cluster.Member,
	unreachable map[int32]struct{},
	nm *core.NodeManager,
	cm *cluster.ClusterManager,
) MemberInfo {
	addrIdx := m.GetAddressIndex()

	address := ""
	var host string
	var port uint32
	isSelf := false
	if int(addrIdx) < len(gossip.GetAllAddresses()) {
		ua := gossip.GetAllAddresses()[addrIdx]
		if a := ua.GetAddress(); a != nil {
			host = a.GetHostname()
			port = a.GetPort()
			scheme := strings.TrimSuffix(a.GetProtocol(), "://")
			if scheme == "" {
				scheme = "pekko"
			}
			address = fmt.Sprintf("%s://%s@%s:%d",
				scheme,
				a.GetSystem(),
				host,
				port,
			)
		}
		if cm != nil && cm.LocalAddress != nil {
			local := cm.LocalAddress
			if ua.GetUid() == local.GetUid() &&
				ua.GetUid2() == local.GetUid2() &&
				ua.GetAddress().GetHostname() == local.GetAddress().GetHostname() &&
				ua.GetAddress().GetPort() == local.GetAddress().GetPort() {
				isSelf = true
			}
		}
	}

	roles := []string{}
	dc := "default"
	for _, idx := range m.GetRolesIndexes() {
		if int(idx) < len(gossip.GetAllRoles()) {
			role := gossip.GetAllRoles()[idx]
			if strings.HasPrefix(role, "dc-") {
				dc = strings.TrimPrefix(role, "dc-")
			} else {
				roles = append(roles, role)
			}
		}
	}

	_, isUnreachable := unreachable[addrIdx]

	var latencyMs int64 = 0
	if nm != nil && host != "" {
		if assoc, ok := nm.GetAssociationByHost(host, port); ok {
			if ga, ok := assoc.(*core.GekkaAssociation); ok {
				latencyMs = ga.GetRTT().Milliseconds()
			}
		}
	}

	return MemberInfo{
		Address:    address,
		Status:     strings.TrimPrefix(m.GetStatus().String(), "MemberStatus_"),
		Roles:      roles,
		DataCenter: dc,
		UpNumber:   m.GetUpNumber(),
		Reachable:  !isUnreachable,
		LatencyMs:  latencyMs,
		Self:       isSelf,
	}
}
