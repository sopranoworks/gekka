/*
 * server.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package management implements the Cluster HTTP Management API.
//
// It exposes read-only REST endpoints that allow operators and tools to
// inspect the live cluster state without having to connect over the Artery
// protocol.  The endpoints match the Pekko Management HTTP API conventions
// so that existing Pekko-compatible tooling can query a gekka node directly.
//
// Endpoints:
//
//	GET /cluster/members
//	    Returns a JSON array of all known cluster members with their current
//	    status, roles, data-center label, and reachability.
//
//	GET /cluster/members/{address}
//	    Returns detailed JSON for the member identified by the URL-encoded
//	    Artery address (e.g. "pekko%3A%2F%2FSystem%40127.0.0.1%3A2552").
//	    Responds 404 when no matching member is found.
package management

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/sopranoworks/gekka/cluster"
	"github.com/sopranoworks/gekka/internal/core"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
)

// ClusterStateProvider is the subset of the Cluster that the ManagementServer
// needs.  Keeping this narrow allows the server to be tested with a lightweight
// stub rather than a full Cluster.
type ClusterStateProvider interface {
	ClusterManager() *cluster.ClusterManager
	NodeManager() *core.NodeManager
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
}

// ManagementServer hosts the Cluster HTTP Management API.
type ManagementServer struct {
	provider ClusterStateProvider
	srv      *http.Server
	listener net.Listener
}

// NewManagementServer creates a ManagementServer that will listen on
// hostname:port as specified by cfg.
func NewManagementServer(provider ClusterStateProvider, hostname string, port int) (*ManagementServer, error) {
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", hostname, port))
	if err != nil {
		return nil, fmt.Errorf("management: listen on %s:%d: %w", hostname, port, err)
	}

	ms := &ManagementServer{
		provider: provider,
		listener: ln,
	}

	mux := http.NewServeMux()
	// NOTE: Go's default mux matches the longest prefix, so registering both
	// "/cluster/members/" (trailing slash) and "/cluster/members" covers both
	// the list endpoint and any sub-path (address lookup).
	mux.HandleFunc("/cluster/members/", ms.handleMemberByAddress)
	mux.HandleFunc("/cluster/members", ms.handleMembers)

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
// shutdown when ctx is cancelled.
func (ms *ManagementServer) Start(ctx context.Context) {
	go ms.srv.Serve(ms.listener) //nolint:errcheck
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ms.srv.Shutdown(shutCtx) //nolint:errcheck
	}()
}

// Stop gracefully shuts down the HTTP server, waiting at most until ctx expires.
func (ms *ManagementServer) Stop(ctx context.Context) error {
	return ms.srv.Shutdown(ctx)
}

// ── Handlers ──────────────────────────────────────────────────────────────────

// handleMembers serves GET /cluster/members — returns all members as a JSON
// array.
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

// handleMemberByAddress serves GET /cluster/members/{address} — looks up a
// specific member by its URL-encoded Artery address and returns detailed JSON,
// or 404 when not found.
func (ms *ManagementServer) handleMemberByAddress(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Strip the "/cluster/members/" prefix to get the raw (URL-decoded) address.
	raw := strings.TrimPrefix(r.URL.Path, "/cluster/members/")
	raw = strings.TrimSpace(raw)
	if raw == "" {
		// Treat bare /cluster/members/ as a list request.
		ms.handleMembers(w, r)
		return
	}

	members := ms.buildMemberList()
	for i := range members {
		if members[i].Address == raw {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			enc := json.NewEncoder(w)
			enc.SetIndent("", "  ")
			enc.Encode(members[i]) //nolint:errcheck
			return
		}
	}

	http.Error(w, fmt.Sprintf(`{"error":"member not found","address":%q}`+"\n", raw), http.StatusNotFound)
}

// ── Internal helpers ──────────────────────────────────────────────────────────

// buildMemberList reads the current gossip state under the ClusterManager's
// read lock and returns one MemberInfo per member.
func (ms *ManagementServer) buildMemberList() []MemberInfo {
	cm := ms.provider.ClusterManager()
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()

	gossip := cm.State
	if gossip == nil {
		return []MemberInfo{}
	}

	// Build a set of unreachable address indices from the local observer's
	// reachability table.  Index 0 is always the local node itself.
	unreachable := buildUnreachableSet(gossip)

	result := make([]MemberInfo, 0, len(gossip.GetMembers()))
	for _, m := range gossip.GetMembers() {
		info := memberInfoFromProto(gossip, m, unreachable)
		result = append(result, info)
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
			// Only consider the local node's view (index 0).
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
) MemberInfo {
	addrIdx := m.GetAddressIndex()

	// Resolve the Artery address string.
	address := ""
	if int(addrIdx) < len(gossip.GetAllAddresses()) {
		ua := gossip.GetAllAddresses()[addrIdx]
		if a := ua.GetAddress(); a != nil {
			proto := strings.TrimSuffix(a.GetProtocol(), "://")
			if proto == "" {
				proto = "pekko"
			}
			address = fmt.Sprintf("%s://%s@%s:%d",
				proto,
				a.GetSystem(),
				a.GetHostname(),
				a.GetPort(),
			)
		}
	}

	// Collect non-DC roles.
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

	return MemberInfo{
		Address:    address,
		Status:     m.GetStatus().String(),
		Roles:      roles,
		DataCenter: dc,
		UpNumber:   m.GetUpNumber(),
		Reachable:  !isUnreachable,
	}
}
