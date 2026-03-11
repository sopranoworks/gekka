/*
 * monitoring.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"gekka/cluster"
)

// monitoringServer hosts the optional HTTP monitoring endpoints.
// It is created and started by GekkaNode.Spawn when MonitoringPort > 0 or
// EnableMonitoring is true (with a non-zero MonitoringPort).
type monitoringServer struct {
	node     *GekkaNode
	srv      *http.Server
	listener net.Listener
}

func newMonitoringServer(node *GekkaNode, port int) (*monitoringServer, error) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("monitoring: listen on :%d: %w", port, err)
	}

	ms := &monitoringServer{node: node, listener: ln}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", ms.handleHealthz)
	mux.HandleFunc("/metrics", ms.handleMetrics)

	ms.srv = &http.Server{
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	return ms, nil
}

// Addr returns the TCP address the monitoring server is bound to.
func (ms *monitoringServer) Addr() net.Addr {
	return ms.listener.Addr()
}

// Port returns the monitoring server's listen port.
func (ms *monitoringServer) Port() int {
	if addr, ok := ms.listener.Addr().(*net.TCPAddr); ok {
		return addr.Port
	}
	return 0
}

// start begins serving HTTP requests in the background.  It shuts down
// gracefully when ctx is cancelled.
func (ms *monitoringServer) start(ctx context.Context) {
	go ms.srv.Serve(ms.listener) //nolint:errcheck
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ms.srv.Shutdown(shutCtx) //nolint:errcheck
	}()
}

// isAssociated returns true when the node has at least one ASSOCIATED Artery
// connection — meaning the low-level TCP handshake with a peer has completed.
func (ms *monitoringServer) isAssociated() bool {
	return ms.node.nm.CountAssociations() > 0
}

// isJoined returns true when this node has received a Welcome message from the
// cluster seed, confirming participation in the Pekko cluster.
func (ms *monitoringServer) isJoined() bool {
	return ms.node.cm.IsWelcomeReceived()
}

// handleHealthz serves the readiness probe.
//
//	GET /healthz
//
// Returns 200 {"status":"ok"} when:
//   - At least one Artery association is in ASSOCIATED state, AND
//   - The node has successfully joined the cluster (Welcome received).
//
// Returns 503 {"status":"not_ready"} otherwise.  Kubernetes liveness probes
// that must always return 200 should use a separate /livez endpoint managed
// by the application itself.
func (ms *monitoringServer) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if ms.isAssociated() && ms.isJoined() {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}` + "\n")) //nolint:errcheck
		return
	}

	detail := "no_association"
	if ms.isAssociated() {
		detail = "not_joined"
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	fmt.Fprintf(w, `{"status":"not_ready","detail":%q}`+"\n", detail) //nolint:errcheck
}

// handleMetrics serves the internal metrics snapshot.
//
//	GET /metrics          → JSON (default)
//	GET /metrics?fmt=prom → Prometheus text exposition format
func (ms *monitoringServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	snapshot := ms.node.metrics.Snapshot(ms.node.nm.CountAssociations())

	if r.URL.Query().Get("fmt") == "prom" {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(snapshot.PrometheusText())) //nolint:errcheck
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(snapshot) //nolint:errcheck
}

// ── ClusterManager helpers wired by monitoring ────────────────────────────────

// IsWelcomeReceived returns true once this node has processed a Welcome
// message from the cluster seed — i.e. it is considered "joined".
func (cm *ClusterManager) IsWelcomeReceived() bool {
	return cm.welcomeReceived.Load()
}

// IsUp returns true when the cluster state contains at least one Up member.
func (cm *ClusterManager) IsUp() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	for _, m := range cm.state.GetMembers() {
		if m.GetStatus() == cluster.MemberStatus_Up {
			return true
		}
	}
	return false
}
