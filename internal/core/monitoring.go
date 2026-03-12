package core

/*
 * monitoring.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/sopranoworks/gekka/cluster"
)

// ClusterProvider defines the subset of Cluster functionality needed by MonitoringServer.
type ClusterProvider interface {
	NodeManager() *NodeManager
	ClusterManager() *cluster.ClusterManager
	Metrics() *NodeMetrics
}

// MonitoringServer hosts the optional HTTP monitoring endpoints.
type MonitoringServer struct {
	provider ClusterProvider
	srv      *http.Server
	listener net.Listener
}

// NewMonitoringServer creates a new monitoring server.
func NewMonitoringServer(provider ClusterProvider, port int) (*MonitoringServer, error) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("monitoring: listen on :%d: %w", port, err)
	}

	ms := &MonitoringServer{provider: provider, listener: ln}

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
func (ms *MonitoringServer) Addr() net.Addr {
	return ms.listener.Addr()
}

// Port returns the monitoring server's listen port.
func (ms *MonitoringServer) Port() int {
	if addr, ok := ms.listener.Addr().(*net.TCPAddr); ok {
		return addr.Port
	}
	return 0
}

// Start begins serving HTTP requests in the background.
func (ms *MonitoringServer) Start(ctx context.Context) {
	go ms.srv.Serve(ms.listener) //nolint:errcheck
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ms.srv.Shutdown(shutCtx) //nolint:errcheck
	}()
}

func (ms *MonitoringServer) isAssociated() bool {
	return ms.provider.NodeManager().CountAssociations() > 0
}

func (ms *MonitoringServer) isJoined() bool {
	return ms.provider.ClusterManager().IsWelcomeReceived()
}

func (ms *MonitoringServer) handleHealthz(w http.ResponseWriter, _ *http.Request) {
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

func (ms *MonitoringServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	snapshot := ms.provider.Metrics().Snapshot(ms.provider.NodeManager().CountAssociations())

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
