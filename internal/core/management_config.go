/*
 * management_config.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

// ManagementConfig holds configuration for the Cluster HTTP Management API.
//
// HOCON schema:
//
//	gekka.management.http {
//	    hostname = "127.0.0.1"
//	    port     = 8558
//	    enabled  = false
//
//	    health-checks {
//	        enabled = true
//	    }
//	}
//
// Cluster member endpoints:
//
//	GET /cluster/members
//	    Returns a JSON array of all cluster members and their current status.
//
//	GET /cluster/members/{address}
//	    Returns detailed status for the node identified by the URL-encoded address.
//	    404 when the address is not a known cluster member.
//
//	PUT /cluster/members/{address}
//	    Initiates graceful leave for the named member.
//
//	DELETE /cluster/members/{address}
//	    Marks the named member as Down immediately.
//
// Kubernetes health-check endpoints (when health-checks.enabled = true):
//
//	GET /health/alive
//	    Liveness probe — always 200 OK while the HTTP server is running.
//
//	GET /health/ready
//	    Readiness probe — 200 OK only when the local node is Up, has no
//	    unreachable cluster members, and no quarantined Artery associations.
//	    Returns 503 Service Unavailable otherwise, with a JSON body describing
//	    the reason: "not_up", "unreachable_members", or "quarantined".
// MetricsExporterConfig holds configuration for the optional metrics exporter
// that periodically scrapes the Management HTTP API and emits cluster metrics.
//
// HOCON schema:
//
//	gekka.metrics {
//	    enabled          = false
//	    management-url   = "http://127.0.0.1:8558"
//	    scrape-interval  = "15s"
//	}
type MetricsExporterConfig struct {
	// Enabled controls whether the metrics exporter starts automatically.
	// Defaults to false.
	//
	// HOCON: gekka.metrics.enabled
	Enabled bool

	// ManagementURL is the base URL of the Management HTTP API to scrape.
	// Defaults to "http://127.0.0.1:8558".
	//
	// HOCON: gekka.metrics.management-url
	ManagementURL string

	// ScrapeInterval is how often the exporter fetches cluster state.
	// Defaults to 15 seconds.
	//
	// HOCON: gekka.metrics.scrape-interval
	ScrapeInterval string
}

// DefaultMetricsExporterConfig returns MetricsExporterConfig populated with
// recommended defaults.
func DefaultMetricsExporterConfig() MetricsExporterConfig {
	return MetricsExporterConfig{
		Enabled:        false,
		ManagementURL:  "http://127.0.0.1:8558",
		ScrapeInterval: "15s",
	}
}

type ManagementConfig struct {
	// Hostname is the interface the HTTP management server binds to.
	// Defaults to "127.0.0.1" (loopback only).
	// Set to "0.0.0.0" to listen on all interfaces.
	//
	// HOCON: gekka.management.http.hostname
	Hostname string

	// Port is the TCP port for the HTTP management server.
	// Defaults to 8558 (Pekko Management convention).
	//
	// HOCON: gekka.management.http.port
	Port int

	// Enabled controls whether the management server starts automatically
	// when the Cluster is created. Defaults to false.
	//
	// HOCON: gekka.management.http.enabled
	Enabled bool

	// HealthChecksEnabled controls whether the /health/alive and /health/ready
	// endpoints are registered on the management server.  Defaults to true when
	// the management server is enabled so that Kubernetes probes work out of the
	// box.
	//
	// HOCON: gekka.management.http.health-checks.enabled
	HealthChecksEnabled bool
}

// DefaultManagementConfig returns a ManagementConfig populated with the
// recommended defaults:
//
//	hostname              = "127.0.0.1"
//	port                  = 8558
//	enabled               = false
//	health-checks.enabled = true
func DefaultManagementConfig() ManagementConfig {
	return ManagementConfig{
		Hostname:            "127.0.0.1",
		Port:                8558,
		Enabled:             false,
		HealthChecksEnabled: true,
	}
}
