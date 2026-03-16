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
//	}
//
// Endpoints (draft — implemented in v0.8.0):
//
//	GET /cluster/members
//	    Returns a JSON array of all cluster members and their current status.
//	    Example response:
//	      [
//	        {"address":"pekko://GekkaSystem@127.0.0.1:2552","status":"Up","roles":["backend"]},
//	        {"address":"pekko://GekkaSystem@127.0.0.2:2552","status":"Up","roles":[]}
//	      ]
//
//	GET /cluster/members/{address}
//	    Returns detailed status for the node identified by {address}.
//	    {address} is URL-encoded, e.g. "pekko%3A%2F%2FGekkaSystem%40127.0.0.1%3A2552".
//	    404 when the address is not a known cluster member.
//	    Example response:
//	      {
//	        "address":"pekko://GekkaSystem@127.0.0.1:2552",
//	        "status":"Up",
//	        "roles":["backend"],
//	        "upNumber":1,
//	        "dataCenter":"default",
//	        "reachable":true
//	      }
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
}

// DefaultManagementConfig returns a ManagementConfig populated with the
// recommended defaults matching the Pekko Management defaults:
//
//	hostname = "127.0.0.1"
//	port     = 8558
//	enabled  = false
func DefaultManagementConfig() ManagementConfig {
	return ManagementConfig{
		Hostname: "127.0.0.1",
		Port:     8558,
		Enabled:  false,
	}
}
