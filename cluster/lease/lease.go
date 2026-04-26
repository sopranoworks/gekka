/*
 * lease.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package lease provides the public coordination-lease API.
//
// The interface mirrors Pekko's pekko-coordination Lease and is intentionally
// satisfied by the older internal/cluster.Lease so existing Singleton/Sharding
// integration paths keep working through type aliases.
//
// Round-2 session 18 ships:
//
//   - The public Lease / LeaseProvider / LeaseManager type aliases.
//   - A reference in-memory provider (MemoryLeaseProvider) suitable for tests
//     and single-process coordination.
//   - NewDefaultManager: a Manager pre-populated with the in-memory provider
//     under the implementation name "memory".
//
// SBR lease-majority wiring (session 19) and Singleton/Sharding use-lease
// wiring (session 20) consume this surface.
package lease

import (
	icluster "github.com/sopranoworks/gekka/internal/cluster"
)

// MemoryProviderName is the registry key under which NewDefaultManager
// registers the in-memory LeaseProvider.
const MemoryProviderName = "memory"

// Lease represents a distributed coordination lease.  Acquire attempts to
// take the lease, Release surrenders it, and CheckLease reports whether
// this holder currently owns it.  The exact semantics (TTL, fencing,
// renewal) are defined by the underlying LeaseProvider.
type Lease = icluster.Lease

// LeaseSettings configures a single Lease instance.
//
//   - LeaseName  identifies the lease across all holders.
//   - OwnerName  identifies *this* holder.
//   - LeaseDuration is the time-to-live; if the holder fails to renew
//     within this window the lease may be re-acquired by a competitor.
//   - RetryInterval is the recommended cadence at which holders should
//     re-check liveness with the underlying coordination service.
type LeaseSettings = icluster.LeaseSettings

// LeaseProvider creates Lease instances bound to specific LeaseSettings.
// Implementations are registered under a name in a LeaseManager and resolved
// by callers that read pekko.coordination.lease.lease-implementation HOCON.
type LeaseProvider = icluster.LeaseProvider

// LeaseManager is a name-keyed registry of LeaseProviders.
type LeaseManager = icluster.LeaseManager

// NewManager returns an empty LeaseManager.  Use RegisterProvider to wire
// custom backends, or prefer NewDefaultManager which pre-registers the
// in-memory reference provider.
func NewManager() *LeaseManager {
	return icluster.NewLeaseManager()
}

// NewDefaultManager returns a LeaseManager with the in-memory reference
// provider registered under MemoryProviderName ("memory").
//
// The returned manager is safe to extend further with RegisterProvider — for
// example, to add a Kubernetes-backed provider for production use.
func NewDefaultManager() *LeaseManager {
	mgr := icluster.NewLeaseManager()
	mgr.RegisterProvider(MemoryProviderName, NewMemoryLeaseProvider())
	return mgr
}
