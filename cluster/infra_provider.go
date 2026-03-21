/*
 * infra_provider.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import "context"

// InfraStatus is the infrastructure-observed liveness of a cluster member.
type InfraStatus int

const (
	// InfraUnknown means the infrastructure cannot determine liveness:
	// the API is unreachable, the call timed out, or the node's identity
	// could not be resolved.  The SBR falls back to the stable-after timeout
	// and the configured strategy when status is unknown.
	InfraUnknown InfraStatus = iota

	// InfraAlive means the infrastructure confirms the member is running
	// (e.g. Pod phase=Running and no deletion timestamp).
	InfraAlive

	// InfraDead means the infrastructure confirms the member is gone:
	// the Pod has been deleted, its phase is Failed or Succeeded, or it
	// carries a DeletionTimestamp.  The SBR can immediately down the member
	// without waiting for the stable-after timeout.
	InfraDead
)

// InfrastructureProvider allows the SBR to query out-of-band infrastructure
// status (e.g. Kubernetes API) to confirm node deaths without relying solely
// on network-level failure detection timeouts.
//
// Implementations must be safe for concurrent use.  PodStatus is called from
// the SBR event loop so it should complete quickly; use a deadline derived from
// the supplied context to avoid stalling the loop.
type InfrastructureProvider interface {
	// PodStatus returns the infrastructure-observed liveness of the cluster
	// member identified by address.
	//
	// Returns InfraUnknown when liveness cannot be determined (API error,
	// network issue, or no matching resource found when existence is
	// ambiguous).  Returns InfraDead only when the provider is confident the
	// node/pod is gone.
	PodStatus(ctx context.Context, address MemberAddress) InfraStatus
}
