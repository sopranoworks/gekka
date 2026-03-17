/*
 * shutdown_phases.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

// ShutdownPhase is a typed constant that identifies a specific phase in the
// CoordinatedShutdown sequence.  Using typed constants instead of raw strings
// eliminates typos and makes call sites self-documenting.
//
// The values match the keys in DefaultPhases exactly so that they can be passed
// directly to CoordinatedShutdown.AddTask and CoordinatedShutdown.SetPhaseTimeout.
type ShutdownPhase string

const (
	// PhaseBeforeServiceUnbind is the first phase — runs tasks that must
	// complete before the node stops accepting new requests (e.g. draining
	// connection-acceptance queues or notifying upstream load-balancers).
	PhaseBeforeServiceUnbind ShutdownPhase = "before-service-unbind"

	// PhaseServiceUnbind unbinds the HTTP Management API from its listener so
	// that the Kubernetes readiness probe immediately begins returning 503.
	// Cluster membership is still active at this point; only the management
	// HTTP surface is withdrawn.
	PhaseServiceUnbind ShutdownPhase = "service-unbind"

	// PhaseServiceRequestsDone waits for any in-flight HTTP requests on the
	// management server to drain before the actor system begins shutting down.
	PhaseServiceRequestsDone ShutdownPhase = "service-requests-done"

	// PhaseServiceStop stops internal services that depend on the actor system
	// but do not require cluster membership (e.g. local scheduling, timers).
	PhaseServiceStop ShutdownPhase = "service-stop"

	// PhaseBeforeClusterShutdown is the last phase before cluster-level
	// operations begin.  Use it for tasks that must finish while the node is
	// still considered a cluster member by its peers.
	PhaseBeforeClusterShutdown ShutdownPhase = "before-cluster-shutdown"

	// PhaseShardingShutdownRegion stops all locally registered ShardRegions
	// and waits for each region to confirm shard handoff to the coordinator.
	// Completing this phase before PhaseClusterLeave lets the coordinator
	// reassign shards to surviving members before the local node departs,
	// which is the critical invariant for rolling-update stability.
	PhaseShardingShutdownRegion ShutdownPhase = "cluster-sharding-shutdown-region"

	// PhaseClusterLeave sends a Leave message to all Up/WeaklyUp peers and
	// waits for the local node's membership status to reach Removed.
	PhaseClusterLeave ShutdownPhase = "cluster-leave"

	// PhaseClusterExiting is a barrier phase — external tooling may hook here
	// to observe that the node has started its Exiting transition.
	PhaseClusterExiting ShutdownPhase = "cluster-exiting"

	// PhaseClusterExitingDone is a barrier phase — fires once the node's
	// Exiting transition is acknowledged by the cluster leader.
	PhaseClusterExitingDone ShutdownPhase = "cluster-exiting-done"

	// PhaseClusterShutdown runs after the node is Removed from the cluster.
	// Use it to stop cluster-aware services such as the CRDT replicator.
	PhaseClusterShutdown ShutdownPhase = "cluster-shutdown"

	// PhaseBeforeActorSystemTermination is the last opportunity to run tasks
	// before the TCP transport is closed and all actor goroutines are stopped.
	PhaseBeforeActorSystemTermination ShutdownPhase = "before-actor-system-terminate"

	// PhaseActorSystemTermination cancels the root context and closes all
	// Artery TCP connections.  This is always the final phase.
	PhaseActorSystemTermination ShutdownPhase = "actor-system-terminate"
)
