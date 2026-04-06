/*
 * sbr.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package cluster contains internal cluster infrastructure primitives.
package cluster

// Action is the outcome of an SBR strategy decision.
type Action int

const (
	// Keep means the local partition has sufficient quorum — stay up and down
	// any unreachable members that are on the losing side.
	Keep Action = iota

	// Down means the local partition lacks quorum — the node must terminate
	// itself to prevent split-brain data inconsistency.
	Down

	// Wait means the detector does not yet have enough information to decide
	// (e.g. the observation window is still too short).  The caller should
	// defer the decision until more data arrives.
	Wait
)

// Member is the SBR-visible representation of a cluster member.
// It carries only the fields required to evaluate a partitioning strategy.
type Member struct {
	Host       string
	Port       uint32
	Roles      []string
	UpNumber   int32  // monotonic join order; lower value = older member
	AppVersion string // application version string (e.g. "1.2.3"); empty when unset
}

// Strategy is the interface implemented by each SBR algorithm.
// Decide receives the full member list and the subset currently considered
// unreachable by the local node, and returns the Action the local node
// should take.
type Strategy interface {
	Decide(members []Member, unreachable []Member) Action
}
