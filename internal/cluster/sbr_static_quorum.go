/*
 * sbr_static_quorum.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

// StaticQuorumStrategy implements the static-quorum Split Brain Resolver.
//
// A partition survives (Keep) when it can observe at least QuorumSize reachable
// members.  Any partition with fewer reachable members must down itself (Down).
//
// Edge case: when the total cluster size is smaller than QuorumSize the
// strategy always returns Down — the cluster is fundamentally unable to form a
// safe quorum.
type StaticQuorumStrategy struct {
	// QuorumSize is the minimum number of reachable members required for the
	// local partition to stay alive.  Must be > 0.
	QuorumSize int
}

// Decide evaluates the static quorum condition.
//
//   - members    — all known cluster members (Up / WeaklyUp).
//   - unreachable — the subset of members that the local node cannot reach.
//
// Returns:
//   - Keep  — reachable count (len(members) - len(unreachable)) >= QuorumSize.
//   - Down  — reachable count < QuorumSize, or total < QuorumSize.
func (s *StaticQuorumStrategy) Decide(members []Member, unreachable []Member) Action {
	total := len(members)

	// Edge case: cluster too small to ever achieve quorum.
	if total < s.QuorumSize {
		return Down
	}

	reachable := total - len(unreachable)
	if reachable >= s.QuorumSize {
		return Keep
	}
	return Down
}
