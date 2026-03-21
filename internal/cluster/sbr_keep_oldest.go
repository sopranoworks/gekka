/*
 * sbr_keep_oldest.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import "fmt"

// KeepOldestStrategy implements the keep-oldest Split Brain Resolver.
//
// The partition that contains the globally oldest member (lowest UpNumber,
// tie-broken by address) survives; all other partitions down themselves.
//
// When Role is non-empty only members carrying that role are considered for
// the "oldest" election — members without the role are ignored entirely.
//
// DownIfAlone guards against island scenarios: if the oldest member is alone
// on its side of the partition it downs itself rather than surviving as an
// isolated singleton.
type KeepOldestStrategy struct {
	// Role restricts the oldest-member election to members with this role.
	// Empty means all Up/WeaklyUp members participate.
	Role string

	// DownIfAlone, when true, causes the oldest member to down itself if it is
	// the only reachable member in the (optionally role-filtered) set.
	DownIfAlone bool
}

// Decide returns the Action the local node should take.
//
//   - members    — all known Up/WeaklyUp cluster members.
//   - unreachable — the subset the local node cannot currently reach.
//
// Decision rules:
//
//  1. Filter members by Role (if set).
//  2. Find the globally oldest member across the filtered set.
//  3. If the oldest is reachable (not in unreachable) → local partition wins.
//     a. If DownIfAlone and the reachable filtered set has only 1 member → Down.
//     b. Otherwise → Keep.
//  4. If the oldest is unreachable → Down.
//  5. If no eligible members exist → Wait (insufficient data).
func (s *KeepOldestStrategy) Decide(members []Member, unreachable []Member) Action {
	// Build a fast-lookup set for unreachable members.
	unreachableSet := make(map[string]struct{}, len(unreachable))
	for _, m := range unreachable {
		unreachableSet[memberKey(m)] = struct{}{}
	}

	// Filter all members by role.
	eligible := sbrFilterByRole(members, s.Role)
	if len(eligible) == 0 {
		return Wait
	}

	// Find the globally oldest eligible member.
	oldest := sbrOldestMember(eligible)

	// Is the oldest reachable (i.e. on our side)?
	_, oldestUnreachable := unreachableSet[memberKey(oldest)]
	if oldestUnreachable {
		// Oldest is on the other side of the partition — we must down ourselves.
		return Down
	}

	// Oldest is on our side.  Check DownIfAlone.
	if s.DownIfAlone {
		reachableEligible := sbrFilterByRole(sbrExclude(members, unreachableSet), s.Role)
		if len(reachableEligible) == 1 {
			// The oldest is the sole survivor in its role-filtered partition.
			return Down
		}
	}

	return Keep
}

// ── helpers ───────────────────────────────────────────────────────────────────

// memberKey returns a unique string key for a Member, used for set membership.
func memberKey(m Member) string {
	return fmt.Sprintf("%s:%d", m.Host, m.Port)
}

// sbrFilterByRole returns members that carry the given role.
// An empty role means all members are returned unchanged.
func sbrFilterByRole(members []Member, role string) []Member {
	if role == "" {
		return members
	}
	out := make([]Member, 0, len(members))
	for _, m := range members {
		for _, r := range m.Roles {
			if r == role {
				out = append(out, m)
				break
			}
		}
	}
	return out
}

// sbrOldestMember returns the member with the lowest UpNumber.
// On a tie the member with the lexicographically lower Host (then lower Port)
// is chosen as oldest, providing a deterministic total order.
func sbrOldestMember(members []Member) Member {
	best := members[0]
	for _, m := range members[1:] {
		switch {
		case m.UpNumber < best.UpNumber:
			best = m
		case m.UpNumber == best.UpNumber && m.Host < best.Host:
			best = m
		case m.UpNumber == best.UpNumber && m.Host == best.Host && m.Port < best.Port:
			best = m
		}
	}
	return best
}

// sbrExclude returns the members from all that are not present in the
// exclusionSet (keyed by memberKey).
func sbrExclude(all []Member, exclusionSet map[string]struct{}) []Member {
	out := make([]Member, 0, len(all))
	for _, m := range all {
		if _, excluded := exclusionSet[memberKey(m)]; !excluded {
			out = append(out, m)
		}
	}
	return out
}
