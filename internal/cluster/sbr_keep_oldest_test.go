/*
 * sbr_keep_oldest_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"testing"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func node(host string, port uint32, upNumber int32, roles ...string) Member {
	return Member{Host: host, Port: port, UpNumber: upNumber, Roles: roles}
}

// ── mandated test cases ───────────────────────────────────────────────────────

// TestKeepOldest_OldestReachable: 3 nodes A(oldest), B, C.
// A and B are reachable; C is unreachable.
// Local node is on A's side → oldest (A) is reachable → Keep.
func TestKeepOldest_OldestReachable(t *testing.T) {
	s := &KeepOldestStrategy{}

	A := node("10.0.0.1", 2551, 1) // oldest
	B := node("10.0.0.2", 2551, 2)
	C := node("10.0.0.3", 2551, 3)

	members := []Member{A, B, C}
	unreachable := []Member{C}

	action := s.Decide(members, unreachable)
	if action != Keep {
		t.Errorf("oldest reachable: expected Keep, got %v", action)
	}
}

// TestKeepOldest_OldestUnreachable: 3 nodes A(oldest), B, C.
// B and C are reachable; A is unreachable.
// Local node is on B's side → oldest (A) is unreachable → Down.
func TestKeepOldest_OldestUnreachable(t *testing.T) {
	s := &KeepOldestStrategy{}

	A := node("10.0.0.1", 2551, 1) // oldest
	B := node("10.0.0.2", 2551, 2)
	C := node("10.0.0.3", 2551, 3)

	members := []Member{A, B, C}
	unreachable := []Member{A}

	action := s.Decide(members, unreachable)
	if action != Down {
		t.Errorf("oldest unreachable: expected Down, got %v", action)
	}
}

// TestKeepOldest_RoleFilter: A is globally oldest (no role); B is oldest among
// "worker" members.  With Role="worker", B's partition should win (Keep) even
// though A is on the unreachable side.
func TestKeepOldest_RoleFilter(t *testing.T) {
	s := &KeepOldestStrategy{Role: "worker"}

	A := node("10.0.0.1", 2551, 1)           // globally oldest, no role
	B := node("10.0.0.2", 2551, 2, "worker") // oldest among workers
	C := node("10.0.0.3", 2551, 3, "worker")

	// A is unreachable; B and C are reachable (local side).
	members := []Member{A, B, C}
	unreachable := []Member{A}

	action := s.Decide(members, unreachable)
	if action != Keep {
		t.Errorf("role=worker, B(oldest worker) reachable: expected Keep, got %v", action)
	}
}

// ── additional edge-case tests ────────────────────────────────────────────────

// TestKeepOldest_DownIfAlone_OldestAlone: A is the oldest and the only reachable
// member.  With DownIfAlone=true, A downs itself to avoid an island.
func TestKeepOldest_DownIfAlone_OldestAlone(t *testing.T) {
	s := &KeepOldestStrategy{DownIfAlone: true}

	A := node("10.0.0.1", 2551, 1) // oldest
	B := node("10.0.0.2", 2551, 2)
	C := node("10.0.0.3", 2551, 3)

	members := []Member{A, B, C}
	unreachable := []Member{B, C} // A is alone

	action := s.Decide(members, unreachable)
	if action != Down {
		t.Errorf("oldest alone, DownIfAlone=true: expected Down, got %v", action)
	}
}

// TestKeepOldest_DownIfAlone_NotAlone: A is oldest but has company (B also
// reachable).  DownIfAlone=true should NOT trigger → Keep.
func TestKeepOldest_DownIfAlone_NotAlone(t *testing.T) {
	s := &KeepOldestStrategy{DownIfAlone: true}

	A := node("10.0.0.1", 2551, 1) // oldest
	B := node("10.0.0.2", 2551, 2)
	C := node("10.0.0.3", 2551, 3)

	members := []Member{A, B, C}
	unreachable := []Member{C} // A and B reachable — not alone

	action := s.Decide(members, unreachable)
	if action != Keep {
		t.Errorf("oldest with company, DownIfAlone=true: expected Keep, got %v", action)
	}
}

// TestKeepOldest_TieBreakByAddress: two members with equal UpNumber — the one
// with the lexicographically lower address is elected oldest.
func TestKeepOldest_TieBreakByAddress(t *testing.T) {
	s := &KeepOldestStrategy{}

	// Both have UpNumber=1; "10.0.0.1" < "10.0.0.2" lexicographically.
	A := node("10.0.0.1", 2551, 1) // wins tie-break — oldest
	B := node("10.0.0.2", 2551, 1)
	C := node("10.0.0.3", 2551, 2)

	// A unreachable → Down.
	action := s.Decide([]Member{A, B, C}, []Member{A})
	if action != Down {
		t.Errorf("tie-break oldest (A) unreachable: expected Down, got %v", action)
	}

	// A reachable, C unreachable → Keep.
	action2 := s.Decide([]Member{A, B, C}, []Member{C})
	if action2 != Keep {
		t.Errorf("tie-break oldest (A) reachable: expected Keep, got %v", action2)
	}
}

// TestKeepOldest_NoEligibleMembers: role filter eliminates all members → Wait.
func TestKeepOldest_NoEligibleMembers(t *testing.T) {
	s := &KeepOldestStrategy{Role: "gpu"}

	A := node("10.0.0.1", 2551, 1, "worker")
	B := node("10.0.0.2", 2551, 2, "worker")

	action := s.Decide([]Member{A, B}, []Member{})
	if action != Wait {
		t.Errorf("no eligible members: expected Wait, got %v", action)
	}
}
