/*
 * coordinator_majority_plus_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"math"
	"testing"

	"github.com/sopranoworks/gekka/cluster/ddata"
)

// Phase 8 — pin the wiring from ShardSettings.CoordinatorWriteMajorityPlus /
// CoordinatorReadMajorityPlus through to the ddata.WriteConsistency value the
// coordinator uses when writing/reading coordinator state.
//
// The coordinator already accepts a ShardSettings (via SetShardSettings); the
// new WriteConsistency() / ReadConsistency() helpers must derive the
// configured plus values into ddata.WriteMajorityPlus(n) / ddata.ReadMajorityPlus(n).
// Defaults (zero) collapse to plain ddata.WriteMajority — matching Pekko's
// reference.conf default of write/read-majority-plus = 0.

func TestCoordinator_WriteConsistency_DefaultsToMajority(t *testing.T) {
	c := NewShardCoordinator(NewLeastShardAllocationStrategy(1, 1))
	// No ShardSettings → defaults to plain WriteMajority.

	if got := c.WriteConsistency(); got != ddata.WriteMajority {
		t.Errorf("WriteConsistency() default = %#v, want WriteMajority", got)
	}
	if got := c.ReadConsistency(); got != ddata.WriteMajority {
		t.Errorf("ReadConsistency() default = %#v, want WriteMajority", got)
	}
}

func TestCoordinator_WriteConsistency_AppliesPlus(t *testing.T) {
	c := NewShardCoordinator(NewLeastShardAllocationStrategy(1, 1))
	c.SetShardSettings(ShardSettings{
		CoordinatorWriteMajorityPlus: 3,
		CoordinatorReadMajorityPlus:  5,
	})

	if got, want := c.WriteConsistency(), ddata.WriteMajorityPlus(3); got != want {
		t.Errorf("WriteConsistency() = %#v, want WriteMajorityPlus(3)", got)
	}
	if got, want := c.ReadConsistency(), ddata.ReadMajorityPlus(5); got != want {
		t.Errorf("ReadConsistency() = %#v, want ReadMajorityPlus(5)", got)
	}
}

func TestCoordinator_WriteConsistency_AllSentinelMapsToMaxInt(t *testing.T) {
	c := NewShardCoordinator(NewLeastShardAllocationStrategy(1, 1))
	c.SetShardSettings(ShardSettings{
		CoordinatorWriteMajorityPlus: math.MaxInt,
		CoordinatorReadMajorityPlus:  math.MaxInt,
	})

	if got, want := c.WriteConsistency(), ddata.WriteMajorityPlus(math.MaxInt); got != want {
		t.Errorf(`WriteConsistency() for "all" = %#v, want WriteMajorityPlus(MaxInt)`, got)
	}
	if got, want := c.ReadConsistency(), ddata.ReadMajorityPlus(math.MaxInt); got != want {
		t.Errorf(`ReadConsistency() for "all" = %#v, want ReadMajorityPlus(MaxInt)`, got)
	}
}

// TestCoordinator_WriteConsistency_QuorumSizingIntegration ties the
// coordinator-level setting to the gossip-fanout end of the chain via a
// direct quorum computation, providing a single integration point for the
// "7-node, plus=3 → quorum=7" guarantee called out in the Phase 8 plan.
func TestCoordinator_WriteConsistency_QuorumSizingIntegration(t *testing.T) {
	c := NewShardCoordinator(NewLeastShardAllocationStrategy(1, 1))
	c.SetShardSettings(ShardSettings{CoordinatorWriteMajorityPlus: 3})

	r := ddata.NewReplicator("node-1", nil)
	r.MajorityMinCap = 0

	consistency := c.WriteConsistency()
	plus := ddata.MajorityPlusFromConsistency(consistency)
	if plus != 3 {
		t.Fatalf("MajorityPlusFromConsistency(%#v) = %d, want 3", consistency, plus)
	}
	// 7-node cluster (6 peers + self), plus=3 → quorum=7 (all).
	if got := r.EffectiveMajorityQuorumPlus(6, plus); got != 7 {
		t.Errorf("EffectiveMajorityQuorumPlus(6, 3) = %d, want 7 (capped at total)", got)
	}
}
