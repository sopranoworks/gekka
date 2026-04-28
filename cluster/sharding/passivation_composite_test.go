/*
 * passivation_composite_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"fmt"
	"testing"
)

// Round-2 session 26 — F3 Passivation: composite (W-TinyLFU) tests.
//
// These tests focus on the strategy state machine in isolation.  The
// runtime integration with Shard (eviction → handlePassivate → entity
// stop) is exercised separately in sharding_advanced_test.go.

// TestIsCompositeStrategy locks down the alias-resolution table.
// "composite-strategy" must alias "default-strategy" so plan-internal
// configs and Pekko-canonical configs both reach the same code path.
func TestIsCompositeStrategy(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{DefaultStrategyName, true},
		{CompositeStrategyAlias, true},
		{LRUStrategyName, false},
		{MRUStrategyName, false},
		{LFUStrategyName, false},
		{"default-idle-strategy", false},
		{"", false},
	}
	for _, c := range cases {
		if got := isCompositeStrategy(c.name); got != c.want {
			t.Errorf("isCompositeStrategy(%q) = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestCountMinSketch_FrequencyOrdering is the lower-bound for the sketch:
// after N-fold-larger access skew the higher-frequency entity must
// estimate strictly above the lower-frequency one.  Anything else and the
// W-TinyLFU admission filter would route entities backward.
func TestCountMinSketch_FrequencyOrdering(t *testing.T) {
	cs := newCountMinSketch(4, 256, 0)
	for i := 0; i < 50; i++ {
		cs.Increment("hot")
	}
	cs.Increment("cold")

	hot := cs.Estimate("hot")
	cold := cs.Estimate("cold")
	if hot <= cold {
		t.Errorf("expected hot estimate > cold estimate, got hot=%d cold=%d", hot, cold)
	}
}

// TestCountMinSketch_ResetHalvesCounters proves the "tiny" reset
// behaviour: once the access count crosses resetEvery, every cell is
// halved so newer touches dominate.  Without this the sketch would
// gradually saturate and lose discrimination.
func TestCountMinSketch_ResetHalvesCounters(t *testing.T) {
	cs := newCountMinSketch(2, 64, 8) // reset every 8 accesses
	for i := 0; i < 8; i++ {
		cs.Increment("k")
	}
	// after the 8th increment the reset fired; estimate should be the
	// pre-reset value (8, capped at 0x0F) shifted right by 1.
	if got := cs.Estimate("k"); got > 4 {
		t.Errorf("expected estimate ≤ 4 after reset, got %d", got)
	}
}

// TestCountMinSketch_CounterSaturation guards against the 4-bit cap
// regressing into wrap-around.  Hammer one key past the saturation point
// and the estimate must stay pinned at 0x0F.
func TestCountMinSketch_CounterSaturation(t *testing.T) {
	cs := newCountMinSketch(2, 64, 0)
	for i := 0; i < 100; i++ {
		cs.Increment("k")
	}
	if got := cs.Estimate("k"); got != frequencySketchMaxCount {
		t.Errorf("expected saturated counter (%d), got %d", frequencySketchMaxCount, got)
	}
}

// TestCompositeStrategy_AdmitsToWindowFirst is the core W-TinyLFU
// invariant: brand-new entities land in the small admission window, not
// directly in main.  This is what protects main from "scan" workloads.
func TestCompositeStrategy_AdmitsToWindowFirst(t *testing.T) {
	cs := newCompositeStrategy(compositeConfig{
		activeEntityLimit: 100,
		windowProportion:  0.1, // window cap = 10
		filter:            FrequencySketchFilterName,
	})
	cs.OnAccess("e1", true)
	if got := cs.window.Len(); got != 1 {
		t.Fatalf("expected new entity in window, got window.Len = %d", got)
	}
	if got := cs.main.Len(); got != 0 {
		t.Errorf("expected main empty after first access, got main.Len = %d", got)
	}
}

// TestCompositeStrategy_PromotesWindowOverflow is the next-step
// invariant: once the window is full, the next arrival evicts the LRU
// window entry — and because main has free capacity the evicted entry
// is unconditionally promoted (no admission filter check needed yet).
func TestCompositeStrategy_PromotesWindowOverflow(t *testing.T) {
	cs := newCompositeStrategy(compositeConfig{
		activeEntityLimit: 10,
		windowProportion:  0.2, // window cap = 2
		filter:            FrequencySketchFilterName,
	})
	cs.OnAccess("e1", true)
	cs.OnAccess("e2", true)
	cs.OnAccess("e3", true)

	// e1 is the LRU window candidate; main has free capacity (8 slots),
	// so e1 promotes silently — no pending evictions.
	if id, ok := cs.NextEviction(); ok {
		t.Errorf("expected no evictions during free-capacity promotion, got %q", id)
	}
	if cs.activeCount() != 3 {
		t.Errorf("expected activeCount=3, got %d", cs.activeCount())
	}
}

// TestCompositeStrategy_AdmissionFilterProtectsMain proves the heart of
// W-TinyLFU: when main is full, a low-frequency window candidate cannot
// displace a high-frequency main resident.  Without this the strategy
// would degrade to plain LRU and lose its skew-resistance.
func TestCompositeStrategy_AdmissionFilterProtectsMain(t *testing.T) {
	cs := newCompositeStrategy(compositeConfig{
		activeEntityLimit: 4,
		windowProportion:  0.5, // window cap = 2; main cap = 2
		filter:            FrequencySketchFilterName,
	})

	// Build heavy frequency on the entities destined for main so they
	// have a clear edge in the sketch.
	cs.OnAccess("hot1", true)
	cs.OnAccess("hot2", true)
	for i := 0; i < 50; i++ {
		cs.OnAccess("hot1", false)
		cs.OnAccess("hot2", false)
	}
	// Drain hot1/hot2 into main by overflowing the window with two
	// "filler" entities so the previous occupants get promoted.
	cs.OnAccess("filler1", true)
	cs.OnAccess("filler2", true)
	cs.OnAccess("filler3", true)
	cs.OnAccess("filler4", true)
	// Drain any pending evictions from the warmup phase.
	for {
		if _, ok := cs.NextEviction(); !ok {
			break
		}
	}

	// Now feed many low-frequency "scan" entities.  Their evictions
	// should drain through the filter; none of them should displace
	// hot1/hot2 (whose frequencies dwarf any scan-entity touch).
	for i := 0; i < 20; i++ {
		cs.OnAccess(EntityId(fmt.Sprintf("scan-%d", i)), true)
	}
	// Drain the eviction queue — these are the entities that lost the
	// admission contest or were trivially evicted from window-overflow.
	for {
		if _, ok := cs.NextEviction(); !ok {
			break
		}
	}

	// hot1 / hot2 must still be in the strategy at this point.  Either
	// they survived in main, or they were touched into the window again;
	// the cardinal sin is dropping them entirely.
	if _, ok := cs.nodes["hot1"]; !ok {
		t.Error("hot1 was evicted by low-freq scan entities; admission filter failed")
	}
	if _, ok := cs.nodes["hot2"]; !ok {
		t.Error("hot2 was evicted by low-freq scan entities; admission filter failed")
	}
}

// TestCompositeStrategy_OnRemove drops an entity from whichever area
// it currently occupies so the Shard's handlePassivate path keeps the
// strategy in sync after explicit termination.  Forgetting this would
// double-count the entity on next OnAccess and leak window slots.
func TestCompositeStrategy_OnRemove(t *testing.T) {
	cs := newCompositeStrategy(compositeConfig{
		activeEntityLimit: 10,
		windowProportion:  0.5,
		filter:            "off",
	})
	cs.OnAccess("e1", true)
	cs.OnAccess("e2", true)
	if cs.activeCount() != 2 {
		t.Fatalf("setup: activeCount=%d, want 2", cs.activeCount())
	}
	cs.OnRemove("e1")
	if cs.activeCount() != 1 {
		t.Errorf("after OnRemove: activeCount=%d, want 1", cs.activeCount())
	}
	if _, ok := cs.nodes["e1"]; ok {
		t.Error("OnRemove did not delete e1 from node index")
	}
	// OnRemove on an unknown id must be a no-op.
	cs.OnRemove("never-existed")
	if cs.activeCount() != 1 {
		t.Errorf("OnRemove of unknown id mutated state: activeCount=%d", cs.activeCount())
	}
}

// TestCompositeStrategy_ResolvedDefaults locks the Pekko fallback
// values so we don't silently regress to a 0-cap window or empty
// sketch when the operator declines to override anything.
func TestCompositeStrategy_ResolvedDefaults(t *testing.T) {
	cs := newCompositeStrategy(compositeConfig{
		filter: FrequencySketchFilterName,
	})
	if cs.cfg.activeEntityLimit != 100000 {
		t.Errorf("default activeEntityLimit = %d, want 100000", cs.cfg.activeEntityLimit)
	}
	if cs.cfg.windowProportion != 0.01 {
		t.Errorf("default windowProportion = %v, want 0.01", cs.cfg.windowProportion)
	}
	if cs.cfg.frequencySketchDepth != 4 {
		t.Errorf("default sketch depth = %d, want 4", cs.cfg.frequencySketchDepth)
	}
	if cs.sketch == nil {
		t.Error("expected sketch to be constructed when filter = frequency-sketch")
	}
}
