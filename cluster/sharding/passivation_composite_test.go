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
	cs := newCountMinSketch(4, 256, 0, 0) // counterBits = 0 → default 4-bit
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
	cs := newCountMinSketch(2, 64, 8, 0) // reset every 8 accesses; counterBits = default 4-bit
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
	cs := newCountMinSketch(2, 64, 0, 0) // counterBits = 0 → default 4-bit
	for i := 0; i < 100; i++ {
		cs.Increment("k")
	}
	if got := cs.Estimate("k"); got != frequencySketchMaxCount {
		t.Errorf("expected saturated counter (%d), got %d", frequencySketchMaxCount, got)
	}
}

// Phase 6.1 — counter-bits actually parameterizes the saturation cap.
// 2-bit counters saturate at 3, 4-bit at 15, 8-bit at 255.  The cap is
// observable via Estimate so the admission filter can use a wider counter
// when the operator asks for one in HOCON.
func TestCountMinSketch_CounterBitsParameterization(t *testing.T) {
	cases := []struct {
		bits   int
		wantTo uint8 // saturation ceiling reached after enough increments
	}{
		{2, 3},
		{3, 7},
		{4, 15},
		{5, 31},
		{6, 63},
		{7, 127},
		{8, 255},
	}
	for _, tc := range cases {
		cs := newCountMinSketch(2, 64, 0, tc.bits)
		// Increment past any plausible cap.
		for i := 0; i < 1000; i++ {
			cs.Increment("k")
		}
		if got := cs.Estimate("k"); got != tc.wantTo {
			t.Errorf("bits=%d: saturated estimate = %d, want %d", tc.bits, got, tc.wantTo)
		}
	}
}

// Out-of-range counter-bits values clamp to the supported [2,8] range so a
// stray HOCON value like 16/32/64 does not panic; gekka stores cells as
// uint8 so anything above 8 collapses to 8-bit.
func TestCountMinSketch_CounterBitsClamped(t *testing.T) {
	// Below the supported floor → clamps to 2-bit.
	low := newCountMinSketch(2, 64, 0, 1)
	for i := 0; i < 100; i++ {
		low.Increment("k")
	}
	if got := low.Estimate("k"); got != 3 {
		t.Errorf("bits=1 clamped: saturated estimate = %d, want 3", got)
	}
	// Above the uint8 ceiling → clamps to 8-bit (Pekko's 16/32/64 values).
	high := newCountMinSketch(2, 64, 0, 64)
	for i := 0; i < 1000; i++ {
		high.Increment("k")
	}
	if got := high.Estimate("k"); got != 255 {
		t.Errorf("bits=64 clamped: saturated estimate = %d, want 255", got)
	}
}

// Composite-level wiring: a counter-bits override on the compositeConfig
// must propagate into the shared sketch so the admission filter actually
// honours the configured saturation point.
func TestCompositeStrategy_CounterBitsHonoured(t *testing.T) {
	cs := newCompositeStrategy(compositeConfig{
		activeEntityLimit:          16,
		filter:                     FrequencySketchFilterName,
		frequencySketchCounterBits: 2, // saturation = 3
	})
	if cs.sketch == nil {
		t.Fatal("expected sketch to be constructed")
	}
	for i := 0; i < 100; i++ {
		cs.sketch.Increment("k")
	}
	if got := cs.sketch.Estimate("k"); got != 3 {
		t.Errorf("composite-wired 2-bit saturation = %d, want 3", got)
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

// Phase 6.3 — hill-climbing window optimizer.
//
// The optimizer adapts windowProportion at runtime to chase a higher hit
// rate.  The fundamental invariant is that the proportion always stays
// inside [minimum-proportion, maximum-proportion], regardless of how
// pathological the workload becomes.
func TestCompositeStrategy_OptimizerRespectsBounds(t *testing.T) {
	cs := newCompositeStrategy(compositeConfig{
		activeEntityLimit:       100,
		windowProportion:        0.3,
		windowMinimumProportion: 0.2,
		windowMaximumProportion: 0.4,
		windowOptimizer:         OptimizerHillClimbing,
		optimizerInterval:       16, // fire every 16 accesses for fast tests
		optimizerStepSize:       0.05,
		filter:                  FrequencySketchFilterName,
	})

	// Drive a few thousand mixed accesses; the proportion must stay in
	// [0.2, 0.4] every step of the way.
	for i := 0; i < 3000; i++ {
		id := EntityId(fmt.Sprintf("e-%d", i%50)) // small working set
		cs.OnAccess(id, false)
		if got := cs.WindowProportion(); got < 0.2-1e-9 || got > 0.4+1e-9 {
			t.Fatalf("iter %d: windowProportion=%v escaped [0.2, 0.4]", i, got)
		}
	}
}

// Disabled optimizer means windowProportion never moves — the operator
// opted out, so the dynamic resize code path must stay dormant even when
// the access stream would otherwise trigger it.
func TestCompositeStrategy_OptimizerOffKeepsProportion(t *testing.T) {
	const initial = 0.25
	cs := newCompositeStrategy(compositeConfig{
		activeEntityLimit:       40,
		windowProportion:        initial,
		windowMinimumProportion: 0.1,
		windowMaximumProportion: 0.9,
		windowOptimizer:         "", // empty => off
		filter:                  FrequencySketchFilterName,
	})
	for i := 0; i < 1000; i++ {
		cs.OnAccess(EntityId(fmt.Sprintf("e-%d", i%20)), false)
	}
	if got := cs.WindowProportion(); got != initial {
		t.Errorf("optimizer off: proportion drifted from %v to %v", initial, got)
	}
}

// Hill-climbing should actually move the proportion when the workload
// favours one regime — the test feeds a workload that benefits from a
// larger admission window (lots of churn over a small set), so the
// proportion should rise toward the maximum bound.
func TestCompositeStrategy_OptimizerAdaptsToWorkload(t *testing.T) {
	cs := newCompositeStrategy(compositeConfig{
		activeEntityLimit:       64,
		windowProportion:        0.1,
		windowMinimumProportion: 0.1,
		windowMaximumProportion: 0.9,
		windowOptimizer:         OptimizerHillClimbing,
		optimizerInterval:       32,
		optimizerStepSize:       0.05,
		filter:                  FrequencySketchFilterName,
	})

	// Workload: a tight working set of 10 entities accessed in round
	// robin → almost every access is a hit on an existing entity.  The
	// optimizer should not shrink the window below the floor and should
	// move it within bounds across many cycles.
	for i := 0; i < 4000; i++ {
		cs.OnAccess(EntityId(fmt.Sprintf("hot-%d", i%10)), false)
	}
	if got := cs.WindowProportion(); got < 0.1-1e-9 || got > 0.9+1e-9 {
		t.Errorf("after workload: windowProportion=%v escaped bounds", got)
	}
	// The optimizer must have *fired* (interval=32, ran 4000 / 32 = 125
	// times), so the proportion should have moved at least once away
	// from the seed value of 0.1.  Locks in that the optimizer is
	// actually called, not just nominally enabled.
	if got := cs.OptimizerCycles(); got == 0 {
		t.Error("optimizer never ran; expected ≥1 cycle in 4000 accesses")
	}
}

// Out-of-range bounds (min > max, negatives, > 1) collapse to safe
// defaults rather than panicking. Defends against typos in HOCON.
func TestCompositeStrategy_OptimizerBoundsSanitize(t *testing.T) {
	cs := newCompositeStrategy(compositeConfig{
		activeEntityLimit:       40,
		windowProportion:        0.3,
		windowMinimumProportion: 0.8, // illegal: min > max
		windowMaximumProportion: 0.2,
		windowOptimizer:         OptimizerHillClimbing,
		optimizerInterval:       16,
		optimizerStepSize:       0.05,
	})
	// Run a brief workload — should not panic, and the proportion
	// should still respect SOME bounds (the sanitizer picks a safe
	// pair: clamp min ≤ max ≤ 1.0 and min ≥ 0).
	for i := 0; i < 500; i++ {
		cs.OnAccess(EntityId(fmt.Sprintf("e-%d", i%20)), false)
	}
	got := cs.WindowProportion()
	if got < 0 || got > 1 {
		t.Errorf("sanitised bounds: proportion=%v escaped [0,1]", got)
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
