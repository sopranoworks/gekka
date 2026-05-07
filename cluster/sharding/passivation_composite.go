/*
 * passivation_composite.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"container/list"
)

// Round-2 session 26 — F3 Passivation: composite (W-TinyLFU) strategy.
//
// Pekko's recommended default passivation strategy is the W-TinyLFU
// composite: a small admission window catches newly-touched entities and
// a frequency-sketch admission filter decides whether evicted-from-window
// entities are allowed to displace the LRU candidate in the main area.
// The window-then-main split protects the main area from "scan" workloads
// (rare entities that touch the cache once and never come back), and the
// sketch protects against blind LRU promotion of low-frequency arrivals.
//
// Pekko names this strategy "default-strategy" in HOCON.  The plan
// originally referred to it as "composite-strategy"; we accept that name
// as an alias so configs that follow either convention work.
//
// Implementation notes:
//   - Window and main areas are doubly-linked lists; map[EntityId]*Element
//     gives O(1) MoveToFront on touch.
//   - The sketch is owned by the strategy (see passivation_admission.go).
//   - We don't yet implement the SLRU subdivision of main, the
//     hill-climbing window optimizer, or the per-entity dynamic-aging
//     pass.  Those knobs are parsed (forward-compat) but the runtime
//     behaves as a single-segment LRU main + LRU window + count-min
//     filter — which already delivers the W-TinyLFU hit-rate win on
//     skewed workloads (see passivation_composite_test.go).
//   - Eviction is queued: the Shard polls `NextEviction` after every
//     `OnAccess` to discover which entity to passivate, so the eviction
//     policy stays decoupled from the actor lifecycle.

const (
	// DefaultStrategyName is Pekko's canonical name for the recommended
	// W-TinyLFU composite passivation strategy.  HOCON path:
	//   pekko.cluster.sharding.passivation.strategy = "default-strategy"
	DefaultStrategyName = "default-strategy"

	// CompositeStrategyAlias is the gekka-internal alias for
	// DefaultStrategyName.  Accepted on the strategy parse path to keep
	// configs that follow either convention working.
	CompositeStrategyAlias = "composite-strategy"

	// FrequencySketchFilterName is the only admission filter currently
	// implemented.  Mirrors Pekko's
	//   passivation.strategy-defaults.admission.filter = "frequency-sketch"
	FrequencySketchFilterName = "frequency-sketch"

	// OptimizerHillClimbing is Pekko's canonical optimizer name for the
	// hill-climbing window-proportion adapter.  Mirrors HOCON
	//   passivation.default-strategy.admission.window.optimizer = "hill-climbing"
	OptimizerHillClimbing = "hill-climbing"
)

const (
	// optimizerDefaultInterval is the access count between two
	// hill-climbing decisions when the operator does not pin one in
	// HOCON. 1024 keeps the optimizer's overhead negligible while still
	// reacting to workload shifts within seconds at moderate throughput.
	optimizerDefaultInterval uint64 = 1024
	// optimizerDefaultStepSize is the proportion delta applied per
	// optimizer cycle when no explicit step is configured.  1% of the
	// active-entity-limit is small enough not to oscillate violently
	// yet large enough to traverse the [min,max] range in O(100) cycles.
	optimizerDefaultStepSize float64 = 0.01
)

// isCompositeStrategy reports whether name selects the W-TinyLFU composite
// strategy.  Both the Pekko-canonical name and the gekka alias resolve true.
func isCompositeStrategy(name string) bool {
	return name == DefaultStrategyName || name == CompositeStrategyAlias
}

// compositeConfig is the fully-resolved knob bundle the composite strategy
// needs to operate.  Built by the Shard from ShardSettings; exported solely
// so passivation_composite_test.go can construct strategies directly without
// going through the HOCON pipeline.
type compositeConfig struct {
	activeEntityLimit            int
	windowProportion             float64
	windowPolicy                 string
	filter                       string
	frequencySketchDepth         int
	frequencySketchCounterBits   int
	frequencySketchWidthMult     int
	frequencySketchResetMultpler float64

	// Phase 6.3 — hill-climbing window optimizer parameters.  Empty
	// windowOptimizer (or any value other than OptimizerHillClimbing)
	// keeps the strategy frozen at the seed windowProportion.
	windowMinimumProportion float64
	windowMaximumProportion float64
	windowOptimizer         string
	optimizerInterval       uint64  // accesses between optimizer firings
	optimizerStepSize       float64 // proportion delta per firing
}

// compositeArea tags an entry's residence: window admits new arrivals,
// main holds promoted entries.  Eviction logic dispatches on this tag.
type compositeArea int

const (
	areaWindow compositeArea = iota
	areaMain
)

// compositeEntry is the per-entity record stored in a list.Element's Value.
type compositeEntry struct {
	id   EntityId
	area compositeArea
}

// compositeStrategy owns the window/main LRU lists, the optional admission
// filter, and the queue of pending eviction victims.  All operations are
// O(1) amortised.
type compositeStrategy struct {
	cfg       compositeConfig
	sketch    *countMinSketch
	window    *list.List
	main      *list.List
	nodes     map[EntityId]*list.Element
	pending   []EntityId
	windowCap int
	mainCap   int

	// Phase 6.3 — hill-climbing window optimizer state.  All fields
	// are zero-valued / dormant when optimizerEnabled is false.
	optimizerEnabled  bool
	windowMinProp     float64
	windowMaxProp     float64
	windowProp        float64 // current dynamic proportion
	optimizerInterval uint64
	optimizerStep     float64 // unsigned step size; sign comes from optimizerDir
	optimizerDir      int     // +1 grows window, -1 shrinks it
	optimizerHits     uint64  // hits seen since last firing
	optimizerAccesses uint64  // total accesses seen since last firing
	optimizerLastHits float64 // hit ratio observed at the previous firing; -1 means none yet
	optimizerCycles   uint64  // diagnostic: how many times the optimizer ran
}

// newCompositeStrategy constructs a strategy from cfg.  Zero-valued knobs
// fall back to Pekko defaults: 100k active entities, 1% window, depth=4,
// width=4× active limit, reset every 10× active limit accesses.
func newCompositeStrategy(cfg compositeConfig) *compositeStrategy {
	if cfg.activeEntityLimit <= 0 {
		cfg.activeEntityLimit = 100000
	}
	if cfg.windowProportion <= 0 {
		cfg.windowProportion = 0.01
	}
	if cfg.frequencySketchDepth <= 0 {
		cfg.frequencySketchDepth = 4
	}
	if cfg.frequencySketchWidthMult <= 0 {
		cfg.frequencySketchWidthMult = 4
	}
	if cfg.frequencySketchResetMultpler <= 0 {
		cfg.frequencySketchResetMultpler = 10
	}

	windowCap := int(float64(cfg.activeEntityLimit) * cfg.windowProportion)
	if windowCap < 1 {
		windowCap = 1
	}
	mainCap := cfg.activeEntityLimit - windowCap
	if mainCap < 1 {
		mainCap = 1
	}

	cs := &compositeStrategy{
		cfg:        cfg,
		window:     list.New(),
		main:       list.New(),
		nodes:      make(map[EntityId]*list.Element),
		windowCap:  windowCap,
		mainCap:    mainCap,
		windowProp: cfg.windowProportion,
	}

	if cfg.filter == FrequencySketchFilterName {
		width := cfg.activeEntityLimit * cfg.frequencySketchWidthMult
		resetEvery := uint64(float64(cfg.activeEntityLimit) * cfg.frequencySketchResetMultpler)
		cs.sketch = newCountMinSketch(cfg.frequencySketchDepth, width, resetEvery, cfg.frequencySketchCounterBits)
	}

	cs.initOptimizer()
	return cs
}

// initOptimizer resolves the hill-climbing parameters and clamps the
// supplied min/max bounds into a sane shape.  Centralising the
// sanitisation keeps newCompositeStrategy readable and gives the test
// suite a single place to lock in the boundary semantics.
func (cs *compositeStrategy) initOptimizer() {
	cs.optimizerLastHits = -1
	cs.optimizerDir = 1
	if cs.cfg.windowOptimizer != OptimizerHillClimbing {
		return
	}
	cs.optimizerEnabled = true

	minP := cs.cfg.windowMinimumProportion
	maxP := cs.cfg.windowMaximumProportion
	if minP < 0 {
		minP = 0
	}
	if maxP <= 0 || maxP > 1 {
		maxP = 1
	}
	if minP > maxP {
		// Flip the pair when the operator typo'd them — preserves the
		// spread but swaps endpoints so the proportion has somewhere to
		// move.
		minP, maxP = maxP, minP
	}
	cs.windowMinProp = minP
	cs.windowMaxProp = maxP

	// Clamp the seed proportion into the resolved bounds before the
	// optimizer starts walking, so we never start outside the legal
	// range and immediately bail out on the boundary check.
	if cs.windowProp < minP {
		cs.windowProp = minP
	}
	if cs.windowProp > maxP {
		cs.windowProp = maxP
	}

	cs.optimizerInterval = cs.cfg.optimizerInterval
	if cs.optimizerInterval == 0 {
		cs.optimizerInterval = optimizerDefaultInterval
	}
	cs.optimizerStep = cs.cfg.optimizerStepSize
	if cs.optimizerStep <= 0 {
		cs.optimizerStep = optimizerDefaultStepSize
	}
	cs.recomputeCaps()
}

// recomputeCaps maps the current windowProp onto the integer windowCap /
// mainCap pair the LRU lists actually consult.  Caps cap each side to a
// minimum of 1 so a degenerate proportion never zeroes a list out.
func (cs *compositeStrategy) recomputeCaps() {
	limit := cs.cfg.activeEntityLimit
	w := int(float64(limit) * cs.windowProp)
	if w < 1 {
		w = 1
	}
	if w >= limit {
		w = limit - 1
	}
	m := limit - w
	if m < 1 {
		m = 1
	}
	cs.windowCap = w
	cs.mainCap = m
}

// WindowProportion returns the current effective windowProportion.  When
// the hill-climbing optimizer is disabled this is the seed value the
// strategy was constructed with; when enabled it reflects the latest
// optimizer decision.
func (cs *compositeStrategy) WindowProportion() float64 {
	return cs.windowProp
}

// OptimizerCycles is a diagnostic counter exposing how many times the
// hill-climbing loop has fired.  Unit tests use it to assert the
// optimizer is actually being driven, not just nominally enabled.
func (cs *compositeStrategy) OptimizerCycles() uint64 {
	return cs.optimizerCycles
}

// runOptimizer applies one hill-climbing decision: if the most recent
// hit ratio improved on the previous one, keep walking the proportion in
// the same direction; otherwise reverse.  Bounded clamps reverse the
// direction at each endpoint so the optimizer naturally oscillates back
// into the legal range when it brushes a wall.
func (cs *compositeStrategy) runOptimizer() {
	hitRate := 0.0
	if cs.optimizerAccesses > 0 {
		hitRate = float64(cs.optimizerHits) / float64(cs.optimizerAccesses)
	}
	if cs.optimizerLastHits >= 0 && hitRate < cs.optimizerLastHits {
		cs.optimizerDir = -cs.optimizerDir
	}
	cs.optimizerLastHits = hitRate

	delta := cs.optimizerStep * float64(cs.optimizerDir)
	next := cs.windowProp + delta
	if next < cs.windowMinProp {
		next = cs.windowMinProp
		cs.optimizerDir = 1
	}
	if next > cs.windowMaxProp {
		next = cs.windowMaxProp
		cs.optimizerDir = -1
	}
	cs.windowProp = next
	cs.recomputeCaps()
	cs.optimizerHits = 0
	cs.optimizerAccesses = 0
	cs.optimizerCycles++
}

// OnAccess records a touch of id.  fresh=true means the caller spawned the
// entity in this same step; fresh=false means an existing entity was
// re-touched.  The strategy increments its sketch (if any), moves the
// touched entry to MRU position in its area, and — when a new arrival
// pushes the window past windowCap — drains the LRU window victim through
// the admission filter, queuing any actual evictions for the caller to pick
// up via NextEviction.
func (cs *compositeStrategy) OnAccess(id EntityId, fresh bool) {
	if cs.sketch != nil {
		cs.sketch.Increment(string(id))
	}
	// Phase 6.3 — hit/miss tally for the hill-climbing optimizer.  The
	// presence check happens before mutation so we can count an existing
	// entry as a hit even though we are about to MoveToFront it.
	hit := false
	if elem, ok := cs.nodes[id]; ok {
		hit = true
		ent := elem.Value.(*compositeEntry)
		switch ent.area {
		case areaWindow:
			cs.window.MoveToFront(elem)
		case areaMain:
			cs.main.MoveToFront(elem)
		}
	} else {
		// Brand-new entity → admit to window.
		ent := &compositeEntry{id: id, area: areaWindow}
		cs.nodes[id] = cs.window.PushFront(ent)
		for cs.window.Len() > cs.windowCap {
			cs.admitOrEvictWindowVictim()
		}
	}

	if cs.optimizerEnabled {
		cs.optimizerAccesses++
		if hit {
			cs.optimizerHits++
		}
		if cs.optimizerAccesses >= cs.optimizerInterval {
			cs.runOptimizer()
			// A shrunk window may now hold over-capacity entries; drain
			// them through the same admission filter the natural overflow
			// path uses so the resize takes effect immediately rather
			// than waiting for the next OnAccess to notice.
			for cs.window.Len() > cs.windowCap {
				cs.admitOrEvictWindowVictim()
			}
		}
	}
}

// admitOrEvictWindowVictim pops the LRU window entry and either promotes it
// to main or queues it for eviction.  When main has free capacity the
// promotion is unconditional; otherwise the admission filter compares the
// candidate's estimated frequency against the main LRU victim's and the
// loser is evicted.
func (cs *compositeStrategy) admitOrEvictWindowVictim() {
	back := cs.window.Back()
	if back == nil {
		return
	}
	candidate := back.Value.(*compositeEntry)
	cs.window.Remove(back)
	delete(cs.nodes, candidate.id)

	if cs.main.Len() < cs.mainCap {
		candidate.area = areaMain
		cs.nodes[candidate.id] = cs.main.PushFront(candidate)
		return
	}

	mainBack := cs.main.Back()
	mainVictim := mainBack.Value.(*compositeEntry)

	admit := true
	if cs.sketch != nil {
		candidateFreq := cs.sketch.Estimate(string(candidate.id))
		victimFreq := cs.sketch.Estimate(string(mainVictim.id))
		admit = candidateFreq > victimFreq
	}

	if admit {
		cs.main.Remove(mainBack)
		delete(cs.nodes, mainVictim.id)
		cs.pending = append(cs.pending, mainVictim.id)
		candidate.area = areaMain
		cs.nodes[candidate.id] = cs.main.PushFront(candidate)
	} else {
		cs.pending = append(cs.pending, candidate.id)
	}
}

// OnRemove drops id from whichever area it currently sits in.  Called by
// the Shard after explicit Passivate / Terminated handling so the strategy
// stops tracking an entity that no longer has a corresponding actor.
// Silently no-ops on unknown ids so handlePassivate can call it
// unconditionally.
func (cs *compositeStrategy) OnRemove(id EntityId) {
	elem, ok := cs.nodes[id]
	if !ok {
		return
	}
	ent := elem.Value.(*compositeEntry)
	switch ent.area {
	case areaWindow:
		cs.window.Remove(elem)
	case areaMain:
		cs.main.Remove(elem)
	}
	delete(cs.nodes, id)
}

// NextEviction returns the next entity the Shard should passivate.  Empty
// queue → ("", false).  The Shard polls in a loop after every OnAccess so
// admission-filter cascades drain in a single envelope handling.
func (cs *compositeStrategy) NextEviction() (EntityId, bool) {
	if len(cs.pending) == 0 {
		return "", false
	}
	id := cs.pending[0]
	cs.pending = cs.pending[1:]
	return id, true
}

// activeCount is the total number of entities currently tracked by the
// strategy across both areas.  Used by tests to assert the strategy
// honours its activeEntityLimit budget.
func (cs *compositeStrategy) activeCount() int {
	return cs.window.Len() + cs.main.Len()
}
