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
		cfg:       cfg,
		window:    list.New(),
		main:      list.New(),
		nodes:     make(map[EntityId]*list.Element),
		windowCap: windowCap,
		mainCap:   mainCap,
	}

	if cfg.filter == FrequencySketchFilterName {
		width := cfg.activeEntityLimit * cfg.frequencySketchWidthMult
		resetEvery := uint64(float64(cfg.activeEntityLimit) * cfg.frequencySketchResetMultpler)
		cs.sketch = newCountMinSketch(cfg.frequencySketchDepth, width, resetEvery)
	}
	return cs
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
	if elem, ok := cs.nodes[id]; ok {
		ent := elem.Value.(*compositeEntry)
		switch ent.area {
		case areaWindow:
			cs.window.MoveToFront(elem)
		case areaMain:
			cs.main.MoveToFront(elem)
		}
		return
	}
	// Brand-new entity → admit to window.
	ent := &compositeEntry{id: id, area: areaWindow}
	cs.nodes[id] = cs.window.PushFront(ent)
	for cs.window.Len() > cs.windowCap {
		cs.admitOrEvictWindowVictim()
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
