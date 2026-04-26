/*
 * pruning.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"sync"
	"time"
)

// PruningPhase tracks the lifecycle of a removed node's CRDT data.
type PruningPhase int

const (
	// PruningInitiated — the oldest node is rewriting CRDT state to move the
	// removed node's entries onto the surviving collapseInto owner.
	PruningInitiated PruningPhase = iota
	// PruningDisseminating — rewrite complete locally; waiting for gossip to
	// carry the change to every reachable peer before the tombstone is
	// collected.
	PruningDisseminating
	// PruningComplete — dissemination window expired; tombstone is eligible
	// for collection on the next tick.
	PruningComplete
)

// PruningState tracks the lifecycle for a single removed node.
type PruningState struct {
	RemovedNode  string
	CollapseInto string
	InitiatedAt  time.Time
	Phase        PruningPhase
}

// Prunable is implemented by CRDTs that hold per-node data and can have
// that data rewritten onto a surviving owner when a node is removed from
// the cluster.
type Prunable interface {
	// NeedsPruning returns true if this CRDT still carries data attributed
	// to removedNode and therefore needs a Prune rewrite.
	NeedsPruning(removedNode string) bool
	// Prune transfers all data owned by removedNode onto collapseInto.
	// After Prune, NeedsPruning(removedNode) must return false.
	Prune(removedNode, collapseInto string)
}

// PruningManager tracks removed nodes and coordinates the rewrite/
// dissemination lifecycle for CRDTs that hold per-node data.
//
// Only the oldest live node actually rewrites CRDT state (initiates
// pruning). The surviving-peers just see the rewritten state via gossip.
// After MaxPruningDissemination has elapsed, the tombstone is collected
// and the removed node is forgotten.
type PruningManager struct {
	mu            sync.Mutex
	removedNodes  map[string]time.Time // nodeID → removal time
	pruningStates map[string]*PruningState

	pruningInterval         time.Duration
	maxPruningDissemination time.Duration
	// pruningMarkerTimeToLive, when > 0, retains a tombstone in the
	// PruningComplete phase for this long (measured from InitiatedAt) before
	// the marker is forgotten. Zero preserves the legacy "delete on
	// dissemination complete" behavior used by callers that don't set a TTL.
	pruningMarkerTimeToLive time.Duration

	isOldest     func() bool
	oldestNodeID func() string
}

// NewPruningManager constructs a manager. pruningInterval is informational —
// the replicator owns the Tick cadence.
func NewPruningManager(
	pruningInterval, maxDissemination time.Duration,
	isOldest func() bool,
	oldestNodeID func() string,
) *PruningManager {
	if pruningInterval <= 0 {
		pruningInterval = 120 * time.Second
	}
	if maxDissemination <= 0 {
		maxDissemination = 300 * time.Second
	}
	if isOldest == nil {
		isOldest = func() bool { return false }
	}
	if oldestNodeID == nil {
		oldestNodeID = func() string { return "" }
	}
	return &PruningManager{
		removedNodes:            make(map[string]time.Time),
		pruningStates:           make(map[string]*PruningState),
		pruningInterval:         pruningInterval,
		maxPruningDissemination: maxDissemination,
		isOldest:                isOldest,
		oldestNodeID:            oldestNodeID,
	}
}

// SetPruningMarkerTimeToLive configures how long a tombstone is retained in
// the PruningComplete phase before the marker is forgotten. Set to 0 to
// preserve the legacy "delete immediately on dissemination complete" behavior.
func (pm *PruningManager) SetPruningMarkerTimeToLive(ttl time.Duration) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.pruningMarkerTimeToLive = ttl
}

// NodeRemoved records that nodeID has been removed from the cluster.
// Subsequent Tick calls will advance the pruning lifecycle for this node.
func (pm *PruningManager) NodeRemoved(nodeID string) {
	if nodeID == "" {
		return
	}
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if _, exists := pm.removedNodes[nodeID]; !exists {
		pm.removedNodes[nodeID] = time.Now()
	}
}

// Tick advances the pruning lifecycle for all tracked removed nodes.
// prunables is a snapshot of every Prunable CRDT currently known to the
// replicator; it is supplied by the caller so the pruning manager stays
// decoupled from the replicator's map bookkeeping.
func (pm *PruningManager) Tick(prunables map[string]Prunable) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	now := time.Now()

	for nodeID := range pm.removedNodes {
		state, exists := pm.pruningStates[nodeID]
		if !exists {
			// Only the oldest node initiates the rewrite — every other
			// node learns about it via gossip. If we aren't the oldest,
			// skip until the oldest rewrites our local replica.
			if !pm.isOldest() {
				continue
			}
			needsPruning := false
			for _, p := range prunables {
				if p == nil {
					continue
				}
				if p.NeedsPruning(nodeID) {
					needsPruning = true
					break
				}
			}
			if !needsPruning {
				continue
			}
			collapseInto := pm.oldestNodeID()
			if collapseInto == "" || collapseInto == nodeID {
				continue
			}
			for _, p := range prunables {
				if p == nil {
					continue
				}
				if p.NeedsPruning(nodeID) {
					p.Prune(nodeID, collapseInto)
				}
			}
			pm.pruningStates[nodeID] = &PruningState{
				RemovedNode:  nodeID,
				CollapseInto: collapseInto,
				InitiatedAt:  now,
				Phase:        PruningDisseminating,
			}
			continue
		}
		if state.Phase == PruningDisseminating {
			if now.Sub(state.InitiatedAt) >= pm.maxPruningDissemination {
				state.Phase = PruningComplete
				if pm.pruningMarkerTimeToLive <= 0 {
					// Legacy path: forget the marker as soon as dissemination
					// finishes. Preserved so existing tests / callers that
					// never opt into pruning-marker-time-to-live keep their
					// current semantics.
					delete(pm.removedNodes, nodeID)
					delete(pm.pruningStates, nodeID)
				}
			}
		}
		if state.Phase == PruningComplete && pm.pruningMarkerTimeToLive > 0 {
			if now.Sub(state.InitiatedAt) >= pm.pruningMarkerTimeToLive {
				delete(pm.removedNodes, nodeID)
				delete(pm.pruningStates, nodeID)
			}
		}
	}
}

// TrackedRemovedNodes returns the set of nodes currently being pruned.
// Intended for tests and debug introspection.
func (pm *PruningManager) TrackedRemovedNodes() []string {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	out := make([]string, 0, len(pm.removedNodes))
	for k := range pm.removedNodes {
		out = append(out, k)
	}
	return out
}
