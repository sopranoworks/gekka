/*
 * orset.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package crdt

import "sync"

// Dot identifies a unique addition event: (nodeID, counter).
type Dot struct {
	NodeID  string
	Counter uint64
}

// ORSet is an Observed-Remove Set CRDT.
// Elements are tagged with dots; removal removes all observed dots.
// Merge = union of dots for elements present in either side,
// filtered to only include dots known to both sides (for removed elements).
type ORSet struct {
	mu   sync.RWMutex
	dots map[string]map[Dot]struct{} // element -> set of dots
	vv   map[string]uint64           // version vector: nodeID -> max counter seen
}

func NewORSet() *ORSet {
	return &ORSet{
		dots: make(map[string]map[Dot]struct{}),
		vv:   make(map[string]uint64),
	}
}

// Add inserts element with a new dot from nodeID.
func (s *ORSet) Add(nodeID, element string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vv[nodeID]++
	dot := Dot{NodeID: nodeID, Counter: s.vv[nodeID]}
	if s.dots[element] == nil {
		s.dots[element] = make(map[Dot]struct{})
	}
	s.dots[element][dot] = struct{}{}
}

// Remove removes element by clearing all its dots.
func (s *ORSet) Remove(element string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.dots, element)
}

// Contains returns true if element is in the set.
func (s *ORSet) Contains(element string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.dots[element]) > 0
}

// Elements returns all elements currently in the set.
func (s *ORSet) Elements() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.dots))
	for elem, ds := range s.dots {
		if len(ds) > 0 {
			out = append(out, elem)
		}
	}
	return out
}

// ORSetSnapshot is the serializable form used for gossip.
type ORSetSnapshot struct {
	Dots map[string][]Dot  `json:"dots"` // element -> dots
	VV   map[string]uint64 `json:"vv"`
}

// Snapshot returns a serializable copy of the state.
func (s *ORSet) Snapshot() ORSetSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := ORSetSnapshot{
		Dots: make(map[string][]Dot, len(s.dots)),
		VV:   make(map[string]uint64, len(s.vv)),
	}
	for elem, ds := range s.dots {
		dots := make([]Dot, 0, len(ds))
		for d := range ds {
			dots = append(dots, d)
		}
		snap.Dots[elem] = dots
	}
	for k, v := range s.vv {
		snap.VV[k] = v
	}
	return snap
}

// MergeSnapshot merges an incoming snapshot into this set.
// Uses the standard OR-Set merge: for each element, keep dots that exist
// in the incoming side OR are not yet dominated by the incoming VV.
func (s *ORSet) MergeSnapshot(snap ORSetSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Build incoming dots index
	incomingDots := make(map[string]map[Dot]struct{}, len(snap.Dots))
	for elem, dots := range snap.Dots {
		m := make(map[Dot]struct{}, len(dots))
		for _, d := range dots {
			m[d] = struct{}{}
		}
		incomingDots[elem] = m
	}

	// All elements from both sides
	allElems := make(map[string]struct{})
	for e := range s.dots {
		allElems[e] = struct{}{}
	}
	for e := range incomingDots {
		allElems[e] = struct{}{}
	}

	newDots := make(map[string]map[Dot]struct{}, len(allElems))
	for elem := range allElems {
		merged := make(map[Dot]struct{})

		// Dots in local: keep if also in incoming OR not dominated by incoming VV
		for d := range s.dots[elem] {
			inVV, seen := snap.VV[d.NodeID]
			if _, inIncoming := incomingDots[elem][d]; inIncoming {
				merged[d] = struct{}{}
			} else if !seen || d.Counter > inVV {
				// Not yet seen by remote — keep it
				merged[d] = struct{}{}
			}
		}

		// Dots in incoming: keep if also in local OR not dominated by local VV
		for d := range incomingDots[elem] {
			localVV, seen := s.vv[d.NodeID]
			if _, inLocal := s.dots[elem][d]; inLocal {
				merged[d] = struct{}{}
			} else if !seen || d.Counter > localVV {
				merged[d] = struct{}{}
			}
		}

		if len(merged) > 0 {
			newDots[elem] = merged
		}
	}

	s.dots = newDots

	// Merge version vectors (pairwise max)
	for k, v := range snap.VV {
		if v > s.vv[k] {
			s.vv[k] = v
		}
	}
}
