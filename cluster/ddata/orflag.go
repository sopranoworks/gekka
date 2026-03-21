/*
 * orflag.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

// ORFlag is an Observed-Remove Flag CRDT.
//
// The flag is backed by an ORSet that holds the single sentinel element
// "enabled".  The observable value is true when at least one dot exists for
// that element (i.e. SwitchOn was called without a subsequent SwitchOff that
// dominated it).
//
// Concurrent SwitchOn and SwitchOff from different nodes resolve with
// add-wins semantics inherited from ORSet: a concurrent SwitchOn beats a
// concurrent SwitchOff, which is the desired behaviour for distributed feature
// flags.
type ORFlag struct {
	set *ORSet
}

const orFlagSentinel = "enabled"

// NewORFlag returns an ORFlag that is initially false.
func NewORFlag() *ORFlag {
	return &ORFlag{set: NewORSet()}
}

// SwitchOn enables the flag for nodeID.  Concurrent SwitchOn calls from
// different nodes are all preserved; SwitchOff must dominate each dot
// individually (add-wins).
func (f *ORFlag) SwitchOn(nodeID string) {
	f.set.Add(nodeID, orFlagSentinel)
}

// SwitchOff disables the flag by removing all currently observed dots for the
// sentinel element.  Dots added concurrently on other nodes survive the merge.
func (f *ORFlag) SwitchOff() {
	f.set.Remove(orFlagSentinel)
}

// Value returns true when the flag is enabled (at least one dot exists).
func (f *ORFlag) Value() bool {
	return f.set.Contains(orFlagSentinel)
}

// Snapshot returns a serialisable copy of the underlying ORSet state.
func (f *ORFlag) Snapshot() ORSetSnapshot {
	return f.set.Snapshot()
}

// MergeSnapshot merges an incoming gossip snapshot into this flag.
func (f *ORFlag) MergeSnapshot(snap ORSetSnapshot) {
	f.set.MergeSnapshot(snap)
}
