/*
 * passivation_lfu.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

// Round-2 session 25 — F3 Passivation: least-frequently-used strategy.
//
// LFU evicts the entity with the lowest cumulative access count when
// the active limit is exceeded.  Compared to LRU it favours retaining
// frequently-touched entities even when they go briefly idle, which is
// the right call for highly skewed workloads (a small set of "hot"
// entities receives the bulk of traffic).
//
// The frequency map lives on Shard (accessCount) and is incremented on
// every entity message before the eviction check.  Pekko also exposes
// a "dynamic-aging" knob that periodically halves all counters to keep
// long-running entities from ossifying their lead; gekka parses the
// knob in S25 (informational) and will start aging counters in a later
// session once we have a concrete workload to calibrate against.

// LFUStrategyName is Pekko's canonical strategy name for the
// least-frequently-used eviction policy.
const LFUStrategyName = "least-frequently-used"

// isLFUStrategy reports whether name selects the LFU eviction policy.
func isLFUStrategy(name string) bool {
	return name == LFUStrategyName
}
