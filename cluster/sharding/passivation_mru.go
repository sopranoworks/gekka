/*
 * passivation_mru.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

// Round-2 session 25 — F3 Passivation: most-recently-used strategy.
//
// MRU evicts the entity that was touched most recently when the active
// limit is exceeded.  It exists for workloads where freshly-touched
// entities are unlikely to be accessed again — typical of bulk-import or
// scan-style traffic.  The eviction loop reuses the lastActivity map
// the LRU path already maintains; only the comparison flips.

// MRUStrategyName is Pekko's canonical strategy name for the
// most-recently-used eviction policy.
const MRUStrategyName = "most-recently-used"

// isMRUStrategy reports whether name selects the MRU eviction policy.
// Empty input falls through to false so the default-idle-strategy path
// stays intact for unset configs.
func isMRUStrategy(name string) bool {
	return name == MRUStrategyName
}
