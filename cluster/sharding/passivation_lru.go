/*
 * passivation_lru.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

// Round-2 session 24 — F3 Passivation: least-recently-used strategy.
//
// gekka shipped LRU eviction in v0.7.0 under the in-house strategy name
// "custom-lru-strategy".  Pekko's reference.conf names the same algorithm
// "least-recently-used".  Session 24 introduces the Pekko-canonical name
// to ShardingConfig + HOCON parsing and treats the legacy name as an
// alias so existing user configs keep working unchanged.
//
// The algorithm itself lives in shard.go (checkLRUEviction).  This file
// is the strategy-name registry and the place where future replacement
// policies (MRU in session 25, LFU/composite in session 26) will plug in.

// LRUStrategyName is the Pekko-canonical strategy name for the
// least-recently-used eviction policy.
const LRUStrategyName = "least-recently-used"

// LegacyLRUStrategyName is the gekka v0.7.0 strategy name; recognised as
// an alias for LRUStrategyName so HOCON configs that predate the
// alignment continue to work without edits.
const LegacyLRUStrategyName = "custom-lru-strategy"

// isLRUStrategy reports whether name selects the LRU eviction policy.
// Both the canonical Pekko name and the gekka legacy alias resolve true.
// Empty input falls through to false so the default-idle-strategy path
// stays intact.
func isLRUStrategy(name string) bool {
	switch name {
	case LRUStrategyName, LegacyLRUStrategyName:
		return true
	default:
		return false
	}
}
