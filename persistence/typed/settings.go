/*
 * settings.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"sync/atomic"

	"github.com/sopranoworks/gekka/actor"
)

// Round-2 session 17: typed-persistence small features.
//
// Package-level settings that mirror Pekko's pekko.persistence.typed.* knobs.
// gekka.LoadConfig populates these at cluster bring-up; they are read by
// persistentActor.PreStart and the recovery completion path.

var (
	stashCapacity         atomic.Int64 // 0 → fall back to actor.DefaultStashCapacity
	stashOverflowStrategy atomic.Value // string ("drop" | "fail")
	snapshotOnRecovery    atomic.Bool
	logStashing           atomic.Bool
)

func init() {
	stashOverflowStrategy.Store("drop")
}

// SetDefaultStashCapacity installs the default stash capacity used by typed
// persistent actors. Non-positive values revert to actor.DefaultStashCapacity.
//
// HOCON: pekko.persistence.typed.stash-capacity
func SetDefaultStashCapacity(n int) {
	if n <= 0 {
		stashCapacity.Store(0)
		return
	}
	stashCapacity.Store(int64(n))
}

// GetDefaultStashCapacity returns the currently configured default stash
// capacity for typed persistent actors. When unconfigured, actor.DefaultStashCapacity.
func GetDefaultStashCapacity() int {
	v := stashCapacity.Load()
	if v <= 0 {
		return actor.DefaultStashCapacity
	}
	return int(v)
}

// SetDefaultStashOverflowStrategy installs the strategy applied when a typed
// persistent actor's recovery stash hits capacity. Currently honored values:
//
//	"drop" — drop the incoming command (default; Pekko-compatible).
//	"fail" — log an error; subsequent dispatch behaves as if recovery aborted.
//
// HOCON: pekko.persistence.typed.stash-overflow-strategy
func SetDefaultStashOverflowStrategy(strategy string) {
	if strategy == "" {
		strategy = "drop"
	}
	stashOverflowStrategy.Store(strategy)
}

// GetDefaultStashOverflowStrategy returns the currently configured stash
// overflow strategy.
func GetDefaultStashOverflowStrategy() string {
	v, _ := stashOverflowStrategy.Load().(string)
	if v == "" {
		return "drop"
	}
	return v
}

// SetSnapshotOnRecovery toggles whether typed persistent actors should
// automatically save a snapshot once recovery completes (in addition to any
// SnapshotInterval / SnapshotWhen configured per behavior).
//
// HOCON: pekko.persistence.typed.snapshot-on-recovery
func SetSnapshotOnRecovery(enabled bool) {
	snapshotOnRecovery.Store(enabled)
}

// GetSnapshotOnRecovery reports whether the snapshot-on-recovery flag is set.
func GetSnapshotOnRecovery() bool {
	return snapshotOnRecovery.Load()
}

// SetLogStashing toggles emission of DEBUG log lines around the typed
// persistent actor's stash + unstash operations.
//
// HOCON: pekko.persistence.typed.log-stashing (default off).
func SetLogStashing(enabled bool) {
	logStashing.Store(enabled)
}

// GetLogStashing reports whether stash/unstash DEBUG logging is enabled.
func GetLogStashing() bool {
	return logStashing.Load()
}
