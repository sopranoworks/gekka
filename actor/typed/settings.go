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

// Package-level settings that mirror Pekko's pekko.actor.typed.* knobs.
// gekka.LoadConfig populates these at cluster bring-up; they are read by
// TypedActor.PreStart when allocating the per-actor StashBuffer used while
// the actor is restarting.

var (
	restartStashCapacity atomic.Int64 // 0 → fall back to actor.DefaultStashCapacity
)

// SetDefaultRestartStashCapacity installs the default stash capacity used by
// typed actors while they restart. Non-positive values revert to
// actor.DefaultStashCapacity.
//
// HOCON: pekko.actor.typed.restart-stash-capacity (default 1000).
func SetDefaultRestartStashCapacity(n int) {
	if n <= 0 {
		restartStashCapacity.Store(0)
		return
	}
	restartStashCapacity.Store(int64(n))
}

// GetDefaultRestartStashCapacity returns the currently configured restart
// stash capacity for typed actors. Falls back to actor.DefaultStashCapacity
// when unconfigured.
func GetDefaultRestartStashCapacity() int {
	v := restartStashCapacity.Load()
	if v <= 0 {
		return actor.DefaultStashCapacity
	}
	return int(v)
}
