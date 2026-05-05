/*
 * flush_production_wiring.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Sub-plan 8i — production consumers for the two flush-timeout knobs:
//
//   pekko.remote.artery.advanced.shutdown-flush-timeout
//   pekko.remote.artery.advanced.death-watch-notification-flush-timeout
//
// Before this file existed, EffectiveShutdownFlushTimeout and
// EffectiveDeathWatchNotificationFlushTimeout were referenced only by
// the unit-test accessors in lifecycle_timers_test.go, so the two
// config paths were parsed onto NodeManager but never observed at
// runtime. WaitForOutboundFlush is invoked by the
// before-actor-system-terminate phase task registered in
// Cluster.registerBuiltinShutdownTasks; DeathWatchNotificationFlushDelay
// is invoked by the goroutine that Cluster.triggerLocalActorDeath
// spawns before emitting cross-network DeathWatchNotification frames.

package core

import (
	"context"
	"time"
)

const (
	minFlushPollInterval = 1 * time.Millisecond
	maxFlushPollInterval = 50 * time.Millisecond
)

// WaitForOutboundFlush blocks until every NodeManager outbox is empty,
// the timeout elapses, or ctx is cancelled. Returns true if all
// outboxes drained within the window, false on timeout or
// cancellation.
//
// timeout <= 0 means: poll once and return whatever the current state
// is. This matches the Pekko semantic where a zero
// shutdown-flush-timeout disables the flush window without skipping
// the empty-state check.
//
// The function never holds nm.mu across a sleep — each poll acquires
// nm.mu.RLock() briefly so registration churn during shutdown is
// safe.
func WaitForOutboundFlush(ctx context.Context, nm *NodeManager, timeout time.Duration) bool {
	if outboxesEmpty(nm) {
		return true
	}
	if timeout <= 0 {
		return false
	}
	deadline := time.Now().Add(timeout)
	interval := flushPollInterval(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			if outboxesEmpty(nm) {
				return true
			}
			if !time.Now().Before(deadline) {
				return false
			}
		}
	}
}

// DeathWatchNotificationFlushDelay sleeps for timeout, returning early
// if ctx is cancelled. timeout <= 0 returns true immediately (Pekko's
// "no delay" semantic). Returns true if the full delay elapsed,
// false on cancellation.
func DeathWatchNotificationFlushDelay(ctx context.Context, timeout time.Duration) bool {
	if timeout <= 0 {
		return true
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// flushPollInterval picks a poll cadence as timeout/20, clamped to
// [minFlushPollInterval, maxFlushPollInterval]. Tight test timeouts
// (e.g. 80ms) collapse to the floor; production defaults (1s)
// collapse to the ceiling.
func flushPollInterval(timeout time.Duration) time.Duration {
	interval := timeout / 20
	if interval < minFlushPollInterval {
		interval = minFlushPollInterval
	}
	if interval > maxFlushPollInterval {
		interval = maxFlushPollInterval
	}
	return interval
}

// outboxesEmpty reports whether every association on nm currently has
// empty outbox channels. Checks the primary outbox, the optional
// UDP-ordinary and UDP-large outboxes, and every per-lane outbox on
// streamId=2 outbound associations.
func outboxesEmpty(nm *NodeManager) bool {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	for _, assoc := range nm.associations {
		if len(assoc.outbox) > 0 {
			return false
		}
		if len(assoc.udpOrdinaryOutbox) > 0 {
			return false
		}
		if len(assoc.udpLargeOutbox) > 0 {
			return false
		}
		for _, lane := range assoc.lanes {
			if lane != nil && len(lane.outbox) > 0 {
				return false
			}
		}
	}
	return true
}
