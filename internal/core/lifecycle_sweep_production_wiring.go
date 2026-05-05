/*
 * lifecycle_sweep_production_wiring.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Sub-plan 8h — production wiring for the four idle/quarantine sweep
// methods on NodeManager. Before this file existed, the sweeps were
// invoked only from tests, so the four config paths
//
//   pekko.remote.artery.advanced.stop-idle-outbound-after
//   pekko.remote.artery.advanced.quarantine-idle-outbound-after
//   pekko.remote.artery.advanced.stop-quarantined-after-idle
//   pekko.remote.artery.advanced.remove-quarantined-association-after
//
// were parsed onto NodeManager fields but never observed at runtime.
// StartLifecycleSweepers spawns a single ticker goroutine bound to the
// system-lifetime context provided by cluster.NewCluster; each tick
// invokes all four sweeps in dependency order (stop-idle → quarantine →
// stop-quarantined → remove-quarantined).

package core

import (
	"context"
	"time"
)

const (
	minLifecycleSweepInterval = 50 * time.Millisecond
	maxLifecycleSweepInterval = 1 * time.Second
)

// StartLifecycleSweepers spawns a single goroutine that periodically
// invokes the four idle/quarantine sweep methods on nm. The scheduler
// is bound to ctx and exits cleanly when ctx is cancelled. Sub-plan 8h.
func StartLifecycleSweepers(ctx context.Context, nm *NodeManager) {
	go runLifecycleSweepTicker(ctx, nm, lifecycleSweepInterval(nm))
}

// lifecycleSweepInterval picks the ticker cadence as min(thresholds)/4
// clamped to [minLifecycleSweepInterval, maxLifecycleSweepInterval].
// Tight test thresholds collapse to the floor (50ms); production
// defaults (5m–6h) collapse to the ceiling (1s).
func lifecycleSweepInterval(nm *NodeManager) time.Duration {
	smallest := nm.EffectiveStopIdleOutboundAfter()
	for _, c := range []time.Duration{
		nm.EffectiveQuarantineIdleOutboundAfter(),
		nm.EffectiveStopQuarantinedAfterIdle(),
		nm.EffectiveRemoveQuarantinedAssociationAfter(),
	} {
		if c > 0 && c < smallest {
			smallest = c
		}
	}
	interval := smallest / 4
	if interval < minLifecycleSweepInterval {
		interval = minLifecycleSweepInterval
	}
	if interval > maxLifecycleSweepInterval {
		interval = maxLifecycleSweepInterval
	}
	return interval
}

func runLifecycleSweepTicker(ctx context.Context, nm *NodeManager, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = nm.SweepIdleOutboundStop()
			_ = nm.SweepIdleOutboundQuarantine()
			_ = nm.SweepStopQuarantinedAfterIdle()
			_ = nm.SweepRemoveQuarantinedAssociation()
		}
	}
}
