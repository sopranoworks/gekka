/*
 * lanes.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"context"
	"hash/fnv"
	"time"
)

// laneIndex hashes recipient to a lane in [0, n). Returns 0 when n <= 1.
// Used by inbound-lanes (sub-plan 8f) and outbound-lanes (sub-plan 8j).
func laneIndex(recipient string, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(recipient))
	return int(h.Sum32() % uint32(n))
}

// dispatchSharded routes meta to an inbound lane goroutine when lanes are
// configured, otherwise dispatches inline on the calling (read) goroutine.
// Falls back to inline dispatch when:
//   - inboundLanes is empty (default for streamId=1 control or lanes <= 1)
//   - meta.Recipient is nil (cluster heartbeats / system messages without a
//     recipient stay on the read goroutine to preserve ordering)
//   - the target lane channel is full (saturation: emit CatInboundLaneFull
//     flight event and dispatch inline; never block the read loop)
func (assoc *GekkaAssociation) dispatchSharded(ctx context.Context, meta *ArteryMetadata) error {
	// Inbound coalescence: when this assoc has been merged into a primary
	// (streamId=2 inbound, second/third TCP from the same peer UID), all
	// frames flow through the primary's lane fan-out. Sub-plan 8f
	// outbound half.
	assoc.mu.RLock()
	delegate := assoc.delegate
	assoc.mu.RUnlock()
	if delegate != nil {
		return delegate.dispatchSharded(ctx, meta)
	}
	if len(assoc.inboundLanes) == 0 {
		return assoc.dispatch(ctx, meta)
	}
	if meta.Recipient == nil {
		return assoc.dispatch(ctx, meta)
	}
	idx := laneIndex(meta.Recipient.GetPath(), len(assoc.inboundLanes))
	select {
	case assoc.inboundLanes[idx] <- meta:
		return nil
	default:
		if assoc.nodeMgr != nil && assoc.nodeMgr.FlightRec != nil {
			assoc.nodeMgr.FlightRec.Emit(assoc.remoteKey(), FlightEvent{
				Timestamp: time.Now(),
				Severity:  SeverityWarn,
				Category:  CatInboundLaneFull,
				Message:   "lane full, falling back to inline dispatch",
				Fields: map[string]any{
					"lane":      idx,
					"recipient": meta.Recipient.GetPath(),
				},
			})
		}
		return assoc.dispatch(ctx, meta)
	}
}
