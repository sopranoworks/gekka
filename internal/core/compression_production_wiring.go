/*
 * compression_production_wiring.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Sub-plan 8g — production construction of CompressionTableManager.
// Before this file existed, NewCompressionTableManager was invoked only
// from tests, so the four config paths
// (pekko.remote.artery.advanced.compression.{actor-refs,manifests}.{max,advertisement-interval})
// were parsed onto NodeManager fields but never observed at runtime.
// StartCompressionTableManager wires the CTM into the production
// NodeManager and starts the advertisement scheduler under the
// system-lifetime context provided by cluster.NewCluster.

package core

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/sopranoworks/gekka/actor"
	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// StartCompressionTableManager constructs a CompressionTableManager,
// applies the caps and advertisement intervals plumbed onto nm by
// cluster.NewCluster (sourced from
// pekko.remote.artery.advanced.compression.*), installs the production
// advertisement callback (sendCompressionAdvertisement), attaches the
// CTM to nm via SetCompressionManager, and starts the scheduler under
// ctx. The returned CTM is also reachable via nm.compressionMgr; the
// caller is not required to retain the pointer because the scheduler's
// lifetime is bound to ctx.
func StartCompressionTableManager(ctx context.Context, nm *NodeManager, router *actor.Router) *CompressionTableManager {
	ctm := NewCompressionTableManager(router)
	ctm.SetActorRefsMax(nm.EffectiveCompressionActorRefsMax())
	ctm.SetManifestsMax(nm.EffectiveCompressionManifestsMax())
	ctm.SetAdvertisementIntervals(
		nm.EffectiveCompressionActorRefsAdvertisementInterval(),
		nm.EffectiveCompressionManifestsAdvertisementInterval(),
	)
	ctm.SetAdvertiseCallback(func(isActorRef bool) {
		sendCompressionAdvertisement(ctx, nm, router, isActorRef)
	})
	nm.SetCompressionManager(ctm)
	ctm.StartAdvertisementScheduler(ctx)
	return ctm
}

// sendCompressionAdvertisement is the production tick body invoked by
// the CompressionTableManager scheduler. gekka does not yet accumulate
// outbound compression dictionaries — every outbound frame still carries
// literal strings — so the advert payload carries TableVersion=0 and an
// empty key/value set. An empty advert is Pekko-compatible: the receiver
// records "peer N has nothing compressed yet" and continues to decode
// using literal tags, which is exactly what gekka still emits.
func sendCompressionAdvertisement(ctx context.Context, nm *NodeManager, router *actor.Router, isActorRef bool) {
	peers := nm.SnapshotAssociatedAddresses()
	if len(peers) == 0 {
		return
	}
	localUid := nm.localUid
	localUA := &gproto_remote.UniqueAddress{
		Address: nm.LocalAddr,
		Uid:     &localUid,
	}
	adv := &gproto_remote.CompressionTableAdvertisement{
		From:         localUA,
		OriginUid:    proto.Uint64(localUid),
		TableVersion: proto.Uint32(0),
	}
	for _, peer := range peers {
		addr := peer.GetAddress()
		if addr == nil {
			continue
		}
		path := fmt.Sprintf("%s://%s@%s:%d/system/cluster/core/daemon",
			addr.GetProtocol(), addr.GetSystem(),
			addr.GetHostname(), addr.GetPort())
		if err := router.Send(ctx, path, adv); err != nil {
			slog.Debug("artery: compression advertisement send failed",
				"peer", path, "isActorRef", isActorRef, "err", err)
		}
	}
}
