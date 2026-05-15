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

	"github.com/sopranoworks/gekka/actor"
	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"github.com/sopranoworks/gekka/logger"
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
	ctm.SetNodeManager(nm)
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
// the CompressionTableManager scheduler.
//
// Akka 2.6.x's ArteryMessageSerializer (id 17) splits compression
// advertisements into two distinct messages with single-letter manifests:
//
//   "f"  ActorRefCompressionAdvertisement
//   "h"  ClassManifestCompressionAdvertisement
//
// (See akka-remote/src/main/scala/akka/remote/serialization/ArteryMessageSerializer.scala.)
//
// gekka models both with the single Go type CompressionTableAdvertisement
// (the proto fields are identical between the two variants — only the
// semantic intent differs).  Bypass router.Send so we can set the
// correct serializer id and manifest directly; the router fall-through
// for proto.Message would emit (sid=2, manifest=*remote.Compression...)
// which Akka rejects with NotSerializableException, leaving the joiner
// stuck in Joining forever.
//
// gekka does not yet accumulate outbound compression dictionaries — every
// outbound frame still carries literal strings — so the advert payload
// carries TableVersion=0 with no keys/values.  An empty advert is Akka-
// compatible: the receiver records "peer N has nothing compressed yet"
// and continues to decode using literal tags, which is exactly what
// gekka still emits.
func sendCompressionAdvertisement(_ context.Context, nm *NodeManager, _ *actor.Router, isActorRef bool) {
	if nm == nil {
		return
	}
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
	payload, err := proto.Marshal(adv)
	if err != nil {
		logger.Default().Warn("artery: failed to marshal CompressionTableAdvertisement", "err", err)
		return
	}
	manifest := "h" // ClassManifestCompressionAdvertisement
	if isActorRef {
		manifest = "f" // ActorRefCompressionAdvertisement
	}
	for _, peer := range peers {
		addr := peer.GetAddress()
		if addr == nil {
			continue
		}
		path := fmt.Sprintf("%s://%s@%s:%d/system/cluster/core/daemon",
			addr.GetProtocol(), addr.GetSystem(),
			addr.GetHostname(), addr.GetPort())
		assoc, ok := nm.GetAssociationByHost(addr.GetHostname(), addr.GetPort())
		if !ok {
			continue
		}
		if err := assoc.Send(path, payload, actor.ArteryInternalSerializerID, manifest); err != nil {
			logger.Default().Debug("artery: compression advertisement send failed",
				"peer", path, "isActorRef", isActorRef, "err", err)
		}
	}
}
