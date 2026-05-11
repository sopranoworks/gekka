/*
 * sharding/pekko_shim.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"fmt"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/logger"
)

// SendFunc routes an outbound translated reply to the Pekko sender that
// originated the matching inbound request. It is injected by the wiring code
// in cluster_sharding.go so the shim itself never imports the cluster Router.
type SendFunc func(recipientPath string, msg any) error

// PekkoCoordinatorShim is the inbound-translation actor registered at the
// Pekko-expected coordinator path
// (/system/sharding/<typeName>Coordinator/singleton/coordinator) on every
// gekka node that hosts a sharding type. When a Pekko ShardRegion sends
// Register / GetShardHome / etc., the Artery dispatch resolves the path to
// this actor, ShardingSerializer has already decoded the bytes into the
// corresponding PekkoSharding_* type, and Receive translates and forwards
// to the local gekka coordinator (typically a ShardCoordinatorProxy that
// auto-resolves to the singleton's current location).
//
// Reverse direction: when the gekka coordinator's reply lands in the
// shim's mailbox, the shim looks up the originating Pekko sender path —
// keyed by shardId for ShardHome — re-encodes the reply into a
// PekkoSharding_* and routes it back via SendFunc.
type PekkoCoordinatorShim struct {
	actor.BaseActor

	coord  actor.Ref
	sendFn SendFunc

	// pekkoSenders is keyed by shardId so multiple in-flight GetShardHome
	// requests for different shards do not clobber each other's senders.
	// The actor is single-mailbox, so the plain map needs no lock.
	pekkoSenders map[string]string
}

// NewPekkoCoordinatorShim constructs a shim that forwards translated
// messages to coord (the local gekka ShardCoordinatorProxy) and dispatches
// outbound replies via sendFn.
func NewPekkoCoordinatorShim(coord actor.Ref, sendFn SendFunc) *PekkoCoordinatorShim {
	return &PekkoCoordinatorShim{
		BaseActor:    actor.NewBaseActor(),
		coord:        coord,
		sendFn:       sendFn,
		pekkoSenders: make(map[string]string),
	}
}

// Receive translates inbound Pekko-wire messages to gekka-internal types
// and forwards them, then translates outbound replies back to the wire.
func (s *PekkoCoordinatorShim) Receive(msg any) {
	switch m := msg.(type) {
	case *PekkoSharding_Register:
		// Pekko region announces itself. Forward to the local gekka
		// coordinator with the shim as apparent sender so any future
		// RegisterAck-style reply lands in our mailbox where we can
		// translate it.
		s.coord.Tell(RegisterRegion{RegionPath: m.Ref}, s.Self())

	case *PekkoSharding_GetShardHome:
		// Remember the originating Pekko sender keyed by shardId, then
		// forward with the shim as apparent sender so the gekka
		// coordinator's ShardHome reply comes back to us for translation.
		if sender := s.Sender(); sender != nil {
			s.pekkoSenders[m.Shard] = sender.Path()
		}
		s.coord.Tell(GetShardHome{ShardId: m.Shard}, s.Self())

	case ShardHome:
		// Reverse direction: gekka coordinator answered our forwarded
		// GetShardHome. Look up the Pekko sender by shardId, encode the
		// reply, and route via SendFunc.
		pekkoSender, ok := s.pekkoSenders[m.ShardId]
		if !ok {
			logger.Default().Debug("PekkoCoordinatorShim: ShardHome for shard with no recorded Pekko sender",
				"shardId", m.ShardId)
			return
		}
		delete(s.pekkoSenders, m.ShardId)
		reply := &PekkoSharding_ShardHome{Shard: m.ShardId, Region: m.RegionPath}
		if err := s.sendFn(pekkoSender, reply); err != nil {
			logger.Default().Warn("PekkoCoordinatorShim: failed to route ShardHome reply",
				"recipient", pekkoSender, "shardId", m.ShardId, "err", err.Error())
		}

	default:
		logger.Default().Debug("PekkoCoordinatorShim: ignoring unsupported message",
			"type", fmt.Sprintf("%T", m))
	}
}
