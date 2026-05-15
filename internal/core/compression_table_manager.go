/*
 * compression_table_manager.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sopranoworks/gekka/actor"
	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"github.com/sopranoworks/gekka/logger"
	"google.golang.org/protobuf/proto"
)

// CompressionTableManager holds mapping tables for ActorRef and ClassManifest strings to integer IDs.
// It maintains separate tables for each remote Node via their originUid.
type CompressionTableManager struct {
	mu            sync.RWMutex
	actorRefTable map[uint64]*CompressionTable
	manifestTable map[uint64]*CompressionTable
	router        *actor.Router // needed to send acks back
	// nm provides access to peer associations so HandleAdvertisement can
	// bypass router.Send when emitting the Ack — router.Send routes
	// *CompressionTableAdvertisementAck through prepareMessage's default
	// (proto.Message → manifest = reflect type name) which produces
	// "*remote.CompressionTableAdvertisementAck" on the wire. Akka's
	// ArteryMessageSerializer rejects that with NotSerializableException,
	// the peer's CompressionTableManager never marks our advertisement as
	// acknowledged, and the peer's failure detector eventually downs us.
	// Set via SetNodeManager from StartCompressionTableManager. Nil in
	// the unit tests (NewCompressionTableManager + HandleAdvertisement
	// path) — those tests only verify the table-update side effect and
	// tolerate a no-op send.
	nm        *NodeManager
	flightRec *FlightRecorder

	// Configured caps (pekko.remote.artery.advanced.compression.*.max).
	// Zero means "no cap". Updates whose key count exceeds these caps are
	// rejected by UpdateActorRefTable / UpdateManifestTable.
	actorRefsMax int
	manifestsMax int

	// Advertisement intervals
	// (pekko.remote.artery.advanced.compression.*.advertisement-interval).
	// Consumed by StartAdvertisementScheduler to drive the periodic
	// advertisement ticker. Zero leaves the scheduler dormant.
	actorRefsAdvInterval time.Duration
	manifestsAdvInterval time.Duration

	// advertiseCallback is invoked each tick of the advertisement scheduler
	// with isActorRef=true for actor-ref ticks and false for manifest ticks.
	// Tests set it to observe cadence; production sets it to the real sender
	// once outbound advertisement is implemented.
	advertiseCallback func(isActorRef bool)
}

// CompressionTable represents a single versioned dictionary of string -> uint32 mapping.
type CompressionTable struct {
	Version uint32
	// For decoding: uint32 -> string
	IdToString map[uint32]string
	// For encoding: string -> uint32 (future use, assuming we eventually compress outbound)
	StringToId map[string]uint32
}

func NewCompressionTableManager(router *actor.Router) *CompressionTableManager {
	return &CompressionTableManager{
		actorRefTable: make(map[uint64]*CompressionTable),
		manifestTable: make(map[uint64]*CompressionTable),
		router:        router,
	}
}

func newCompressionTable(version uint32, keys []string, values []uint32) *CompressionTable {
	ct := &CompressionTable{
		Version:    version,
		IdToString: make(map[uint32]string),
		StringToId: make(map[string]uint32),
	}
	for i := 0; i < len(keys) && i < len(values); i++ {
		ct.IdToString[values[i]] = keys[i]
		ct.StringToId[keys[i]] = values[i]
	}
	return ct
}

// SetActorRefsMax sets the cap on actor-ref compression table entries.
// Zero disables the cap. Corresponds to
// pekko.remote.artery.advanced.compression.actor-refs.max.
func (ctm *CompressionTableManager) SetActorRefsMax(max int) {
	ctm.mu.Lock()
	ctm.actorRefsMax = max
	ctm.mu.Unlock()
}

// SetManifestsMax sets the cap on manifest compression table entries.
// Zero disables the cap. Corresponds to
// pekko.remote.artery.advanced.compression.manifests.max.
func (ctm *CompressionTableManager) SetManifestsMax(max int) {
	ctm.mu.Lock()
	ctm.manifestsMax = max
	ctm.mu.Unlock()
}

// SetAdvertisementIntervals sets the cadences at which the advertisement
// scheduler fires for actor-refs and manifests respectively. Corresponds to
// pekko.remote.artery.advanced.compression.{actor-refs,manifests}.advertisement-interval.
func (ctm *CompressionTableManager) SetAdvertisementIntervals(actorRefs, manifests time.Duration) {
	ctm.mu.Lock()
	ctm.actorRefsAdvInterval = actorRefs
	ctm.manifestsAdvInterval = manifests
	ctm.mu.Unlock()
}

// SetAdvertiseCallback registers the function invoked each advertisement
// tick. isActorRef distinguishes actor-ref ticks (true) from manifest ticks
// (false). Tests use this to observe scheduler cadence.
func (ctm *CompressionTableManager) SetAdvertiseCallback(fn func(isActorRef bool)) {
	ctm.mu.Lock()
	ctm.advertiseCallback = fn
	ctm.mu.Unlock()
}

// StartAdvertisementScheduler spawns two goroutines that invoke the
// registered advertise callback at the configured actor-ref and manifest
// advertisement intervals. A zero interval skips that ticker. The scheduler
// stops when ctx is cancelled.
func (ctm *CompressionTableManager) StartAdvertisementScheduler(ctx context.Context) {
	ctm.mu.RLock()
	actorRefs := ctm.actorRefsAdvInterval
	manifests := ctm.manifestsAdvInterval
	ctm.mu.RUnlock()

	if actorRefs > 0 {
		go ctm.runAdvertisementTicker(ctx, actorRefs, true)
	}
	if manifests > 0 {
		go ctm.runAdvertisementTicker(ctx, manifests, false)
	}
}

func (ctm *CompressionTableManager) runAdvertisementTicker(ctx context.Context, interval time.Duration, isActorRef bool) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ctm.mu.RLock()
			fn := ctm.advertiseCallback
			ctm.mu.RUnlock()
			if fn != nil {
				fn(isActorRef)
			}
		}
	}
}

// UpdateActorRefTable updates the actor ref dictionary for a specific originUid.
// Updates whose key count exceeds ActorRefsMax are rejected.
func (ctm *CompressionTableManager) UpdateActorRefTable(originUid uint64, version uint32, keys []string, values []uint32) {
	ctm.mu.Lock()
	defer ctm.mu.Unlock()

	if ctm.actorRefsMax > 0 && len(keys) > ctm.actorRefsMax {
		logger.Default().Warn("CompressionTableManager: rejected ActorRef table update — keys exceeds cap", "originUid", originUid, "keys", len(keys), "cap", ctm.actorRefsMax)
		return
	}

	existing, ok := ctm.actorRefTable[originUid]
	if !ok || version > existing.Version {
		ctm.actorRefTable[originUid] = newCompressionTable(version, keys, values)
		logger.Default().Info("CompressionTableManager: updated ActorRef table", "originUid", originUid, "version", version, "keys", len(keys))
	} else {
		logger.Default().Debug("CompressionTableManager: ignored ActorRef table update", "originUid", originUid, "incomingVersion", version, "existingVersion", existing.Version)
	}
}

// UpdateManifestTable updates the manifest dictionary for a specific originUid.
// Updates whose key count exceeds ManifestsMax are rejected.
func (ctm *CompressionTableManager) UpdateManifestTable(originUid uint64, version uint32, keys []string, values []uint32) {
	ctm.mu.Lock()
	defer ctm.mu.Unlock()

	if ctm.manifestsMax > 0 && len(keys) > ctm.manifestsMax {
		logger.Default().Warn("CompressionTableManager: rejected Manifest table update — keys exceeds cap", "originUid", originUid, "keys", len(keys), "cap", ctm.manifestsMax)
		return
	}

	existing, ok := ctm.manifestTable[originUid]
	if !ok || version > existing.Version {
		ctm.manifestTable[originUid] = newCompressionTable(version, keys, values)
		logger.Default().Info("CompressionTableManager: updated Manifest table", "originUid", originUid, "version", version, "keys", len(keys))
	} else {
		logger.Default().Debug("CompressionTableManager: ignored Manifest table update", "originUid", originUid, "incomingVersion", version, "existingVersion", existing.Version)
	}
}

// LookupActorRef resolves a compressed ID back to the string literal for a given originUid.
func (ctm *CompressionTableManager) LookupActorRef(originUid uint64, id uint32) (string, error) {
	ctm.mu.RLock()
	defer ctm.mu.RUnlock()

	ct, ok := ctm.actorRefTable[originUid]
	if !ok {
		return "", fmt.Errorf("CompressionTableManager: no ActorRef table found for originUid %d", originUid)
	}
	str, ok := ct.IdToString[id]
	if !ok {
		return "", fmt.Errorf("CompressionTableManager: actorRef id %d not found in table version %d for originUid %d", id, ct.Version, originUid)
	}
	return str, nil
}

// LookupManifest resolves a compressed ID back to the string literal for a given originUid.
func (ctm *CompressionTableManager) LookupManifest(originUid uint64, id uint32) (string, error) {
	ctm.mu.RLock()
	defer ctm.mu.RUnlock()

	ct, ok := ctm.manifestTable[originUid]
	if !ok {
		return "", fmt.Errorf("CompressionTableManager: no Manifest table found for originUid %d", originUid)
	}
	str, ok := ct.IdToString[id]
	if !ok {
		return "", fmt.Errorf("CompressionTableManager: manifest id %d not found in table version %d for originUid %d", id, ct.Version, originUid)
	}
	return str, nil
}

// SetNodeManager wires the NodeManager that owns the per-peer associations.
// Required for HandleAdvertisement to emit a wire-correct Ack — router.Send
// alone routes *CompressionTableAdvertisementAck through prepareMessage's
// default (proto.Message → reflect type name) which Akka rejects with
// NotSerializableException; with nm set the Ack path bypasses router.Send
// and writes the frame directly with the Akka-canonical manifest ("g" or
// "i" depending on isActorRef).
func (ctm *CompressionTableManager) SetNodeManager(nm *NodeManager) {
	ctm.mu.Lock()
	defer ctm.mu.Unlock()
	ctm.nm = nm
}

// HandleAdvertisement processes an incoming advertisement and sends an Ack.
func (ctm *CompressionTableManager) HandleAdvertisement(ctx context.Context, adv *gproto_remote.CompressionTableAdvertisement, isActorRef bool, localAddress *gproto_remote.UniqueAddress) error {
	_ = ctx // sender path no longer routes via router.Send
	originUid := adv.GetOriginUid()
	version := adv.GetTableVersion()
	keys := adv.GetKeys()
	values := adv.GetValues()

	if isActorRef {
		ctm.UpdateActorRefTable(originUid, version, keys, values)
	} else {
		ctm.UpdateManifestTable(originUid, version, keys, values)
	}

	if ctm.flightRec != nil {
		tableType := "manifest"
		if isActorRef {
			tableType = "actorref"
		}
		remoteKey := fmt.Sprintf("%s:%d", adv.GetFrom().GetAddress().GetHostname(), adv.GetFrom().GetAddress().GetPort())
		ctm.flightRec.Emit(remoteKey, FlightEvent{
			Timestamp: time.Now(),
			Severity:  SeverityInfo,
			Category:  CatCompression,
			Message:   "advert_received",
			Fields:    map[string]any{"version": adv.GetTableVersion(), "type": tableType},
		})
	}

	// Send Ack — bypass router.Send so we can stamp the wire-correct
	// Akka manifest ("g" = ActorRefCompressionAdvertisementAck, "i" =
	// ClassManifestCompressionAdvertisementAck) and serializer id 17.
	// Without this the proto.Message default in Router.prepareMessage
	// produces manifest "*remote.CompressionTableAdvertisementAck" which
	// Akka's ArteryMessageSerializer can't deserialize:
	//
	//   WARN Deserializer Failed to deserialize message from [<peer>]
	//   with serializer id [2] and manifest [*remote.CompressionTableAdvertisementAck].
	//   java.io.NotSerializableException: Cannot find manifest class
	//   [*remote.CompressionTableAdvertisementAck] for serializer with id [2].
	//
	// The peer's CompressionTableManager never sees the Ack, treats us
	// as unresponsive for compression, and (in multi-member Akka clusters
	// with SBR active) the failure detector eventually marks us
	// unreachable → Down.
	target := adv.GetFrom().GetAddress()
	ackManifest := "i" // ClassManifestCompressionAdvertisementAck
	if isActorRef {
		ackManifest = "g" // ActorRefCompressionAdvertisementAck
	}
	ctm.mu.RLock()
	nm := ctm.nm
	ctm.mu.RUnlock()
	if nm == nil {
		// Test / no-association path — table update already happened; the
		// Ack would have nowhere to land. Skip without erroring.
		return nil
	}
	assoc, ok := nm.GetAssociationByHost(target.GetHostname(), target.GetPort())
	if !ok || assoc == nil {
		// Peer association not yet established; the next gossip cycle
		// will trigger a fresh advertisement which we'll Ack then.
		return nil
	}
	ack := &gproto_remote.CompressionTableAdvertisementAck{
		From:    localAddress,
		Version: adv.TableVersion,
	}
	payload, err := proto.Marshal(ack)
	if err != nil {
		return fmt.Errorf("marshal CompressionTableAdvertisementAck: %w", err)
	}
	path := fmt.Sprintf("%s://%s@%s:%d/system/cluster",
		target.GetProtocol(), target.GetSystem(),
		target.GetHostname(), target.GetPort())
	if err := assoc.Send(path, payload, actor.ArteryInternalSerializerID, ackManifest); err != nil {
		return fmt.Errorf("send CompressionTableAdvertisementAck (%s): %w", ackManifest, err)
	}

	if ctm.flightRec != nil {
		remoteKey := fmt.Sprintf("%s:%d", adv.GetFrom().GetAddress().GetHostname(), adv.GetFrom().GetAddress().GetPort())
		ctm.flightRec.Emit(remoteKey, FlightEvent{
			Timestamp: time.Now(),
			Severity:  SeverityInfo,
			Category:  CatCompression,
			Message:   "ack_sent",
			Fields:    map[string]any{"version": adv.GetTableVersion(), "manifest": ackManifest},
		})
	}

	return nil
}
