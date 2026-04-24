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
	"log"
	"sync"
	"time"

	"github.com/sopranoworks/gekka/actor"
	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
)

// CompressionTableManager holds mapping tables for ActorRef and ClassManifest strings to integer IDs.
// It maintains separate tables for each remote Node via their originUid.
type CompressionTableManager struct {
	mu            sync.RWMutex
	actorRefTable map[uint64]*CompressionTable
	manifestTable map[uint64]*CompressionTable
	router        *actor.Router // needed to send acks back
	flightRec     *FlightRecorder

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
		log.Printf("CompressionTableManager: rejected ActorRef table update for originUid %d — %d keys exceeds cap %d", originUid, len(keys), ctm.actorRefsMax)
		return
	}

	existing, ok := ctm.actorRefTable[originUid]
	if !ok || version > existing.Version {
		ctm.actorRefTable[originUid] = newCompressionTable(version, keys, values)
		log.Printf("CompressionTableManager: updated ActorRef table for originUid %d to version %d (keys: %d)", originUid, version, len(keys))
	} else {
		log.Printf("CompressionTableManager: ignored ActorRef table update for originUid %d (version %d <= %d)", originUid, version, existing.Version)
	}
}

// UpdateManifestTable updates the manifest dictionary for a specific originUid.
// Updates whose key count exceeds ManifestsMax are rejected.
func (ctm *CompressionTableManager) UpdateManifestTable(originUid uint64, version uint32, keys []string, values []uint32) {
	ctm.mu.Lock()
	defer ctm.mu.Unlock()

	if ctm.manifestsMax > 0 && len(keys) > ctm.manifestsMax {
		log.Printf("CompressionTableManager: rejected Manifest table update for originUid %d — %d keys exceeds cap %d", originUid, len(keys), ctm.manifestsMax)
		return
	}

	existing, ok := ctm.manifestTable[originUid]
	if !ok || version > existing.Version {
		ctm.manifestTable[originUid] = newCompressionTable(version, keys, values)
		log.Printf("CompressionTableManager: updated Manifest table for originUid %d to version %d (keys: %d)", originUid, version, len(keys))
	} else {
		log.Printf("CompressionTableManager: ignored Manifest table update for originUid %d (version %d <= %d)", originUid, version, existing.Version)
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

// HandleAdvertisement processes an incoming advertisement and sends an Ack.
func (ctm *CompressionTableManager) HandleAdvertisement(ctx context.Context, adv *gproto_remote.CompressionTableAdvertisement, isActorRef bool, localAddress *gproto_remote.UniqueAddress) error {
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

	// Send Ack
	ack := &gproto_remote.CompressionTableAdvertisementAck{
		From:    localAddress,
		Version: adv.TableVersion,
	}

	target := adv.GetFrom().GetAddress()
	path := fmt.Sprintf("%s://%s@%s:%d/system/cluster", target.GetProtocol(), target.GetSystem(), target.GetHostname(), target.GetPort())

	// ArteryControl messages usually use SerializerId 17 (or 1 depending on version, check later)

	if err := ctm.router.Send(ctx, path, ack); err != nil {
		return err
	}

	if ctm.flightRec != nil {
		remoteKey := fmt.Sprintf("%s:%d", adv.GetFrom().GetAddress().GetHostname(), adv.GetFrom().GetAddress().GetPort())
		ctm.flightRec.Emit(remoteKey, FlightEvent{
			Timestamp: time.Now(),
			Severity:  SeverityInfo,
			Category:  CatCompression,
			Message:   "ack_sent",
			Fields:    map[string]any{"version": adv.GetTableVersion()},
		})
	}

	return nil
}
