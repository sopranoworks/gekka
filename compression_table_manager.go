/*
 * compression_table_manager.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"fmt"
	"log"
	"sync"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
)

// CompressionTableManager holds mapping tables for ActorRef and ClassManifest strings to integer IDs.
// It maintains separate tables for each remote Node via their originUid.
type CompressionTableManager struct {
	mu            sync.RWMutex
	actorRefTable map[uint64]*CompressionTable
	manifestTable map[uint64]*CompressionTable
	router        *Router // needed to send acks back
}

// CompressionTable represents a single versioned dictionary of string -> uint32 mapping.
type CompressionTable struct {
	Version uint32
	// For decoding: uint32 -> string
	IdToString map[uint32]string
	// For encoding: string -> uint32 (future use, assuming we eventually compress outbound)
	StringToId map[string]uint32
}

func NewCompressionTableManager(router *Router) *CompressionTableManager {
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

// UpdateActorRefTable updates the actor ref dictionary for a specific originUid.
func (ctm *CompressionTableManager) UpdateActorRefTable(originUid uint64, version uint32, keys []string, values []uint32) {
	ctm.mu.Lock()
	defer ctm.mu.Unlock()

	existing, ok := ctm.actorRefTable[originUid]
	if !ok || version > existing.Version {
		ctm.actorRefTable[originUid] = newCompressionTable(version, keys, values)
		log.Printf("CompressionTableManager: updated ActorRef table for originUid %d to version %d (keys: %d)", originUid, version, len(keys))
	} else {
		log.Printf("CompressionTableManager: ignored ActorRef table update for originUid %d (version %d <= %d)", originUid, version, existing.Version)
	}
}

// UpdateManifestTable updates the manifest dictionary for a specific originUid.
func (ctm *CompressionTableManager) UpdateManifestTable(originUid uint64, version uint32, keys []string, values []uint32) {
	ctm.mu.Lock()
	defer ctm.mu.Unlock()

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

	// Send Ack
	ack := &gproto_remote.CompressionTableAdvertisementAck{
		From:    localAddress,
		Version: adv.TableVersion,
	}

	target := adv.GetFrom().GetAddress()
	path := fmt.Sprintf("%s://%s@%s:%d/system/cluster", target.GetProtocol(), target.GetSystem(), target.GetHostname(), target.GetPort())

	// ArteryControl messages usually use SerializerId 17 (or 1 depending on version, check later)

	return ctm.router.Send(ctx, path, ack)
}
