/*
 * ddata_store.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"github.com/sopranoworks/gekka/cluster/ddata"
)

// DDataEntityStore is a ShardStore backed by an ORSet replicated through
// Distributed Data. One ORSet per shard (keyed as "shard-<typeName>-<shardID>")
// holds the set of live entity IDs. Writes are local with gossip
// consistency: AddEntity / RemoveEntity return before the change has been
// disseminated to peer nodes.
//
// Use this when pekko.cluster.sharding.remember-entities-store = "ddata".
type DDataEntityStore struct {
	replicator *ddata.Replicator
	keyPrefix  string // "shard-<typeName>-"
}

// NewDDataEntityStore creates a store that namespaces shard entity sets by
// the given type name (the sharded entity type registered with
// ClusterSharding). Passing a nil replicator returns a store that degrades
// to a no-op — useful to avoid guarding every call site when DData is
// optional.
func NewDDataEntityStore(replicator *ddata.Replicator, typeName string) *DDataEntityStore {
	return &DDataEntityStore{
		replicator: replicator,
		keyPrefix:  "shard-" + typeName + "-",
	}
}

func (s *DDataEntityStore) key(shardID ShardId) string {
	return s.keyPrefix + shardID
}

// AddEntity records entityID as active in shardID. Idempotent — the
// underlying ORSet handles repeat adds safely (dots are unique per add but
// the observable set membership is stable).
func (s *DDataEntityStore) AddEntity(shardID ShardId, entityID EntityId) error {
	if s.replicator == nil {
		return nil
	}
	s.replicator.AddToSet(s.key(shardID), entityID, ddata.WriteLocal)
	return nil
}

// RemoveEntity deletes entityID from shardID's set. Only called on explicit
// entity termination — passivation leaves the entity in the remember set so
// it will be re-spawned after a Shard restart.
func (s *DDataEntityStore) RemoveEntity(shardID ShardId, entityID EntityId) error {
	if s.replicator == nil {
		return nil
	}
	s.replicator.RemoveFromSet(s.key(shardID), entityID, ddata.WriteLocal)
	return nil
}

// GetEntities returns the current set of remembered entities for shardID.
// The returned slice is a copy and safe to retain.
func (s *DDataEntityStore) GetEntities(shardID ShardId) ([]EntityId, error) {
	if s.replicator == nil {
		return nil, nil
	}
	set, ok := s.replicator.LookupORSet(s.key(shardID))
	if !ok {
		return nil, nil
	}
	elements := set.Elements()
	result := make([]EntityId, len(elements))
	copy(result, elements)
	return result, nil
}

// Ensure DDataEntityStore satisfies ShardStore at compile time.
var _ ShardStore = (*DDataEntityStore)(nil)
