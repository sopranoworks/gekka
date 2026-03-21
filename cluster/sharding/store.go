/*
 * store.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"encoding/json"
	"os"
	"sync"
)

// ShardStore is a simple persistent set of entity IDs per shard.
//
// Unlike the event-sourced Journal, ShardStore tracks the current set of live
// entities directly.  The critical semantic difference: passivation does NOT
// call RemoveEntity — entities are kept remembered across restarts regardless
// of whether they were passivated.  RemoveEntity is called only on explicit
// entity termination (actor.TerminatedMessage).
type ShardStore interface {
	// AddEntity records entityID as active within shardID.
	// Calling AddEntity for an already-present entity is a no-op.
	AddEntity(shardID ShardId, entityID EntityId) error

	// RemoveEntity deletes entityID from shardID's set.
	// This is only called on explicit termination, never on passivation.
	RemoveEntity(shardID ShardId, entityID EntityId) error

	// GetEntities returns all entity IDs recorded for shardID.
	GetEntities(shardID ShardId) ([]EntityId, error)
}

// NoOpStore is the default ShardStore: all operations succeed and no data is
// persisted.  Used when RememberEntities is false or no store is configured.
type NoOpStore struct{}

func (NoOpStore) AddEntity(ShardId, EntityId) error          { return nil }
func (NoOpStore) RemoveEntity(ShardId, EntityId) error       { return nil }
func (NoOpStore) GetEntities(ShardId) ([]EntityId, error)    { return nil, nil }

// FileStore persists entity membership to a single JSON file on disk.
//
// File layout (one top-level key per shardId):
//
//	{ "shard-0": ["entity-1", "entity-2"], "shard-1": ["entity-3"] }
//
// All mutations are protected by a mutex so concurrent Shard goroutines
// sharing the same FileStore are safe.
type FileStore struct {
	path string
	mu   sync.Mutex
}

// NewFileStore creates a FileStore backed by the file at path.  The file is
// created (with an empty map) on the first write if it does not yet exist.
func NewFileStore(path string) (*FileStore, error) {
	return &FileStore{path: path}, nil
}

// fileStoreData is the in-memory representation of the JSON file.
type fileStoreData map[string][]string

// load reads the JSON file from disk.  Must be called with f.mu held.
func (f *FileStore) load() (fileStoreData, error) {
	data := make(fileStoreData)
	b, err := os.ReadFile(f.path)
	if os.IsNotExist(err) {
		return data, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &data); err != nil {
		return nil, err
	}
	return data, nil
}

// save writes data to disk as JSON.  Must be called with f.mu held.
func (f *FileStore) save(data fileStoreData) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return os.WriteFile(f.path, b, 0o644)
}

// AddEntity adds entityID to shardID's set (idempotent).
func (f *FileStore) AddEntity(shardID ShardId, entityID EntityId) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := f.load()
	if err != nil {
		return err
	}
	for _, id := range data[shardID] {
		if id == entityID {
			return nil // already present
		}
	}
	data[shardID] = append(data[shardID], entityID)
	return f.save(data)
}

// RemoveEntity removes entityID from shardID's set.
func (f *FileStore) RemoveEntity(shardID ShardId, entityID EntityId) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := f.load()
	if err != nil {
		return err
	}
	current := data[shardID]
	updated := current[:0:0]
	for _, id := range current {
		if id != entityID {
			updated = append(updated, id)
		}
	}
	data[shardID] = updated
	return f.save(data)
}

// GetEntities returns all entity IDs recorded for shardID.
func (f *FileStore) GetEntities(shardID ShardId) ([]EntityId, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := f.load()
	if err != nil {
		return nil, err
	}
	ids := data[shardID]
	result := make([]EntityId, len(ids))
	copy(result, ids)
	return result, nil
}
