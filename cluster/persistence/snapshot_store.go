/*
 * snapshot_store.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"
)

// SnapshotMetadata describes where in the event log a snapshot was taken.
type SnapshotMetadata struct {
	PersistenceId string `json:"pid"`
	SequenceNr    int64  `json:"seqNr"`
	Timestamp     int64  `json:"timestamp"`
}

// SelectedSnapshot pairs the snapshot state with its metadata.
type SelectedSnapshot struct {
	Metadata SnapshotMetadata
	Snapshot any
}

// SnapshotStore persists and retrieves a single (latest) snapshot per
// persistence ID.  Implementations must be safe for concurrent use.
type SnapshotStore interface {
	// Save overwrites the latest snapshot for persistenceId.
	Save(persistenceId string, metadata SnapshotMetadata, snapshot any) error

	// Load returns the latest snapshot for persistenceId.
	// Returns (nil, nil) when no snapshot exists.
	Load(persistenceId string) (*SelectedSnapshot, error)
}

// SnapshotDecoder reconstructs a snapshot state from its serialised form.
//
// typeName is the bare struct name stored at save time.
// raw is the JSON-encoded snapshot payload.
type SnapshotDecoder func(typeName string, raw json.RawMessage) (any, error)

// ── FileSnapshotStore ─────────────────────────────────────────────────────────

// FileSnapshotStore stores snapshots as JSON files under baseDir.
//
// One file per persistence ID: <baseDir>/<sanitized-pid>.snap.json
// Each save overwrites the previous file, keeping only the latest snapshot.
// File format:
//
//	{"pid":"...","seqNr":50,"timestamp":1234567890,"type":"CounterState","data":{...}}
type FileSnapshotStore struct {
	baseDir string
	decoder SnapshotDecoder
	mu      sync.Mutex
}

// NewFileSnapshotStore creates a FileSnapshotStore rooted at baseDir.
// The directory is created if it does not exist.
// decoder is called during Load to reconstruct the snapshot state.
func NewFileSnapshotStore(baseDir string, decoder SnapshotDecoder) (*FileSnapshotStore, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("FileSnapshotStore: create base directory: %w", err)
	}
	return &FileSnapshotStore{baseDir: baseDir, decoder: decoder}, nil
}

// snapshotRecord is the on-disk envelope for a snapshot.
type snapshotRecord struct {
	PersistenceId string          `json:"pid"`
	SequenceNr    int64           `json:"seqNr"`
	Timestamp     int64           `json:"timestamp"`
	Type          string          `json:"type"`
	Data          json.RawMessage `json:"data"`
}

// filePath returns the path of the snapshot file for persistenceId.
// Path-unsafe characters are replaced with underscores.
func (s *FileSnapshotStore) filePath(persistenceId string) string {
	safe := strings.NewReplacer("/", "_", ":", "_", " ", "_").Replace(persistenceId)
	return filepath.Join(s.baseDir, safe+".snap.json")
}

// Save serialises snapshot as JSON and atomically overwrites the snapshot file.
func (s *FileSnapshotStore) Save(persistenceId string, metadata SnapshotMetadata, snapshot any) error {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("FileSnapshotStore.Save: marshal snapshot: %w", err)
	}

	if metadata.Timestamp == 0 {
		metadata.Timestamp = time.Now().Unix()
	}
	rec := snapshotRecord{
		PersistenceId: persistenceId,
		SequenceNr:    metadata.SequenceNr,
		Timestamp:     metadata.Timestamp,
		Type:          reflect.TypeOf(snapshot).Name(),
		Data:          json.RawMessage(data),
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("FileSnapshotStore.Save: marshal record: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return os.WriteFile(s.filePath(persistenceId), b, 0o644)
}

// Load reads the latest snapshot for persistenceId.
// Returns (nil, nil) when the snapshot file does not exist.
func (s *FileSnapshotStore) Load(persistenceId string) (*SelectedSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, err := os.ReadFile(s.filePath(persistenceId))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("FileSnapshotStore.Load: read file: %w", err)
	}

	var rec snapshotRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, fmt.Errorf("FileSnapshotStore.Load: parse record: %w", err)
	}

	var snapshot any
	if s.decoder != nil {
		snapshot, err = s.decoder(rec.Type, rec.Data)
		if err != nil {
			return nil, fmt.Errorf("FileSnapshotStore.Load: decode snapshot (type=%q): %w",
				rec.Type, err)
		}
	} else {
		snapshot = rec.Data
	}

	return &SelectedSnapshot{
		Metadata: SnapshotMetadata{
			PersistenceId: rec.PersistenceId,
			SequenceNr:    rec.SequenceNr,
			Timestamp:     rec.Timestamp,
		},
		Snapshot: snapshot,
	}, nil
}
