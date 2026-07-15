/*
 * store.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package bboltstore provides a bbolt-backed (embedded, file-based) durable
// persistence backend for Gekka's Persistence module.
//
// Unlike the SQL (Postgres/MySQL) and Redis backends, bbolt needs no external
// database process — the state lives in a single local file — which makes it a
// good fit for lightweight, dependency-free deployments and for consumers that
// only need a persistence.DurableStateStore (latest-state-per-persistence-ID)
// rather than a full event journal.
//
// This backend implements persistence.DurableStateStore. It is registered as
// the "bbolt" provider so it is selectable via HOCON exactly like the
// "in-memory" and "redis" providers:
//
//	persistence {
//	  state-store {
//	    plugin = "bbolt"
//	    settings { path = "/var/lib/gekka/durable-state.db" }
//	  }
//	}
//
// Register the concrete state types on DefaultCodec (or a codec passed to Open)
// before use so that Get returns the original Go type after a restart.
package bboltstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sopranoworks/gekka/persistence"
	bolt "go.etcd.io/bbolt"
)

// stateBucket is the single bbolt bucket holding every durable-state record,
// keyed by persistence ID. One bucket keeps the on-disk format trivial: a Get
// is a single keyed lookup and the whole store is one flat namespace.
const stateBucket = "durable_state"

// defaultFileMode mirrors cluster/ddata's BoltDurableStore and Pekko's
// LmdbDurableStore: the db file is owner read/write only so a service-account
// deployment does not leak persisted state to group/other.
const defaultFileMode os.FileMode = 0o600

// defaultOpenTimeout bounds how long Open waits for the exclusive file lock
// before giving up, so a stale/duplicate opener surfaces an error instead of
// blocking forever. Matches the ddata store's 5s timeout.
const defaultOpenTimeout = 5 * time.Second

// record is the on-disk value stored under each persistence ID. Revision,
// manifest, and tag travel with the payload so a reopened file is fully
// self-describing — no external schema is needed to decode it after a restart.
type record struct {
	Revision uint64          `json:"revision"`
	Manifest string          `json:"manifest"`
	Payload  json.RawMessage `json:"payload"`
	Tag      string          `json:"tag,omitempty"`
}

// BoltDurableStateStore implements persistence.DurableStateStore on top of a
// single bbolt file.
//
// Durability: every Upsert and Delete commits its own bbolt read-write
// transaction, which bbolt fsyncs to disk before the call returns. State
// therefore survives a process restart (clean shutdown or crash-after-commit) —
// reopening the same file recovers the last committed value for each
// persistence ID.
//
// Concurrency: all methods are safe for concurrent use. bbolt is a
// single-writer / multi-reader engine, so Upsert/Delete serialise through a
// read-write transaction while Get uses a read transaction; no additional
// locking is required in this type.
type BoltDurableStateStore struct {
	db    *bolt.DB
	path  string
	codec PayloadCodec

	closeOnce sync.Once
	closeErr  error
}

// Ensure BoltDurableStateStore satisfies DurableStateStore at compile time.
var _ persistence.DurableStateStore = (*BoltDurableStateStore)(nil)

// Open opens (or creates) a bbolt DurableStateStore at path. The parent
// directory is created if it does not exist. When codec is nil a fresh
// JSONCodec is used; register the concrete state types on it (via Codec) before
// calling Get so decoded values come back as their original Go type rather than
// json.RawMessage.
func Open(path string, codec PayloadCodec) (*BoltDurableStateStore, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("bboltstore: Open: path must not be empty")
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("bboltstore: Open: mkdir %q: %w", dir, err)
		}
	}

	db, err := bolt.Open(path, defaultFileMode, &bolt.Options{Timeout: defaultOpenTimeout})
	if err != nil {
		return nil, fmt.Errorf("bboltstore: Open: open %q: %w", path, err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists([]byte(stateBucket))
		return e
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("bboltstore: Open: create bucket: %w", err)
	}

	if codec == nil {
		codec = NewJSONCodec()
	}
	return &BoltDurableStateStore{db: db, path: path, codec: codec}, nil
}

// Codec returns the codec this store encodes/decodes payloads with, so callers
// that opened the store without a pre-populated codec can register their state
// types on it.
func (s *BoltDurableStateStore) Codec() PayloadCodec { return s.codec }

// Path returns the on-disk file path backing this store.
func (s *BoltDurableStateStore) Path() string { return s.path }

// Get implements persistence.DurableStateStore. It returns (nil, 0, nil) when
// no state has been stored for persistenceID.
func (s *BoltDurableStateStore) Get(ctx context.Context, persistenceID string) (any, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}

	var rec *record
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket([]byte(stateBucket)).Get([]byte(persistenceID))
		if v == nil {
			return nil
		}
		var r record
		if err := json.Unmarshal(v, &r); err != nil {
			return fmt.Errorf("bboltstore: get %q: unmarshal record: %w", persistenceID, err)
		}
		rec = &r
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	if rec == nil {
		return nil, 0, nil
	}

	state, err := s.codec.Decode(rec.Manifest, rec.Payload)
	if err != nil {
		return nil, 0, fmt.Errorf("bboltstore: get %q: decode payload: %w", persistenceID, err)
	}
	return state, rec.Revision, nil
}

// Upsert implements persistence.DurableStateStore. It stores state as the
// latest durable state for persistenceID at the given revision, overwriting any
// previous value. The revision is stored as supplied — matching the in-memory,
// SQL, and Redis backends, which record the caller's revision without enforcing
// an optimistic-concurrency check.
func (s *BoltDurableStateStore) Upsert(ctx context.Context, persistenceID string, revision uint64, state any, tag string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	manifest, payload, err := s.codec.Encode(state)
	if err != nil {
		return fmt.Errorf("bboltstore: upsert %q: encode payload: %w", persistenceID, err)
	}

	buf, err := json.Marshal(record{
		Revision: revision,
		Manifest: manifest,
		Payload:  json.RawMessage(payload),
		Tag:      tag,
	})
	if err != nil {
		return fmt.Errorf("bboltstore: upsert %q: marshal record: %w", persistenceID, err)
	}

	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(stateBucket)).Put([]byte(persistenceID), buf)
	}); err != nil {
		return fmt.Errorf("bboltstore: upsert %q: %w", persistenceID, err)
	}
	return nil
}

// Delete implements persistence.DurableStateStore. Deleting a persistence ID
// with no stored state is a no-op.
func (s *BoltDurableStateStore) Delete(ctx context.Context, persistenceID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(stateBucket)).Delete([]byte(persistenceID))
	}); err != nil {
		return fmt.Errorf("bboltstore: delete %q: %w", persistenceID, err)
	}
	return nil
}

// Close flushes and releases the underlying bbolt file handle. It is idempotent
// and safe to call once the store is no longer needed; durability does not
// depend on Close because every Upsert/Delete has already been committed and
// fsynced. Closing is required before the same file can be reopened, since
// bbolt holds an exclusive lock for the lifetime of the handle.
func (s *BoltDurableStateStore) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.db.Close()
	})
	return s.closeErr
}
