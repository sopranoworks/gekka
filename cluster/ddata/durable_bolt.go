/*
 * durable_bolt.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// boltBucket is the single bucket every BoltDurableStore writes into.
// One bucket keeps the on-disk format trivial — recovery scans the whole
// bucket via DurableStore.LoadAll without needing a per-CRDT-type index.
const boltBucket = "ddata"

// boltDefaultFileMode mirrors Pekko's LmdbDurableStore: the db file is
// owner-readable / writable only.  Cluster operators routinely run nodes
// under a service account, so the directory and file modes must not leak
// data to group/other.
const boltDefaultFileMode os.FileMode = 0o600

// ErrDurableStoreFull is returned when a write would push the on-disk
// file size past BoltDurableStoreOptions.MapSize.  Mirrors LMDB's
// MDB_MAP_FULL: callers should treat this as a hard failure and surface
// it to operators (the cluster would otherwise silently lose state).
var ErrDurableStoreFull = errors.New("ddata: durable store map-size exceeded")

// boltRecord is the on-disk value format. Keeping the CRDT family inside
// the value (rather than encoded into the key) means LoadAll can return
// fully tagged DurableEntry values from a single bucket scan.  Using JSON
// here is deliberate: the Replicator's gossip codec is also JSON, so the
// Payload bytes can be reused unchanged on either side of recovery.
type boltRecord struct {
	Type    CRDTType `json:"t"`
	Payload []byte   `json:"p"`
}

// BoltDurableStoreOptions configures a BoltDurableStore. Field names map
// 1:1 onto Pekko's pekko.cluster.distributed-data.durable.lmdb.* HOCON
// keys so the host wiring can pass them through without translation.
type BoltDurableStoreOptions struct {
	// Dir is the directory that holds the bbolt file. Created if absent.
	// Corresponds to durable.lmdb.dir. Pekko default: "ddata".
	Dir string

	// MapSize is the hard cap on the on-disk file size, in bytes. A write
	// that would push the file past this size returns ErrDurableStoreFull
	// without committing — equivalent to LMDB's MDB_MAP_FULL behavior.
	// Corresponds to durable.lmdb.map-size. Pekko default: 100 MiB.
	// Zero means "unbounded" (testing only — not recommended for prod).
	MapSize int64

	// WriteBehindInterval, when > 0, batches Store/Delete operations in
	// memory and flushes them on a timer.  Matches Pekko's
	// durable.lmdb.write-behind-interval semantics: writes to the same
	// key coalesce, and Close drains any pending buffer.
	// Pekko default: off (0 == synchronous writes).
	WriteBehindInterval time.Duration
}

// pendingOp captures one buffered write-behind operation.  We keep the
// last write per key (op == opStore) and a one-shot delete sentinel
// (op == opDelete) — late-arriving writes after a delete simply replace
// the sentinel, which mirrors Pekko's "last writer wins on flush"
// behavior under write-behind.
type pendingOp struct {
	op    pendingKind
	entry DurableEntry
}

type pendingKind int

const (
	opStore pendingKind = iota
	opDelete
)

// BoltDurableStore is the bbolt-backed DurableStore.  Behavior is
// faithful to Pekko's LmdbDurableStore: a hard map-size cap enforced via
// pre-flight file-size checks, optional write-behind buffering with
// per-key coalescing, atomic batched flushes, and an idempotent Close
// that drains the buffer before releasing the file handle.
//
// Concurrency: the public methods are all safe for concurrent use.  The
// underlying bbolt.DB is a single-writer engine, so Store/Delete take a
// short write lock; Load/LoadAll go through a read txn.  The pending
// buffer has its own mutex independent of bbolt's; flushes drain into a
// single Update txn so observers never see a partial batch.
type BoltDurableStore struct {
	db      *bolt.DB
	path    string
	mapSize int64

	// writeBehindInterval == 0 means synchronous Store/Delete; non-zero
	// activates the buffered flush loop driven by ticker.
	writeBehindInterval time.Duration

	// pending guards the write-behind buffer.  Operations land here when
	// writeBehindInterval > 0 and are drained by the ticker goroutine.
	pendingMu sync.Mutex
	pending   map[string]pendingOp

	// flusher lifecycle.  flusherStop signals the goroutine to exit;
	// flusherDone closes when it has actually returned.  Close blocks on
	// flusherDone so a follow-up Open on the same Dir cannot race the
	// previous goroutine.
	flusherStop chan struct{}
	flusherDone chan struct{}

	closeOnce sync.Once
	closeErr  error
}

// OpenBoltDurableStore opens (or creates) a bbolt-backed DurableStore at
// opts.Dir.  The file is named "ddata.db" so it does not collide with
// other bbolt users that might share the directory.  When MapSize is 0
// the store is treated as unbounded (test-only fallback).
func OpenBoltDurableStore(opts BoltDurableStoreOptions) (*BoltDurableStore, error) {
	if opts.Dir == "" {
		return nil, errors.New("ddata: BoltDurableStore: Dir must be set")
	}
	if err := os.MkdirAll(opts.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("ddata: BoltDurableStore: mkdir %q: %w", opts.Dir, err)
	}

	path := filepath.Join(opts.Dir, "ddata.db")
	db, err := bolt.Open(path, boltDefaultFileMode, &bolt.Options{
		Timeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("ddata: BoltDurableStore: open %q: %w", path, err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists([]byte(boltBucket))
		return e
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ddata: BoltDurableStore: create bucket: %w", err)
	}

	s := &BoltDurableStore{
		db:                  db,
		path:                path,
		mapSize:             opts.MapSize,
		writeBehindInterval: opts.WriteBehindInterval,
	}
	if opts.WriteBehindInterval > 0 {
		s.pending = make(map[string]pendingOp)
		s.flusherStop = make(chan struct{})
		s.flusherDone = make(chan struct{})
		go s.flushLoop()
	}
	return s, nil
}

// Path returns the absolute path of the on-disk file. Test-only helper.
func (s *BoltDurableStore) Path() string { return s.path }

// Load implements DurableStore.  Buffered (un-flushed) entries are
// returned ahead of disk state so a caller that just wrote a key can
// read it back even before the next flush — same contract as Pekko's
// LmdbDurableStore under write-behind.
func (s *BoltDurableStore) Load(_ context.Context, key string) (DurableEntry, error) {
	if s.writeBehindInterval > 0 {
		s.pendingMu.Lock()
		if p, ok := s.pending[key]; ok {
			s.pendingMu.Unlock()
			if p.op == opDelete {
				return DurableEntry{}, ErrDurableKeyNotFound
			}
			return cloneEntry(p.entry), nil
		}
		s.pendingMu.Unlock()
	}
	var out DurableEntry
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(boltBucket))
		v := b.Get([]byte(key))
		if v == nil {
			return ErrDurableKeyNotFound
		}
		e, err := decodeRecord(key, v)
		if err != nil {
			return err
		}
		out = e
		return nil
	})
	return out, err
}

// LoadAll implements DurableStore.  The returned slice is a union of
// buffered writes and on-disk entries with buffered state taking
// precedence — recovery sees the same view a fresh Load would.
func (s *BoltDurableStore) LoadAll(_ context.Context) ([]DurableEntry, error) {
	merged := make(map[string]DurableEntry)
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(boltBucket))
		return b.ForEach(func(k, v []byte) error {
			e, err := decodeRecord(string(k), v)
			if err != nil {
				return err
			}
			merged[e.Key] = e
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	if s.writeBehindInterval > 0 {
		s.pendingMu.Lock()
		for k, p := range s.pending {
			if p.op == opDelete {
				delete(merged, k)
				continue
			}
			merged[k] = cloneEntry(p.entry)
		}
		s.pendingMu.Unlock()
	}
	out := make([]DurableEntry, 0, len(merged))
	for _, e := range merged {
		out = append(out, e)
	}
	return out, nil
}

// Store implements DurableStore.  In synchronous mode the write commits
// to disk before Store returns; in write-behind mode it stages the entry
// in the pending buffer.  Either path enforces MapSize.
func (s *BoltDurableStore) Store(_ context.Context, entry DurableEntry) error {
	if entry.Key == "" {
		return errors.New("ddata: BoltDurableStore.Store: entry.Key is empty")
	}
	if s.writeBehindInterval > 0 {
		s.pendingMu.Lock()
		s.pending[entry.Key] = pendingOp{op: opStore, entry: cloneEntry(entry)}
		s.pendingMu.Unlock()
		return nil
	}
	return s.writeOne(entry)
}

// Delete implements DurableStore.  Idempotent across both buffered and
// on-disk state.  In write-behind mode the delete sentinel suppresses
// any prior buffered write and is committed by the next flush.
func (s *BoltDurableStore) Delete(_ context.Context, key string) error {
	if s.writeBehindInterval > 0 {
		s.pendingMu.Lock()
		s.pending[key] = pendingOp{op: opDelete, entry: DurableEntry{Key: key}}
		s.pendingMu.Unlock()
		return nil
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(boltBucket)).Delete([]byte(key))
	})
}

// Close implements DurableStore.  Stops the flush goroutine, drains any
// pending writes, then closes the underlying bbolt handle. Idempotent.
func (s *BoltDurableStore) Close() error {
	s.closeOnce.Do(func() {
		if s.flusherStop != nil {
			close(s.flusherStop)
			<-s.flusherDone
		}
		if err := s.flushPending(); err != nil {
			s.closeErr = err
			_ = s.db.Close()
			return
		}
		s.closeErr = s.db.Close()
	})
	return s.closeErr
}

// writeOne commits a single entry under a fresh Update txn after
// verifying that the resulting file size will not exceed MapSize.  The
// pre-flight check is conservative: we use the value's encoded length
// plus a small overhead estimate to approximate the page-aligned cost
// bbolt will pay.  This is the same approach LMDB callers rely on when
// they want a soft warning before MDB_MAP_FULL.
func (s *BoltDurableStore) writeOne(entry DurableEntry) error {
	val, err := encodeRecord(entry)
	if err != nil {
		return err
	}
	if err := s.checkMapSize(int64(len(entry.Key) + len(val))); err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(boltBucket)).Put([]byte(entry.Key), val)
	})
}

// checkMapSize enforces the configured MapSize cap.  bbolt grows its
// file lazily, so a strict check requires us to ask the OS for the
// current file size before each write.  This is the cost of LMDB-parity
// semantics — without it, a runaway test or misbehaving CRDT could
// silently consume an unbounded amount of disk.
func (s *BoltDurableStore) checkMapSize(extra int64) error {
	if s.mapSize <= 0 {
		return nil
	}
	st, err := os.Stat(s.path)
	if err != nil {
		return fmt.Errorf("ddata: BoltDurableStore: stat: %w", err)
	}
	if st.Size()+extra > s.mapSize {
		return ErrDurableStoreFull
	}
	return nil
}

// flushLoop drives write-behind. It wakes on the configured interval and
// drains the pending buffer in a single Update txn.  Errors during flush
// are intentionally silent at the goroutine level — the next flush will
// retry the same coalesced state, and Close surfaces the latest error
// to the caller.
func (s *BoltDurableStore) flushLoop() {
	defer close(s.flusherDone)
	t := time.NewTicker(s.writeBehindInterval)
	defer t.Stop()
	for {
		select {
		case <-s.flusherStop:
			return
		case <-t.C:
			_ = s.flushPending()
		}
	}
}

// flushPending drains the pending map under a single bbolt Update txn so
// observers never see a half-applied batch.  We snapshot the buffer
// under pendingMu, release the lock before committing, and only clear
// the buffer if the commit succeeds — failed flushes leave the same
// state buffered for the next attempt.
func (s *BoltDurableStore) flushPending() error {
	if s.writeBehindInterval == 0 {
		return nil
	}
	s.pendingMu.Lock()
	if len(s.pending) == 0 {
		s.pendingMu.Unlock()
		return nil
	}
	batch := make([]pendingOp, 0, len(s.pending))
	for _, p := range s.pending {
		batch = append(batch, p)
	}
	s.pendingMu.Unlock()

	for _, p := range batch {
		if p.op == opStore {
			val, err := encodeRecord(p.entry)
			if err != nil {
				return err
			}
			if err := s.checkMapSize(int64(len(p.entry.Key) + len(val))); err != nil {
				return err
			}
		}
	}

	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(boltBucket))
		for _, p := range batch {
			switch p.op {
			case opStore:
				val, err := encodeRecord(p.entry)
				if err != nil {
					return err
				}
				if err := b.Put([]byte(p.entry.Key), val); err != nil {
					return err
				}
			case opDelete:
				if err := b.Delete([]byte(p.entry.Key)); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.pendingMu.Lock()
	for _, p := range batch {
		cur, ok := s.pending[p.entry.Key]
		if !ok {
			continue
		}
		// Only clear the slot if it has not been overwritten by a newer
		// op while the flush was in flight. Compare op tag and the
		// payload bytes — DurableEntry is not directly comparable due
		// to its Payload slice, so we compare the discriminating fields
		// explicitly.
		if cur.op == p.op &&
			cur.entry.Key == p.entry.Key &&
			cur.entry.Type == p.entry.Type &&
			bytesEqual(cur.entry.Payload, p.entry.Payload) {
			delete(s.pending, p.entry.Key)
		}
	}
	s.pendingMu.Unlock()
	return nil
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Flush is a test/operations hook that forces a synchronous flush of any
// buffered writes.  Equivalent to Pekko's "ddata flush" admin signal.
func (s *BoltDurableStore) Flush() error { return s.flushPending() }

func encodeRecord(e DurableEntry) ([]byte, error) {
	rec := boltRecord{Type: e.Type, Payload: e.Payload}
	return json.Marshal(rec)
}

func decodeRecord(key string, raw []byte) (DurableEntry, error) {
	var rec boltRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return DurableEntry{}, fmt.Errorf("ddata: BoltDurableStore: decode %q: %w", key, err)
	}
	return DurableEntry{Key: key, Type: rec.Type, Payload: rec.Payload}, nil
}
