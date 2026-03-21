/*
 * read_journal.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"slices"
)

// Offset is a position in the global event log expressed as a 0-based line
// index into the JSONL file.
//
// Semantics used throughout the read-journal and projection APIs:
//   - Offset(0)  — beginning of the log (no events processed yet).
//   - Offset(N)  — the next line to read starts at index N, meaning all lines
//     with index < N have already been consumed.
//
// After processing an event at offset N, store N+1 so the next poll resumes
// from the line immediately after.
type Offset int64

// TaggedEvent is an event returned by the ReadJournal; it carries full
// cross-actor context needed for CQRS read models.
type TaggedEvent struct {
	// PersistenceId is the actor that produced this event.
	PersistenceId string
	// SeqNr is the per-actor sequence number.
	SeqNr int64
	// Tags are the classification labels stored at write time.
	Tags []string
	// Payload is the decoded event value.
	Payload any
	// Offset is the 0-based line index of this event in the JSONL file.
	// Store Offset+1 as the next read position to resume after this event.
	Offset Offset
}

// ReadJournal provides query-side (read model) access to the event journal.
//
// Both methods scan the underlying storage eagerly and return all matching
// events in global append order.  For long-lived projections call them
// repeatedly with the last saved Offset to poll for new events.
type ReadJournal interface {
	// EventsByPersistenceId returns all events written by persistenceId with
	// seqNr >= fromSeqNr.
	EventsByPersistenceId(pid string, fromSeqNr int64) ([]TaggedEvent, error)

	// EventsByTag returns all events that carry tag and whose global Offset is
	// >= fromOffset.  This is the primary query for CQRS projections.
	EventsByTag(tag string, fromOffset Offset) ([]TaggedEvent, error)
}

// ── FileReadJournal ───────────────────────────────────────────────────────────

// FileReadJournal wraps a FileJournal and implements ReadJournal by scanning
// the JSONL file.  It shares the FileJournal's mutex to avoid torn reads
// during concurrent writes.
type FileReadJournal struct {
	j *FileJournal
}

// NewFileReadJournal returns a FileReadJournal backed by journal.
func NewFileReadJournal(journal *FileJournal) *FileReadJournal {
	return &FileReadJournal{j: journal}
}

// EventsByPersistenceId returns all events for pid with seqNr >= fromSeqNr.
func (r *FileReadJournal) EventsByPersistenceId(pid string, fromSeqNr int64) ([]TaggedEvent, error) {
	r.j.mu.Lock()
	defer r.j.mu.Unlock()

	records, err := r.scanAll()
	if err != nil {
		return nil, fmt.Errorf("FileReadJournal.EventsByPersistenceId: %w", err)
	}

	var out []TaggedEvent
	for _, rec := range records {
		if rec.record.PersistenceId != pid || rec.record.SeqNr < fromSeqNr {
			continue
		}
		ev, err := r.decode(rec.record)
		if err != nil {
			return nil, err
		}
		ev.Offset = rec.offset
		out = append(out, ev)
	}
	return out, nil
}

// EventsByTag returns all events that carry tag and whose Offset >= fromOffset.
func (r *FileReadJournal) EventsByTag(tag string, fromOffset Offset) ([]TaggedEvent, error) {
	r.j.mu.Lock()
	defer r.j.mu.Unlock()

	records, err := r.scanAll()
	if err != nil {
		return nil, fmt.Errorf("FileReadJournal.EventsByTag: %w", err)
	}

	var out []TaggedEvent
	for _, rec := range records {
		if rec.offset < fromOffset {
			continue
		}
		if !hasTag(rec.record.Tags, tag) {
			continue
		}
		ev, err := r.decode(rec.record)
		if err != nil {
			return nil, err
		}
		ev.Offset = rec.offset
		out = append(out, ev)
	}
	return out, nil
}

// ── internal helpers ──────────────────────────────────────────────────────────

type indexedRecord struct {
	offset Offset
	record journalRecord
}

// scanAll reads every line from the JSONL file and returns the raw records
// with their 0-based line offsets.  Caller must hold j.mu.
func (r *FileReadJournal) scanAll() ([]indexedRecord, error) {
	f, err := os.Open(r.j.path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	var out []indexedRecord
	scanner := bufio.NewScanner(f)
	var lineIdx Offset
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			lineIdx++
			continue
		}
		var rec journalRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("parse line %d: %w", lineIdx, err)
		}
		out = append(out, indexedRecord{offset: lineIdx, record: rec})
		lineIdx++
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}
	return out, nil
}

// decode calls the FileJournal's decoder to reconstruct the event payload.
func (r *FileReadJournal) decode(rec journalRecord) (TaggedEvent, error) {
	var payload any
	if r.j.decoder != nil {
		var err error
		payload, err = r.j.decoder(rec.Type, rec.Data)
		if err != nil {
			return TaggedEvent{}, fmt.Errorf("decode %q seqNr=%d: %w", rec.PersistenceId, rec.SeqNr, err)
		}
	} else {
		payload = rec.Data
	}
	return TaggedEvent{
		PersistenceId: rec.PersistenceId,
		SeqNr:         rec.SeqNr,
		Tags:          rec.Tags,
		Payload:       payload,
	}, nil
}

func hasTag(tags []string, target string) bool {
	return slices.Contains(tags, target)
}
