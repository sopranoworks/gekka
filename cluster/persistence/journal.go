/*
 * journal.go
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
	"reflect"
	"sync"
)

// Event is a single persisted entry returned during replay.
type Event struct {
	// SeqNr is the monotonically increasing sequence number within one
	// persistence ID, starting at 1.
	SeqNr int64
	// Payload is the reconstructed event value as produced by the
	// EventDecoder registered with the FileJournal.
	Payload any
}

// EventStream is a forward-only cursor over replayed events.
type EventStream interface {
	// Next advances the stream and returns the next event.
	// ok is false when there are no more events.
	Next() (Event, bool)
	// Close releases any resources held by the stream.
	Close() error
}

// Journal is the storage interface for event sourcing.
//
// Write appends a single event; Read returns all events for a persistence ID
// starting at fromSeqNr (inclusive).  Implementations must be safe for
// concurrent use.
type Journal interface {
	Write(persistenceId string, seqNr int64, event any) error
	Read(persistenceId string, fromSeqNr int64) (EventStream, error)
}

// EventDecoder reconstructs a typed event from its serialised form.
//
// typeName is the bare struct name stored at write time (e.g. "IncrementedEvent").
// raw is the JSON-encoded payload.  Return an error to skip the event.
type EventDecoder func(typeName string, raw json.RawMessage) (any, error)

// ── FileJournal ───────────────────────────────────────────────────────────────

// FileJournal stores events in JSONL format (one JSON object per line).
//
// Each line is a journalRecord with:
//
//	{"pid":"...","seqNr":1,"type":"MyEvent","data":{...}}
//
// The caller must supply an EventDecoder so events can be round-tripped through
// JSON.  Write uses reflect.TypeOf(event).Name() to derive the type tag.
type FileJournal struct {
	path    string
	decoder EventDecoder
	mu      sync.Mutex
}

// NewFileJournal returns a FileJournal that writes to path.
// decoder is called during Read to reconstruct events; it must handle every
// event type written by this journal.
func NewFileJournal(path string, decoder EventDecoder) *FileJournal {
	return &FileJournal{path: path, decoder: decoder}
}

// journalRecord is the on-disk envelope for a single event.
type journalRecord struct {
	PersistenceId string          `json:"pid"`
	SeqNr         int64           `json:"seqNr"`
	Type          string          `json:"type"`
	Data          json.RawMessage `json:"data"`
}

// Write appends event to the JSONL file.  The type tag is derived from the
// concrete dynamic type of event (e.g. "IncrementedEvent").
func (j *FileJournal) Write(persistenceId string, seqNr int64, event any) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("FileJournal.Write: marshal event: %w", err)
	}

	typeName := reflect.TypeOf(event).Name()
	rec := journalRecord{
		PersistenceId: persistenceId,
		SeqNr:         seqNr,
		Type:          typeName,
		Data:          json.RawMessage(data),
	}
	line, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("FileJournal.Write: marshal record: %w", err)
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	f, err := os.OpenFile(j.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("FileJournal.Write: open file: %w", err)
	}
	defer f.Close()

	_, err = fmt.Fprintf(f, "%s\n", line)
	return err
}

// Read scans the JSONL file and returns all events for persistenceId with
// seqNr >= fromSeqNr.  If the file does not exist the stream is empty.
func (j *FileJournal) Read(persistenceId string, fromSeqNr int64) (EventStream, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	f, err := os.Open(j.path)
	if os.IsNotExist(err) {
		return &sliceStream{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("FileJournal.Read: open file: %w", err)
	}
	defer f.Close()

	var events []Event
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec journalRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("FileJournal.Read: parse record: %w", err)
		}
		if rec.PersistenceId != persistenceId || rec.SeqNr < fromSeqNr {
			continue
		}
		if j.decoder == nil {
			// No decoder: return raw JSON message as payload.
			events = append(events, Event{SeqNr: rec.SeqNr, Payload: rec.Data})
			continue
		}
		payload, err := j.decoder(rec.Type, rec.Data)
		if err != nil {
			return nil, fmt.Errorf("FileJournal.Read: decode event (type=%q seqNr=%d): %w",
				rec.Type, rec.SeqNr, err)
		}
		events = append(events, Event{SeqNr: rec.SeqNr, Payload: payload})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("FileJournal.Read: scan: %w", err)
	}
	return &sliceStream{events: events}, nil
}

// ── sliceStream ───────────────────────────────────────────────────────────────

type sliceStream struct {
	events []Event
	idx    int
}

func (s *sliceStream) Next() (Event, bool) {
	if s.idx >= len(s.events) {
		return Event{}, false
	}
	e := s.events[s.idx]
	s.idx++
	return e, true
}

func (s *sliceStream) Close() error { return nil }
