/*
 * flight_recorder.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// flightRecorderBufSize is the fixed ring buffer capacity (shared across all levels).
const flightRecorderBufSize = 8192

// EventLevel controls flight recorder verbosity.
type EventLevel int

const (
	// LevelLifecycle records state transitions, quarantine, heartbeat miss/RTT,
	// compression adverts, and large-frame events. Default.
	LevelLifecycle EventLevel = iota
	// LevelSampled adds 1-in-100 ordinary frame send/recv events.
	LevelSampled
	// LevelFull records every frame send/receive.
	LevelFull
)

// ParseEventLevel parses a HOCON level string into an EventLevel.
// Unknown values default to LevelLifecycle.
func ParseEventLevel(s string) EventLevel {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "sampled":
		return LevelSampled
	case "full":
		return LevelFull
	default:
		return LevelLifecycle
	}
}

// EventCategory tags which subsystem produced the event.
type EventCategory string

const (
	CatHandshake   EventCategory = "handshake"
	CatQuarantine  EventCategory = "quarantine"
	CatHeartbeat   EventCategory = "heartbeat"
	CatCompression EventCategory = "compression"
	CatLargeFrame  EventCategory = "large-frame"
	CatFrame       EventCategory = "frame"
)

// EventSeverity is the human-visible log level for an event.
type EventSeverity string

const (
	SeverityInfo  EventSeverity = "INFO"
	SeverityWarn  EventSeverity = "WARN"
	SeverityError EventSeverity = "ERROR"
)

// FlightEvent is one ring buffer entry.
type FlightEvent struct {
	Timestamp time.Time      `json:"ts"`
	Severity  EventSeverity  `json:"severity"`
	Category  EventCategory  `json:"category"`
	Message   string         `json:"msg"`
	Fields    map[string]any `json:"fields,omitempty"`
}

// FormatText renders the event as a fixed-width human-readable log line.
func (e *FlightEvent) FormatText() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s %-5s [%-14s] %-22s",
		e.Timestamp.UTC().Format(time.RFC3339Nano),
		string(e.Severity),
		string(e.Category),
		e.Message,
	)
	for k, v := range e.Fields {
		fmt.Fprintf(&b, " %s=%v", k, v)
	}
	return b.String()
}

// RingBuffer is a fixed-size circular buffer of FlightEvents. Thread-safe.
type RingBuffer struct {
	mu     sync.Mutex
	events [flightRecorderBufSize]FlightEvent
	head   int // next write position
	count  int // total events written (may exceed capacity)
}

// Append adds an event, overwriting the oldest event when the buffer is full.
func (rb *RingBuffer) Append(e FlightEvent) {
	rb.mu.Lock()
	rb.events[rb.head] = e
	rb.head = (rb.head + 1) % flightRecorderBufSize
	rb.count++
	rb.mu.Unlock()
}

// Snapshot returns a copy of all buffered events in chronological order (oldest first).
func (rb *RingBuffer) Snapshot() []FlightEvent {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	n := rb.count
	if n > flightRecorderBufSize {
		n = flightRecorderBufSize
	}
	if n == 0 {
		return nil
	}

	result := make([]FlightEvent, n)
	start := rb.head - n
	if start < 0 {
		start += flightRecorderBufSize
	}
	for i := 0; i < n; i++ {
		result[i] = rb.events[(start+i)%flightRecorderBufSize]
	}
	return result
}

// Clear resets the buffer, discarding all events.
func (rb *RingBuffer) Clear() {
	rb.mu.Lock()
	rb.head = 0
	rb.count = 0
	rb.mu.Unlock()
}

// FlightRecorder is the centralized event recorder owned by NodeManager.
// One RingBuffer per remote association, keyed by "host:port".
type FlightRecorder struct {
	mu       sync.RWMutex
	logs     map[string]*RingBuffer
	level    EventLevel
	enabled  bool
	sampleN  uint64             // frame sampling denominator (default 100)
	counters map[string]*uint64 // per-association atomic frame counters
}

// NewFlightRecorder creates a FlightRecorder. When !enabled, all methods are no-ops.
func NewFlightRecorder(enabled bool, level EventLevel) *FlightRecorder {
	return &FlightRecorder{
		logs:     make(map[string]*RingBuffer),
		level:    level,
		enabled:  enabled,
		sampleN:  100,
		counters: make(map[string]*uint64),
	}
}

// Emit records an event for the given association address ("host:port").
// No-op when the recorder is disabled.
func (fr *FlightRecorder) Emit(remoteAddr string, e FlightEvent) {
	if !fr.enabled {
		return
	}
	rb := fr.getOrCreateBuffer(remoteAddr)
	rb.Append(e)
}

// EmitFrame records a frame-level event, gated by the configured level.
// At LevelLifecycle: no-op.
// At LevelSampled: records 1 in sampleN frames.
// At LevelFull: records every frame.
func (fr *FlightRecorder) EmitFrame(remoteAddr string, direction string, size int, serializerID int32) {
	if !fr.enabled || fr.level < LevelSampled {
		return
	}

	counter := fr.getOrCreateCounter(remoteAddr)
	seq := atomic.AddUint64(counter, 1)

	if fr.level == LevelSampled && seq%fr.sampleN != 0 {
		return
	}

	fr.Emit(remoteAddr, FlightEvent{
		Timestamp: time.Now(),
		Severity:  SeverityInfo,
		Category:  CatFrame,
		Message:   direction,
		Fields: map[string]any{
			"size":         size,
			"serializerID": serializerID,
		},
	})
}

// Snapshot returns all events for one association in chronological order.
// Returns nil when the association is unknown or the recorder is disabled.
func (fr *FlightRecorder) Snapshot(remoteAddr string) []FlightEvent {
	if !fr.enabled {
		return nil
	}
	fr.mu.RLock()
	rb, ok := fr.logs[remoteAddr]
	fr.mu.RUnlock()
	if !ok {
		return nil
	}
	return rb.Snapshot()
}

// SnapshotAll returns events for every known association.
// Returns nil when the recorder is disabled.
func (fr *FlightRecorder) SnapshotAll() map[string][]FlightEvent {
	if !fr.enabled {
		return nil
	}
	fr.mu.RLock()
	keys := make([]string, 0, len(fr.logs))
	for k := range fr.logs {
		keys = append(keys, k)
	}
	fr.mu.RUnlock()

	result := make(map[string][]FlightEvent, len(keys))
	for _, k := range keys {
		if snap := fr.Snapshot(k); len(snap) > 0 {
			result[k] = snap
		}
	}
	return result
}

// DumpOnQuarantine logs the full event snapshot for an association to slog at WARN level.
// Called automatically when an association enters QUARANTINED state.
func (fr *FlightRecorder) DumpOnQuarantine(remoteAddr string) {
	if !fr.enabled {
		return
	}
	events := fr.Snapshot(remoteAddr)
	if len(events) == 0 {
		return
	}
	var b strings.Builder
	fmt.Fprintf(&b, "flight-recorder dump for quarantined association %s (%d events):\n", remoteAddr, len(events))
	for i := range events {
		b.WriteString("  ")
		b.WriteString(events[i].FormatText())
		b.WriteByte('\n')
	}
	slog.Warn(b.String())
}

// DumpAll writes all association event logs to w in human-readable text format.
// Called on process shutdown.
func (fr *FlightRecorder) DumpAll(w io.Writer) {
	if !fr.enabled {
		return
	}
	all := fr.SnapshotAll()
	for addr, events := range all {
		fmt.Fprintf(w, "=== flight-recorder: %s (%d events) ===\n", addr, len(events))
		for i := range events {
			fmt.Fprintln(w, events[i].FormatText())
		}
	}
}

func (fr *FlightRecorder) getOrCreateBuffer(remoteAddr string) *RingBuffer {
	fr.mu.RLock()
	rb, ok := fr.logs[remoteAddr]
	fr.mu.RUnlock()
	if ok {
		return rb
	}

	fr.mu.Lock()
	defer fr.mu.Unlock()
	if rb, ok = fr.logs[remoteAddr]; ok {
		return rb
	}
	rb = &RingBuffer{}
	fr.logs[remoteAddr] = rb
	return rb
}

func (fr *FlightRecorder) getOrCreateCounter(remoteAddr string) *uint64 {
	fr.mu.RLock()
	c, ok := fr.counters[remoteAddr]
	fr.mu.RUnlock()
	if ok {
		return c
	}

	fr.mu.Lock()
	defer fr.mu.Unlock()
	if c, ok = fr.counters[remoteAddr]; ok {
		return c
	}
	c = new(uint64)
	fr.counters[remoteAddr] = c
	return c
}
