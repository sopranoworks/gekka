/*
 * replay_filter.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

// ReplayFilterMode controls how the replay filter reacts to a
// detected anomaly (a duplicated SequenceNr from a different writer
// or a sequence-number step that goes backwards within the same
// persistenceId). The four modes mirror Pekko's:
//
//   - ReplayFilterRepairByDiscardOld: discard the older writer's events
//     once a newer writer is observed at the same sequence number;
//     emit a warning. This is Pekko's default and matches the name in
//     reference.conf.
//   - ReplayFilterFail: stop the replay with ErrReplayFilterFailed.
//   - ReplayFilterWarn: emit a warning but pass every event through
//     untouched.
//   - ReplayFilterOff: bypass the filter entirely.
type ReplayFilterMode string

const (
	ReplayFilterRepairByDiscardOld ReplayFilterMode = "repair-by-discard-old"
	ReplayFilterFail               ReplayFilterMode = "fail"
	ReplayFilterWarn               ReplayFilterMode = "warn"
	ReplayFilterOff                ReplayFilterMode = "off"
)

// ReplayFilterConfig mirrors pekko.persistence.journal-plugin-fallback.replay-filter.*.
//
// WindowSize bounds the look-ahead buffer used to disambiguate writers;
// MaxOldWriters bounds how many distinct old WriterUuid values the
// filter remembers (older entries are pruned). Debug enables verbose
// per-event logging — off by default to keep recovery hot-paths quiet.
type ReplayFilterConfig struct {
	Mode          ReplayFilterMode
	WindowSize    int
	MaxOldWriters int
	Debug         bool
}

// DefaultReplayFilterConfig returns Pekko's documented defaults
// (repair-by-discard-old, window-size=100, max-old-writers=10).
func DefaultReplayFilterConfig() ReplayFilterConfig {
	return ReplayFilterConfig{
		Mode:          ReplayFilterRepairByDiscardOld,
		WindowSize:    100,
		MaxOldWriters: 10,
		Debug:         false,
	}
}

// ErrReplayFilterFailed is returned by FilteringJournal.ReplayMessages
// when the configured mode is "fail" and an anomaly is detected.
var ErrReplayFilterFailed = errors.New("persistence: replay filter rejected the event stream")

// ── package-level config (HOCON-driven defaults) ──────────────────────────

var (
	replayFilterCfgValue        atomic.Value // ReplayFilterConfig
	recoveryEventTimeoutNanos   atomic.Int64
	recoveryEventTimeoutDefault = 30 * time.Second
)

func init() {
	replayFilterCfgValue.Store(DefaultReplayFilterConfig())
	recoveryEventTimeoutNanos.Store(int64(recoveryEventTimeoutDefault))
}

// SetReplayFilterConfig installs the replay-filter config used by
// WrapJournalWithReplayFilter and FilteringJournal default. Empty Mode
// keeps the current value; non-positive numerics keep the current value.
//
// HOCON: pekko.persistence.journal-plugin-fallback.replay-filter.*
func SetReplayFilterConfig(cfg ReplayFilterConfig) {
	merged := GetReplayFilterConfig()
	if cfg.Mode != "" {
		merged.Mode = cfg.Mode
	}
	if cfg.WindowSize > 0 {
		merged.WindowSize = cfg.WindowSize
	}
	if cfg.MaxOldWriters > 0 {
		merged.MaxOldWriters = cfg.MaxOldWriters
	}
	merged.Debug = cfg.Debug
	replayFilterCfgValue.Store(merged)
}

// GetReplayFilterConfig returns the currently active replay-filter config.
func GetReplayFilterConfig() ReplayFilterConfig {
	v, ok := replayFilterCfgValue.Load().(ReplayFilterConfig)
	if !ok || v.Mode == "" {
		return DefaultReplayFilterConfig()
	}
	return v
}

// SetRecoveryEventTimeout installs the per-event recovery deadline used
// by the recovery-event-timeout decorator. Non-positive values restore
// the 30s default.
//
// HOCON: pekko.persistence.journal-plugin-fallback.recovery-event-timeout
func SetRecoveryEventTimeout(d time.Duration) {
	if d <= 0 {
		d = recoveryEventTimeoutDefault
	}
	recoveryEventTimeoutNanos.Store(int64(d))
}

// GetRecoveryEventTimeout returns the currently active recovery-event
// timeout (defaults to 30s).
func GetRecoveryEventTimeout() time.Duration {
	v := recoveryEventTimeoutNanos.Load()
	if v <= 0 {
		return recoveryEventTimeoutDefault
	}
	return time.Duration(v)
}

// ── FilteringJournal ──────────────────────────────────────────────────────

// FilteringJournal wraps a Journal and applies a replay-filter to the
// ReplayMessages call path. The other Journal methods (write/delete/
// highest-seq-nr) are forwarded unmodified — the filter is a recovery-
// time guard, not a write-side one.
type FilteringJournal struct {
	inner Journal
	cfg   ReplayFilterConfig
}

// NewFilteringJournal returns a journal that filters replay according
// to cfg. When cfg.Mode is empty, the active package-level config is
// used.
func NewFilteringJournal(inner Journal, cfg ReplayFilterConfig) *FilteringJournal {
	if cfg.Mode == "" {
		cfg = GetReplayFilterConfig()
	}
	if cfg.WindowSize <= 0 {
		cfg.WindowSize = 100
	}
	if cfg.MaxOldWriters <= 0 {
		cfg.MaxOldWriters = 10
	}
	return &FilteringJournal{inner: inner, cfg: cfg}
}

// Mode reports the active filter mode.
func (f *FilteringJournal) Mode() ReplayFilterMode { return f.cfg.Mode }

// AsyncWriteMessages forwards directly — the filter is read-side only.
func (f *FilteringJournal) AsyncWriteMessages(ctx context.Context, messages []PersistentRepr) error {
	return f.inner.AsyncWriteMessages(ctx, messages)
}

// ReadHighestSequenceNr forwards.
func (f *FilteringJournal) ReadHighestSequenceNr(ctx context.Context, persistenceId string, fromSequenceNr uint64) (uint64, error) {
	return f.inner.ReadHighestSequenceNr(ctx, persistenceId, fromSequenceNr)
}

// AsyncDeleteMessagesTo forwards.
func (f *FilteringJournal) AsyncDeleteMessagesTo(ctx context.Context, persistenceId string, toSequenceNr uint64) error {
	return f.inner.AsyncDeleteMessagesTo(ctx, persistenceId, toSequenceNr)
}

// ReplayMessages applies the configured filter mode to the inner
// journal's stream of PersistentRepr.
//
//	off                   — pass everything; never errors.
//	warn                  — pass everything; emit a single warning per
//	                        anomaly observed.
//	fail                  — emit events until the first anomaly, then
//	                        return ErrReplayFilterFailed.
//	repair-by-discard-old — track the latest writer per sequence number
//	                        through a look-ahead window; on conflict,
//	                        emit only the newest writer's events and
//	                        skip the older ones, with a warning.
//
// Anomaly detection works by tracking writer fingerprints in a sliding
// window. When two PersistentRepr share a (PersistenceID, SequenceNr)
// but disagree on WriterUuid, the newer writer wins. When events go
// out-of-order within the same writer, that is *also* treated as an
// anomaly under "fail" mode (and surfaced as a warning otherwise).
func (f *FilteringJournal) ReplayMessages(ctx context.Context, persistenceId string, fromSequenceNr, toSequenceNr uint64, max uint64, callback func(PersistentRepr)) error {
	if f.cfg.Mode == ReplayFilterOff {
		return f.inner.ReplayMessages(ctx, persistenceId, fromSequenceNr, toSequenceNr, max, callback)
	}

	state := newFilterState(f.cfg)
	var failErr error

	err := f.inner.ReplayMessages(ctx, persistenceId, fromSequenceNr, toSequenceNr, max, func(repr PersistentRepr) {
		if failErr != nil {
			return
		}
		anomaly, message := state.observe(repr)
		switch f.cfg.Mode {
		case ReplayFilterFail:
			if anomaly {
				failErr = fmt.Errorf("%w: %s", ErrReplayFilterFailed, message)
				return
			}
			callback(repr)
		case ReplayFilterWarn:
			if anomaly {
				log.Printf("ReplayFilter[%s]: %s (mode=warn, passthrough)", persistenceId, message)
			}
			callback(repr)
		case ReplayFilterRepairByDiscardOld:
			if anomaly {
				if state.lastDiscarded {
					if f.cfg.Debug {
						log.Printf("ReplayFilter[%s]: discarding old writer event seq=%d (mode=repair-by-discard-old)", persistenceId, repr.SequenceNr)
					}
					return
				}
				log.Printf("ReplayFilter[%s]: %s (mode=repair-by-discard-old)", persistenceId, message)
			}
			callback(repr)
		default:
			callback(repr)
		}
	})
	if err != nil {
		return err
	}
	return failErr
}

var _ Journal = (*FilteringJournal)(nil)

// filterState tracks the look-ahead window that lets the filter decide
// whether a newly observed event is "old" relative to the writer most
// recently winning each sequence number. Implementation is a tiny
// LRU-by-window: the writer fingerprint of the latest event for each
// SequenceNr in the trailing window is remembered, plus a bounded set
// of "old" writer UUIDs to suppress without re-warning.
type filterState struct {
	cfg              ReplayFilterConfig
	latestWriterPerSeq map[uint64]string
	seqOrder         []uint64
	oldWriters       []string
	lastSeq          uint64
	haveLastSeq      bool
	lastWriter       string
	lastDiscarded    bool
}

func newFilterState(cfg ReplayFilterConfig) *filterState {
	return &filterState{
		cfg:                cfg,
		latestWriterPerSeq: make(map[uint64]string),
	}
}

func (s *filterState) rememberOldWriter(uuid string) {
	if uuid == "" {
		return
	}
	for _, w := range s.oldWriters {
		if w == uuid {
			return
		}
	}
	s.oldWriters = append(s.oldWriters, uuid)
	if len(s.oldWriters) > s.cfg.MaxOldWriters {
		s.oldWriters = s.oldWriters[len(s.oldWriters)-s.cfg.MaxOldWriters:]
	}
}

func (s *filterState) isOldWriter(uuid string) bool {
	if uuid == "" {
		return false
	}
	for _, w := range s.oldWriters {
		if w == uuid {
			return true
		}
	}
	return false
}

// observe registers the event in the sliding window and reports
// whether it triggered an anomaly. Two anomaly types are detected:
//
//  1. Writer conflict at the same sequence number (different
//     WriterUuid for an already-seen SequenceNr).
//  2. Out-of-order sequence (next < last within the same writer or
//     across writers in fail mode).
func (s *filterState) observe(repr PersistentRepr) (bool, string) {
	s.lastDiscarded = false
	uuid := repr.WriterUuid
	seq := repr.SequenceNr

	if s.isOldWriter(uuid) {
		s.lastDiscarded = true
		return true, fmt.Sprintf("event from old writer %q at seq=%d", uuid, seq)
	}

	if existing, ok := s.latestWriterPerSeq[seq]; ok && existing != "" && uuid != "" && existing != uuid {
		s.rememberOldWriter(existing)
		s.latestWriterPerSeq[seq] = uuid
		return true, fmt.Sprintf("writer changed at seq=%d: %q -> %q", seq, existing, uuid)
	}

	if s.haveLastSeq && seq < s.lastSeq && uuid == s.lastWriter && uuid != "" {
		return true, fmt.Sprintf("out-of-order seq=%d after %d (writer=%q)", seq, s.lastSeq, uuid)
	}

	if uuid != "" {
		s.latestWriterPerSeq[seq] = uuid
		s.seqOrder = append(s.seqOrder, seq)
		if len(s.seqOrder) > s.cfg.WindowSize {
			drop := s.seqOrder[0]
			s.seqOrder = s.seqOrder[1:]
			delete(s.latestWriterPerSeq, drop)
		}
	}
	s.lastSeq = seq
	s.haveLastSeq = true
	s.lastWriter = uuid
	return false, ""
}

// ── RecoveryEventTimeoutJournal ───────────────────────────────────────────

// RecoveryEventTimeoutJournal enforces a per-event deadline during
// replay: if the journal does not emit the next event within
// recoveryEventTimeout, the replay is aborted with
// ErrRecoveryEventTimeout.
//
// This mirrors Pekko's recovery-event-timeout, which is a watchdog on
// inter-event silence (not on the total replay duration).
type RecoveryEventTimeoutJournal struct {
	inner   Journal
	timeout time.Duration
}

// ErrRecoveryEventTimeout is returned by ReplayMessages when no event
// arrives within the configured per-event window.
var ErrRecoveryEventTimeout = errors.New("persistence: recovery-event-timeout exceeded")

// NewRecoveryEventTimeoutJournal returns a journal that enforces a
// per-event deadline during replay. timeout <= 0 falls back to the
// active package-level value.
func NewRecoveryEventTimeoutJournal(inner Journal, timeout time.Duration) *RecoveryEventTimeoutJournal {
	if timeout <= 0 {
		timeout = GetRecoveryEventTimeout()
	}
	return &RecoveryEventTimeoutJournal{inner: inner, timeout: timeout}
}

func (r *RecoveryEventTimeoutJournal) AsyncWriteMessages(ctx context.Context, messages []PersistentRepr) error {
	return r.inner.AsyncWriteMessages(ctx, messages)
}

func (r *RecoveryEventTimeoutJournal) ReadHighestSequenceNr(ctx context.Context, persistenceId string, fromSequenceNr uint64) (uint64, error) {
	return r.inner.ReadHighestSequenceNr(ctx, persistenceId, fromSequenceNr)
}

func (r *RecoveryEventTimeoutJournal) AsyncDeleteMessagesTo(ctx context.Context, persistenceId string, toSequenceNr uint64) error {
	return r.inner.AsyncDeleteMessagesTo(ctx, persistenceId, toSequenceNr)
}

// ReplayMessages wraps the inner journal's replay with a per-event
// watchdog. The deadline is rearmed every time the underlying journal
// emits an event; if the silence between events (or before the first
// event) exceeds r.timeout, the replay is cancelled and the call
// returns ErrRecoveryEventTimeout.
func (r *RecoveryEventTimeoutJournal) ReplayMessages(ctx context.Context, persistenceId string, fromSequenceNr, toSequenceNr uint64, max uint64, callback func(PersistentRepr)) error {
	if r.timeout <= 0 {
		return r.inner.ReplayMessages(ctx, persistenceId, fromSequenceNr, toSequenceNr, max, callback)
	}

	innerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	beat := make(chan struct{}, 1)
	done := make(chan error, 1)
	timedOut := make(chan struct{})

	go func() {
		err := r.inner.ReplayMessages(innerCtx, persistenceId, fromSequenceNr, toSequenceNr, max, func(repr PersistentRepr) {
			select {
			case <-timedOut:
				return
			default:
			}
			callback(repr)
			select {
			case beat <- struct{}{}:
			default:
			}
		})
		done <- err
	}()

	timer := time.NewTimer(r.timeout)
	defer timer.Stop()
	for {
		select {
		case err := <-done:
			return err
		case <-beat:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(r.timeout)
		case <-timer.C:
			close(timedOut)
			cancel()
			<-done
			return ErrRecoveryEventTimeout
		case <-ctx.Done():
			cancel()
			<-done
			return ctx.Err()
		}
	}
}

var _ Journal = (*RecoveryEventTimeoutJournal)(nil)

// ── public assembly helper ────────────────────────────────────────────────

// WrapJournalWithFallbacks returns inner wrapped with the
// journal-plugin-fallback decorators in the order that mirrors Pekko's
// internal stack:
//
//	circuit-breaker  ──→  replay-filter  ──→  recovery-event-timeout  ──→  inner
//
// Callers who want individual decorators can construct them directly.
// Pass-through behaviour: if the breaker config is the zero value or
// CallTimeout is zero, no breaker is added; if the filter mode is "off"
// no filter is added; if recoveryEventTimeout is zero, no timeout
// watchdog is added. This keeps the wrapping cheap when fallbacks are
// disabled in HOCON.
func WrapJournalWithFallbacks(inner Journal, breaker CircuitBreakerConfig, filter ReplayFilterConfig, recoveryEventTimeout time.Duration) Journal {
	wrapped := inner
	if recoveryEventTimeout > 0 {
		wrapped = NewRecoveryEventTimeoutJournal(wrapped, recoveryEventTimeout)
	}
	if filter.Mode != "" && filter.Mode != ReplayFilterOff {
		wrapped = NewFilteringJournal(wrapped, filter)
	}
	if breaker != (CircuitBreakerConfig{}) {
		wrapped = NewCircuitBreakerJournal(wrapped, breaker)
	}
	return wrapped
}

// WrapSnapshotStoreWithFallbacks wraps inner with the snapshot-store
// circuit-breaker. Returns inner unchanged when breaker is the zero
// value.
func WrapSnapshotStoreWithFallbacks(inner SnapshotStore, breaker CircuitBreakerConfig) SnapshotStore {
	if breaker == (CircuitBreakerConfig{}) {
		return inner
	}
	return NewCircuitBreakerSnapshotStore(inner, breaker)
}
