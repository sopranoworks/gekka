/*
 * circuit_breaker.go
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
	"sync"
	"sync/atomic"
	"time"
)

// CircuitBreakerConfig holds the three knobs that govern a Pekko-style
// persistence circuit breaker:
//
//   - MaxFailures: how many consecutive call failures (or call timeouts)
//     trip the breaker from CLOSED to OPEN.
//   - CallTimeout: per-call deadline; calls that exceed it count as
//     failures and surface ErrCircuitBreakerCallTimeout to the caller.
//   - ResetTimeout: how long the breaker stays OPEN before allowing a
//     single trial call (HALF_OPEN). Trial success resets to CLOSED;
//     trial failure trips back to OPEN with the wall-clock reset.
//
// Mirrors pekko.persistence.{journal,snapshot-store}-plugin-fallback.circuit-breaker.*
// from the Pekko reference.conf.
type CircuitBreakerConfig struct {
	MaxFailures  int
	CallTimeout  time.Duration
	ResetTimeout time.Duration
}

// DefaultJournalBreakerConfig returns Pekko's documented defaults for
// journal-plugin-fallback.circuit-breaker.* (10 failures, 10s call,
// 30s reset).
func DefaultJournalBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		MaxFailures:  10,
		CallTimeout:  10 * time.Second,
		ResetTimeout: 30 * time.Second,
	}
}

// DefaultSnapshotBreakerConfig returns Pekko's documented defaults for
// snapshot-store-plugin-fallback.circuit-breaker.* (5 failures, 20s call,
// 60s reset).
func DefaultSnapshotBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		MaxFailures:  5,
		CallTimeout:  20 * time.Second,
		ResetTimeout: 60 * time.Second,
	}
}

// breakerState is the three-state circuit breaker FSM. Lower-cased
// because the state values are an implementation detail; callers
// observe behaviour through CircuitBreaker.State().
type breakerState int32

const (
	breakerClosed breakerState = iota
	breakerOpen
	breakerHalfOpen
)

// ErrCircuitBreakerOpen is returned by CircuitBreaker.Call when the
// breaker is OPEN and no trial call is permitted. Callers should treat
// this as a fast-fail and not retry inline.
var ErrCircuitBreakerOpen = errors.New("persistence: circuit breaker is open")

// ErrCircuitBreakerCallTimeout is returned when a call exceeds the
// configured CallTimeout. The breaker counts this as a failure.
var ErrCircuitBreakerCallTimeout = errors.New("persistence: circuit breaker call timed out")

// CircuitBreaker is a minimal Pekko-compatible breaker shared by the
// journal and snapshot-store wrappers. The implementation is tuned for
// the persistence call pattern (one call per request, no batching at
// the breaker level) so the locking is intentionally coarse: a single
// mutex guards every state transition and counters.
//
// State transitions:
//
//	CLOSED   --(MaxFailures consecutive failures)--> OPEN
//	OPEN     --(ResetTimeout elapses; next Call())-> HALF_OPEN
//	HALF_OPEN --(trial success)--> CLOSED
//	HALF_OPEN --(trial failure)--> OPEN  (resets ResetTimeout)
type CircuitBreaker struct {
	cfg CircuitBreakerConfig

	mu              sync.Mutex
	state           breakerState
	failures        int
	openedAt        time.Time
	halfOpenInFlight bool

	// nowFn is overridable in tests.
	nowFn func() time.Time
}

// NewCircuitBreaker builds a breaker with the given configuration.
// Non-positive MaxFailures defaults to 1 (every failure trips).
// Non-positive CallTimeout disables the per-call deadline.
// Non-positive ResetTimeout defaults to 1ns (effectively immediate
// half-open re-trial), which keeps the breaker functional under
// degenerate config without panicking.
func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	if cfg.MaxFailures <= 0 {
		cfg.MaxFailures = 1
	}
	if cfg.ResetTimeout <= 0 {
		cfg.ResetTimeout = time.Nanosecond
	}
	return &CircuitBreaker{cfg: cfg, nowFn: time.Now}
}

func (cb *CircuitBreaker) now() time.Time {
	if cb.nowFn != nil {
		return cb.nowFn()
	}
	return time.Now()
}

// State returns the breaker's current state as a human-readable string.
// Used in tests and observability surfaces.
func (cb *CircuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.stateLocked(cb.now()) {
	case breakerOpen:
		return "open"
	case breakerHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

// stateLocked observes the current state, demoting OPEN→HALF_OPEN if
// the reset timeout has elapsed but does *not* claim the trial slot
// (Call() does that under the same lock).
func (cb *CircuitBreaker) stateLocked(now time.Time) breakerState {
	if cb.state == breakerOpen && now.Sub(cb.openedAt) >= cb.cfg.ResetTimeout {
		return breakerHalfOpen
	}
	return cb.state
}

// Call runs fn through the breaker. Behaviour:
//
//   - CLOSED: fn runs; on error or timeout the failure counter
//     increments; reaching MaxFailures trips to OPEN.
//   - OPEN: returns ErrCircuitBreakerOpen without running fn until
//     ResetTimeout has elapsed since openedAt.
//   - HALF_OPEN: exactly one trial call is permitted; concurrent
//     callers that lose the race see ErrCircuitBreakerOpen.
//
// fn is invoked with a context that has the per-call deadline applied
// when CallTimeout is positive; the original ctx's deadline still
// applies if it is shorter.
func (cb *CircuitBreaker) Call(ctx context.Context, fn func(context.Context) error) error {
	cb.mu.Lock()
	now := cb.now()
	switch cb.stateLocked(now) {
	case breakerOpen:
		cb.mu.Unlock()
		return ErrCircuitBreakerOpen
	case breakerHalfOpen:
		if cb.halfOpenInFlight {
			cb.mu.Unlock()
			return ErrCircuitBreakerOpen
		}
		cb.state = breakerHalfOpen
		cb.halfOpenInFlight = true
	}
	cb.mu.Unlock()

	callCtx := ctx
	if cb.cfg.CallTimeout > 0 {
		var cancel context.CancelFunc
		callCtx, cancel = context.WithTimeout(ctx, cb.cfg.CallTimeout)
		defer cancel()
	}

	err := fn(callCtx)
	if err == nil && callCtx.Err() == context.DeadlineExceeded {
		err = ErrCircuitBreakerCallTimeout
	} else if errors.Is(err, context.DeadlineExceeded) && ctx.Err() != context.DeadlineExceeded {
		err = ErrCircuitBreakerCallTimeout
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.halfOpenInFlight {
		cb.halfOpenInFlight = false
	}
	if err != nil {
		cb.failures++
		if cb.state == breakerHalfOpen || cb.failures >= cb.cfg.MaxFailures {
			cb.state = breakerOpen
			cb.openedAt = cb.now()
		}
		return err
	}
	cb.failures = 0
	cb.state = breakerClosed
	return nil
}

// ── package-level config (HOCON-driven defaults) ──────────────────────────

var (
	journalBreakerCfg  atomic.Value // CircuitBreakerConfig
	snapshotBreakerCfg atomic.Value // CircuitBreakerConfig
)

func init() {
	journalBreakerCfg.Store(DefaultJournalBreakerConfig())
	snapshotBreakerCfg.Store(DefaultSnapshotBreakerConfig())
}

// SetJournalBreakerConfig installs the breaker config used by
// WrapJournalWithBreaker (and CircuitBreakerJournal default). Non-positive
// fields keep their previous value.
//
// HOCON: pekko.persistence.journal-plugin-fallback.circuit-breaker.*
func SetJournalBreakerConfig(cfg CircuitBreakerConfig) {
	merged := GetJournalBreakerConfig()
	if cfg.MaxFailures > 0 {
		merged.MaxFailures = cfg.MaxFailures
	}
	if cfg.CallTimeout > 0 {
		merged.CallTimeout = cfg.CallTimeout
	}
	if cfg.ResetTimeout > 0 {
		merged.ResetTimeout = cfg.ResetTimeout
	}
	journalBreakerCfg.Store(merged)
}

// GetJournalBreakerConfig returns the active journal-plugin-fallback
// circuit-breaker configuration.
func GetJournalBreakerConfig() CircuitBreakerConfig {
	v, _ := journalBreakerCfg.Load().(CircuitBreakerConfig)
	if v.MaxFailures == 0 && v.CallTimeout == 0 && v.ResetTimeout == 0 {
		return DefaultJournalBreakerConfig()
	}
	return v
}

// SetSnapshotBreakerConfig installs the breaker config used by
// WrapSnapshotStoreWithBreaker. Non-positive fields keep their previous value.
//
// HOCON: pekko.persistence.snapshot-store-plugin-fallback.circuit-breaker.*
func SetSnapshotBreakerConfig(cfg CircuitBreakerConfig) {
	merged := GetSnapshotBreakerConfig()
	if cfg.MaxFailures > 0 {
		merged.MaxFailures = cfg.MaxFailures
	}
	if cfg.CallTimeout > 0 {
		merged.CallTimeout = cfg.CallTimeout
	}
	if cfg.ResetTimeout > 0 {
		merged.ResetTimeout = cfg.ResetTimeout
	}
	snapshotBreakerCfg.Store(merged)
}

// GetSnapshotBreakerConfig returns the active snapshot-store-plugin-fallback
// circuit-breaker configuration.
func GetSnapshotBreakerConfig() CircuitBreakerConfig {
	v, _ := snapshotBreakerCfg.Load().(CircuitBreakerConfig)
	if v.MaxFailures == 0 && v.CallTimeout == 0 && v.ResetTimeout == 0 {
		return DefaultSnapshotBreakerConfig()
	}
	return v
}

// ── CircuitBreakerJournal ─────────────────────────────────────────────────

// CircuitBreakerJournal wraps a Journal and routes every operation
// through a CircuitBreaker. When the breaker is OPEN, calls fail fast
// with ErrCircuitBreakerOpen instead of hitting the underlying plugin.
type CircuitBreakerJournal struct {
	inner   Journal
	breaker *CircuitBreaker
}

// NewCircuitBreakerJournal wraps inner with a breaker built from cfg.
// Passing the zero value uses the active package-level config.
func NewCircuitBreakerJournal(inner Journal, cfg CircuitBreakerConfig) *CircuitBreakerJournal {
	if cfg == (CircuitBreakerConfig{}) {
		cfg = GetJournalBreakerConfig()
	}
	return &CircuitBreakerJournal{
		inner:   inner,
		breaker: NewCircuitBreaker(cfg),
	}
}

// Breaker exposes the underlying breaker for inspection (mostly for
// tests asserting state transitions).
func (j *CircuitBreakerJournal) Breaker() *CircuitBreaker { return j.breaker }

func (j *CircuitBreakerJournal) AsyncWriteMessages(ctx context.Context, messages []PersistentRepr) error {
	return j.breaker.Call(ctx, func(c context.Context) error {
		return j.inner.AsyncWriteMessages(c, messages)
	})
}

func (j *CircuitBreakerJournal) ReplayMessages(ctx context.Context, persistenceId string, fromSequenceNr, toSequenceNr uint64, max uint64, callback func(PersistentRepr)) error {
	return j.breaker.Call(ctx, func(c context.Context) error {
		return j.inner.ReplayMessages(c, persistenceId, fromSequenceNr, toSequenceNr, max, callback)
	})
}

func (j *CircuitBreakerJournal) ReadHighestSequenceNr(ctx context.Context, persistenceId string, fromSequenceNr uint64) (uint64, error) {
	var hi uint64
	err := j.breaker.Call(ctx, func(c context.Context) error {
		v, e := j.inner.ReadHighestSequenceNr(c, persistenceId, fromSequenceNr)
		if e != nil {
			return e
		}
		hi = v
		return nil
	})
	return hi, err
}

func (j *CircuitBreakerJournal) AsyncDeleteMessagesTo(ctx context.Context, persistenceId string, toSequenceNr uint64) error {
	return j.breaker.Call(ctx, func(c context.Context) error {
		return j.inner.AsyncDeleteMessagesTo(c, persistenceId, toSequenceNr)
	})
}

var _ Journal = (*CircuitBreakerJournal)(nil)

// ── CircuitBreakerSnapshotStore ───────────────────────────────────────────

// CircuitBreakerSnapshotStore wraps a SnapshotStore and routes every
// operation through a CircuitBreaker.
type CircuitBreakerSnapshotStore struct {
	inner   SnapshotStore
	breaker *CircuitBreaker
}

// NewCircuitBreakerSnapshotStore wraps inner with a breaker built from
// cfg. Passing the zero value uses the active package-level snapshot
// config.
func NewCircuitBreakerSnapshotStore(inner SnapshotStore, cfg CircuitBreakerConfig) *CircuitBreakerSnapshotStore {
	if cfg == (CircuitBreakerConfig{}) {
		cfg = GetSnapshotBreakerConfig()
	}
	return &CircuitBreakerSnapshotStore{
		inner:   inner,
		breaker: NewCircuitBreaker(cfg),
	}
}

// Breaker exposes the underlying breaker for inspection.
func (s *CircuitBreakerSnapshotStore) Breaker() *CircuitBreaker { return s.breaker }

func (s *CircuitBreakerSnapshotStore) LoadSnapshot(ctx context.Context, persistenceId string, criteria SnapshotSelectionCriteria) (*SelectedSnapshot, error) {
	var out *SelectedSnapshot
	err := s.breaker.Call(ctx, func(c context.Context) error {
		v, e := s.inner.LoadSnapshot(c, persistenceId, criteria)
		if e != nil {
			return e
		}
		out = v
		return nil
	})
	return out, err
}

func (s *CircuitBreakerSnapshotStore) SaveSnapshot(ctx context.Context, metadata SnapshotMetadata, snapshot any) error {
	return s.breaker.Call(ctx, func(c context.Context) error {
		return s.inner.SaveSnapshot(c, metadata, snapshot)
	})
}

func (s *CircuitBreakerSnapshotStore) DeleteSnapshot(ctx context.Context, metadata SnapshotMetadata) error {
	return s.breaker.Call(ctx, func(c context.Context) error {
		return s.inner.DeleteSnapshot(c, metadata)
	})
}

func (s *CircuitBreakerSnapshotStore) DeleteSnapshots(ctx context.Context, persistenceId string, criteria SnapshotSelectionCriteria) error {
	return s.breaker.Call(ctx, func(c context.Context) error {
		return s.inner.DeleteSnapshots(c, persistenceId, criteria)
	})
}

var _ SnapshotStore = (*CircuitBreakerSnapshotStore)(nil)

// ── helpers ───────────────────────────────────────────────────────────────

// breakerHumanState formats a state value for log/error messages.
func breakerHumanState(s breakerState) string {
	switch s {
	case breakerOpen:
		return "OPEN"
	case breakerHalfOpen:
		return "HALF_OPEN"
	default:
		return "CLOSED"
	}
}

// String pretty-prints the breaker config for log lines.
func (c CircuitBreakerConfig) String() string {
	return fmt.Sprintf("CircuitBreakerConfig{MaxFailures: %d, CallTimeout: %s, ResetTimeout: %s}",
		c.MaxFailures, c.CallTimeout, c.ResetTimeout)
}
