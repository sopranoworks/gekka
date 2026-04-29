/*
 * at_least_once.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// ErrMaxUnconfirmedMessagesExceeded is returned by Deliver when accepting
// the new message would push the number of unconfirmed deliveries beyond
// the configured ceiling (pekko.persistence.at-least-once-delivery.
// max-unconfirmed-messages).
var ErrMaxUnconfirmedMessagesExceeded = errors.New("persistence: max unconfirmed messages exceeded")

// AtLeastOnceConfig captures the values exposed under
// pekko.persistence.at-least-once-delivery.* in reference.conf.
type AtLeastOnceConfig struct {
	// RedeliverInterval is the period between redelivery attempts.
	// Zero falls back to the previously-set default (initially 5s).
	RedeliverInterval time.Duration

	// RedeliveryBurstLimit caps the maximum number of redeliveries
	// fired per redeliver-interval tick.  Zero falls back to the
	// previously-set default (initially 10000).
	RedeliveryBurstLimit int

	// WarnAfterNumberOfUnconfirmedAttempts is the per-message attempt
	// threshold above which the warn callback is invoked once.  Zero
	// falls back to the previously-set default (initially 5).
	WarnAfterNumberOfUnconfirmedAttempts int

	// MaxUnconfirmedMessages is the ceiling of pending unconfirmed
	// deliveries.  Deliver returns ErrMaxUnconfirmedMessagesExceeded
	// when this limit is reached.  Zero falls back to the previously-
	// set default (initially 100000).
	MaxUnconfirmedMessages int
}

var (
	defaultAtLeastOnceConfig atomic.Value // AtLeastOnceConfig
)

func init() {
	defaultAtLeastOnceConfig.Store(AtLeastOnceConfig{
		RedeliverInterval:                    5 * time.Second,
		RedeliveryBurstLimit:                 10000,
		WarnAfterNumberOfUnconfirmedAttempts: 5,
		MaxUnconfirmedMessages:               100000,
	})
}

// SetDefaultAtLeastOnceConfig replaces the package-level defaults that
// govern any AtLeastOnceDelivery created without an explicit override.
// Zero-valued fields retain the existing default for that field.  Called
// from cluster.go once HOCON has been parsed.
func SetDefaultAtLeastOnceConfig(cfg AtLeastOnceConfig) {
	cur := DefaultAtLeastOnceConfig()
	if cfg.RedeliverInterval > 0 {
		cur.RedeliverInterval = cfg.RedeliverInterval
	}
	if cfg.RedeliveryBurstLimit > 0 {
		cur.RedeliveryBurstLimit = cfg.RedeliveryBurstLimit
	}
	if cfg.WarnAfterNumberOfUnconfirmedAttempts > 0 {
		cur.WarnAfterNumberOfUnconfirmedAttempts = cfg.WarnAfterNumberOfUnconfirmedAttempts
	}
	if cfg.MaxUnconfirmedMessages > 0 {
		cur.MaxUnconfirmedMessages = cfg.MaxUnconfirmedMessages
	}
	defaultAtLeastOnceConfig.Store(cur)
}

// DefaultAtLeastOnceConfig returns the current package-level defaults.
func DefaultAtLeastOnceConfig() AtLeastOnceConfig {
	v, _ := defaultAtLeastOnceConfig.Load().(AtLeastOnceConfig)
	return v
}

// AtLeastOnceDelivery provides reliable message delivery with redelivery
// of unconfirmed messages.  It is meant to be embedded in (or owned by) a
// persistent actor: the actor calls Deliver to register a destination,
// and the recipient confirms by calling ConfirmDelivery.  Pending
// deliveries can be snapshotted via GetDeliverySnapshot and restored
// during recovery via SetDeliverySnapshot.
type AtLeastOnceDelivery struct {
	mu             sync.Mutex
	cfg            AtLeastOnceConfig
	nextDeliveryID int64
	unconfirmed    map[int64]*pendingDelivery
	stopCh         chan struct{}
	running        bool
}

type pendingDelivery struct {
	DeliveryID  int64
	Destination string
	Message     any
	Timestamp   time.Time
	Attempts    int
}

// DeliverySnapshot captures the state of pending deliveries for
// persistence across actor restarts.
type DeliverySnapshot struct {
	NextDeliveryID int64
	Pending        []PendingDeliveryEntry
}

// PendingDeliveryEntry is a single unconfirmed delivery in a snapshot.
type PendingDeliveryEntry struct {
	DeliveryID  int64
	Destination string
	Message     any
	Attempts    int
}

// NewAtLeastOnceDelivery creates a new delivery tracker.  The first two
// arguments override the package-level defaults for redeliver-interval
// and max-unconfirmed-messages; pass 0 to inherit each default.  Burst
// limit and warn-after-attempts always inherit the package defaults set
// via SetDefaultAtLeastOnceConfig.
func NewAtLeastOnceDelivery(redeliverInterval time.Duration, maxUnconfirmed int) *AtLeastOnceDelivery {
	cfg := DefaultAtLeastOnceConfig()
	if redeliverInterval > 0 {
		cfg.RedeliverInterval = redeliverInterval
	}
	if maxUnconfirmed > 0 {
		cfg.MaxUnconfirmedMessages = maxUnconfirmed
	}
	return NewAtLeastOnceDeliveryWithConfig(cfg)
}

// NewAtLeastOnceDeliveryWithConfig is the explicit-config constructor.
// Zero-valued fields fall back to the current package defaults.
func NewAtLeastOnceDeliveryWithConfig(cfg AtLeastOnceConfig) *AtLeastOnceDelivery {
	def := DefaultAtLeastOnceConfig()
	if cfg.RedeliverInterval <= 0 {
		cfg.RedeliverInterval = def.RedeliverInterval
	}
	if cfg.RedeliveryBurstLimit <= 0 {
		cfg.RedeliveryBurstLimit = def.RedeliveryBurstLimit
	}
	if cfg.WarnAfterNumberOfUnconfirmedAttempts <= 0 {
		cfg.WarnAfterNumberOfUnconfirmedAttempts = def.WarnAfterNumberOfUnconfirmedAttempts
	}
	if cfg.MaxUnconfirmedMessages <= 0 {
		cfg.MaxUnconfirmedMessages = def.MaxUnconfirmedMessages
	}
	return &AtLeastOnceDelivery{
		cfg:            cfg,
		nextDeliveryID: 1,
		unconfirmed:    make(map[int64]*pendingDelivery),
		stopCh:         make(chan struct{}),
	}
}

// Config returns a snapshot of this instance's configuration.
func (d *AtLeastOnceDelivery) Config() AtLeastOnceConfig {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cfg
}

// Deliver registers a new message for delivery and returns the assigned
// deliveryId.  When accepting the message would exceed
// MaxUnconfirmedMessages, ErrMaxUnconfirmedMessagesExceeded is returned
// and no state is mutated.  The caller is still responsible for actually
// sending the message — Deliver only tracks the pending state.
func (d *AtLeastOnceDelivery) Deliver(destination string, msg any) (deliveryID int64, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.unconfirmed) >= d.cfg.MaxUnconfirmedMessages {
		return 0, ErrMaxUnconfirmedMessagesExceeded
	}

	id := d.nextDeliveryID
	d.nextDeliveryID++

	d.unconfirmed[id] = &pendingDelivery{
		DeliveryID:  id,
		Destination: destination,
		Message:     msg,
		Timestamp:   time.Now(),
		Attempts:    1,
	}
	return id, nil
}

// ConfirmDelivery marks a delivery as confirmed (successfully received).
// Returns true if the deliveryId was pending, false if already confirmed
// or unknown.
func (d *AtLeastOnceDelivery) ConfirmDelivery(deliveryID int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.unconfirmed[deliveryID]; ok {
		delete(d.unconfirmed, deliveryID)
		return true
	}
	return false
}

// UnconfirmedCount returns the number of unconfirmed deliveries.
func (d *AtLeastOnceDelivery) UnconfirmedCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.unconfirmed)
}

// GetDeliverySnapshot returns the current state of all pending deliveries
// for persistence. Call this before saving a snapshot.
func (d *AtLeastOnceDelivery) GetDeliverySnapshot() DeliverySnapshot {
	d.mu.Lock()
	defer d.mu.Unlock()

	snap := DeliverySnapshot{
		NextDeliveryID: d.nextDeliveryID,
		Pending:        make([]PendingDeliveryEntry, 0, len(d.unconfirmed)),
	}

	for _, pd := range d.unconfirmed {
		snap.Pending = append(snap.Pending, PendingDeliveryEntry{
			DeliveryID:  pd.DeliveryID,
			Destination: pd.Destination,
			Message:     pd.Message,
			Attempts:    pd.Attempts,
		})
	}

	return snap
}

// SetDeliverySnapshot restores delivery state from a persisted snapshot.
// Call this during recovery.
func (d *AtLeastOnceDelivery) SetDeliverySnapshot(snap DeliverySnapshot) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.nextDeliveryID = snap.NextDeliveryID
	d.unconfirmed = make(map[int64]*pendingDelivery, len(snap.Pending))

	for _, entry := range snap.Pending {
		d.unconfirmed[entry.DeliveryID] = &pendingDelivery{
			DeliveryID:  entry.DeliveryID,
			Destination: entry.Destination,
			Message:     entry.Message,
			Timestamp:   time.Now(),
			Attempts:    entry.Attempts,
		}
	}
}

// StartRedelivery begins a background goroutine that periodically checks
// for unconfirmed messages and calls the redeliver function for each one.
// The redeliver function should re-send the message to the destination.
// The number of redeliveries fired per tick is capped at
// RedeliveryBurstLimit.
func (d *AtLeastOnceDelivery) StartRedelivery(redeliver func(destination string, deliveryID int64, msg any)) {
	d.mu.Lock()
	if d.running {
		d.mu.Unlock()
		return
	}
	d.running = true
	stop := d.stopCh
	interval := d.cfg.RedeliverInterval
	d.mu.Unlock()

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				d.redeliverPending(redeliver)
			}
		}
	}()
}

// StopRedelivery stops the background redelivery goroutine.
func (d *AtLeastOnceDelivery) StopRedelivery() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.running {
		close(d.stopCh)
		d.running = false
		d.stopCh = make(chan struct{})
	}
}

func (d *AtLeastOnceDelivery) redeliverPending(redeliver func(string, int64, any)) {
	d.mu.Lock()
	burst := d.cfg.RedeliveryBurstLimit
	pending := make([]*pendingDelivery, 0, len(d.unconfirmed))
	for _, pd := range d.unconfirmed {
		pending = append(pending, pd)
	}
	if burst > 0 && len(pending) > burst {
		pending = pending[:burst]
	}
	for _, pd := range pending {
		pd.Attempts++
	}
	d.mu.Unlock()

	for _, pd := range pending {
		redeliver(pd.Destination, pd.DeliveryID, pd.Message)
	}
}

// MaxAttempts returns the highest Attempts count across the currently
// pending deliveries, or 0 when none are pending.  Useful for triggering
// the warn-after-number-of-unconfirmed-attempts log line.
func (d *AtLeastOnceDelivery) MaxAttempts() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	max := 0
	for _, pd := range d.unconfirmed {
		if pd.Attempts > max {
			max = pd.Attempts
		}
	}
	return max
}
