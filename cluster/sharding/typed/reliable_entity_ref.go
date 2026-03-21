/*
 * reliable_entity_ref.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"encoding/json"
	"reflect"
	"sync"
	"time"

	"github.com/sopranoworks/gekka/cluster/sharding"
)

// ReliableEnvelope wraps a user message with a monotonic sequence number for
// at-least-once delivery tracking.  The sharded entity is expected to extract
// the inner message and, when acknowledgement is needed, reply with
// ReliableAck{ProducerID, SeqNr} to confirm processing.
type ReliableEnvelope[M any] struct {
	ProducerID string `json:"producerID"`
	SeqNr      int64  `json:"seqNr"`
	Msg        M      `json:"msg"`
}

// ReliableAck is returned by an entity to acknowledge processing of the
// message identified by SeqNr.
type ReliableAck struct {
	ProducerID string `json:"producerID"`
	SeqNr      int64  `json:"seqNr"`
}

type pendingEntry struct {
	seqNr    int64
	data     json.RawMessage
	manifest string
	sentAt   time.Time
}

// ReliableEntityRef wraps EntityRef[M] and adds at-least-once delivery
// semantics by assigning a monotonic sequence number to every message.
//
// Unacknowledged messages are retransmitted after RetryTimeout has elapsed.
// The entity must call r.Ack(seqNr) (or handle ReliableAck messages and
// forward them to Ack) to clear messages from the pending buffer.
//
// The stash buffer in the Shard actor (Task 2) ensures that no messages are
// lost during a shard handoff; ReliableEntityRef provides the sequence-number
// layer that allows the receiver to detect and discard duplicates produced by
// retransmission.
type ReliableEntityRef[M any] struct {
	inner      *EntityRef[M]
	producerID string
	retryAfter time.Duration

	mu            sync.Mutex
	nextSeqNr     int64
	confirmedUpTo int64
	pending       []*pendingEntry

	stopOnce sync.Once
	stopCh   chan struct{}
}

// NewReliableEntityRef creates a ReliableEntityRef wrapping inner.
//
//   - producerID must be unique per (producer, entity) pair.
//   - retryAfter controls the retransmission timeout; zero defaults to 5 s.
func NewReliableEntityRef[M any](inner *EntityRef[M], producerID string, retryAfter time.Duration) *ReliableEntityRef[M] {
	if retryAfter <= 0 {
		retryAfter = 5 * time.Second
	}
	r := &ReliableEntityRef[M]{
		inner:      inner,
		producerID: producerID,
		retryAfter: retryAfter,
		stopCh:     make(chan struct{}),
	}
	go r.retransmitLoop()
	return r
}

// Tell sends msg to the sharded entity, wrapped in a ReliableEnvelope.
// The message is buffered until Ack is called with the corresponding SeqNr.
func (r *ReliableEntityRef[M]) Tell(msg M) {
	env := ReliableEnvelope[M]{
		ProducerID: r.producerID,
		SeqNr:      r.allocSeqNr(),
		Msg:        msg,
	}
	data, _ := json.Marshal(env)
	manifest := "ReliableEnvelope:" + reflect.TypeOf(msg).String()

	r.mu.Lock()
	r.pending = append(r.pending, &pendingEntry{
		seqNr:    env.SeqNr,
		data:     data,
		manifest: manifest,
		sentAt:   time.Now(),
	})
	r.mu.Unlock()

	r.inner.region.Tell(sharding.ShardingEnvelope{
		EntityId:        r.inner.entityID,
		Message:         data,
		MessageManifest: manifest,
	})
}

// Ack records that the entity has processed the message with the given seqNr.
// All pending entries with seqNr ≤ the given value are pruned from the buffer.
// It is safe to call from any goroutine.
func (r *ReliableEntityRef[M]) Ack(seqNr int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if seqNr > r.confirmedUpTo {
		r.confirmedUpTo = seqNr
	}
	newPending := r.pending[:0]
	for _, e := range r.pending {
		if e.seqNr > r.confirmedUpTo {
			newPending = append(newPending, e)
		}
	}
	r.pending = newPending
}

// PendingCount returns the number of unacknowledged messages.
func (r *ReliableEntityRef[M]) PendingCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pending)
}

// EntityId returns the entity identifier.
func (r *ReliableEntityRef[M]) EntityId() string { return r.inner.entityID }

// Stop releases background resources.  Further Tell calls are safe but
// retransmission will not occur after Stop returns.
func (r *ReliableEntityRef[M]) Stop() {
	r.stopOnce.Do(func() { close(r.stopCh) })
}

// allocSeqNr returns and increments the next sequence number.
func (r *ReliableEntityRef[M]) allocSeqNr() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.nextSeqNr
	r.nextSeqNr++
	return n
}

// retransmitLoop runs in a background goroutine and retransmits messages that
// have not been acknowledged within retryAfter.
func (r *ReliableEntityRef[M]) retransmitLoop() {
	ticker := time.NewTicker(r.retryAfter / 2)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.retransmitExpired()
		}
	}
}

func (r *ReliableEntityRef[M]) retransmitExpired() {
	now := time.Now()
	r.mu.Lock()
	var toRetransmit []*pendingEntry
	for _, e := range r.pending {
		if now.Sub(e.sentAt) >= r.retryAfter {
			e.sentAt = now
			toRetransmit = append(toRetransmit, e)
		}
	}
	r.mu.Unlock()

	for _, e := range toRetransmit {
		r.inner.region.Tell(sharding.ShardingEnvelope{
			EntityId:        r.inner.entityID,
			Message:         e.data,
			MessageManifest: e.manifest,
		})
	}
}
