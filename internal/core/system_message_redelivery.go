/*
 * system_message_redelivery.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"errors"
	"sync"
	"time"
)

// ErrSystemOutboxFull is returned by SystemMessageOutbox.Enqueue when the
// per-association unacked-system-message buffer is at capacity. Pekko
// semantics: a full system-message buffer means the association has fallen
// behind the receiver beyond the redelivery window and must be quarantined.
// Sub-commit 2.4 wires this into the QUARANTINED transition; until then
// callers log and drop.
var ErrSystemOutboxFull = errors.New("artery: system message buffer full")

// unackedSystemMsg is one entry in the per-association sender-side buffer of
// unacknowledged system messages. The frame field is the fully-encoded Artery
// frame as produced by BuildArteryFrame, ready to be re-written onto the
// outbox channel by the resend ticker.
//
// firstAttempt is preserved across resends so the give-up-system-message-after
// deadline counts from the original send time, not the most recent retry.
// lastAttempt is updated by MarkResent for diagnostics and to space resends
// from the resend interval.
type unackedSystemMsg struct {
	seqNo        uint64
	frame        []byte
	firstAttempt time.Time
	lastAttempt  time.Time
}

// SystemMessageOutbox is a per-association sender-side buffer of unacknowledged
// system messages. Sized to pekko.remote.artery.advanced.system-message-buffer-size.
//
// Pekko's wire-level guarantee is that system messages cross the network
// reliably even under transient association loss. The sender enqueues every
// outbound system message into this buffer; a resend ticker (sub-commit 2.2)
// replays unacked entries on a cadence of
// pekko.remote.artery.advanced.system-message-resend-interval; cumulative
// acks from the receiver (sub-commit 2.3) prune the head of the buffer; and
// after pekko.remote.artery.advanced.give-up-system-message-after with no
// progress the association is escalated to QUARANTINED (sub-commit 2.4).
//
// Entries are kept in seqNo-ascending order. Pruning is cumulative — an ack
// of seq N drops every entry with seqNo <= N. The head entry is always the
// longest-pending and the first to hit the give-up deadline.
type SystemMessageOutbox struct {
	mu       sync.Mutex
	capacity int
	entries  []unackedSystemMsg
}

// NewSystemMessageOutbox returns an empty buffer of the given capacity.
// A non-positive capacity is treated as DefaultSystemMessageBufferSize so
// callers can pass NodeManager.EffectiveSystemMessageBufferSize() unchecked.
func NewSystemMessageOutbox(capacity int) *SystemMessageOutbox {
	if capacity <= 0 {
		capacity = DefaultSystemMessageBufferSize
	}
	return &SystemMessageOutbox{
		capacity: capacity,
		entries:  make([]unackedSystemMsg, 0, capacity),
	}
}

// Capacity returns the configured maximum entry count.
func (o *SystemMessageOutbox) Capacity() int {
	return o.capacity
}

// Enqueue records a freshly-sent system message frame. seqNo MUST be strictly
// greater than the seqNo of every existing entry — the buffer relies on
// ascending order for cumulative-ack pruning. now is the wall-clock time of
// the first send attempt and is preserved across resends so the give-up-after
// deadline counts from the original send.
//
// Returns ErrSystemOutboxFull when the buffer is at capacity. The caller is
// expected to escalate the association to QUARANTINED in that case
// (matches Pekko's "buffer-full ⇒ give up" semantics).
func (o *SystemMessageOutbox) Enqueue(seqNo uint64, frame []byte, now time.Time) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.entries) >= o.capacity {
		return ErrSystemOutboxFull
	}
	o.entries = append(o.entries, unackedSystemMsg{
		seqNo:        seqNo,
		frame:        frame,
		firstAttempt: now,
		lastAttempt:  now,
	})
	return nil
}

// PruneAcked drops all entries with seqNo <= ackedSeqNo (cumulative ack
// semantics, matching Pekko's SystemMessageDeliveryAck wire shape).
// Returns the number of entries pruned.
func (o *SystemMessageOutbox) PruneAcked(ackedSeqNo uint64) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	cut := 0
	for cut < len(o.entries) && o.entries[cut].seqNo <= ackedSeqNo {
		cut++
	}
	if cut == 0 {
		return 0
	}
	o.entries = append(o.entries[:0], o.entries[cut:]...)
	return cut
}

// Snapshot returns a defensive copy of the unacked entries in seqNo-ascending
// order. Callers may not mutate the returned slice or its frames. Used by
// the resend ticker (sub-commit 2.2) and by tests.
func (o *SystemMessageOutbox) Snapshot() []unackedSystemMsg {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]unackedSystemMsg, len(o.entries))
	copy(out, o.entries)
	return out
}

// MarkResent updates the lastAttempt timestamp of the entry with the given
// seqNo. firstAttempt is preserved. No-op when the seqNo is not present
// (the entry may have been acked between the snapshot and the resend write).
func (o *SystemMessageOutbox) MarkResent(seqNo uint64, now time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for i := range o.entries {
		if o.entries[i].seqNo == seqNo {
			o.entries[i].lastAttempt = now
			return
		}
	}
}

// Len returns the count of unacked entries.
func (o *SystemMessageOutbox) Len() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.entries)
}

// Drain clears the buffer and returns the count of dropped entries.
// Used when the association is being torn down or quarantined.
func (o *SystemMessageOutbox) Drain() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := len(o.entries)
	o.entries = o.entries[:0]
	return n
}

// OldestFirstAttempt returns the firstAttempt timestamp of the head-of-buffer
// entry — the entry with the lowest seqNo, which is the longest-pending
// unacked system message. Returns the zero time when the buffer is empty.
//
// The give-up timer (sub-commit 2.4) reads this to decide when to escalate
// the association to QUARANTINED: the head entry is always the first to
// expire, so a single comparison against now-give-up-after suffices.
func (o *SystemMessageOutbox) OldestFirstAttempt() time.Time {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.entries) == 0 {
		return time.Time{}
	}
	return o.entries[0].firstAttempt
}
