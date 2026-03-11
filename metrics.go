/*
 * metrics.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"sync/atomic"
	"time"
)

// NodeMetrics is a set of lock-free counters updated on the Artery hot path.
// All fields use sync/atomic — there is zero mutex contention when recording
// events.  Obtain a consistent copy with Snapshot.
type NodeMetrics struct {
	// ── Message traffic (user messages only; cluster-internal messages excluded) ──

	// MessagesSent is incremented each time a user message is handed to the
	// Artery outbox (includes messages buffered during handshake).
	MessagesSent atomic.Int64

	// MessagesReceived is incremented each time a user message arrives and is
	// dispatched to the OnMessage callback (or an Ask reply channel).
	MessagesReceived atomic.Int64

	// BytesSent is the cumulative application payload bytes sent (before
	// Artery framing overhead is added).
	BytesSent atomic.Int64

	// BytesReceived is the cumulative application payload bytes received
	// (raw Artery payload; excludes framing headers).
	BytesReceived atomic.Int64

	// ── Gossip ────────────────────────────────────────────────────────────────

	// GossipsReceived counts every successfully processed GossipEnvelope.
	GossipsReceived atomic.Int64

	// lastConvergenceNs is the UnixNano timestamp of the most recent gossip
	// convergence (all Up members have seen the current state).  Zero means
	// convergence has never been observed.
	lastConvergenceNs atomic.Int64

	// ── Cluster events ────────────────────────────────────────────────────────

	// MemberUpEvents counts MemberUp transitions processed by this node's
	// leader actions (Joining → Up).
	MemberUpEvents atomic.Int64

	// MemberRemovedEvents counts MemberRemoved transitions processed by this
	// node's leader actions (Exiting → Removed).
	MemberRemovedEvents atomic.Int64
}

// MetricsSnapshot is a plain-value copy of NodeMetrics suitable for JSON
// serialization or Prometheus text export.
type MetricsSnapshot struct {
	// Traffic
	MessagesSent     int64 `json:"messages_sent"`
	MessagesReceived int64 `json:"messages_received"`
	BytesSent        int64 `json:"bytes_sent"`
	BytesReceived    int64 `json:"bytes_received"`

	// Associations (computed dynamically — not an atomic counter)
	ActiveAssociations int `json:"active_associations"`

	// Gossip
	GossipsReceived     int64  `json:"gossips_received"`
	LastConvergenceTime string `json:"last_convergence_time"` // RFC3339 or "never"

	// Cluster events
	MemberUpEvents      int64 `json:"member_up_events"`
	MemberRemovedEvents int64 `json:"member_removed_events"`
}

// RecordConvergence stamps the current wall-clock time as the most recent
// gossip convergence.  Call this after verifying that all Up members have
// seen the current gossip version.
func (m *NodeMetrics) RecordConvergence() {
	m.lastConvergenceNs.Store(time.Now().UnixNano())
}

// Snapshot returns an instantaneous, consistent copy of all counters.
// activeAssociations is supplied by the caller so the snapshot can include
// the live association count without embedding a NodeManager reference here.
func (m *NodeMetrics) Snapshot(activeAssociations int) MetricsSnapshot {
	s := MetricsSnapshot{
		MessagesSent:        m.MessagesSent.Load(),
		MessagesReceived:    m.MessagesReceived.Load(),
		BytesSent:           m.BytesSent.Load(),
		BytesReceived:       m.BytesReceived.Load(),
		ActiveAssociations:  activeAssociations,
		GossipsReceived:     m.GossipsReceived.Load(),
		MemberUpEvents:      m.MemberUpEvents.Load(),
		MemberRemovedEvents: m.MemberRemovedEvents.Load(),
	}
	if ns := m.lastConvergenceNs.Load(); ns != 0 {
		s.LastConvergenceTime = time.Unix(0, ns).UTC().Format(time.RFC3339)
	} else {
		s.LastConvergenceTime = "never"
	}
	return s
}

// PrometheusText renders the snapshot in Prometheus exposition format.
// Use this when your scraping infrastructure expects the text-based format
// instead of JSON.
func (s MetricsSnapshot) PrometheusText() string {
	return "# HELP gekka_messages_sent_total Total user messages sent over Artery\n" +
		"# TYPE gekka_messages_sent_total counter\n" +
		itoa("gekka_messages_sent_total", s.MessagesSent) +
		"# HELP gekka_messages_received_total Total user messages received over Artery\n" +
		"# TYPE gekka_messages_received_total counter\n" +
		itoa("gekka_messages_received_total", s.MessagesReceived) +
		"# HELP gekka_bytes_sent_total Cumulative application payload bytes sent\n" +
		"# TYPE gekka_bytes_sent_total counter\n" +
		itoa("gekka_bytes_sent_total", s.BytesSent) +
		"# HELP gekka_bytes_received_total Cumulative application payload bytes received\n" +
		"# TYPE gekka_bytes_received_total counter\n" +
		itoa("gekka_bytes_received_total", s.BytesReceived) +
		"# HELP gekka_active_associations Current number of ASSOCIATED Artery connections\n" +
		"# TYPE gekka_active_associations gauge\n" +
		itoa("gekka_active_associations", int64(s.ActiveAssociations)) +
		"# HELP gekka_gossips_received_total Total GossipEnvelopes processed\n" +
		"# TYPE gekka_gossips_received_total counter\n" +
		itoa("gekka_gossips_received_total", s.GossipsReceived) +
		"# HELP gekka_member_up_events_total MemberUp events processed as cluster leader\n" +
		"# TYPE gekka_member_up_events_total counter\n" +
		itoa("gekka_member_up_events_total", s.MemberUpEvents) +
		"# HELP gekka_member_removed_events_total MemberRemoved events processed as cluster leader\n" +
		"# TYPE gekka_member_removed_events_total counter\n" +
		itoa("gekka_member_removed_events_total", s.MemberRemovedEvents)
}

func itoa(name string, v int64) string {
	return name + " " + int64ToStr(v) + "\n"
}

func int64ToStr(v int64) string {
	if v == 0 {
		return "0"
	}
	negative := v < 0
	if negative {
		v = -v
	}
	buf := make([]byte, 20)
	pos := len(buf)
	for v > 0 {
		pos--
		buf[pos] = byte('0' + v%10)
		v /= 10
	}
	if negative {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
