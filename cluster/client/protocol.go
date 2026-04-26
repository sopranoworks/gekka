/*
 * cluster/client/protocol.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package client implements the Cluster Client extension, allowing external
// (non-member) nodes to communicate with actors inside a Gekka cluster.
//
// The extension is protocol-compatible with Apache Pekko's cluster-tools
// ClusterClient / ClusterReceptionist pair.
//
// Wire format reference (serializer ID 15):
//
//	Send        "SC" : plain Protobuf — point-to-point delivery to one path
//	SendToAll   "SA" : plain Protobuf — broadcast to every subscriber at path
//	Publish     "P"  : plain Protobuf — fan-out to all topic subscribers
//
// Internal control messages (not user-facing):
//
//	Heartbeat           "HB"  : liveness ping from client → receptionist
//	HeartbeatRsp        "HBR" : liveness pong from receptionist → client
//	GetContacts         "GC"  : request for fresh contact points
//	Contacts            "C"   : receptionist reply with contact paths
//	Subscribe           "SUB" : register a service with the receptionist
//	Unsubscribe         "USB" : remove a service registration
package client

import "time"

// ClusterClientSerializerID is the Artery serializer ID used by Pekko's
// ClusterClientMessageSerializer.  Register at ID 15 in the
// SerializationRegistry when enabling Pekko cluster-client interoperability.
const ClusterClientSerializerID = int32(15)

// Manifest codes matching Pekko's ClusterClientMessageSerializer exactly.
const (
	SendManifest        = "SC"
	SendToAllManifest   = "SA"
	PublishManifest     = "P"
	HeartbeatManifest   = "HB"
	HeartbeatRspManifest = "HBR"
	GetContactsManifest = "GC"
	ContactsManifest    = "C"
)

// ── User-facing message types ─────────────────────────────────────────────────

// Send routes a message to exactly one actor at the given path inside the
// cluster (manifest "SC").  The receptionist forwards it to that path on one
// of the cluster nodes it knows about.
type Send struct {
	// Path is the relative actor path, e.g. "/user/myService".
	Path string
	// Msg is the application payload.
	Msg any
	// LocalAffinity, when true, prefers a subscriber on the same node as the
	// receptionist that handles the request.
	LocalAffinity bool
}

// SendToAll delivers a message to every registered instance of Path across
// the entire cluster (manifest "SA").  Use this for broadcast commands.
type SendToAll struct {
	// Path is the relative actor path, e.g. "/user/myService".
	Path string
	// Msg is the application payload.
	Msg any
}

// Publish fans a message out to all Distributed Pub/Sub subscribers of the
// named topic (manifest "P").  Requires a DistributedPubSubMediator on the
// cluster nodes.
type Publish struct {
	// Topic is the pub/sub topic name.
	Topic string
	// Msg is the application payload.
	Msg any
}

// ── Internal control messages ─────────────────────────────────────────────────

// Heartbeat is a liveness ping sent by the client to the receptionist at
// regular intervals.  The receptionist replies with HeartbeatRsp.
type Heartbeat struct{}

// HeartbeatRsp is the liveness acknowledgement returned by the receptionist.
type HeartbeatRsp struct{}

// GetContacts is sent by the client to request a fresh list of contact-point
// actor paths.  The receptionist replies with a Contacts message.
type GetContacts struct{}

// Contacts is sent by the receptionist in response to GetContacts.
// Paths contains the full Artery actor paths to active receptionist actors
// (e.g. "pekko://ClusterSystem@host:2552/system/receptionist").
type Contacts struct {
	Paths []string
}

// Config holds runtime configuration for a ClusterClient instance.
// Populate it from HOCON via LoadClientConfig or fill it in directly.
type Config struct {
	// InitialContacts is the list of receptionist Artery paths used when the
	// client first starts.  At least one entry is mandatory.
	// HOCON: pekko.cluster.client.initial-contacts
	InitialContacts []string

	// EstablishingGetContactsInterval is how often the client retries when
	// it has not yet established a connection to any receptionist.
	// HOCON: pekko.cluster.client.establishing-get-contacts-interval
	// Default: 3s
	EstablishingGetContactsInterval time.Duration

	// RefreshContactsInterval is how often a connected client asks for
	// updated contact-point information.
	// HOCON: pekko.cluster.client.refresh-contacts-interval
	// Default: 60s
	RefreshContactsInterval time.Duration

	// HeartbeatInterval is how often the client sends a liveness ping.
	// HOCON: pekko.cluster.client.heartbeat-interval
	// Default: 2s
	HeartbeatInterval time.Duration

	// AcceptableHeartbeatPause is the maximum gap between received
	// HeartbeatRsp messages before the client considers the connection dead.
	// HOCON: pekko.cluster.client.acceptable-heartbeat-pause
	// Default: 13s
	AcceptableHeartbeatPause time.Duration

	// BufferSize is the maximum number of messages the client queues while
	// not connected.  Old messages are dropped when the buffer is full.
	// HOCON: pekko.cluster.client.buffer-size
	// Default: 1000
	BufferSize int

	// ReconnectTimeout is how long the client waits for reconnection before
	// stopping itself.  Zero or negative means "retry forever" (Pekko's "off").
	// HOCON: pekko.cluster.client.reconnect-timeout
	// Default: 0 (retry forever)
	ReconnectTimeout time.Duration
}

// ReceptionistConfig holds runtime configuration for a ClusterReceptionist.
type ReceptionistConfig struct {
	// Name is the actor name registered under /system/.
	// HOCON: pekko.cluster.client.receptionist.name
	// Default: "receptionist"
	Name string

	// Role restricts receptionist deployment to nodes tagged with this role.
	// Empty means all nodes.
	// HOCON: pekko.cluster.client.receptionist.role
	Role string

	// NumberOfContacts is how many contact-point paths the receptionist
	// includes in each Contacts reply.
	// HOCON: pekko.cluster.client.receptionist.number-of-contacts
	// Default: 3
	NumberOfContacts int

	// HeartbeatInterval is how often the receptionist expects a heartbeat
	// from each known client.
	// HOCON: pekko.cluster.client.receptionist.heartbeat-interval
	// Default: 2s
	HeartbeatInterval time.Duration

	// AcceptableHeartbeatPause is the maximum gap before the receptionist
	// considers a client to be stale.
	// HOCON: pekko.cluster.client.receptionist.acceptable-heartbeat-pause
	// Default: 13s
	AcceptableHeartbeatPause time.Duration

	// ResponseTunnelReceiveTimeout bounds how long the receptionist waits for
	// a Send/SendToAll forwarded delivery to complete before cancelling the
	// outbound context. Pekko equivalent: the per-request response-tunnel
	// actor's idle receive timeout.
	// HOCON: pekko.cluster.client.receptionist.response-tunnel-receive-timeout
	// Default: 30s
	ResponseTunnelReceiveTimeout time.Duration

	// FailureDetectionInterval governs the cadence of the receptionist's
	// stale-client checker. Setting this independently of HeartbeatInterval
	// lets operators run the failure-detection sweep faster (e.g. 250ms on a
	// dense cluster) without altering client-side heartbeat traffic.
	// HOCON: pekko.cluster.client.receptionist.failure-detection-interval
	// Default: 2s
	FailureDetectionInterval time.Duration
}

// DefaultConfig returns a Config with all fields set to Pekko-compatible
// defaults.  Callers must still populate InitialContacts.
func DefaultConfig() Config {
	return Config{
		EstablishingGetContactsInterval: 3 * time.Second,
		RefreshContactsInterval:         60 * time.Second,
		HeartbeatInterval:               2 * time.Second,
		AcceptableHeartbeatPause:        13 * time.Second,
		BufferSize:                      1000,
	}
}

// DefaultReceptionistConfig returns a ReceptionistConfig with all fields set
// to Pekko-compatible defaults.
func DefaultReceptionistConfig() ReceptionistConfig {
	return ReceptionistConfig{
		Name:                         "receptionist",
		NumberOfContacts:             3,
		HeartbeatInterval:            2 * time.Second,
		AcceptableHeartbeatPause:     13 * time.Second,
		ResponseTunnelReceiveTimeout: 30 * time.Second,
		FailureDetectionInterval:     2 * time.Second,
	}
}
