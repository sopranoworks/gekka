/*
 * cluster/pubsub/mediator.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package pubsub implements Distributed Pub/Sub compatible with Apache Pekko's
// DistributedPubSubMediator. Cluster nodes exchange subscription state via a gossip
// protocol using the same binary wire format as Pekko (serializer ID 9).
//
// Wire format reference:
//   - Serializer ID : 9  (DistributedPubSubMessageSerializer)
//   - Status  "A"  : GZIP-compressed Protobuf (subscription version map)
//   - Delta   "B"  : GZIP-compressed Protobuf (subscription bucket changes)
//   - Send    "C"  : plain Protobuf (point-to-point routing envelope)
//   - SendToAll "D": plain Protobuf (broadcast routing envelope)
//   - Publish "E"  : plain Protobuf (topic fan-out envelope)
//   - SendToOneSubscriber "F": plain Protobuf (single group-subscriber envelope)
//
// NOTE on serializer ID 9: Pekko assigns ID 9 to DistributedPubSubMessageSerializer.
// Gekka's internal JSONSerializer also uses ID 9 for CRDT gossip between pure Go nodes.
// When pub-sub interop with Pekko is enabled, register a PubSubSerializer at ID 9 in the
// SerializationRegistry, which supersedes the internal JSON serializer for that slot.
package pubsub

import "context"

// PubSubSerializerID is the Artery serializer ID assigned by Pekko to
// DistributedPubSubMessageSerializer. Register this at ID 9 in the
// SerializationRegistry when enabling Pekko pub-sub interoperability.
//
// WARNING: Pekko uses ID 9 for pub-sub; gekka's internal JSONSerializer also
// uses ID 9 for CRDT gossip between pure-Go nodes. Enabling pub-sub interop
// replaces the JSON serializer slot in the registry for that node.
const PubSubSerializerID = int32(9)

// Manifest codes match Pekko's DistributedPubSubMessageSerializer exactly.
const (
	StatusManifest              = "A"
	DeltaManifest               = "B"
	SendManifest                = "C"
	SendToAllManifest           = "D"
	PublishManifest             = "E"
	SendToOneSubscriberManifest = "F"
)

// Address identifies a cluster node. Matches Pekko's akka.actor.Address.
type Address struct {
	System   string
	Hostname string
	Port     uint32
	Protocol string // typically "pekko" or "akka"; defaults to "pekko" when empty
}

// ValueHolder stores a subscription entry: the version at which the subscriber
// joined, and the serialized actor path of the subscriber (empty when absent).
type ValueHolder struct {
	Version int64
	Ref     string // serialized actor path, e.g. "pekko://System@host:port/user/sub"
}

// Bucket is one node's view of its own subscriptions. Owner identifies the node;
// Version is a monotonically increasing counter incremented on every change;
// Content maps topic/group keys to ValueHolder entries.
type Bucket struct {
	Owner   Address
	Version int64
	Content map[string]ValueHolder // key: "/topic" or "/topic/group"
}

// Status is the gossip heartbeat exchanged between mediators (manifest "A").
// It carries each node's current bucket version so peers can detect stale data.
// Serialized as GZIP-compressed Protobuf.
type Status struct {
	// Versions maps each known node address to its current bucket version.
	Versions map[Address]int64
	// IsReplyToStatus is true when this Status is sent in reply to a received Status.
	IsReplyToStatus bool
}

// Delta carries the actual subscription changes for buckets that are out of date
// (manifest "B"). Serialized as GZIP-compressed Protobuf.
type Delta struct {
	Buckets []Bucket
}

// Publish fans a message out to all subscribers of the named topic (manifest "E").
// Serialized as plain Protobuf (no GZIP).
type Publish struct {
	Topic string
	Msg   any
}

// Send routes a message to one subscriber behind the given actor path (manifest "C").
// If LocalAffinity is true, prefer a subscriber on the local node when one exists.
// Serialized as plain Protobuf (no GZIP).
type Send struct {
	Path          string
	Msg           any
	LocalAffinity bool
}

// SendToAll delivers a message to every subscriber at the given actor path (manifest "D").
// If AllButSelf is true, exclude the local node's subscriber.
// Serialized as plain Protobuf (no GZIP).
type SendToAll struct {
	Path      string
	Msg       any
	AllButSelf bool
}

// SendToOneSubscriber delivers a message to exactly one member of a subscriber group
// (manifest "F"). Serialized as plain Protobuf (no GZIP).
type SendToOneSubscriber struct {
	Msg any
}

// Subscribe registers a receiver at the given actor path for a topic.
// Group is optional; when non-empty messages are load-balanced across group members.
type Subscribe struct {
	Topic string
	Group string // empty for broadcast, non-empty for group-based load balancing
	// ReceiverPath is the full Artery actor path of the subscriber.
	ReceiverPath string
}

// Unsubscribe removes a subscription previously registered with Subscribe.
type Unsubscribe struct {
	Topic        string
	Group        string
	ReceiverPath string
}

// SubscribeAck is the confirmation returned to a Subscribe sender.
type SubscribeAck struct {
	Subscribe Subscribe
}

// UnsubscribeAck is the confirmation returned to an Unsubscribe sender.
type UnsubscribeAck struct {
	Unsubscribe Unsubscribe
}

// Mediator is the cluster-wide distributed pub-sub interface. Implementations
// maintain subscription state via gossip and route messages transparently.
//
// This interface is intentionally minimal for v0.6.0; additional operations
// (GetTopics, ListTopics, cluster-aware group routing) will be added incrementally.
type Mediator interface {
	// Publish sends msg to every subscriber registered for topic across the cluster.
	Publish(ctx context.Context, topic string, msg any) error

	// Send routes msg to exactly one subscriber at path. When localAffinity is true
	// a subscriber on the local node is preferred if one exists.
	Send(ctx context.Context, path string, msg any, localAffinity bool) error

	// Subscribe registers receiverPath as a subscriber for topic (and optional group).
	Subscribe(ctx context.Context, topic, group, receiverPath string) error

	// Unsubscribe removes receiverPath from topic (and optional group).
	Unsubscribe(ctx context.Context, topic, group, receiverPath string) error
}
