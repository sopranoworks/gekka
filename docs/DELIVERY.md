<!--
  DELIVERY.md — Reliable Delivery Design for Gekka
  Copyright (c) 2026 Sopranoworks, Osamu Takahashi
  SPDX-License-Identifier: MIT
-->

# Reliable Delivery

This document describes the design for a new `actor/delivery` package that implements
the **Reliable Delivery** pattern compatible with Pekko's `ProducerController` /
`ConsumerController` API (introduced in Pekko 1.0 / Akka 2.6.4).

---

## 1. Background: Pekko Reliable Delivery

Pekko's `pekko.actor.typed.delivery` module provides at-least-once, flow-controlled,
chunked message delivery between actor pairs.  The protocol is built on top of
Artery and gossip; its key properties are:

| Property | Description |
|----------|-------------|
| **At-least-once** | Every message sent by the producer is delivered at least once, even across producer/consumer restarts. |
| **Back-pressure** | The consumer sends demand (`Request`) upstream; the producer only sends when demand > 0. |
| **Deduplication** | Each message carries a monotonically-increasing `seqNr`.  The consumer filters duplicates on re-delivery. |
| **Chunked transfer** | Large payloads are split into `ChunkedMessage` fragments; the consumer reassembles before delivering to the business actor. |
| **Durable state** | The producer can optionally checkpoint the last confirmed `seqNr` to a `DurableProducerQueue`, enabling recovery after crash. |

### Wire Protocol (Pekko serializer ID 36)

Pekko uses `ReliableDeliverySerializer` (ID **36**) with the following manifests:

| Manifest | Proto message | Direction |
|----------|---------------|-----------|
| `"PA"` | `SequencedMessage` | Producer → Consumer |
| `"PB"` | `Ack` | Consumer → Producer |
| `"PC"` | `Request` | Consumer → Producer |
| `"PD"` | `Resend` | Consumer → Producer |
| `"PE"` | `ProducerRegistered` | ProducerController → ConsumerController |
| `"PF"` | `ProducerTerminated` | ConsumerController → WorkPullingProducerController |
| `"PG"` | `RegisterConsumer` | ConsumerController → ProducerController |
| `"PH"` | `DurableProducerQueue.State` | Durable queue snapshot |
| `"PI"` | `DurableProducerQueue.Confirmed` | Durable confirmation |
| `"PJ"` | `DurableProducerQueue.Cleanup` | Durable cleanup |
| `"PK"` | `ChunkedMessage` | Producer → Consumer (large msg fragment) |

The `SequencedMessage` envelope wraps the user payload along with:
- `producerId` (string) — unique identifier for the producer instance
- `seqNr` (int64) — sequence number, starting at 1, strictly monotonic
- `first` (bool) — true on the first message after a (re)start
- `ack` (bool) — whether an immediate `Ack` is expected

---

## 2. Gekka Design Goals

The `actor/delivery` package will provide:

1. **`ProducerController[T]`** — wraps a sending actor, assigns sequence numbers,
   tracks demand, and handles retransmission on re-registration.
2. **`ConsumerController[T]`** — wraps a receiving actor, tracks the expected
   `seqNr`, sends demand (`Request`), acknowledges, and deduplicates.
3. **Wire compatibility** with Pekko's `ReliableDeliverySerializer` (ID 36), so a
   Go producer can send to a Pekko consumer and vice versa.
4. **Optional durable state** via the existing `persistence.Journal` interface.
5. **Chunked transfer** for payloads larger than a configurable threshold (default
   128 KiB).

---

## 3. Package Layout

```
actor/delivery/
  doc.go                   package-level godoc
  producer.go              ProducerController[T] implementation
  consumer.go              ConsumerController[T] implementation
  chunker.go               payload chunking / reassembly
  serializer.go            ReliableDeliverySerializer (ID 36) encode/decode
  proto/                   generated Go protobuf types (ReliableDelivery.proto)
    reliable_delivery.pb.go
  durable/
    journal_queue.go        JournalDurableQueue — Journal-backed durable queue
  delivery_test.go          unit tests (in-process)
  delivery_compatibility_test.go  //go:build integration — wire format tests vs Pekko
```

---

## 4. Core Types

### 4.1 `ProducerController[T]`

```go
// ProducerController manages the producer side of a reliable delivery channel.
// T is the message type delivered to the consumer.
type ProducerController[T any] struct {
    producerID string
    consumer   actor.Ref          // ConsumerController's ActorRef
    seqNr      int64              // next sequence number to assign
    unacked    map[int64]T        // messages sent but not yet acked
    demand     int64              // outstanding consumer demand
    durable    DurableProducerQueue // nil = no durability
    cfg        ProducerConfig
}

type ProducerConfig struct {
    // ResendInterval is how often to re-send unacknowledged messages.
    // Default: 1s.
    ResendInterval time.Duration

    // ChunkThreshold is the payload byte size above which messages are split.
    // Default: 128 KiB.  Set to 0 to disable chunking.
    ChunkThreshold int

    // DurableQueue enables crash-recovery of unconfirmed sequence numbers.
    DurableQueue DurableProducerQueue
}

// Send enqueues msg for delivery.  It blocks (with back-pressure) if consumer
// demand is zero.  Returns when the message is accepted into the send buffer.
func (p *ProducerController[T]) Send(ctx context.Context, msg T) error

// Confirmed is called when an Ack from the consumer arrives.
func (p *ProducerController[T]) Confirmed(seqNr int64)
```

### 4.2 `ConsumerController[T]`

```go
// ConsumerController manages the consumer side of a reliable delivery channel.
// Verified messages (after dedup) are forwarded to the business actor.
type ConsumerController[T any] struct {
    producerID  string
    producer    actor.Ref           // ProducerController's ActorRef
    expectedSeq int64               // next seqNr expected from producer
    buffer      map[int64]T         // out-of-order buffer
    downstream  actor.Ref           // business actor receiving messages
    demand      int64               // how many more messages we are willing to receive
    cfg         ConsumerConfig
}

type ConsumerConfig struct {
    // FlowControlWindow is how many unacknowledged messages the consumer
    // allows.  Default: 20.
    FlowControlWindow int

    // ResendRequestAfter triggers a Resend request if no message arrives
    // within this duration.  Default: 1s.
    ResendRequestAfter time.Duration
}

// Receive dispatches an incoming wire message (SequencedMessage, etc.) from the
// producer side.
func (c *ConsumerController[T]) Receive(ctx context.Context, msg any) error

// RequestMore sends a Request to the producer, granting additional demand.
func (c *ConsumerController[T]) RequestMore(n int64)
```

### 4.3 `DurableProducerQueue` interface

```go
// DurableProducerQueue persists the producer's delivery state for crash recovery.
type DurableProducerQueue interface {
    // LoadState returns the last persisted state (highest confirmed seqNr, and
    // the set of messages in-flight that were not yet confirmed).
    LoadState(ctx context.Context, producerID string) (*QueueState, error)

    // Store persists a new message before it is sent.
    Store(ctx context.Context, producerID string, seqNr int64, qualifiedSeqNr int64, msg []byte) error

    // Confirm marks seqNr as delivered and safe to drop from the durable log.
    Confirm(ctx context.Context, producerID string, seqNr int64, qualifiedSeqNr int64) error

    // Cleanup removes all stored state for producerID up to and including seqNr.
    Cleanup(ctx context.Context, producerID string, seqNr int64) error
}

// QueueState is the recovered state returned by DurableProducerQueue.LoadState.
type QueueState struct {
    HighestConfirmedSeqNr int64
    Unconfirmed           []UnconfirmedEntry
}
```

`JournalDurableQueue` in `durable/journal_queue.go` implements this interface
using the `persistence.Journal` backend (PostgreSQL, Pebble, or in-memory).

---

## 5. Protocol State Machine

### Producer side

```
IDLE ──register──► REGISTERED ──demand > 0──► SENDING
                       ▲                          │
                       │   ack (all sent)         │ send SequencedMessage
                       └──────────────────────────┘
                                   │
                          consumer gone / timeout
                                   ▼
                             WAITING_CONSUMER
```

### Consumer side

```
IDLE ──receive first msg──► ACTIVE
                                │
                    seqNr == expected: deliver + ack
                    seqNr > expected:  buffer, send Resend
                    seqNr < expected:  drop (duplicate)
                                │
                    demand == 0: stop requesting
                    demand > 0:  send Request(window)
```

---

## 6. Serializer (ID 36)

`serializer.go` implements `ReliableDeliverySerializer` compatible with Pekko's
wire format.  It uses the protobuf schema in `proto/ReliableDelivery.proto`
(sourced from Pekko's `akka-remote/src/main/protobuf`).

```go
const ReliableDeliverySerializerID = 36

// Encode serializes msg into the Pekko-compatible wire format.
// manifest is one of the "P*" constants defined by Pekko.
func Encode(msg proto.Message, manifest string) ([]byte, error)

// Decode deserializes payload using the given manifest.
func Decode(payload []byte, manifest string) (proto.Message, error)
```

Manifests are string constants mirroring Pekko's:

```go
const (
    ManifestSequencedMessage    = "PA"
    ManifestAck                 = "PB"
    ManifestRequest             = "PC"
    ManifestResend              = "PD"
    ManifestProducerRegistered  = "PE"
    ManifestProducerTerminated  = "PF"
    ManifestRegisterConsumer    = "PG"
    ManifestChunkedMessage      = "PK"
)
```

---

## 7. Chunking

When `ProducerConfig.ChunkThreshold > 0` and `len(payload) > ChunkThreshold`,
`chunker.go` splits the payload:

```
ChunkedMessage {
    serializer_id: <user payload serializer>
    manifest:      <user payload manifest>
    chunk:         <bytes[0 : threshold]>
    first_chunk:   true
    last_chunk:    false
    seq_nr:        1
}
ChunkedMessage {
    chunk:      <bytes[threshold : ...]>
    first_chunk: false
    last_chunk:  true
    seq_nr:      1
}
```

The consumer reassembles chunks by `seqNr` before forwarding to the downstream
actor.  The business actor never sees `ChunkedMessage` directly.

---

## 8. Public API Surface

The package exposes typed actor behaviors for use with `gekka.SpawnTyped`:

```go
// NewProducerBehavior returns a Behavior that drives a ProducerController.
// Use with SpawnTyped to obtain a TypedActorRef[ProducerMessage[T]].
func NewProducerBehavior[T any](consumer actor.Ref, cfg ProducerConfig) actor.Behavior[ProducerMessage[T]]

// NewConsumerBehavior returns a Behavior that drives a ConsumerController.
// downstream receives T after dedup + in-order reassembly.
func NewConsumerBehavior[T any](producer actor.Ref, downstream actor.Ref, cfg ConsumerConfig) actor.Behavior[any]
```

Example:

```go
// Sender side
producer, _ := gekka.SpawnTyped(sys,
    delivery.NewProducerBehavior[MyMsg](consumerRef, delivery.ProducerConfig{}),
    "myProducer",
)

// Receiver side
consumer, _ := gekka.SpawnTyped(sys,
    delivery.NewConsumerBehavior[MyMsg](producerRef, businessActor, delivery.ConsumerConfig{}),
    "myConsumer",
)

producer.Tell(delivery.Send(myMessage))
```

---

## 9. Binary Compatibility Requirement

Per the project MANDATORY RULE, the following must exist before any merge:

- `actor/delivery/delivery_compatibility_test.go` — tagged `//go:build integration`
- Tests must verify that:
  1. A Go `ProducerController` can deliver 100 messages to a Pekko `ConsumerController`.
  2. A Pekko `ProducerController` can deliver 100 messages to a Go `ConsumerController`.
  3. Chunked transfer works end-to-end across the Go ↔ Pekko boundary.
  4. The Go consumer correctly deduplicates a re-sent message (simulated by
     triggering `Resend`).

Reference Scala harness: `scala-server/src/main/scala/com/example/DeliveryNode.scala`
(to be created alongside the Go implementation).

---

## 10. Milestones

| Step | Deliverable |
|------|-------------|
| 1 | Protobuf schema (`ReliableDelivery.proto`) — generated Go bindings |
| 2 | `serializer.go` + unit tests for encode/decode of all manifests |
| 3 | `producer.go` + `consumer.go` — in-process ping-pong unit test |
| 4 | `chunker.go` — large-payload splitting/reassembly unit test |
| 5 | `durable/journal_queue.go` — persisted producer queue |
| 6 | Public typed-actor API (`NewProducerBehavior`, `NewConsumerBehavior`) |
| 7 | Scala harness + `delivery_compatibility_test.go` (integration) |
| 8 | Docs + example in `examples/reliable-delivery/` |
