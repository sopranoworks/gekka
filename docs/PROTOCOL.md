# Gekka Protocol Details (v1.0.0-rc2)

This document covers technical details of the Artery TCP and Aeron UDP protocol
implementations and Gekka's internal mechanics. For the serialization subsystem see
[SERIALIZATION.md](SERIALIZATION.md).

---

## Aeron (UDP) Framing

Gekka implements the Aeron 1.30.0 wire protocol natively in Go (`internal/core/aeron_proto.go`,
`internal/core/udp_artery_handler.go`). No JVM Media Driver is required. The implementation
is verified against Akka 2.6.21 via `sbt multi-jvm:test`.

### Frame Types

All multi-byte fields use **little-endian** encoding.

| Type Code | Name | Size | Direction |
|-----------|------|------|-----------|
| `0x0000` | PAD | — | — |
| `0x0001` | DATA | 32-byte header + payload | publisher → subscriber |
| `0x0002` | NAK | 28 bytes | subscriber → publisher |
| `0x0003` | SM (Status Message) | 36 bytes | subscriber → publisher |
| `0x0004` | ERROR | — | — |
| `0x0005` | SETUP | 40 bytes | publisher → subscriber |
| `0x0006` | RTT | — | measurement |

### DATA Frame Layout (32 bytes)

```
Offset  Size  Field            Description
──────  ────  ───────────────  ───────────────────────────────────────────────
 0– 3    4    frameLength      Total bytes including this header (little-endian)
  4      1    version          Always 0 (AeronVersion)
  5      1    flags            0x80=BEGIN_FRAG, 0x40=END_FRAG, 0xC0=COMPLETE
 6– 7    2    frameType        0x0001 = DATA
 8–11    4    termOffset       Byte offset within the active term buffer
12–15    4    sessionId        Publisher session identifier
16–19    4    streamId         Artery logical stream (see below)
20–23    4    termId           Active term identifier
24–31    8    reservedValue    Must be 0
```

### Artery Logical Streams

| Stream ID | Name | Purpose |
|-----------|------|---------|
| `1` | Control | Artery handshake (`HandshakeReq`/`HandshakeRsp`), cluster heartbeats, compression advertisements |
| `2` | Ordinary | User-level actor messages |
| `3` | Large | Large messages fragmented across multiple DATA frames |

### SETUP Frame Layout (40 bytes)

Sent once by the publisher at session start. Informs the subscriber of term-buffer
parameters so it can allocate matching state.

```
Offset  Size  Field          Description
──────  ────  ─────────────  ───────────────────────────────────────
 0– 3    4    frameLength    = 40
 6– 7    2    frameType      = 0x0005
 8–11    4    termOffset     = 0
12–15    4    sessionId
16–19    4    streamId
20–23    4    initialTermId  Term ID at session start
24–27    4    activeTermId   Current active term
28–31    4    termLength     Term-buffer size (default: 16 MiB)
32–35    4    mtu            Max DATA payload per frame (default: 1408 bytes)
36–39    4    ttl            Multicast TTL (0 for unicast)
```

### Status Message (SM) Frame Layout (36 bytes)

SM frames flow subscriber → publisher to advertise consumption position and
available receiver window. Aeron 1.30.0 includes an 8-byte `receiverId` field
(`StatusMessageFlyweight.HEADER_LENGTH = 36`).

```
Offset  Size  Field                  Description
──────  ────  ─────────────────────  ─────────────────────────────────────
 0– 3    4    frameLength            = 36
 6– 7    2    frameType              = 0x0003
 8–11    4    sessionId
12–15    4    streamId
16–19    4    consumptionTermId      Last fully consumed term
20–23    4    consumptionTermOffset  Byte offset consumed within that term
24–27    4    receiverWindowLength   Remaining receive buffer (flow control)
28–35    8    receiverId             Unique subscriber identifier (int64)
```

### NAK Frame Layout (28 bytes)

Sent by the subscriber when it detects a sequence gap (lost DATA frame). The
publisher re-transmits the requested range.

```
Offset  Size  Field        Description
──────  ────  ───────────  ─────────────────────────────────────
 0– 3    4    frameLength  = 28
 6– 7    2    frameType    = 0x0002
 8–11    4    sessionId
12–15    4    streamId
16–19    4    termId       Term containing the missing data
20–23    4    termOffset   Start of the missing byte range
24–27    4    length       Byte length of the missing range
```

### Reliability Mechanism

Aeron uses **NACK-based selective retransmission** rather than cumulative ACKs:

1. The publisher maintains a circular **term buffer** (default 16 MiB).  Each DATA
   frame is placed at a monotonically increasing `termOffset`.
2. The subscriber tracks the highest contiguous `termOffset` it has received.
   On detecting a gap it sends a **NAK** frame identifying the missing range.
3. The publisher re-sends the requested range from its term buffer.
4. The subscriber periodically sends **SM** frames to report its consumption
   position and available window.  The publisher pauses when the window is exhausted
   (back-pressure).

### Transport Parameters (Gekka defaults)

| Parameter | Value | Description |
|-----------|-------|-------------|
| `termLength` | 16 MiB | Circular term-buffer size |
| `mtu` | 1408 bytes | Max DATA payload (fits in 1500-byte Ethernet MTU) |
| `windowLength` | 256 KiB | Initial receiver window advertised in SM |
| Frame alignment | 32 bytes | All term offsets are multiples of 32 |

### HOCON Configuration

```hocon
pekko.remote.artery {
  transport = aeron-udp          # "tcp" | "tls-tcp" | "aeron-udp"
  canonical {
    hostname = "127.0.0.1"
    port     = 2552
  }
}
```

---

## Artery TCP Protocol

| Constant | Value | Meaning |
|----------|-------|---------|
| Frame length prefix | 4-byte **little-endian** | Outer TCP framing |
| Connection magic | `[A,K,K,A,streamId]` | 5 bytes; outbound sender writes this |
| StreamId 1 | Control stream | Handshake, heartbeats, compression advertisements |
| StreamId 2 | Ordinary messages | User-level application messages |
| Serializer ID 2 | ProtobufSerializer | `proto.Message` types |
| Serializer ID 4 | ByteArraySerializer | Raw `[]byte`, empty manifest |
| Serializer ID 5 | ClusterMessageSerializer | Short manifests: `"IJ"`, `"W"`, `"GE"`, `"HB"`, … |
| Serializer ID 9 | JSONSerializer | Jackson-compatible JSON; manifest = fully-qualified type name |
| Serializer ID 17 | ArteryMessageSerializer | Artery transport frames; handshake (`"d"`/`"e"`) and `RemoteWatcher` heartbeats (`"Heartbeat"` / `"HeartbeatRsp"`) |
| Serializer ID 20 | StringSerializer | `java.lang.String` as raw UTF-8 bytes (Akka 2.6 built-in; **not** Java ObjectOutputStream format) |
| Welcome payload | GZIP-compressed | |
| GossipEnvelope.serializedGossip | GZIP-compressed | |

---

## Connection Lifecycle

```
Client (Go)                          Server (Pekko/Go)
   |-- TCP connect ------------------>|
   |-- AKKA magic + streamId -------->|   [A,K,K,A, streamId=2]
   |-- HandshakeReq (ID=17, "d") ---->|   ArteryControlFormats.HandshakeReq
   |<- HandshakeReq (ID=17, "d") -----|   mutual exchange
   |-- Artery user messages ---------->|
   |<- Artery user messages -----------|
```

The handshake payload is a `HandshakeReq` proto message (serializer ID 17,
manifest `"d"`). After both sides have exchanged handshakes, the association
moves to `ASSOCIATED` and buffered messages are flushed.

---

## Cluster Membership Protocol

Cluster state is propagated by gossip. Every member broadcasts:

1. **InitJoin** (`"IJ"`) — sent to `/system/cluster/core/daemon`; includes
   the current HOCON config hash (required by Pekko's `JoinConfigCompatCheck`).
2. **InitJoinAck** (`"IJA"`) — response from the seed node.
3. **Join** (`"J"`) — formal join request; contains the node's `UniqueAddress`.
4. **Welcome** (`"W"`) — full gossip state sent by the seed to the joiner
   (payload is GZIP-compressed).
5. **GossipEnvelope** (`"GE"`) — periodic full gossip state exchange
   (`serializedGossip` field is GZIP-compressed).
6. **GossipStatus** (`"GS"`) — lightweight vector-clock summary for push-pull.
7. **Heartbeat** (`"HB"`) / **HeartbeatRsp** (`"HBR"`) — liveness probes sent
   to `/system/cluster/heartbeatReceiver`.
8. **Leave** (`"L"`) — graceful departure; serialized as an `Address` proto.

---

## Failure Detection

Gekka uses a **phi-accrual failure detector** to compute per-node suspicion
scores from heartbeat inter-arrival times.

| Parameter | Default | Description |
|-----------|---------|-------------|
| Heartbeat interval | 1 s | How often heartbeats are sent |
| Acceptable pause | 15 s | Maximum heartbeat gap before suspicion rises |
| Phi threshold | 8.0 | Score above which a node is marked Unreachable |

When a node is marked Unreachable, the SplitBrainResolver (SBR) is engaged
after `stable-after` seconds. The default strategy (`keep-oldest`) retains the
partition containing the oldest node.

---

## Pending Coverage & Prioritization (v0.14.x)

To achieve full Artery TCP parity, the following areas are prioritized for the next development cycle:

### 1. Remote Death Watch (System Messages)
Current implementation handles Artery-level `RemoteWatcher` heartbeats (ID 17), but lacks full support for **System Messages** (Reliable delivery of Watch/Unwatch).
- **Goal**: Implement `SystemMessage` envelope processing for `Watch` (ID 2), `Unwatch` (ID 3), and `DeathWatchNotification` (ID 7).
- **Focus**: Ensure `SystemMessage` sequence numbering and acknowledgement (ID 1) are strictly followed for exactly-once delivery of lifecycle events.

### 2. Actor Selection
While direct `ActorRef` messaging is stable, `ActorSelection` (path-based lookups) requires explicit integration.
- **Protocol**: `MessageContainerSerializer` (ID 6) with manifest `"sel"`.
- **Payload**: `ActorSelectionMessage` containing the path elements and the original message.

### 3. Protobuf (ID 2) Serialization Coverage
The `ProtobufSerializer` is currently used for internal Artery/Cluster control, but needs to be promoted for general user-level message serialization.
- **Requirement**: Register common Pekko/Akka system Protobuf types to avoid falling back to expensive JSON/Java serialization.

---

## Serialization

The serialization subsystem is covered in full detail in
[SERIALIZATION.md](SERIALIZATION.md). Quick reference:

- **ID 2** — Protobuf (`proto.Message`)
- **ID 4** — Raw bytes (`[]byte`, empty manifest)
- **ID 6** — Message Container (`ActorSelection`, `Identification`)
- **ID 9** — JSON; manifest = registered Go/Java type name
- **ID 5, 17** — Handled at the transport layer; not exposed via `SerializationRegistry`

---

## Distributed Data (CRDTs)

Gekka ships a gossip-based CRDT replicator in the public `crdt` package.
State is propagated as JSON (serializer ID 4) over Artery.

### CRDT Semantics

- **G-Counter** — each node owns one slot; global value = sum of all slots;
  merge = pairwise max.
- **OR-Set** — each `Add` is tagged with a unique `(nodeID, counter)` dot;
  `Remove` clears all observed dots; merge preserves dots unknown to the remote
  side.

### Consistency Levels

| Level | Behaviour |
|-------|-----------|
| `crdt.WriteLocal` | Update local state only; gossip propagates on the next tick |
| `crdt.WriteAll` | Immediately push the new state to all registered peers |

---

## Multi-Node Cluster Join Behavior

When a Gekka node joins a cluster that already has multiple members, the seed
node's **Welcome** (`"W"`) message contains the full gossip state — including
addresses of members the joiner has never communicated with. The joiner's
phi-accrual failure detector has no heartbeat history for these members and may
immediately mark them as **Unreachable**.

This is expected and transient. The resolution sequence is:

1. Joiner receives `Welcome` containing `N` members.
2. Joiner's failure detector evaluates members with no heartbeat history →
   phi exceeds threshold → `UnreachableMember` event fires for unknown peers.
3. Akka/Pekko seed node propagates the joiner's address via gossip to all
   existing members.
4. Existing members initiate outbound Artery connections to the joiner.
5. Heartbeat exchange begins → phi drops below threshold → members become
   reachable.

**Implementation note:** Gekka treats `UnreachableMember` events for remote
members as warnings, not fatal errors. Only `MemberDowned` or `MemberRemoved`
for the local node triggers a hard failure. This differs from the original
2-node compat test binary which treated any `UnreachableMember` as fatal — that
assumption breaks in clusters with 3+ members where connection establishment is
not instantaneous.

---

## Wire Compatibility Notes

- Pekko sends heartbeats to `/system/cluster/heartbeatReceiver` using
  serializer **6** (`MessageContainerSerializer`). Gekka does not decode ID 6;
  the absence of a heartbeat reply causes Pekko's failure detector to mark the
  Go node as Unreachable after ~18 s. Configure SBR with `keep-oldest` and
  `stable-after = 5s` to auto-resolve this.
- `InitJoin` **must** include a non-empty `currentConfig` field (at minimum
  `pekko.cluster.downing-provider-class = ""`); Pekko's
  `JoinConfigCompatCheckCluster` throws if this field is absent.
- Both `Welcome` and `GossipEnvelope.serializedGossip` payloads are
  GZIP-compressed at the application layer, not by the TCP transport.
