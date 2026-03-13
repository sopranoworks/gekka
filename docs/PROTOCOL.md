# Gekka Protocol Details (v0.5.0)

This document covers technical details of the Artery TCP protocol implementation
and Gekka's internal mechanics. For the serialization subsystem see
[SERIALIZATION.md](SERIALIZATION.md).

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

## Serialization

The serialization subsystem is covered in full detail in
[SERIALIZATION.md](SERIALIZATION.md). Quick reference:

- **ID 2** — Protobuf (`proto.Message`)
- **ID 4** — Raw bytes (`[]byte`, empty manifest)
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
