# Gekka Protocol Details

This document covers technical details of the Artery TCP protocol implementation and Gekka's internal mechanics.

## Artery TCP Protocol

| Constant | Value | Meaning |
|----------|-------|---------|
| Frame length prefix | 4-byte **little-endian** | Outer TCP framing |
| Connection magic | `[A,K,K,A,streamId]` | 5 bytes; outbound sender writes this |
| StreamId 1 | Control stream | Handshake, heartbeats, compression advertisments |
| StreamId 2 | Ordinary messages | User-level application messages |
| Serializer ID 2 | ProtobufSerializer | `proto.Message` types |
| Serializer ID 4 | ByteArraySerializer | Raw `[]byte`, empty manifest |
| Serializer ID 5 | ClusterMessageSerializer | Short manifests: `"IJ"`, `"W"`, `"GE"`, `"HB"`, … |
| Serializer ID 17 | ArteryMessageSerializer | Artery transport frames; used by both handshake (`"d"`/`"e"`) and `RemoteWatcher` heartbeats (`"Heartbeat"` / `"HeartbeatRsp"`) |
| Welcome payload | GZIP-compressed | |
| GossipEnvelope.serializedGossip | GZIP-compressed | |

## Serialization

Gekka uses a flexible `SerializationRegistry` to map Artery serializer IDs and manifest strings to Go types.

### Built-in Serializers

- **Protobuf** (ID 2): For all `proto.Message` types.
- **Raw Bytes** (ID 4): For raw `[]byte` payloads.
- **JSON** (ID 9): Jackson-compatible JSON serialization.

### Registering Custom Types (JSON)

By default, any struct sent via `Tell` is serialized as JSON (ID 9) using its reflect type name as the manifest.

```go
import "reflect"

// Register the manifest "com.example.OrderPlaced" to a Go struct
node.Serialization().RegisterManifest("com.example.OrderPlaced", reflect.TypeOf(OrderPlaced{}))
```

## Distributed Data (CRDTs)

Gekka supports state propagation via CRDTs (Conflict-free Replicated Data Types) using a custom JSON gossip protocol.

### CRDT Semantics

- **G-Counter**: Each node owns one slot; global value = sum of all slots; merge = pairwise max.
- **OR-Set**: Each Add is tagged with a unique (nodeID, counter) dot; Remove clears all observed dots; merge preserves dots unknown to the remote side.

### Consistency Levels

| Level | Behaviour |
|-------|-----------|
| `crdt.WriteLocal` | Update local state only; gossip propagates on the next tick |
| `crdt.WriteAll` | Immediately push to all registered peers |
