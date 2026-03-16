# Gekka Serialization Reference (v0.7.0)

This document covers the serialization subsystem: built-in serializer IDs, the
`SerializationRegistry`, manifest mapping, and how to register custom types for
interoperability with Apache Pekko / Akka.

---

## Serializer IDs

Every Artery TCP frame carries a `serializerId` integer that tells the receiver
which codec to use. Gekka defines the same IDs as Pekko so that cross-language
message exchange works without configuration.

| ID | Name | Description |
|----|------|-------------|
| **2** | `ProtobufSerializer` | `proto.Message` types — lossless, compact binary |
| **4** | `ByteArraySerializer` | Raw `[]byte`; manifest is always empty |
| **5** | `ClusterMessageSerializer` | Cluster-internal protocol messages; short manifests (`"IJ"`, `"W"`, `"GE"`, …) |
| **9** | `JSONSerializer` | Jackson-compatible JSON; manifest is the fully-qualified type name |
| **17** | `ArteryMessageSerializer` | Artery control frames (handshake `"d"`/`"e"`, remote-watcher `"Heartbeat"`/`"HeartbeatRsp"`); handled by the transport layer |

> **IDs 5 and 17** are handled at the TCP transport layer and are **not**
> present in the general-purpose `SerializationRegistry`. Registering a custom
> serializer for these IDs has no effect.

---

## SerializationRegistry

`SerializationRegistry` is the central registry that maps serializer IDs ↔
codec implementations and manifest strings ↔ Go `reflect.Type` values.

```go
// Access the registry from a running Cluster:
reg := cluster.Serialization()

// Or create a standalone registry (e.g. in tests):
reg := gekka.NewSerializationRegistry()
```

### Registering a Custom Java/Scala Manifest

When a Pekko actor sends a message, the wire frame includes the fully-qualified
Java class name as the manifest (e.g. `"com.example.OrderPlaced"`). Register
the corresponding Go type so that the JSON deserializer can reconstruct it:

```go
import "reflect"

type OrderPlaced struct {
    OrderID string `json:"orderId"`
    Amount  int    `json:"amount"`
}

reg.RegisterManifest("com.example.OrderPlaced", reflect.TypeOf(OrderPlaced{}))
```

### Registering Java Primitive Types

| Java / Scala manifest | Go type |
|-----------------------|---------|
| `"java.lang.String"` | `reflect.TypeOf("")` |
| `"java.lang.Integer"` | `reflect.TypeOf(int32(0))` |
| `"java.lang.Long"` | `reflect.TypeOf(int64(0))` |
| `"java.lang.Boolean"` | `reflect.TypeOf(false)` |
| `"java.lang.Double"` | `reflect.TypeOf(float64(0))` |

```go
reg.RegisterManifest("java.lang.String", reflect.TypeOf(""))
```

### Registering a Custom Serializer

Implement the `Serializer` interface and register it against a new ID:

```go
type MySerializer struct{}

func (s *MySerializer) Identifier() int32                            { return 42 }
func (s *MySerializer) ToBinary(msg interface{}) ([]byte, error)    { /* ... */ }
func (s *MySerializer) FromBinary(data []byte, manifest string) (interface{}, error) { /* ... */ }

reg.RegisterSerializer(42, &MySerializer{})
```

---

## JSON Serialization (ID 9)

By default any Go struct is serialized as JSON with serializer ID 9. The
manifest is derived from the registered manifest string, or defaults to the
Go `reflect.Type` name if no manifest is registered.

### Sending a struct to a remote Pekko actor

```go
type Ping struct {
    ID string `json:"id"`
}

// Optional: register a Pekko-compatible manifest so Pekko can decode it.
cluster.Serialization().RegisterManifest("com.example.Ping", reflect.TypeOf(Ping{}))

cluster.Send(ctx, "pekko://MySystem@10.0.0.1:2552/user/service", Ping{ID: "abc"})
```

### Receiving a struct from Pekko

```go
cluster.Serialization().RegisterManifest("com.example.Pong", reflect.TypeOf(Pong{}))

cluster.OnMessage(func(ctx context.Context, msg *gekka.IncomingMessage) error {
    if msg.DeserializedMessage != nil {
        if pong, ok := msg.DeserializedMessage.(Pong); ok {
            fmt.Println("Pong:", pong.Reply)
        }
    }
    return nil
})
```

---

## Protobuf Serialization (ID 2)

Use Protobuf for maximum efficiency when both sides share the same `.proto`
file:

```go
import (
    "google.golang.org/protobuf/proto"
    mypb "github.com/example/myproto"
)

// Serializer ID 2 is used automatically for proto.Message values.
cluster.Send(ctx, remotePath, &mypb.OrderRequest{OrderId: "123"})
```

Protobuf deserialization requires a type-aware registry entry because the
wire format does not carry a self-describing schema. Register a custom
`ProtobufSerializer` subtype if you need `FromBinary` support for a specific
proto message.

---

## Raw Bytes (ID 4)

Pass opaque binary payloads with no encoding overhead:

```go
cluster.Send(ctx, remotePath, []byte("raw payload"))
```

On the Pekko side, the actor receives an `Array[Byte]`. The manifest is always
the empty string.

---

## Cluster Message Manifests (ID 5)

These are fixed short strings used internally. Do **not** register Go types for
them; the artery handler decodes them directly:

| Manifest | Pekko Message |
|----------|--------------|
| `"IJ"` | `InitJoin` |
| `"IJA"` | `InitJoinAck` |
| `"J"` | `Join` |
| `"W"` | `Welcome` (GZIP-compressed payload) |
| `"GE"` | `GossipEnvelope` (GZIP-compressed `SerializedGossip`) |
| `"GS"` | `GossipStatus` |
| `"HB"` | `Heartbeat` |
| `"HBR"` | `HeartbeatRsp` |
| `"L"` | `Leave` |
