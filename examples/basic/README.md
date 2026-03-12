# examples/basic — Ping-Pong Cluster Node

A minimal, fully-commented example that demonstrates the core Gekka v0.2.0 API:

| Step | API call | What it does |
|------|----------|--------------|
| 1 | `NewClusterFromConfig("application.conf")` | Parse HOCON, bind TCP listener |
| 2 | `node.System.ActorOf(Props{...}, "echo")` | Create and register a local ping-pong actor |
| 3 | `node.ActorSelection("...").Resolve(ctx)` | Resolve a remote actor reference |
| 4 | `node.JoinSeeds()` + `WaitForHandshake` | Join cluster, await Artery handshake |
| 5 | `ref.Tell([]byte{…})` / `ref.Ask(ctx, ...)` | Deliver test messages to remote actor |
| 6 | `node.Leave()` | Graceful cluster departure on SIGINT |

---

## Prerequisites

| Requirement | Version |
|-------------|---------|
| Go | 1.23+ |
| A running Pekko seed node at `127.0.0.1:2552` | Apache Pekko 1.x |

The example is pre-configured to point at `pekko://ClusterSystem@127.0.0.1:2552`.
Edit `application.conf` if your seed runs elsewhere.

---

## Running the example

### 1. Start a Pekko seed node (Scala)

The companion Scala server lives in `../../scala-server`.
Start the single-node seed:

```bash
cd ../../scala-server
sbt "runMain com.example.ClusterSeedNode"
```

Wait until you see:

```
--- CLUSTER SEED READY ---
```

The seed exposes `/user/echo`, which replies `"Echo: <original>"` to every
`Array[Byte]` message it receives.

### 2. Run the Go example

From the repository root (where `go.mod` lives):

```bash
go run ./examples/basic
```

Or change into the directory first:

```bash
cd examples/basic
go run .
```

### Expected output

```
[gekka] node listening on  127.0.0.1:54321 (port 54321)
[gekka] actor-system name: ClusterSystem
[gekka] joining cluster via seed pekko://ClusterSystem@127.0.0.1:2552 …
[gekka] Artery handshake complete — joined cluster.
[gekka] -> "Ping at 12:00:00"  →  pekko://ClusterSystem@127.0.0.1:2552/user/echo
[gekka] <- received (serializerId=4): "Echo: Ping at 12:00:00"
[gekka] -> "Ping at 12:00:03"  →  pekko://ClusterSystem@127.0.0.1:2552/user/echo
[gekka] <- received (serializerId=4): "Echo: Ping at 12:00:03"
…
```

Press **Ctrl-C** to trigger a graceful `Leave → Exiting → Removed` cycle.

---

## Ping-Pong handler

When the seed (or any cluster member) sends the raw bytes `"Ping"` to this
node's local `/user/echo` actor, the `Receive` method is triggered and replies
`"Pong"` to the seed's actor via its remote `ActorRef`:

```go
type EchoActor struct {
    actor.BaseActor
    echoRef gekka.ActorRef
}

func (a *EchoActor) Receive(msg any) {
    incoming, ok := msg.(*gekka.IncomingMessage)
    if !ok {
        return
    }

    if string(incoming.Payload) == "Ping" {
        a.echoRef.Tell([]byte("Pong"))
    }
}
```

The actor is registered via `node.System.ActorOf(gekka.Props{...}, "echo")`,
which launches a dedicated receive goroutine and binds it to the `/user/echo`
Artery path.

---

## Configuration reference

`application.conf` uses standard Pekko HOCON keys that `gekka.LoadConfig`
recognises:

| Key | Purpose |
|-----|---------|
| `pekko.remote.artery.canonical.hostname` | IP this node advertises to peers |
| `pekko.remote.artery.canonical.port` | Listen port (`0` = OS-assigned) |
| `pekko.cluster.seed-nodes` | Bootstrap seed addresses |
| `pekko.cluster.downing-provider-class` | Split-Brain Resolver (required) |
| `pekko.cluster.failure-detector.*` | Heartbeat tuning |

---

---

## Custom serializers

Gekka's `SerializationRegistry` lets you plug in any wire format used by your
existing Pekko cluster.  Implement the `gekka.Serializer` interface and
register it before sending or receiving messages with that serializer ID.

### Interface

```go
type Serializer interface {
    Identifier() int32
    ToBinary(obj interface{}) ([]byte, error)
    FromBinary(data []byte, manifest string) (interface{}, error)
}
```

### Example — Jackson-compatible JSON serializer (ID 100)

Pekko's Jackson module embeds the fully-qualified class name as the manifest
(e.g. `"com.example.OrderPlaced"`).  Implement `FromBinary` to switch on that
string and unmarshal into the correct Go struct:

```go
import (
    "encoding/json"
    "fmt"
    "gekka/gekka"
)

type OrderPlaced struct {
    OrderID  string  `json:"orderId"`
    Product  string  `json:"product"`
    Quantity int     `json:"quantity"`
}

type jacksonSerializer struct{}

func (j *jacksonSerializer) Identifier() int32 { return 100 }

func (j *jacksonSerializer) ToBinary(obj interface{}) ([]byte, error) {
    return json.Marshal(obj)
}

func (j *jacksonSerializer) FromBinary(data []byte, manifest string) (interface{}, error) {
    switch manifest {
    case "com.example.OrderPlaced":
        var e OrderPlaced
        return e, json.Unmarshal(data, &e)
    }
    return nil, fmt.Errorf("unknown manifest: %s", manifest)
}

// Registration — call early before any messages use this serializer.
node.Serialization().RegisterSerializer(100, &jacksonSerializer{})
```

### Sending a custom-serialized message

Actors can handle `[]byte` (ID 4) and `proto.Message` (ID 2) natively. For
a custom format, serialize manually (or rely on `Tell` to pass to the serializer
if it's registered) and check `msg.SerializerId` inside your actor's `Receive`:

```go
// Outbound: serialize with your serializer, then Tell.
// The remote Pekko actor must know to expect serializerId=100.
payload, _ := json.Marshal(OrderPlaced{OrderID: "ORD-1", Product: "widget", Quantity: 3})
remoteRef.Tell(payload)

// Inbound: dispatch by SerializerId in Receive.
func (a *MyActor) Receive(msg any) {
    incoming := msg.(*gekka.IncomingMessage)
    if incoming.SerializerId == 100 {
        obj, err := a.node.Serialization().DeserializePayload(
            incoming.SerializerId, incoming.Manifest, incoming.Payload)
        if err != nil {
            return
        }
        switch e := obj.(type) {
        case OrderPlaced:
            log.Printf("order received: %+v", e)
        }
    }
}
```

### Built-in serializers

| ID | Type | Notes |
|----|------|-------|
| 2 | `ProtobufSerializer` | `proto.Message`; requires `RegisterManifest` for deserialization |
| 4 | `ByteArraySerializer` | Raw `[]byte` passthrough; manifest is ignored |

---

## Next steps

- **CRDT distributed data** — see `examples/crdt-dashboard/`
- **Cluster Singleton** — `node.SingletonProxy("/user/manager", "")` routes
  messages to the singleton on the oldest Up member automatically
- **Ask (request-response)** — `node.Ask(ctx, path, msg)` blocks until the
  remote actor replies; see `examples/ask-pattern/`
- **Multiple Go nodes** — spawn additional nodes with `Port: 0`; all can join
  the same seed and communicate directly once the Artery handshake completes
