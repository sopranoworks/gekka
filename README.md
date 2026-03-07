# gekka &nbsp;[![Version](https://img.shields.io/badge/version-0.1.0-blue)](https://github.com/gekka/gekka) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Version 0.1.0**

A Go library for connecting to [Apache Pekko](https://pekko.apache.org/) clusters over the Artery TCP remoting protocol. Write Go services that participate in Pekko clusters alongside Scala/Java actors — joining, sending messages, routing to cluster singletons, and sharing distributed state via CRDTs.

## Features

- **Artery TCP remoting** — binary-compatible framing, compression-table handshake, sequence/ACK
- **Cluster membership** — InitJoin → Join → Welcome flow, gossip, vector-clock convergence, heartbeat/failure detection
- **Cluster Singleton Proxy** — route to the singleton on the current oldest node with automatic failover
- **Distributed Data (CRDTs)** — G-Counter and OR-Set with JSON gossip propagation to Scala peers
- **Serialization registry** — Protobuf (ID 2) and raw-bytes (ID 4) built in; register custom types
- **Multi-provider support** — select `ProviderPekko` (default) or `ProviderAkka` at node creation; all actor paths and internal cluster paths adjust automatically
- **Type-safe actor paths** — `actor.Address` and `actor.ActorPath` builder API (equivalent to Pekko's `Address` / `ActorPath`)

---

## Quick Start

```go
import (
    "gekka/gekka"
    "gekka/gekka/actor"
)

self := actor.Address{Protocol: "pekko", System: "ClusterSystem", Host: "127.0.0.1", Port: 2553}

node, err := gekka.Spawn(gekka.NodeConfig{Address: self})
if err != nil {
    log.Fatal(err)
}
defer node.Shutdown()

// Register a handler for incoming messages
node.OnMessage(func(ctx context.Context, msg *gekka.IncomingMessage) error {
    fmt.Printf("received: %s\n", msg.Payload)
    return nil
})

// Join a Pekko cluster seed node
if err := node.Join("127.0.0.1", 2552); err != nil {
    log.Fatal(err)
}

// Send a raw-bytes message to a remote actor using a typed path
seed := actor.Address{Protocol: "pekko", System: "ClusterSystem", Host: "127.0.0.1", Port: 2552}
echoPath := seed.WithRoot("user").Child("echo")
err = node.Send(ctx, echoPath, []byte("hello"))
```

---

## Configuration via HOCON

If you already have a Pekko or Akka `application.conf`, you can load it directly instead of constructing `NodeConfig` manually. gekka reads the same HOCON paths that Pekko uses.

### Example `application.conf`

```hocon
pekko {
  actor.provider = cluster

  remote.artery {
    transport = tcp
    canonical {
      hostname = "127.0.0.1"
      port = 2553
    }
  }

  cluster {
    seed-nodes = [
      "pekko://ClusterSystem@127.0.0.1:2552"
    ]
    downing-provider-class = "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"
    split-brain-resolver {
      active-strategy = keep-oldest
      stable-after    = 5s
    }
    failure-detector {
      acceptable-heartbeat-pause = 15s
      heartbeat-interval         = 1s
    }
  }
}
```

### Loading the config in Go

```go
// SpawnFromConfig reads application.conf and starts the node.
node, err := gekka.SpawnFromConfig("application.conf")
if err != nil {
    log.Fatal(err)
}
defer node.Shutdown()

node.OnMessage(func(ctx context.Context, msg *gekka.IncomingMessage) error {
    fmt.Printf("received: %s\n", msg.Payload)
    return nil
})

// JoinSeeds uses the seed-nodes list parsed from HOCON.
if err := node.JoinSeeds(); err != nil {
    log.Fatal(err)
}

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
node.WaitForHandshake(ctx, "127.0.0.1", 2552)
```

### Config fallback (reference.conf)

Layer a `reference.conf` with default values under your `application.conf`.
The primary file always wins — the fallback only fills in missing keys:

```go
node, err := gekka.SpawnFromConfig("application.conf", "reference.conf")
```

### Load config without spawning

Use `LoadConfig` when you need the parsed values before creating a node:

```go
cfg, err := gekka.LoadConfig("application.conf")
// cfg.Address   → actor.Address{Protocol:"pekko", System:"ClusterSystem", Host:"127.0.0.1", Port:2553}
// cfg.SeedNodes → []actor.Address{{..., Port:2552}}

node, err := gekka.Spawn(cfg)
// join manually or via JoinSeeds
node.JoinSeeds()
```

You can also parse an in-memory HOCON string (useful for embedding config in tests):

```go
cfg, err := gekka.ParseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1"\n  port = 2553 }
  cluster.seed-nodes = ["pekko://ClusterSystem@127.0.0.1:2552"]
}
`)
```

### Reusing Pekko config files

The HOCON keys gekka reads are identical to the ones Pekko itself uses, so you can point both the JVM service and the Go node at the **same** `application.conf`:

| HOCON path | Maps to |
|---|---|
| `pekko.remote.artery.canonical.hostname` | `Address.Host` |
| `pekko.remote.artery.canonical.port` | `Address.Port` |
| `pekko.cluster.seed-nodes` | `SeedNodes []actor.Address` |
| top-level key (`pekko` / `akka`) | protocol prefix (`"pekko"` / `"akka"`) |

The actor-system name is derived from the first seed-node URI
(e.g. `"pekko://ClusterSystem@…"` → `"ClusterSystem"`).

For Akka clusters, use the same config structure under the `akka {}` block
instead of `pekko {}` — the protocol is auto-detected.

---

## Actor Addresses and Paths

The `actor` sub-package provides type-safe equivalents of `org.apache.pekko.actor.{Address,ActorPath}`.

### `actor.Address`

```go
addr := actor.Address{
    Protocol: "pekko",         // "pekko" or "akka"
    System:   "ClusterSystem",
    Host:     "127.0.0.1",
    Port:     2552,
}
fmt.Println(addr) // → pekko://ClusterSystem@127.0.0.1:2552

// Parse from URI string
addr, err := actor.ParseAddress("pekko://ClusterSystem@127.0.0.1:2552")
```

### `actor.ActorPath`

```go
path := addr.WithRoot("user").Child("myActor")
fmt.Println(path) // → pekko://ClusterSystem@127.0.0.1:2552/user/myActor

// Common operations
path.Name()           // → "myActor"
path.Parent().String() // → pekko://ClusterSystem@127.0.0.1:2552/user
path.Depth()          // → 2

// Parse from URI string
path, err := actor.ParseActorPath("pekko://ClusterSystem@127.0.0.1:2552/user/myActor")
```

`ActorPath` is immutable — `Child` and `Parent` always return new values.

---

## Joining a Pekko Cluster from Go

### 1. Pekko configuration (Scala side)

```hocon
pekko {
  actor.provider = cluster
  remote.artery {
    transport = tcp
    canonical { hostname = "127.0.0.1"; port = 2552 }
  }
  cluster {
    seed-nodes = ["pekko://ClusterSystem@127.0.0.1:2552"]
    downing-provider-class = "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"
    split-brain-resolver {
      active-strategy = keep-oldest
      stable-after = 5s
    }
    failure-detector {
      acceptable-heartbeat-pause = 15s
      heartbeat-interval = 1s
    }
  }
}
```

> **Note on unreachability:** Pekko's `ClusterHeartbeatSender` uses `actorSelection` (serializer ID 6 — `MessageContainerSerializer`). Go cannot decode these envelopes, so Pekko will mark the Go node as UNREACHABLE after ~18 s. The `keep-oldest` SBR strategy automatically downs and removes the Go node when it becomes truly unreachable. Configure `stable-after` and `acceptable-heartbeat-pause` to suit your scenario.

### 2. Spawn and join

```go
self := actor.Address{Protocol: "pekko", System: "ClusterSystem", Host: "127.0.0.1", Port: 2553}

node, _ := gekka.Spawn(gekka.NodeConfig{Address: self})
defer node.Shutdown()

node.Join("127.0.0.1", 2552) // async — returns after sending InitJoin
```

You can also use flat fields when you don't need a typed address object:

```go
node, _ := gekka.Spawn(gekka.NodeConfig{
    SystemName: "ClusterSystem",
    Host:       "127.0.0.1",
    Port:       2553,
})
```

`Join` internally:
1. Sends `InitJoin` → receives `InitJoinAck` → sends `Join` → receives `Welcome`
2. Starts the background gossip loop (1 s tick)
3. Starts heartbeats to the seed node (1 s interval)

### 3. Wait for the connection to be established

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := node.WaitForHandshake(ctx, "127.0.0.1", 2552); err != nil {
    log.Fatal("handshake failed:", err)
}
// Association is now ASSOCIATED; you can send messages safely.
```

### 4. Inspect the node's own address

```go
self := node.SelfAddress() // actor.Address
fmt.Println(self)          // → pekko://ClusterSystem@127.0.0.1:2553

// Build actor paths relative to self
localPath := self.WithRoot("user").Child("worker")
```

### 5. Leave gracefully

```go
node.Leave() // broadcasts Leave to all known Up members; SBR removes the node
```

---

## Sending and Receiving Messages

### Sending

`Send` accepts a typed `actor.ActorPath`, a raw string URI, or any `fmt.Stringer`:

```go
seed := actor.Address{Protocol: "pekko", System: "ClusterSystem", Host: "127.0.0.1", Port: 2552}

// Raw bytes (Pekko ByteArraySerializer, ID 4)
node.Send(ctx, seed.WithRoot("user").Child("myActor"), []byte("ping"))

// Protobuf message (ID 2) — any proto.Message
node.Send(ctx, seed.WithRoot("user").Child("myActor"), &mypb.MyMessage{...})

// Plain string URI also works
node.Send(ctx, "pekko://ClusterSystem@127.0.0.1:2552/user/myActor", []byte("ping"))
```

Actor paths follow the format: `pekko://<system>@<host>:<port>/<path>`.

### Receiving

```go
node.OnMessage(func(ctx context.Context, msg *gekka.IncomingMessage) error {
    switch msg.SerializerId {
    case 4: // raw bytes
        fmt.Println("got bytes:", string(msg.Payload))
    case 2: // Protobuf — use your registry to decode
        obj, err := node.Serialization().DeserializePayload(msg.SerializerId, msg.Manifest, msg.Payload)
        _ = obj.(*mypb.MyMessage)
    }
    return nil
})
```

### Registering Custom Protobuf Types

```go
import "reflect"

reg := node.Serialization()
reg.RegisterManifest("com.example.MyMessage", reflect.TypeOf((*mypb.MyMessage)(nil)))

// Incoming frames with that manifest are now decoded automatically.
obj, _ := reg.DeserializePayload(2, "com.example.MyMessage", payload)
msg := obj.(*mypb.MyMessage)
```

---

## Cluster Singleton Proxy

Route messages to a Pekko `ClusterSingletonManager`-managed actor. The proxy automatically targets the **oldest Up** cluster member (matching Pekko's `keep-oldest` strategy).

```go
// Scala side: system.actorOf(ClusterSingletonManager.props(...), "singletonManager")
// Singleton actor lives at: pekko://System@host:port/user/singletonManager/singleton

proxy := node.SingletonProxy("/user/singletonManager", "") // "" = any role
path, _ := proxy.CurrentOldestPath() // resolve current singleton location
proxy.Send(ctx, []byte("work item"))  // re-resolves oldest node on every call
```

On each `Send`, the proxy re-queries the cluster membership to find the current oldest node, providing automatic handover resilience.

---

## Distributed Data (CRDTs)

Propagate replicated state between Go and Scala nodes using a custom JSON gossip protocol.

### Scala setup

Deploy a `GoReplicator` actor (see `scala-server/src/main/scala/com/example/DistributedDataServer.scala`) that listens at `/user/goReplicator` and understands the JSON gossip envelope:

```json
{"type": "gcounter-gossip", "key": "hits", "payload": {"state": {"node:2553": 7}}}
{"type": "orset-gossip",   "key": "tags", "payload": {"dots": {"go": [{"NodeID":"node:2553","Counter":1}]}, "vv": {"node:2553": 1}}}
```

### Go setup

```go
seed := actor.Address{Protocol: "pekko", System: "ClusterSystem", Host: "127.0.0.1", Port: 2552}

repl := node.Replicator()
repl.AddPeer(seed.WithRoot("user").Child("goReplicator").String())

// Pipe incoming JSON gossip to the replicator
node.OnMessage(func(ctx context.Context, msg *gekka.IncomingMessage) error {
    if len(msg.Payload) > 0 && msg.Payload[0] == '{' {
        return repl.HandleIncoming(msg.Payload)
    }
    return nil
})

node.Join("127.0.0.1", 2552)
node.WaitForHandshake(ctx, "127.0.0.1", 2552)

// G-Counter
repl.IncrementCounter("hits", 1, gekka.WriteLocal)
fmt.Println(repl.GCounter("hits").Value()) // 1

// OR-Set
repl.AddToSet("tags", "golang", gekka.WriteLocal)
repl.RemoveFromSet("tags", "java", gekka.WriteLocal)
fmt.Println(repl.ORSet("tags").Elements()) // ["golang"]

// Start periodic gossip (every 2 s by default)
repl.Start(ctx)
defer repl.Stop()
```

### Consistency levels

| Level | Behaviour |
|-------|-----------|
| `WriteLocal` | Update local state only; gossip propagates on the next tick |
| `WriteAll` | Immediately push to all registered peers, then continue gossiping |

### CRDT semantics

**G-Counter** — each node owns one slot; global value = sum of all slots; merge = pairwise max.

**OR-Set** — each Add is tagged with a unique (nodeID, counter) dot; Remove clears all observed dots for the element; merge preserves dots unknown to the remote side (concurrent adds survive).

---

## Multi-Provider Support

gekka supports both **Apache Pekko** (≥ 1.0) and **Lightbend Akka** clusters. The two systems share an identical Artery TCP wire format, serializer IDs, and cluster message manifests. The only difference is the actor-path scheme: `pekko://` vs `akka://`.

Set `Protocol` in `actor.Address` (or `NodeConfig.Provider`) when calling `Spawn`:

```go
// Apache Pekko (default)
self := actor.Address{Protocol: "pekko", System: "ClusterSystem", Host: "127.0.0.1", Port: 2553}
node, err := gekka.Spawn(gekka.NodeConfig{Address: self})

// Lightbend Akka — just change the Protocol field
self := actor.Address{Protocol: "akka", System: "ClusterSystem", Host: "127.0.0.1", Port: 2553}
node, err := gekka.Spawn(gekka.NodeConfig{Address: self})
```

Alternatively, use the flat `Provider` field:

```go
node, err := gekka.Spawn(gekka.NodeConfig{
    SystemName: "ClusterSystem",
    Host:       "127.0.0.1",
    Port:       2553,
    Provider:   gekka.ProviderAkka,
})
```

Everything downstream adjusts automatically — no other code changes are required:

| What changes | ProviderPekko | ProviderAkka |
|---|---|---|
| `actor.Address.Protocol` | `"pekko"` | `"akka"` |
| Cluster core daemon path | `pekko://Sys@host:port/system/cluster/core/daemon` | `akka://Sys@host:port/system/cluster/core/daemon` |
| Heartbeat receiver path | `pekko://Sys@host:port/system/cluster/heartbeatReceiver` | `akka://Sys@host:port/system/cluster/heartbeatReceiver` |
| Singleton proxy path | `pekko://Sys@host:port/user/manager/singleton` | `akka://Sys@host:port/user/manager/singleton` |
| CRDT replicator peer path | user-supplied (pass `akka://…` to `AddPeer`) | same |

> **Wire format note:** The Artery TCP framing (4-byte LE length prefix, `AKKA` magic, stream IDs) and all serializer IDs (2, 4, 5, 17 …) are **identical** between Pekko and Akka. The provider setting is purely a path-string configuration; no protocol bytes change.

### Akka cluster configuration (application.conf)

```hocon
akka {
  actor.provider = cluster
  remote.artery {
    transport = tcp
    canonical { hostname = "127.0.0.1"; port = 2552 }
  }
  cluster {
    seed-nodes = ["akka://ClusterSystem@127.0.0.1:2552"]
    downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
    split-brain-resolver { active-strategy = keep-oldest; stable-after = 5s }
    failure-detector {
      acceptable-heartbeat-pause = 15s
      heartbeat-interval = 1s
    }
  }
}
```

### Joining an Akka cluster

```go
self := actor.Address{Protocol: "akka", System: "ClusterSystem", Host: "127.0.0.1", Port: 2553}
node, _ := gekka.Spawn(gekka.NodeConfig{Address: self})
defer node.Shutdown()

node.OnMessage(func(ctx context.Context, msg *gekka.IncomingMessage) error {
    fmt.Printf("got: %s\n", msg.Payload)
    return nil
})

node.Join("127.0.0.1", 2552)
node.WaitForHandshake(ctx, "127.0.0.1", 2552)

seed := actor.Address{Protocol: "akka", System: "ClusterSystem", Host: "127.0.0.1", Port: 2552}
node.Send(ctx, seed.WithRoot("user").Child("myActor"), []byte("hello from Go"))
```

---

## API Reference

### Top-level functions

| Function | Description |
|----------|-------------|
| `Spawn(cfg NodeConfig) (*GekkaNode, error)` | Create and start a node from a `NodeConfig` |
| `SpawnFromConfig(path string, fallbacks ...string) (*GekkaNode, error)` | Load HOCON file and start a node |
| `LoadConfig(path string, fallbacks ...string) (NodeConfig, error)` | Parse HOCON into `NodeConfig` without spawning |
| `ParseHOCONString(text string) (NodeConfig, error)` | Parse an in-memory HOCON string |

### `NodeConfig`

```go
type NodeConfig struct {
    // Address sets the node's own address using the typed actor.Address.
    // When non-zero it overrides SystemName, Host, Port, and Provider.
    Address actor.Address

    SystemName string         // actor system name (default: "GekkaSystem")
    Host       string         // bind address (default: "127.0.0.1")
    Port       uint32         // 0 = OS-assigned; read actual port with node.Addr()
    Provider   Provider       // ProviderPekko (default) or ProviderAkka
    SeedNodes  []actor.Address // populated by LoadConfig; used by JoinSeeds()
}
```

### `GekkaNode` methods

| Method | Description |
|--------|-------------|
| `Addr() net.Addr` | Bound TCP address |
| `SelfAddress() actor.Address` | Node's own address as a typed value |
| `Join(host, port)` | Send InitJoin, start heartbeat + gossip loop |
| `JoinSeeds() error` | Join the first non-self seed from `NodeConfig.SeedNodes` |
| `Seeds() []actor.Address` | Seed nodes parsed from HOCON config |
| `Leave() error` | Broadcast Leave to cluster members |
| `Send(ctx, dst, msg)` | Deliver a message; dst can be `actor.ActorPath`, `string`, or `fmt.Stringer` |
| `OnMessage(fn)` | Register user-message callback |
| `WaitForHandshake(ctx, host, port)` | Block until Artery handshake completes |
| `SingletonProxy(path, role)` | Return a ClusterSingletonProxy |
| `Replicator() *Replicator` | Return the CRDT replicator |
| `Serialization()` | Return the SerializationRegistry |
| `StopHeartbeat()` | Pause heartbeats to seed (simulate failure) |
| `StartHeartbeat()` | Resume heartbeats to seed |
| `Shutdown() error` | Stop server and cancel goroutines |

---

## Integration Tests

The integration tests require a running Scala sbt project under `../scala-server`.

```bash
# Run a specific test (each starts its own sbt process)
cd gekka
go test -v -run TestIntegration_PekkoServer    -timeout 5m
go test -v -run TestClusterSingletonProxy      -timeout 5m
go test -v -run TestClusterJoinLeave           -timeout 5m
go test -v -run TestDistributedData            -timeout 5m

# Unit tests only (no Scala required)
go test -v -run "^(TestGCounter|TestORSet|TestReplicator|TestCluster_JoinHandshake|TestVectorClockComparison|TestCheckConvergence|TestClusterRouter)" -timeout 30s
```

---

## Project Layout

```
gekka/
├── LICENSE
├── README.md
├── go.mod
└── gekka/
    ├── version.go                    ← const Version = "0.1.0"
    ├── gekka_node.go                 ← top-level API (Spawn, SpawnFromConfig, Join, Send, …)
    ├── hocon_config.go               ← HOCON loader (LoadConfig, ParseHOCONString)
    ├── association.go                ← per-connection state machine
    ├── tcp_artery_handler.go         ← Artery framing / dispatch
    ├── router.go                     ← actor-path resolution + serializer selection
    ├── cluster_manager.go            ← gossip, heartbeat, membership
    ├── cluster_singleton_proxy.go    ← oldest-node routing
    ├── replicator.go                 ← CRDT gossip engine
    ├── gcounter.go                   ← Grow-only Counter CRDT
    ├── orset.go                      ← Observed-Remove Set CRDT
    ├── serialization_registry.go     ← Protobuf + JSON serializers
    ├── integration_test.go           ← end-to-end tests vs. Scala
    ├── testdata/
    │   ├── pekko.conf                ← sample Pekko application.conf
    │   ├── akka.conf                 ← sample Akka application.conf
    │   └── reference.conf            ← sample fallback defaults
    ├── actor/
    │   └── actor.go                  ← actor.Address and actor.ActorPath types
    └── cluster/
        └── ClusterMessages.pb.go     ← generated Protobuf types (Apache 2.0)
scala-server/
└── src/main/scala/com/example/
    ├── PekkoServer.scala             ← echo actor for TestIntegration_PekkoServer
    ├── ClusterSeedNode.scala         ← seed node for TestClusterJoinLeave
    ├── ClusterSingletonServer.scala  ← singleton for TestClusterSingletonProxy
    └── DistributedDataServer.scala   ← GoReplicator for TestDistributedData
```

## Protocol Notes

| Constant | Value | Meaning |
|----------|-------|---------|
| Frame length prefix | 4-byte **little-endian** | |
| Connection magic | `[A,K,K,A,streamId]` | 5 bytes; outbound sender writes this |
| StreamId 1 | Control stream | |
| StreamId 2 | Ordinary messages | |
| Serializer ID 2 | ProtobufSerializer | |
| Serializer ID 4 | ByteArraySerializer | raw `[]byte`, empty manifest |
| Serializer ID 5 | ClusterMessageSerializer | short manifests: `"IJ"`, `"W"`, `"GE"`, … |
| Serializer ID 17 | ArteryMessageSerializer | handshake frames |
| Welcome payload | GZIP-compressed | |
| GossipEnvelope.serializedGossip | GZIP-compressed | |

---

## License

This project is licensed under the **MIT License** — see [LICENSE](LICENSE) for the full text.

This library is a Go implementation that interacts with and references the [Apache Pekko](https://pekko.apache.org/) project.

Specific files, such as `gekka/cluster/ClusterMessages.pb.go`, are generated from Protobuf definitions provided by the Apache Pekko project and retain their original **Apache License 2.0** and copyright notices from the **Apache Software Foundation** and **Lightbend Inc.** Those notices are reproduced in full within the respective files, as required by the Apache License 2.0.
