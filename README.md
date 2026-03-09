# gekka &nbsp;[![Version](https://img.shields.io/badge/version-0.3.0-blue)](https://github.com/sopranoworks/gekka) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE) [![Go CI](https://github.com/sopranoworks/gekka/actions/workflows/go.yml/badge.svg)](https://github.com/sopranoworks/gekka/actions/workflows/go.yml)

**Version 0.3.0**
`gekka` is a distributed actor model library for Go, engineered for seamless interoperability with [Apache Pekko](https://pekko.apache.org/) via the Artery TCP protocol. Moving beyond simple messaging, it provides a robust **Hierarchical Actor System**, **Self-Healing Supervision**, and true **Location Transparency**.
Powered by its own high-performance HOCON engine, [`gekka-config`](https://github.com/sopranoworks/gekka-config), `gekka` supports both automatic cluster formation and direct node-to-node communication using the standard `pekko://` URI scheme.

## Key Features

- **Hierarchical Actor System** — Create actors using `node.System.ActorOf(Props{...}, "name")`. `gekka` enforces the `/user/` namespace and manages parent-child relationships, ensuring reliable actor lifecycle management.
- **Self-Healing Supervision** — Implement robust fault tolerance with `OneForOneStrategy`. Supported directives include `Restart`, `Resume`, `Stop`, and `Escalate` to automatically handle child actor failures.
- **Pekko Remote & Cluster Compatibility** — Seamlessly communicate with Scala/Java actors. Supports the standard `pekko://` URI scheme for direct node-to-node addressing via `ActorSelection`.
- **Location Transparency** — `ActorRef.Tell(msg)` and `ActorRef.Ask(ctx, msg)` work identically for local and remote actors, abstracting away the network layer.
- **Location Transparent Senders** — Access the originator of any message using `a.Sender()`. Reply directly with `a.Sender().Tell(reply, a.Self())` without manual address tracking.
- **Extensible Serialization** — Built-in support for **Protobuf** (ID 2), **Raw Bytes** (ID 4), and **JSON** (Jackson-compatible, ID 9) via `gekka-config`. Easily register custom serializers for domain-specific types.
- **Actor-aware Logging** — Integrated structured logging (via `log/slog`) that automatically contextualizes entries with actor paths, system names, and current sender information.
- **High-Performance Remoting** — Binary-compatible Artery TCP implementation featuring handshake authentication, sequence management, and transport-level heartbeats.
- **Observability** — Built-in monitoring server providing health checks (`/healthz`) and live metrics (`/metrics`) in JSON and Prometheus formats with zero external dependencies.

---

## Quick Start

```go
import (
    "context"
    "log"

    "gekka/gekka"
    "gekka/gekka/actor"
)

// ── 1. Define your actor ────────────────────────────────────────────────────

type EchoActor struct {
    actor.BaseActor // provides Mailbox(), Log(), Sender(), Self()
}

func (a *EchoActor) Receive(msg any) {
    a.Log().Info("Received message", "sender", a.Sender().Path())

    if s, ok := msg.(string); ok {
        a.Log().Info("Responding to Ping", "payload", s)
        a.Sender().Tell("Hello from Go!", a.Self())
    }
}

// ── 2. Spawn the node ────────────────────────────────────────────────────────

node, err := gekka.Spawn(gekka.NodeConfig{
    SystemName: "ClusterSystem",
    Host:       "127.0.0.1",
    Port:       2553,
})
if err != nil {
    log.Fatal(err)
}
defer node.Shutdown()

// ── 3. Resolve the remote actor via ActorSelection ───────────────────────────
//
// Resolve(nil) falls back to node.Context() — the node's own root context,
// cancelled when node.Shutdown() is called. Pass a context with a deadline
// only when you need per-call cancellation during resolution.

remoteRef, err := node.ActorSelection("pekko://ClusterSystem@127.0.0.1:2552/user/echo").
    Resolve(nil) // nil → node's own context; safe for long-lived refs
if err != nil {
    log.Fatal(err)
}

// ── 4. Create a local actor with node.System.ActorOf ─────────────────────────
//
// Props.New is a factory closure — dependencies are captured by value.
// ActorOf starts the goroutine, registers at /user/echo, and returns an ActorRef.

ref, err := node.System.ActorOf(gekka.Props{
    New: func() actor.Actor {
        return &EchoActor{
            BaseActor: actor.NewBaseActor(),
        }
    },
}, "echo") // registered as /user/echo
if err != nil {
    log.Fatal(err)
}
log.Printf("local actor at %s", ref)

// ── 5. Join a Pekko cluster seed node ────────────────────────────────────────

if err := node.Join("127.0.0.1", 2552); err != nil {
    log.Fatal(err)
}

// ── 6. Subscribe to cluster events ───────────────────────────────────────────

watcherProps := gekka.Props{
    New: func() actor.Actor { return &MyClusterWatcher{BaseActor: actor.NewBaseActor()} },
}
watcherRef, _ := node.System.ActorOf(watcherProps, "watcher")

node.Subscribe(watcherRef)
defer node.Unsubscribe(watcherRef)

// ── 7. Send messages — Tell (fire-and-forget) and Ask (request-reply) ────────

remoteRef.Tell([]byte("Ping"))

ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
reply, err := remoteRef.Ask(ctx, []byte("Ping"))
if b, ok := reply.([]byte); ok {
    log.Printf("Ask reply: %s", b) // → "Pong"
}
```

---

## Configuration via HOCON

`gekka` uses the `gekka-config` library to parse and unmarshal HOCON files. If you already have a Pekko or Akka `application.conf`, you can load it directly instead of constructing `NodeConfig` manually. `gekka` reads the same HOCON paths that Pekko uses.

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

> **Note on failure detection:** Pekko's `ClusterHeartbeatSender` (Serializer 5, manifest `"HB"`) runs on 1 s intervals. Configure `acceptable-heartbeat-pause` in your HOCON to give Go nodes sufficient headroom. gekka handles these automatically via `ClusterManager.StartHeartbeat`.

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

## Channel-based Actor Model

gekka provides a lightweight, idiomatic Go actor model built on channels. Each actor lives in its own goroutine and processes messages sequentially — no mutexes required inside `Receive`.

### Defining an actor

Embed `actor.BaseActor` to get the mailbox channel for free, then implement `Receive`:

```go
type GreetActor struct {
    actor.BaseActor
    remoteRef gekka.ActorRef
}

func (a *GreetActor) Receive(msg any) {
    incoming, ok := msg.(*gekka.IncomingMessage)
    if !ok {
        return
    }
    switch string(incoming.Payload) {
    case "hello":
        log.Println("<- hello received")
        a.remoteRef.Tell([]byte("world"))
    default:
        log.Printf("<- unknown: %s", incoming.Payload)
    }
}
```

### Creating actors with node.System.ActorOf (recommended)

`ActorSystem` is the single entry point for actor lifecycle management.
`node.System.ActorOf` creates the actor, starts its goroutine, registers it at `/user/<name>`, and returns a location-transparent `ActorRef` — mirroring Pekko's `system.actorOf(Props(...), "name")`:

```go
ref, err := node.System.ActorOf(gekka.Props{
    New: func() actor.Actor {
        // Dependencies are captured by the closure.
        return &GreetActor{
            BaseActor:  actor.NewBaseActor(),
            remoteRef:  remoteRef, // ActorRef to the remote peer
        }
    },
}, "greeter") // registers at /user/greeter
if err != nil {
    log.Fatal(err)
}

// ref is an ActorRef pointing to the new local actor.
ref.Tell("hello from the outside")
```

`Props.New` is called exactly once. Any configuration or external dependencies the actor needs should be injected through the closure:

```go
// Custom mailbox size — pass it in via the closure
ref, _ := node.System.ActorOf(gekka.Props{
    New: func() actor.Actor {
        return &MyActor{BaseActor: actor.NewBaseActorWithSize(1024)}
    },
}, "high-throughput-actor")
```

### Low-level registration (when you need manual control)

Use `RegisterActor` directly when you need to start the goroutine and register the actor separately:

```go
a := &GreetActor{BaseActor: actor.NewBaseActor()}

// Start the receive goroutine explicitly
actor.Start(a)

// Map Artery recipient path → actor
node.RegisterActor("/user/greeter", a)

// Remove the mapping later if needed
node.UnregisterActor("/user/greeter")
```

Incoming Artery frames whose recipient path matches `/user/greeter` are automatically pushed into the actor's mailbox as `*gekka.IncomingMessage`. Unmatched messages fall through to the `OnMessage` callback (if set).

### Custom mailbox size

```go
// Actor with a larger buffer — useful under high throughput
a := &MyActor{BaseActor: actor.NewBaseActorWithSize(1024)}
```

If the mailbox is full, the message is silently dropped (equivalent to Pekko's dead-letter behaviour).

> **Zero-value safety** — If you construct the struct with a literal (`&MyActor{}` without calling `NewBaseActor()`), `actor.Start` calls `initMailbox()` automatically before launching the goroutine.

---

## Reactive Cluster Events

gekka delivers cluster membership changes natively into the Actor System as a push-based stream of typed values — no polling required.
When the subscriber actor is stopped, it is unsubscribed automatically.

### Subscribing

```go
type MyEventsWatcher struct {
    actor.BaseActor
}

func (w *MyEventsWatcher) Receive(msg any) {
    if evt, ok := msg.(gekka.ClusterDomainEvent); ok {
        switch e := evt.(type) {
        case gekka.MemberUp:
            log.Printf("[up]          %s", e.Member)
        case gekka.MemberLeft:
            log.Printf("[left]        %s", e.Member)
        case gekka.MemberExited:
            log.Printf("[exited]      %s", e.Member)
        case gekka.MemberRemoved:
            log.Printf("[removed]     %s", e.Member)
        case gekka.UnreachableMember:
            log.Printf("[unreachable] %s", e.Member)
        case gekka.ReachableMember:
            log.Printf("[reachable]   %s", e.Member)
        }
    }
}()

defer node.Unsubscribe(events)
```

Events are dropped (not queued) when the channel buffer is full — size it for your slowest consumer.

### Available event types

| Go type | Pekko equivalent | When fired |
|---------|-----------------|------------|
| `MemberUp` | `MemberUp` | Node transitions to Up |
| `MemberLeft` | `MemberLeft` | Node voluntarily leaves |
| `MemberExited` | `MemberExited` | Node finished leaving |
| `MemberRemoved` | `MemberRemoved` | Node fully removed |
| `UnreachableMember` | `UnreachableMember` | Failure detector triggers |
| `ReachableMember` | `ReachableMember` | Connectivity restored |

---

## Built-in Observability

Enable the built-in HTTP server by setting `MonitoringPort` in `NodeConfig`:

```go
node, _ := gekka.Spawn(gekka.NodeConfig{
    SystemName:    "ClusterSystem",
    Host:          "127.0.0.1",
    Port:          2553,
    MonitoringPort: 8080,
})
```

Or via `SpawnFromConfig` when `monitoring.port` is set in `application.conf`.

### Endpoints

| Endpoint | Method | Description |
|----------|--------|--------------|
| `/healthz` | GET | `200 OK` when joined (`Up`); `503 Service Unavailable` otherwise |
| `/metrics` | GET | JSON snapshot of internal counters |
| `/metrics?fmt=prom` | GET | Prometheus text exposition format |

### Sample JSON metrics

```json
{
  "messages_sent":     142,
  "messages_received": 139,
  "gossips_received":   27,
  "heartbeats_sent":    60,
  "associations":        2
}
```

No external dependencies are required; the server binds only when `MonitoringPort > 0`.

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

### Receiving via a registered Actor (recommended)

See [Channel-based Actor Model](#channel-based-actor-model) above.

### Receiving via OnMessage callback

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

> **Priority:** registered actors take precedence. `OnMessage` only fires when no actor is registered for the recipient path.

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

### `ActorSystem` and `Props`

`node.System` is of type `ActorSystem`. Use it to create and register actors:

```go
// Props lives in the actor package so actors can reference it without an
// import cycle. gekka.Props is a type alias for actor.Props.
type Props = actor.Props // New func() actor.Actor

type ActorSystem interface {
    ActorOf(props Props, name string) (ActorRef, error)
    Context() context.Context // root context of the node
    Watch(watcher, target ActorRef)
    Unwatch(watcher, target ActorRef)
    Stop(target ActorRef)
}
```

`ActorOf` registers the actor at `/user/<name>`, starts its goroutine, and returns an `ActorRef`.

`Context()` returns the node's root context — cancelled when `node.Shutdown()` is called. Use it as the parent for any background goroutines that should stop with the node:

```go
go doWork(node.System.Context())
```

### Accessing `System` and `Context` from inside an Actor

Every actor that embeds `BaseActor` automatically receives the `ActorContext`
after `SpawnActor` / `ActorOf` returns. Access it through `a.System()`:

```go
type WorkerActor struct {
    actor.BaseActor
}

func (a *WorkerActor) Receive(msg any) {
    switch msg.(type) {
    case StartChild:
        // Spawn a peer actor from inside Receive — no external reference needed.
        ref, err := a.System().ActorOf(actor.Props{
            New: func() actor.Actor {
                return &ChildActor{BaseActor: actor.NewBaseActor()}
            },
        }, "child")
        if err != nil {
            a.Log().Error("spawn child failed", "err", err)
            return
        }
        ref.Tell("hello from parent")

    case StartWork:
        // Tie a background goroutine to the node's lifecycle so it stops
        // automatically when the node shuts down.
        go doWork(a.System().Context())
    }
}
```

`a.System()` returns `actor.ActorContext`, the subset of `ActorSystem` that is
safe to use from the `actor` package without introducing an import cycle:

```go
type ActorContext interface {
    ActorOf(props Props, name string) (Ref, error) // Ref instead of ActorRef
    Context() context.Context
}
```

### `ActorRef`

A location-transparent reference to an actor (local or remote):

| Method | Description |
|--------|-------------|
| `ref.Tell(msg any)` | Fire-and-forget delivery; local skips serialization; remote uses Artery |
| `ref.Ask(ctx, msg) (any, error)` | Request-reply; blocks until response or ctx cancelled |
| `ref.Path() string` | Full actor-path URI |
| `ref.String() string` | Same as `Path()` — implements `fmt.Stringer` |

### `ActorSelection`

Lazy handle for actor discovery:

```go
// Resolve by local path suffix — nil means use the node's own context
ref, err := node.ActorSelection("/user/greeter").Resolve(nil)

// Resolve by absolute remote URI (no network call; connection is lazy)
// nil is idiomatic here too — the ref lives as long as the node
ref, err := node.ActorSelection("pekko://Sys@10.0.0.1:2552/user/greeter").Resolve(nil)

// When you do need per-call cancellation, pass a real context:
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
ref, err = node.ActorSelection("/user/greeter").Resolve(ctx)

// Shortcut: Tell without resolving first
node.ActorSelection("/user/greeter").Tell([]byte("ping"))
```

### `GekkaNode` methods

| Method | Description |
|--------|-------------|
| `System` | `ActorSystem` — create actors with `System.ActorOf(props, name)` |
| `Addr() net.Addr` | Bound TCP address |
| `Port() uint32` | Bound TCP port |
| `SelfAddress() actor.Address` | Node's own address as a typed value |
| `Join(host, port)` | Send InitJoin, start heartbeat + gossip loop |
| `JoinSeeds() error` | Join the first non-self seed from `NodeConfig.SeedNodes` |
| `Seeds() []actor.Address` | Seed nodes parsed from HOCON config |
| `Leave() error` | Broadcast Leave to cluster members |
| `JoinSeeds()` | Trigger the multi-node `Join` cluster initiation using seed-nodes specified in HOCON config |
| `Leave()` | Gracefully detach this node from the cluster and trigger node exit |
| `Send(ctx, dst, msg)` | Deliver a message; dst can be `actor.ActorPath`, `string`, or `fmt.Stringer` |
| `Ask(ctx, dst, msg)` | Request-response; blocks until reply or ctx cancellation |
| `ActorSelection(path) ActorSelection` | Lazy handle for local or remote actor discovery |
| `SpawnActor(path, a) ActorRef` | Start + register actor at a full path suffix; returns ActorRef |
| `RegisterActor(path, a)` | Bind an Actor to an Artery recipient path |
| `UnregisterActor(path)` | Remove an actor binding |
| `OnMessage(fn)` | Register a fallback user-message callback |
| `Subscribe(ref, types...)` | Receive cluster domain events directly into a subscriber actor |
| `Unsubscribe(ref)` | Stop receiving events on the given actor |
| `WaitForHandshake(ctx, host, port)` | Block until Artery handshake completes |
| `SingletonProxy(path, role)` | Return a ClusterSingletonProxy |
| `Replicator() *Replicator` | Return the CRDT replicator |
| `Serialization()` | Return the SerializationRegistry |
| `Metrics() MetricsSnapshot` | Point-in-time snapshot of internal counters |
| `MonitoringAddr() net.Addr` | Address of the built-in HTTP monitoring server (nil if disabled) |
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

## Examples

Three self-contained examples live in the `examples/` directory.
Each has its own `application.conf` and `README.md` with step-by-step
setup instructions.

| Example | Directory | Key APIs demonstrated |
|---------|-----------|----------------------|
| **Basic Ping-Pong** | [`examples/basic/`](examples/basic/) | `SpawnFromConfig`, `node.System.ActorOf`, `ActorSelection`, `ActorRef.Tell`, `ActorRef.Ask`, `JoinSeeds`, `Subscribe`, `Leave` |
| **Distributed Worker** | [`examples/distributed-worker/`](examples/distributed-worker/) | `node.System.ActorOf`, `SingletonProxy`, automatic failover, JSON job dispatch |
| **CRDT Dashboard** | [`examples/crdt-dashboard/`](examples/crdt-dashboard/) | `Replicator`, `GCounter`, `ORSet`, live terminal dashboard |
| **Ask Pattern** | [`examples/ask-pattern/`](examples/ask-pattern/) | `Ask` request-response, HTTP frontend, context timeout |
| **Monitoring** | [`examples/monitoring/`](examples/monitoring/) | `MonitoringPort`, `/healthz`, `/metrics` endpoints |

### Quick run (after starting a Pekko seed at :2552)

```bash
# Basic Ping-Pong — joins cluster, sends "Ping" every 3 s, receives "Echo: …"
go run ./examples/basic

# Distributed Worker — dispatches JSON jobs to a ClusterSingleton every 2 s
go run ./examples/distributed-worker

# CRDT Dashboard — live terminal showing GCounter + ORSet convergence
go run ./examples/crdt-dashboard

# Ask Pattern — HTTP server; curl http://localhost:8080/ask?msg=hello
go run ./examples/ask-pattern
```

---

## Project Layout

```
gekka/
├── LICENSE
├── README.md
├── go.mod
└── gekka/
    ├── version.go                    ← const Version = "0.3.0"
    ├── gekka_node.go                 ← top-level API (Spawn, Join, RegisterActor, Subscribe, …)
    ├── actor_system.go               ← ActorSystem interface, Props, nodeActorSystem
    ├── actor_ref.go                  ← ActorRef, ActorSelection, SpawnActor
    ├── hocon_config.go               ← HOCON loader (LoadConfig, ParseHOCONString)
    ├── association.go                ← per-connection state machine
    ├── tcp_artery_handler.go         ← Artery framing / dispatch
    ├── router.go                     ← actor-path resolution + serializer selection
    ├── cluster_manager.go            ← gossip, heartbeat, membership
    ├── cluster_singleton_proxy.go    ← oldest-node routing
    ├── monitoring_server.go          ← /healthz and /metrics HTTP server
    ├── replicator.go                 ← CRDT gossip engine
    ├── gcounter.go                   ← Grow-only Counter CRDT
    ├── orset.go                      ← Observed-Remove Set CRDT
    ├── serialization_registry.go     ← Protobuf + custom serializers
    ├── integration_test.go           ← end-to-end tests vs. Scala
    ├── testdata/
    │   ├── pekko.conf                ← sample Pekko application.conf
    │   ├── akka.conf                 ← sample Akka application.conf
    │   └── reference.conf            ← sample fallback defaults
    ├── actor/
    │   ├── actor.go                  ← actor.Address and actor.ActorPath types
    │   └── base_actor.go             ← Actor interface, BaseActor, Start()
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

---

## Serialization

gekka uses a flexible `SerializationRegistry` to map Artery serializer IDs and manifest strings to Go types.

### Registering Custom Types (JSON)

By default, any struct sent via `Tell` is serialized as JSON (ID 9) using its reflect type name as the manifest, powered by the [`gekka-config`](https://github.com/sopranoworks/gekka-config) engine for Jackson compatibility. To receive these messages, the remote node must register the manifest:

```go
import "reflect"

// Register the manifest "com.example.OrderPlaced" to a Go struct
node.Serialization().RegisterManifest("com.example.OrderPlaced", reflect.TypeOf(OrderPlaced{}))
```

### Custom Serializers

You can implement the `Serializer` interface to support any wire format (e.g., Jackson, Kryo, Avro):

```go
type MySerializer struct {}

func (s *MySerializer) Identifier() int32 { return 101 } // Choice of ID >= 100
func (s *MySerializer) ToBinary(obj any) ([]byte, error) { /* ... */ }
func (s *MySerializer) FromBinary(data []byte, manifest string) (any, error) { /* ... */ }

// Register the serializer with the node
node.Serialization().RegisterSerializer(101, &MySerializer{})
```

---

## License

This project is licensed under the **MIT License** — see [LICENSE](LICENSE) for the full text.

This library is a Go implementation that interacts with and references the [Apache Pekko](https://pekko.apache.org/) project.

Specific files, such as `gekka/cluster/ClusterMessages.pb.go`, are generated from Protobuf definitions provided by the Apache Pekko project and retain their original **Apache License 2.0** and copyright notices from the **Apache Software Foundation** and **Lightbend Inc.** Those notices are reproduced in full within the respective files, as required by the Apache License 2.0.
