# Gekka API Reference

This document provides a comprehensive reference for the Gekka API.

## Top-level functions

| Function | Description |
|----------|-------------|
| `NewCluster(cfg ClusterConfig) (*Cluster, error)` | Create and start a node from a `ClusterConfig` |
| `NewClusterFromConfig(path string, fallbacks ...string) (*Cluster, error)` | Load HOCON file and start a node |
| `LoadConfig(path string, fallbacks ...string) (ClusterConfig, error)` | Parse HOCON into `ClusterConfig` without spawning |
| `ParseHOCONString(text string) (ClusterConfig, error)` | Parse an in-memory HOCON string |

## ClusterConfig

```go
type ClusterConfig struct {
    // Address sets the node's own address using the typed actor.Address.
    // When non-zero it overrides SystemName, Host, Port, and Provider.
    Address actor.Address

    SystemName string         // actor system name (default: "GekkaSystem")
    Host       string         // bind address (default: "127.0.0.1")
    Port       uint32         // 0 = OS-assigned; read actual port with cluster.Addr()
    Provider   Provider       // ProviderPekko (default) or ProviderAkka
    SeedNodes  []actor.Address // populated by LoadConfig; used by JoinSeeds()
}
```

## ActorSystem and Props

`cluster.System` is of type `ActorSystem`. Use it to create and register actors:

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

`Context()` returns the node's root context — cancelled when `cluster.Shutdown()` is called.

## ActorRef

A location-transparent reference to an actor (local or remote):

| Method | Description |
|--------|-------------|
| `ref.Tell(msg any)` | Fire-and-forget delivery; local skips serialization; remote uses Artery |
| `ref.Ask(ctx, msg) (any, error)` | Request-reply; blocks until response or ctx cancelled |
| `ref.Path() string` | Full actor-path URI |
| `ref.String() string` | Same as `Path()` — implements `fmt.Stringer` |

## ActorSelection

Lazy handle for actor discovery:

```go
// Resolve by local path suffix — nil means use the node's own context
ref, err := node.ActorSelection("/user/greeter").Resolve(nil)

// Resolve by absolute remote URI (no network call; connection is lazy)
ref, err := node.ActorSelection("pekko://Sys@10.0.0.1:2552/user/greeter").Resolve(nil)
```

## Cluster methods

| Method | Description |
|--------|-------------|
| `System` | `ActorSystem` — create actors with `System.ActorOf(props, name)` |
| `Addr() net.Addr` | Bound TCP address |
| `Port() uint32` | Bound TCP port |
| `SelfAddress() actor.Address` | Node's own address as a typed value |
| `Join(host, port)` | Send InitJoin, start heartbeat + gossip loop |
| `JoinSeeds() error` | Join the first non-self seed from `ClusterConfig.SeedNodes` |
| `Seeds() []actor.Address` | Seed nodes parsed from HOCON config |
| `Leave() error` | Broadcast Leave to cluster members |
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
