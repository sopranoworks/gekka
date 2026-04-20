# Gekka API Reference (v1.0.0-rc2)

This document provides a comprehensive reference for the Gekka public API.

---

## Top-level Functions

| Function | Description |
|----------|-------------|
| `NewCluster(cfg ClusterConfig) (*Cluster, error)` | Create and start a node from a `ClusterConfig` |
| `NewClusterFromConfig(path string, fallbacks ...string)` | Load HOCON file and start a node |
| `NewActorSystemWithBehavior[T](behavior, name, config)` | Create system with a root behavior |
| `Spawn[T, S Spawner](spawner, behavior, name)` | Spawn a type-safe actor (integrated API) |
| `SpawnPersistent[C,E,S](sys, behavior, name, props...) (TypedActorRef[C], error)` | Spawn a persistent (event-sourced) actor |
| `SpawnDurableState[C,S](sys, behavior, name, props...) (TypedActorRef[C], error)` | Spawn a state-persistent actor |
| `Ask[T,R](ctx, target, timeout, msgFactory)` | Type-safe request-response |
| `StartSharding[C,E,S](sys, typeName, behaviorFactory, extract, settings)` | Start cluster sharding for an entity type |
| `EntityRefFor[T](sys, typeName, entityId)` | Obtain a typed ref to a specific sharded entity |
| `NewTypedSingleton[M](cm, behavior, role)` | Create a typed cluster singleton |
| `NewTypedSingletonProxy[M](cm, managerPath, role)` | Create a typed singleton proxy |

---

## ClusterConfig

```go
type ClusterConfig struct {
    // Address sets the node's own address using the typed actor.Address.
    // When non-zero it overrides SystemName, Host, Port, and Provider.
    Address actor.Address

    SystemName string          // actor system name (default: "GekkaSystem")
    Host       string          // bind address (default: "127.0.0.1")
    Port       uint32          // 0 = OS-assigned; read actual port with cluster.Addr()
    Provider   Provider        // ProviderPekko (default) or ProviderAkka
    SeedNodes  []actor.Address // populated by LoadConfig; used by JoinSeeds()

    EnableMonitoring bool // enable /healthz and /metrics HTTP endpoints
    MonitoringPort   int  // 0 = OS-assigned
}
```

---

## ActorSystem and Props

`cluster.System` is of type `ActorSystem`. Use it to create and register actors:

```go
// Props lives in the actor package so actors can reference it without an
// import cycle. gekka.Props is a type alias for actor.Props.
type Props = actor.Props // New func() actor.Actor

type ActorSystem interface {
    ActorOf(props Props, name string) (ActorRef, error)
    Spawn(behavior any, name string) (ActorRef, error)
    SpawnAnonymous(behavior any) (ActorRef, error)
    SystemActorOf(behavior any, name string) (ActorRef, error)
    Context() context.Context // root context of the node
    Watch(watcher, target ActorRef)
    Unwatch(watcher, target ActorRef)
    Stop(target ActorRef)
    Serialization() *SerializationRegistry
    RegisterType(manifest string, typ reflect.Type)
    GetTypeByManifest(manifest string) (reflect.Type, bool)
    ActorSelection(path string) ActorSelection
    Receptionist() TypedActorRef[any]
}

type ActorContext interface {
    ActorOf(props Props, name string) (Ref, error)
    Spawn(behavior any, name string) (Ref, error)
    SpawnAnonymous(behavior any) (Ref, error)
    SystemActorOf(behavior any, name string) (Ref, error)
    Context() context.Context
    Watch(watcher, target Ref)
    Unwatch(watcher, target Ref)
    Stop(target Ref)
    Self() Ref
    Parent() Ref
    Sender() Ref
}
```

`ActorOf` registers the actor at `/user/<name>`, starts its goroutine, and
returns an `ActorRef`. When the actor path matches a HOCON deployment entry,
the system transparently provisions the appropriate router instead of a plain
actor.

---

## ActorRef

A location-transparent reference to an actor (local or remote):

| Method | Description |
|--------|-------------|
| `ref.Tell(msg any, sender ...actor.Ref)` | Fire-and-forget delivery; local skips serialization; remote uses Artery |
| `ref.Ask(ctx, msg) (any, error)` | Request-reply; blocks until response or ctx cancelled |
| `ref.Path() string` | Full actor-path URI |
| `ref.String() string` | Same as `Path()` — implements `fmt.Stringer` |

---

## TypedActorRef[T]

A generic, compile-time type-safe actor reference:

```go
ref, err := gekka.Spawn(cluster.System, myBehavior, "greeter")
ref.Tell(Greet{Name: "world"})  // only Greet accepted — compile-time checked
```

`TypedActorRef[T]` is a type alias for `actor.TypedActorRef[T]`.

---

## Typed Actor API

### Behavior[T]

```go
type Behavior[T any] = typed.Behavior[T]
```

A `Behavior` is a pure function: it receives the current context and a message
and returns the *next* behavior (which may be itself via `actor.Same[T]()`).

### TypedContext[T]

```go
type TypedContext[T any] interface {
    Self() TypedActorRef[T]
    System() ActorContext
    Spawn(behavior any, name string) (actor.Ref, error)
    SpawnAnonymous(behavior any) (actor.Ref, error)
    SystemActorOf(behavior any, name string) (actor.Ref, error)
    Log() *slog.Logger
    Watch(target Ref)
    Unwatch(target Ref)
    Stop(target Ref)
    Passivate() // request graceful shutdown from sharding
    Timers() TimerScheduler[T]
    Stash() StashBuffer[T]
    Ask(target Ref, msgFactory func(Ref) any, transform func(any, error) T)
}
```

### Convenience behaviors

| Function | Description |
|----------|-------------|
| `actor.Same[T]()` | Return the unchanged behavior |
| `actor.Stopped[T]()` | Request actor shutdown |
| `actor.Setup[T](factory)` | One-shot initialization before the first message |

### Type-safe Ask

```go
result, err := gekka.Ask[Greet, GreetingReply](
    ctx, ref, 5*time.Second,
    func(replyTo TypedActorRef[GreetingReply]) Greet {
        return Greet{Name: "world", ReplyTo: replyTo}
    },
)
```

---

## EventSourcedBehavior (Persistence)

```go
behavior := actor.NewEventSourcedBehavior[Command, Event, State]().
    WithPersistenceID("my-actor-1").
    WithJournal(persistence.NewInMemoryJournal()).
    WithCommandHandler(func(ctx TypedContext[Command], state State, cmd Command) actor.Effect[Event, State] {
        return actor.Persist[Event, State](MyEvent{})
    }).
    WithEventHandler(func(state State, evt Event) State {
        return state.Apply(evt)
    })

ref, err := gekka.SpawnPersistent(cluster.System, behavior, "my-actor-1")
```

See [PERSISTENCE.md](PERSISTENCE.md) for the full reference.

---

## ActorSelection

Lazy handle for actor discovery:

```go
// Resolve by local path suffix — nil means use the node's own context
ref, err := node.ActorSelection("/user/greeter").Resolve(nil)

// Resolve by absolute remote URI (no network call; connection is lazy)
ref, err := node.ActorSelection("pekko://Sys@10.0.0.1:2552/user/greeter").Resolve(nil)
```

---

## Cluster Sharding (Typed)

```go
type EntityTypeKey[M any] struct { Name string }

type EntityRef[M any] struct {
    // ...
}

func (r *EntityRef[M]) Tell(msg M)
```

Use `gekka.EntityRefFor[M](sys, typeName, entityId)` to obtain a reference.

---

## Cluster Singleton (Typed)

```go
// Create a singleton manager
singleton := gekka.NewTypedSingleton(cluster, myBehavior, "worker-role")
cluster.System.ActorOf(singleton.Props(), "mySingleton")

// Access via proxy
proxy := gekka.NewTypedSingletonProxy[MyMsg](cluster, "/user/mySingleton", "worker-role")
proxy.Tell(MyMsg{})
```

---

## Cluster Methods

| Method | Description |
|--------|-------------|
| `System` | `ActorSystem` — create actors with `System.ActorOf(props, name)` |
| `Addr() net.Addr` | Bound TCP address |
| `Port() uint32` | Bound TCP port |
| `IsUp() bool` | True once the Welcome message has been received |
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
| `SingletonProxy(path, role)` | Return a `ClusterSingletonProxy` |
| `Replicator() *crdt.Replicator` | Return the CRDT replicator |
| `Serialization() *SerializationRegistry` | Return the serialization registry |
| `Metrics() *NodeMetrics` | Live internal counters (atomic; no locking) |
| `MonitoringAddr() net.Addr` | Address of the built-in HTTP monitoring server (nil if disabled) |
| `StopHeartbeat()` | Pause heartbeats to seed (simulate failure in tests) |
| `StartHeartbeat()` | Resume heartbeats to seed |
| `Shutdown() error` | Stop server and cancel goroutines |

---

## Routers

See [ROUTING.md](ROUTING.md) for the full router reference.

Quick reference:

```go
// Pool router — spawns and supervises routees.
pool := actor.NewPoolRouter(&actor.RoundRobinRoutingLogic{}, 3, workerProps)
cluster.System.ActorOf(actor.Props{New: func() actor.Actor { return pool }}, "pool")

// Group router — routes to pre-existing actors.
group := actor.NewGroupRouterWithPaths(&actor.RandomRoutingLogic{},
    []string{"/user/w1", "/user/w2"})
cluster.System.ActorOf(actor.Props{New: func() actor.Actor { return group }}, "group")
```

---

## Cluster Sharding

See [SHARDING.md](SHARDING.md) for the full sharding reference.

---

## Monitoring

When `EnableMonitoring: true` is set in `ClusterConfig`, an HTTP server is
started on `MonitoringPort`:

| Endpoint | Description |
|----------|-------------|
| `GET /healthz` | `200 {"status":"ok"}` when associated and cluster-joined; `503` otherwise |
| `GET /metrics` | JSON snapshot of internal counters |
| `GET /metrics?fmt=prom` | Prometheus text exposition format |

---

## Stream Package (`stream`)

For a full narrative guide see [docs/STREAMS.md](STREAMS.md).

### Source constructors

| Function | Returns | Description |
|----------|---------|-------------|
| `FromSlice[T](s []T)` | `Source[T, NotUsed]` | Emit all elements from a slice |
| `FromIteratorFunc[T](fn)` | `Source[T, NotUsed]` | Custom pull function |
| `Repeat[T](elem T)` | `Source[T, NotUsed]` | Infinite; combine with `.Take(n)` |
| `Failed[T](err)` | `Source[T, NotUsed]` | Immediately fails |
| `ActorSource[T](bufSize)` | `(Source, TypedActorRef[T], func())` | Push-fed source |

### Flow operators (type-preserving methods)

| Method | Description |
|--------|-------------|
| `Map(fn)` | Transform each element |
| `MapE(fn)` | Transform; may return error |
| `Filter(pred)` | Keep elements matching pred |
| `Take(n)` / `Drop(n)` | Limit / skip elements |
| `Delay(d)` / `Throttle(n, per, burst)` | Flow control |
| `Buffer(size, strategy)` | Bounded buffer |
| `Async()` | Insert async goroutine boundary |
| `Log(name)` | Debug logging of each element |
| `WithSupervisionStrategy(decider)` | Error handling policy |
| `Recover(fn)` | Emit fallback element on error |
| `RecoverWith(fn)` | Failover to backup source on error |

### Package-level Flow functions (type-changing)

| Function | Description |
|----------|-------------|
| `MapAsync[In,Out](src, n, fn)` | Ordered parallel mapping |
| `FilterMap[In,Out](src, pf)` | Filter and transform in one step |
| `StatefulMapConcat[In,S,Out](src, init, fn)` | Stateful flat-map |
| `Grouped[T](src, n)` | Batch into `[]T` slices of size n |
| `GroupedWithin[T](src, n, d)` | Batch by count or time |
| `GroupBy[T](src, max, key)` | Split into keyed sub-streams |

### Graph operators

| Function | Description |
|----------|-------------|
| `Merge[T](sources...)` | Fan-in; non-deterministic order |
| `Broadcast[T](src, fanOut, buf)` | Fan-out; every branch gets all elements |
| `Balance[T](src, fanOut, buf)` | Fan-out; work-stealing distribution |
| `Zip[T1,T2](s1, s2)` | Pair elements; completes on shorter |
| `ZipWith[T1,T2,Out](s1, s2, fn)` | Zip with combine function |
| `Concat[T](s1, s2)` | Sequential: drain s1 then s2 |

### Sink constructors

| Function | Returns | Description |
|----------|---------|-------------|
| `Collect[T]()` | `Sink[T, []T]` | Accumulate all into a slice |
| `Foreach[T](fn)` | `Sink[T, NotUsed]` | Call fn per element |
| `ForeachErr[T](fn)` | `Sink[T, NotUsed]` | Like Foreach; fn may fail stream |
| `Ignore[T]()` | `Sink[T, NotUsed]` | Discard all |
| `Head[T]()` | `Sink[T, *T]` | First element only |
| `SinkToFile(path)` | `Sink[[]byte, *Future[int64]]` | Write chunks to file |

### Resilience

| Function | Description |
|----------|-------------|
| `RestartSource(settings, factory)` | Restart source on failure with backoff |
| `RestartFlow(settings, factory)` | Restart flow on failure with backoff |

### KillSwitch

| Function / Method | Description |
|----------|-------------|
| `NewKillSwitch[T]()` | Returns `(Flow[T,T,NotUsed], KillSwitch)` |
| `KillSwitch.Shutdown()` | Complete stream cleanly |
| `KillSwitch.Abort(err)` | Fail stream with err |
| `NewSharedKillSwitch()` | Controls multiple streams |
| `SharedKillSwitchFlow[T](ks)` | Attach to a stream |

### File IO

| Function | Returns | Description |
|----------|---------|-------------|
| `SourceFromFile(path, chunkSize)` | `(Source[[]byte, NotUsed], *Future[int64])` | Read file in chunks |
| `SinkToFile(path)` | `Sink[[]byte, *Future[int64]]` | Write chunks to file |

### Remote Streaming (StreamRefs)

| Function | Returns | Description |
|----------|---------|-------------|
| `ToSourceRef[T](src, encode, decode)` | `(*TypedSourceRef[T], func(), error)` | Materialize source as stage actor |
| `FromSourceRef[T](ref)` | `Source[T, NotUsed]` | Subscribe to remote source |
| `ToSinkRef[T](sink, decode, encode)` | `(*TypedSinkRef[T], func(), error)` | Materialize sink as stage actor |
| `FromSinkRef[T](ref)` | `Sink[T, NotUsed]` | Push to remote sink |
| `NewTcpListener[T](addr, decode)` | `(*TcpListener[T], error)` | Raw TCP receive server |
| `TcpOut[T](ref, encode)` | `Sink[T, NotUsed]` | Raw TCP send client |
| `NewTcpListenerWithTLS[T](addr, decode, cfg)` | `(*TcpListener[T], error)` | TLS TCP receive server |
| `TcpOutWithTLS[T](ref, encode, cfg)` | `Sink[T, NotUsed]` | TLS TCP send client |
