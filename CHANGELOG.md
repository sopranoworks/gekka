# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.16.0] - 2026-04-09

### Added

- 🚀 **PinnedDispatcher**: `DispatcherPinned` locks the actor goroutine to a single OS thread via `runtime.LockOSThread`, enabling actors that depend on thread-local state (CGo, OpenGL, etc.).
- 🚀 **CallingThreadDispatcher**: `DispatcherCallingThread` executes `Receive` synchronously on the `Tell` caller's goroutine, enabling deterministic single-threaded unit tests without goroutine scheduling.
- 🚀 **LoggingMailbox**: `NewLoggingMailbox(inner, level, logger)` decorator that logs every enqueued message before delegating to the inner mailbox factory.
- 🚀 **HOCON Dispatcher Configuration**: `Props{DispatcherKey: "my-dispatcher"}` resolved at spawn time from `pekko.dispatchers.*` HOCON config, enabling per-actor dispatcher tuning without code changes.
- 🚀 **Behaviors.monitor** (Typed DeathWatch): `Monitor[T, U](ref, onTerminated, inner)` wraps a typed behavior to synthesize a custom message when a watched actor terminates, avoiding explicit signal handling.
- 🚀 **Token-bucket Throttle**: Replaced the basic rate limiter with a proper token-bucket `Throttle[T](elements, per, maximumBurst, costCalculation)` stream operator, supporting per-element cost functions and burst absorption.
- 🚀 **ConsistentHashRouter — Virtual-node ring**: `ConsistentHashRouter` now uses a 100-replica virtual-node ring with binary-search selection, guaranteeing ≥80% key stability when one routee is added and ≤40% disruption when one of three routees is removed.
- 🚀 **Rolling Updates (AppVersion)**: `AppVersion{Major, Minor, Patch}` field in `NodeConfig`/`ClusterConfig`; included in `InitJoin` and `Welcome` handshake protos; `ClusterManager` emits `AppVersionChanged` events as new versions join, enabling blue/green and rolling upgrade orchestration.
- 🚀 **ctx.PipeToSelf**: Typed actor `PipeToSelf[T, R](ctx, fn, adapt)` runs an async function in a goroutine and delivers the result to `ctx.Self()` via an adapter, safely bridging async I/O into the actor model.
- 🚀 **MessageAdapter**: `MessageAdapter[T, U](ctx, adapt)` returns a typed `ActorRef[U]` that applies an adapter function before forwarding to `ctx.Self()`, enabling external-protocol bridging without exposing the actor's internal message type.
- 🚀 **AskWithStatus / StatusReply**: `AskWithStatus[Req, Res](ctx, target, createRequest, timeout)` sends a request and unwraps a `StatusReply[Res]` response, surfacing errors directly.
- 🚀 **WatchWith**: `WatchWith[T](ctx, target, msg)` registers a watch and delivers a custom message (rather than the generic `Terminated` signal) when the target actor stops.
- 🚀 **SetReceiveTimeout (typed)**: `SetReceiveTimeout[T](ctx, d, msg)` and `CancelReceiveTimeout[T](ctx)` — typed actor API for idle-timeout message injection, mirroring the classic actor pattern.
- 🚀 **become / unbecome** (classic): `BaseActor.Become(fn)` pushes a new `Receive` handler onto an internal stack; `Unbecome()` pops it, restoring the previous handler. Enables finite-state behavior switching without subclassing.
- 🚀 **PoisonPill / Kill**: `PoisonPill` triggers a graceful post-stop shutdown; `Kill` panics with `ActorKilledException`, invoking supervision. Both are sent as ordinary messages.
- 🚀 **Identify / ActorIdentity**: Classic actors auto-respond to `Identify{MessageID}` with `ActorIdentity{MessageID, Ref}`, enabling actor path resolution without cluster infrastructure.
- 🚀 **GracefulStop**: `GracefulStop(ref, timeout, stopMsg)` sends a stop message and waits for `PostStop` to complete within the timeout, returning an error if the deadline is exceeded.
- 🚀 **SetReceiveTimeout (classic)**: `BaseActor.SetReceiveTimeout(d)` and `CancelReceiveTimeout()` — idle timeout with automatic timer reset on every received message.
- 🚀 **Custom GraphStage API**: `GraphStage[S Shape]` interface with `GraphStageLogic` (Push/Pull/Complete/Cancel/SetHandler) and `InHandler`/`OutHandler` callbacks, enabling fully custom back-pressure-aware stream stages without modifying the core library.
- 🚀 **LazySource / LazySink / LazyFlow**: Factory is deferred until the first element demand, enabling resource-efficient pipelines where the source or sink should not be created until actually needed.
- 🚀 **Reactive Streams Interop**: `Publisher[T]`, `Subscriber[T]`, `Subscription`, and `Processor[T, R]` interfaces; `FromPublisher[T]` and `AsPublisher[T]` adapters for interoperating with any Reactive Streams-compliant library.
- 🚀 **ClusterBootstrap Coordinator**: `ClusterBootstrap` polls a `SeedProvider` at a configurable interval, waits for a stable quorum of contact points, and performs self-join when the local node has the lowest address — eliminating the need for manually specified seed nodes.
- 🚀 **Replicated Event Sourcing**: `ReplicatedEventSourcing[Cmd, Evt, State]` in `persistence/typed/replicated/` enables multi-DC event sourcing. Events carry `ReplicaId` origin metadata and are streamed across replicas via `ReplicationStream`, guaranteeing eventual consistency without central coordination.
- 🚀 **Singleton Lease Coordination**: Optional `Lease` field on `ClusterSingletonManagerSettings`; the singleton acquires the lease before starting and releases it on handoff/shutdown, preventing split-brain singleton duplication.
- 🚀 **ClusterMetricsRoutingLogic**: Pressure-driven cluster router that selects the least-loaded node using configurable `WeightFunc` over CPU, Memory, and Mailbox pressure scores propagated via `MetricsGossip`.
- 🚀 **MultiNodeTestKit**: `MultiNodeTestKit` in `actor/testkit/` creates N in-process actor systems, forms a real cluster among them, and provides `Barrier(name)` synchronization and `RunOn(node, fn)` for targeted test execution — all without an external cluster.
- 🚀 **Behaviors.empty / ignore / unhandled**: Three new terminal-ish typed behaviors — `Empty[T]()` logs a warning per message; `Ignore[T]()` silently drops; `Unhandled[T]()` logs at debug level.
- 🚀 **WithMdc**: `WithMdc[T](staticMdc, mdcForMessage, behavior)` wraps a typed behavior to inject structured MDC key-value pairs into the actor logger, both as static fields and per-message fields.
- 🚀 **BalancingPool Router**: `BalancingPoolRouter` with a shared channel (work-stealing) ensures all routees pull from the same queue, preventing head-of-line blocking when some routees are slow.
- 🚀 **SmallestMailboxPool Router**: `SmallestMailboxRoutingLogic` queries routee mailbox sizes at send time and routes to the least-loaded routee.
- 🚀 **AtLeastOnceDelivery**: `AtLeastOnceDelivery` mixin with `Deliver`/`ConfirmDelivery`, configurable `RedeliverInterval` and `MaxUnconfirmedMessages`; state survives restart via snapshot.
- 🚀 **PersistenceTestKit**: `persistence/testkit.PersistenceTestKit` with `FailNextPersist`, `FailNextRead`, `ExpectNextPersisted`, `PersistedInStorage`, `ClearAll` — in-memory journal with programmable failure injection.
- 🚀 **LoggingTestKit**: `actor/testkit.LoggingTestKit` with `ExpectLog(level, pattern)`, `ExpectNoLog`, and `Intercept(fn)` for asserting actor log output in unit tests.
- 🚀 **GSet CRDT**: `GSet[T comparable]` grow-only set with `Add`, `Contains`, `Elements`, `Size`, `Merge` (union semantics), and delta propagation.
- 🚀 **CBOR Serializer**: `serialization/cbor` package using `fxamacker/cbor/v2`; implements the `Serializer` interface with manifest-based type resolution and configurable serializer ID.
- 🚀 **Remote Actor Deployment**: `RemoteScope{Address}` and `Props.WithRemoteDeploy(address)` — `ActorOf` with a remote deploy sends a deployment message to the target node, which spawns the actor locally and returns a remote `ActorRef`.
- 🚀 **Stream Source Constructors**: `Single[T](elem)`, `Empty[T]()`, `Range(from, to)`, `Tick[T](interval, elem)`, `FromFuture[T](fn)`, `Queue[T](bufferSize, strategy)` (with `DropHead`/`DropTail`/`DropNew`/`Fail` strategies), `Unfold[S, T]`, `UnfoldAsync[S, T]`, `Cycle[T]`, `ZipWithIndex[T]`, `Combine[T](strategy, sources...)`.
- 🚀 **Stream Flow Operators**: `MapConcat[In, Out]`, `FlatMapMerge[In, Out]`; `TakeWithin`, `DropWithin`; `DivertTo`, `AlsoTo`; `InitialTimeout`, `CompletionTimeout`, `IdleTimeout`, `KeepAlive`; `DelayWith`, `OrElse`, `Prepend`; `WatchTermination`, `Monitor`; `Intersperse`, `RecoverWithRetries`, `MapError`, `StatefulMap`.
- 🚀 **Stream Sink Operators**: `Fold[T, U]`, `Reduce[T]`, `Last[T]`, `LastOption[T]`, `HeadOption[T]`, `Cancelled[T]`; `ActorRefWithBackpressure` (ack-based back-pressure sink); `RestartSink` (automatic restart with backoff).
- 🚀 **Stream Buffering Operators**: `Batch[In, Out]`, `BatchWeighted[In, Out]` (weighted-cost batching), `Expand`, `Extrapolate`, `Sliding`, `SplitWhen`, `SplitAfter`, `ScanAsync`.
- 🚀 **RetentionCriteria (Typed Persistence)**: `SnapshotEvery(n, keepN)` and `DeleteEventsOnSnapshot` flags on `EventSourcedBehavior` automate snapshot scheduling and journal cleanup.
- 🚀 **RecoveryStrategy (Typed Persistence)**: `RecoveryStrategy.Disabled` skips recovery for fresh starts; `SnapshotSelectionCriteria` filters which snapshot to load; `ReplayFilter` skips specific events during recovery.
- 🚀 **RecoveryCompleted signal (Typed Persistence)**: After all events are replayed, a `RecoveryCompleted{HighestSequenceNr}` signal is delivered via `EventSourcedBehavior.SignalHandler`, enabling post-recovery initialization.
- 🚀 **BehaviorTestKit**: `actor/testkit.BehaviorTestKit[T]` for synchronous typed-behavior unit tests — `Run(msg)`, `ReturnedBehavior()`, `SelfInbox()`, and `ExpectEffect(effect)` for spawn/watch/schedule side-effect assertions.
- 🚀 **SerializationTestKit**: `actor/testkit.SerializationTestKit` with `VerifySerialization(t, msg)` and `VerifySerializationOf(t, msg, serializer)` for round-trip codec testing.
- 🚀 **ManualTime**: `actor/testkit.ManualTime` with `Advance(d)`, `Now()`, and `Schedule(key, delay, callback)` for deterministic timer control in unit tests.
- 🚀 **Behaviors.supervise** (Typed): `Supervise[T](behavior).OnFailure(strategy)` wraps a typed behavior with a supervisor that applies Restart/Stop/Resume/RestartWithBackoff on panic, mirroring Pekko's typed supervision DSL.
- 🚀 **SpawnProtocol**: `SpawnProtocolBehavior[T]()` is a typed behavior that handles `SpawnProtocol[T]` messages, spawning child actors and replying with their refs — useful as a guardian for programmatic actor creation.
- 🚀 **TransformMessages**: `TransformMessages(behavior, transform func(any) (any, bool))` wraps a behavior to adapt messages at the type level, returning `Unhandled` for unmatched inputs.
- 🚀 **StoppedWithPostStop**: `StoppedWithPostStop[T](postStop func())` variant of `Stopped` that invokes a callback when the actor stops, enabling cleanup without a full `ReceiveSignal` handler.
- 🚀 **ReceiveSignal**: `ReceiveSignal[T](msgHandler, signalHandler)` typed behavior that handles both messages and lifecycle signals (`Terminated`, `PreRestart`, `PostStop`).
- 🚀 **ReceivePartial**: `ReceivePartial[T](handler func(TypedContext[T], T) (Behavior[T], bool))` — typed behavior that explicitly returns whether the message was handled; unmatched messages are treated as `Unhandled`.
- 🚀 **Extension Framework**: `ExtensionId` / `Extension` API for registering lazy-initialized singleton extensions on `ActorSystem`, with thread-safe `sync.Once`-based initialization.
- 🚀 **AggregateProvider**: `discovery.NewAggregateProvider(providers...)` merges and deduplicates seed nodes from multiple discovery sources, with partial-failure tolerance.

### Bug Fixes

- 🐛 **ProducerController RegisterConsumer type switch**: Added missing pointer-type case for `*RegisterConsumer` in the `ProducerController` message dispatch switch, resolving a consumer registration failure.
- 🐛 **Delivery serializer pointer types**: `RegisterConsumer` messages in the delivery serializer are now correctly handled as pointer types, matching the `ProducerController` fix.
- 🐛 **Serialization registry dispatch**: Fixed 9 additional integration test failures in Artery message routing caused by incorrect serializer ID dispatch ordering in the registry.

### Tests

- 🧪 **4-node multi-JVM cluster test**: New compatibility spec with 2 Akka + 2 Go nodes forming a single cluster, verifying cross-language membership convergence, heartbeat exchange, and gossip propagation across 4 members simultaneously.

## [0.15.0] - 2026-04-05

### Added

- 🚀 **AllForOneStrategy Supervisor**: New supervision strategy that applies the failure directive to all child actors when any single child fails, complementing `OneForOneStrategy`.
- 🚀 **EventStream**: System-wide publish/subscribe bus for broadcasting events across the local actor system. Supports `Publish`, `Subscribe`, and `Unsubscribe` with type-safe filtering.
- 🚀 **DeadLetter Integration**: Undeliverable messages are published to the `EventStream` as `DeadLetter` events for monitoring and debugging.
- 🚀 **Behaviors.intercept API** (`actor/typed/interceptor.go`): `BehaviorInterceptor[T]` interface with `AroundReceive` hook, `Intercept[T]` factory, and built-in `LogMessages[T]` interceptor for transparent message tracing.
- 🚀 **Custom Mailbox Types** (`actor/mailbox.go`): `MailboxFactory` interface with `NewBoundedMailbox(capacity, dropStrategy)` supporting `DropNewest`, `DropOldest`, and `BackPressure` strategies, plus `NewPriorityMailbox(less)` backed by `container/heap` for priority-ordered delivery. Wired via `Props.Mailbox`.
- 🚀 **Extensible Serialization**: HOCON-based registration for user-defined serializers via `gekka.serialization.bindings`, enabling pluggable wire formats without modifying core transport code.
- 🚀 **Persistent FSM** (`persistence/fsm.go`): State-machine-based persistence combining FSM behavior with the event sourcing journal. Includes `persistAsync` and `persistAllAsync` for high-throughput writes that process commands before journal acks.
- 🚀 **Event Adapters**: `EventAdapter` transformation hooks for migrating events between journal representation and actor domain model, enabling schema evolution without data migration.
- 🚀 **Snapshot Lifecycle Management**: Automated snapshot cleanup via `RetentionCriteria`, and `snapshotWhen func(state, event, seqNr) bool` predicate on `EventSourcedBehavior` for fine-grained snapshot control.
- 🚀 **DurableProducerQueue**: Persistence-backed reliable delivery queue ensuring at-least-once semantics survive producer crashes.
- 🚀 **WorkPulling Delivery Pattern** (`actor/typed/delivery/`): Push-pull message distribution where workers pull work at their own pace, preventing mailbox overflow under bursty load.
- 🚀 **Exactly-once Projection Delivery**: Offset-tracked projection delivery with durable checkpointing prevents duplicate event processing during restarts.
- 🚀 **EventsByTag Persistence Query**: `EventsByTag` DSL backed by SQL and Spanner stores for tag-based CQRS event stream filtering.
- 🚀 **GroupBy Sub-Streams & BidiFlow**: `GroupBy` for dynamic fan-out by key; `BidiFlow` for composable bidirectional protocol stacking (framing, codec, encryption layers).
- 🚀 **Stream Hubs**: `MergeHub[T]`, `BroadcastHub[T]`, and `PartitionHub[T]` for dynamic many-to-one and one-to-many topologies with runtime subscriber management.
- 🚀 **Stream Compression**: `Gzip`, `Gunzip`, `Deflate`, and `Inflate` flow operators for transparent in-stream compression and decompression.
- 🚀 **Stream Framing**: Length-field-based and delimiter-based framing flows for TCP protocol stacking.
- 🚀 **Additional Stream Operators**: `TakeWhile`, `DropWhile`, `FlatMapConcat`, `MapAsyncUnordered`, `Scan`, `WireTap` (non-blocking side-tap), `Interleave` (round-robin merge), `MergePrioritized` (weighted fan-in), `UnzipWith2` (typed fan-out), `RetryFlowWithBackoff` (per-element retry), `FlowWithContext`, and `SourceWithContext` (metadata-preserving streams).
- 🚀 **Actor TestKit & TestProbe** (`actor/testkit/`): `TestProbe` for synchronous message interception, with assertion helpers (`ExpectMsg`, `ExpectNoMsg`, `FishForMessage`) and `WithCallingThreadDispatcher` for deterministic unit tests.
- 🚀 **Stream TestKit**: `TestSource` (reactive source probe) and `TestSink` (reactive sink probe) for precise backpressure and element-by-element stream testing.
- 🚀 **Cluster Client Extension**: External client for communicating with a cluster without joining as a full member; supports `Send` and `Publish` to any cluster actor path.
- 🚀 **Artery Quarantine Protocol Parity**: Full quarantine state machine — quarantined associations are tracked via UID registry and refuse all further communication until UID reset.
- 🚀 **LeaseMajority SBR Strategy**: Split-brain resolution strategy using a distributed lease to determine which partition retains the majority and stays up.
- 🚀 **DownAll / DownAllNodesInDataCenter SBR Strategies**: Strategies that down all nodes (or all nodes in a given DC) on partition — suitable for stateless services where fast recovery is preferred over split tolerance.
- 🚀 **TestLease & KubernetesLease**: Lease implementations for testing and Kubernetes-based leader election, usable with `LeaseMajority` SBR.
- 🚀 **Consul & AWS EC2 Discovery Extensions**: New seed provider implementations for Consul service discovery and AWS EC2 tag-based cluster formation.
- 🚀 **Typed Replicator Adapter** (`cluster/ddata/`): `TypedReplicatorAdapter[Cmd, D]` enabling typed actors to interact with Distributed Data via `AskGet`, `AskUpdate`, `Subscribe`, and `Unsubscribe`.
- 🚀 **Typed Cluster Pub/Sub** (`cluster/pubsub/`): `Topic[T]` typed behavior with `Publish[T]`, `Subscribe[T]`, and `Unsubscribe[T]`; backed by `LocalMediator` for in-process testing.
- 🚀 **PNCounterMap & ORMultiMap Typed API** (`cluster/ddata/`): Typed accessor methods — `GetValue`, `Keys` on `PNCounterMap`; `Put`, `Remove`, `GetElements`, `Keys` on `ORMultiMap`.
- 🚀 **Multi-DC Gossip Enhancements**: Configurable `CrossDataCenterGossipProbability` for bandwidth-efficient cross-DC state propagation; DC-aware failure detection tuning.

## [0.14.0] - 2026-03-28

### Added

- 🚀 **Native Aeron UDP Transport**: Full wire-level compatibility with Akka 2.6.x and Pekko 1.x `aeron-udp` transport. Go nodes join hybrid Go/Scala clusters over the Aeron 1.30.0 UDP framing protocol without a JVM Media Driver. Three logical Artery streams are multiplexed over a single UDP port: Stream 1 (Control), Stream 2 (Ordinary), Stream 3 (Large). Verified by `sbt multi-jvm:test` with a 60-second stability window and Ping/Pong message exchange against a live Akka 2.6.21 seed node.
- 🚀 **Aeron Protocol Primitives** (`internal/core/aeron_proto.go`): Type-safe Go structs (`AeronDataHeader`, `AeronSetupHeader`, `AeronStatusMessage`, `AeronNak`) with little-endian `Encode`/`Decode` methods matching Aeron 1.30.0 frame layouts. Frame type constants, stream IDs, and MTU/term-length defaults are derived from the canonical Aeron spec.
- 🚀 **GraphDSL Builder API** (`stream.NewBuilder`, `stream.Add`, `stream.Connect`): Explicit port-wiring DSL for constructing non-linear stream topologies. Supports both `SyncMaterializer` and `ActorMaterializer` execution contexts.
- 🚀 **Junction Stages**: Three new `Graph` components — `NewBroadcast[T](n)` (fan-out to n outlets with back-pressure), `NewMerge[T](n)` (concurrent fan-in from n inlets), and `NewZip[A, B]()` (pair-wise combination blocking on both inputs).
- 🚀 **PersistenceId Discovery** (`ReadJournal` DSL): `CurrentPersistenceIds()` and `EventsByPersistenceId()` queries implemented for both Cloud Spanner and SQL (PostgreSQL) backends, enabling CQRS read-side projections without full table scans.
- 🚀 **Custom Shard Allocation DSL**: `ShardAllocationStrategy` interface and external strategy hook for plugging in custom entity-placement logic (geo-aware, latency-weighted, etc.) at configuration time.
- 🚀 **Adaptive Cluster Rebalancing**: Shards are automatically migrated from high-pressure nodes to low-pressure nodes using real-time CPU, Memory, and Mailbox pressure scores propagated via cluster gossip.

### Changed

- 🔄 **Ultra Thin Core — CBOR Removal**: The `fxamacker/cbor` dependency has been removed from the core module. Artery wire compatibility is maintained without CBOR; the core now carries zero non-stdlib transport dependencies.
- 🔄 **String Serializer ID corrected to 20**: Akka 2.6's built-in `StringSerializer` uses ID 20 with raw UTF-8 encoding (not ID 1 / Java ObjectOutputStream). The serialization registry now correctly handles `java.lang.String` messages from Akka nodes over both TCP and Aeron UDP transports.

### Bug Fixes

- 🐛 **UDP Source Address Tracking** (`udpSrcAssoc` map): Fixed repeated inbound association creation. When `handleHandshakeReq` updated `assoc.remote` to the canonical Akka address, subsequent frames from the ephemeral media-driver port could not be matched to the existing association. The `udpSrcAssoc` map permanently binds the physical UDP source address to the association, surviving any `assoc.remote` mutation.
- 🐛 **MessageContainerSerializer (ID 6) dispatch ordering**: Cluster message deserialization now checks for `ClusterSerializerID` before calling `SerializerRegistry.DeserializePayload`, preventing "unknown serializer ID 5" errors.
- 🐛 **Aeron DATA frame bounds check**: Added frame-length validation before slicing the receive buffer in `handleData`, preventing a runtime panic on short or malformed UDP datagrams.

### Tests

- 🧪 **Multi-JVM Aeron Compatibility Test** (`AeronClusterSpec.scala`): New `sbt multi-jvm:test` spec that launches a Go binary as a second cluster member via `aeron-udp`, verifies `MemberUp`, exercises a Step-1→Step-2→Step-3→Step-4 Ping/Pong echo chain, and asserts 60-second reachability.
- 🧪 **`gekka-aeron-compat-test` binary** (`cmd/gekka-aeron-compat-test/main.go`): Standalone test harness for the multi-JVM Aeron UDP compatibility spec.

## [0.13.0] - 2026-03-24

### Features

- 🚀 **Artery TCP Wire Compatibility**: Achieved full protocol alignment with Akka 2.6.x and Pekko 1.x. This includes verified preamble handling and manifest-based routing for seamless interoperability with JVM nodes.
- 🚀 **Automated Management Activation**: The HTTP Management Server now automatically enables itself if a hostname or port is defined in the HOCON configuration, simplifying cluster bootstrap.
- 🚀 **Granular Log Control**: Integrated `slog` with configurable levels via `gekka.logging.level`. This significantly reduces default terminal noise by moving high-frequency protocol events to DEBUG level.
- 🚀 **Ultra Thin Core**: Successfully extracted heavy third-party SDKs (OpenTelemetry, Kubernetes, YAML) into independent extension modules under `/extensions/`, keeping the core module lean.
- 🚀 **Redis Persistence Extension**: Introduced a new extension for Redis-backed Journal and Durable State storage, including Testcontainers-based integration testing.
- 🚀 **Dynamic Extension Engine**: Implemented config-driven plugin auto-registration and provider loading for modular architecture.

### Bug Fixes

- 🐛 **Artery Preamble Length**: Fixed a critical mismatch between 4-byte and 5-byte preambles during Artery connection initiation.
- 🐛 **Inbound Stream Consumption**: Resolved an issue where inbound Artery control streams were not fully consumed, leading to connection stalls.
- 🐛 **Manifest Alignment**: Corrected message manifest strings to match JVM expectations for Handshake and Cluster internal messages.
- 🐛 **TUI Dashboard Alignment**: Fixed a visual bug causing massive leading indentation and column misalignment when displaying long Artery addresses.
- 🐛 **Local Node Filtering**: Implemented strict 64-bit UID-based filtering to ensure the local node is correctly hidden from the remote member list in the dashboard.
- 🐛 **Bootstrap Status Promotion**: Fixed a bug where the first node in a cluster would remain in JOINING status; it now correctly promotes itself to UP upon convergence.
- 🐛 **Telemetry Endpoint Prefix**: Resolved a double-protocol prefix issue in OTLP telemetry configuration.

### Documentation

- 📚 **v0.13.0 Release Documentation**: Updated all files in `/docs` to reflect the stable release features and configuration keys.
- 📚 **Operational Tooling Guide**: Enhanced documentation for `gekka-cli` and `gekka-metrics` detailing new TUI behaviors like the ESC exit prompt and Roles marquee.
- 📚 **Configuration Reference**: Added detailed HOCON keys for logging, management binding, and telemetry OTLP endpoints.

## [0.12.0] - 2026-03-22

### Added

- ☁️ **Cloud Spanner Native Persistence**: High-performance backend using Spanner Mutations and Streaming Reads for low-latency event sourcing. Supports `SpannerJournal` and `SpannerSnapshotStore`.
- 🏗️ **Advanced Sharding**:
  - **Adaptive Shard Allocation**: Balanced entity distribution based on node pressure (CPU, Heap, Mailbox).
  - **Manual Rebalance API**: Programmatic and CLI control over shard movement.
  - **Remember Entities Recovery**: Efficient recovery of sharded entities from persistent storage.
- 📦 **Exactly-once Reliable Delivery**: Deep integration between Sharding and Reliable Delivery to ensure zero message loss during failovers.
- 🕵️ **End-to-End Distributed Tracing**: Full OpenTelemetry instrumentation across Sharding, Persistence, and Projections.
- 📈 **Performance Benchmarking Suite**: New tools for measuring scale, throughput, and recovery metrics (Phase 20).
- 🧬 **Delta-CRDT Gossip Optimization**: Bandwidth-efficient state synchronization for large-scale clusters (Phase 18).
- 🛡️ **Kubernetes-aware Split Brain Resolver**: Enhanced SBR that monitors Pod lifecycle via K8s API (Phase 17).

### Changed

- 🛠️ **Refactored Reliable Delivery API**: Simplified `ProducerController` and `ConsumerController` initialization with automated registration.
- 📂 **Integration Test Pathing**: Standardized `scala-server` paths across all integration tests for consistent execution in CI and local environments.

### Fixed

- 🐛 **Integration Test Compilation**: Resolved numerous `undefined` errors and API mismatches caused by the v0.10.0 structural refactoring.
- 🧪 **Test Stability**: Fixed a race condition in `TestIntegration_PekkoServer` where a probe message interfered with the echo verification.

## [0.11.0] - 2026-03-21

### Added

- 🌊 **Complete Reactive Streams DSL** (`stream` package): Full Source/Flow/Sink pipeline model with pull-based back-pressure. New operators in this release:
  - **Basic**: `Repeat`, `MapAsync` (ordered parallel), `StatefulMapConcat`, `FilterMap`
  - **Batching**: `Grouped`, `GroupedWithin` (count or time flush)
  - **Graph**: `Merge`, `Broadcast`, `Balance`, `Zip`, `ZipWith`, `Concat`, `GroupBy`
  - **Resilience**: `Recover`, `RecoverWith`, `RestartSource`, `RestartFlow` (backoff with jitter)
  - **Flow control**: `KillSwitch`, `SharedKillSwitch`, `Buffer`, `Throttle`, `Delay`
  - **File IO**: `SourceFromFile`, `SinkToFile`, `Future[T]` promise type
  - **Actor integration**: `Ask` flow, `ActorSource` with overflow strategies, `FromTypedActorRef` sink
  - **Supervision**: `WithSupervisionStrategy` + `Decider` (Stop / Resume / Restart)
- 🌐 **Distributed Streams — StreamRefs**: Share a `Source` or `Sink` across network nodes with end-to-end demand-driven back-pressure.
  - `TypedSourceRef[T]` / `ToSourceRef` / `FromSourceRef` — materialize a local source as a remote-subscribable stage actor (TCP server)
  - `TypedSinkRef[T]` / `ToSinkRef` / `FromSinkRef` — materialize a local sink as a remote-pushable stage actor
  - `NewTcpListenerWithTLS` / `TcpOutWithTLS` — TLS-encrypted TCP streaming via `crypto/tls`
  - Wire protocol compatible with Pekko's `StreamRefSerializer` (ID 36)
- 📡 **Internal Heartbeat RTT Monitoring**: Measure round-trip latency between cluster nodes via the internal heartbeat channel; exposed via `gekka-cli`.
- 📖 **Stream Developer Guide**: Comprehensive reference at `docs/STREAMS.md` covering all operators, codecs, and remote streaming patterns.

### Changed

- `stream.SinkRef` and `stream.SourceRef` structs promoted to typed wrappers (`TypedSinkRef[T]`, `TypedSourceRef[T]`) with bundled codecs for simpler API usage.

---

## [0.10.0] - 2026-03-20

### Added
- 🚀 **Integrated Typed Spawner API**: Unified spawning logic via the `Spawner` interface, now shared by both `ActorSystem` and `ActorContext`. Added `Spawn`, `SpawnAnonymous`, and `SystemActorOf` as global generic functions in the root package.
- 📦 **Structural Parity with Pekko**: Completed the full separation of core modules. Sharding, Cluster Singleton, Distributed Data, and Reliable Delivery now reside in their own specialized subpackages (e.g., `cluster/sharding`, `cluster/singleton`).
- 💾 **Durable State Persistence**: Introduced `DurableStateBehavior` as a state-based alternative to Event Sourcing, complete with a SQL-backed `DurableStateStore`.
- 🔗 **Message Adaptation (Ask)**: Implemented the `TypedContext.Ask` pattern for asynchronous response handling with type-safe transformations and timeout support.
- 📊 **Gekka Projection**: New infrastructure for resilient event stream processing and SQL-based offset management.
- 🛡️ **Circuit Breaker**: Added the `actor.CircuitBreaker` pattern to protect actor communications from cascading failures.
- 🔄 **Classic/Typed Parity**: Achieved full parity for Finite State Machines (`BaseFSM`) and advanced routers across both Classic and Typed APIs.
- ⚖️ **Adaptive Load Balancing**: New routing logic that biases traffic toward nodes with lower pressure scores (CPU, Heap, Mailbox).

### Changed
- 🛠️ **Refactored Typed API**: Moved `Behavior`, `TypedContext`, and `TypedActorRef` into the `gekka` root package via type aliases for a cleaner developer experience.
- 🏗️ **Package Reorganization**: 
  - `actor/delivery/` → `actor/typed/delivery/`
  - `sharding/` → `cluster/sharding/`
  - `crdt/` → `cluster/ddata/`
  - `actor/typed/persistence.go` → `persistence/typed/event_sourcing.go`
- ⚙️ **Go Version Sync**: Standardized the entire project and CI pipelines on **Go 1.26.1**.

### Fixed
- 🐛 **Recursion Bug**: Fixed a critical infinite recursion in `(*Cluster).SelfPathURI` that caused stack overflows in remote messaging.
- 🔒 **Interface Decoupling**: Resolved circular dependencies between `cluster` and its subpackages using interface-based injection.
- 🧪 **Test Stability**: Fixed data races and timing issues in `TestRouter_Broadcast` and `TestReceptionist_Register`.

---

## [0.9.0] - 2026-03-18

### Added
- 🌊 **Gekka Streams**: Initial release of Reactive Streams implementation. Supports `Source`, `Flow`, `Sink`, and `StreamRef` for cross-node back-pressured streaming over Artery TCP.
- ☸️ **Rolling Update Support**:
  - **Readiness Drain Gate**: `/health/ready` now returns 503 during coordinated shutdown to signal Kubernetes to stop routing traffic.
  - **Shard Handoff**: Automated shard rebalancing during graceful departure.
- 🖥️ **Interactive TUI Dashboard**: New `gekka-cli dashboard` for real-time cluster monitoring with minimalist flower-motif branding.
- ⚡ **Zero-Copy Serialization**: Optimized Artery framing achieving 8.5x faster throughput for large payloads.
- 🧪 **Scheduler API**: Global task management via `ActorSystem.Scheduler()`.
- 🏷️ **Event Tagging**: Support for `Tags` in `PersistentRepr` and SQL journals for efficient CQRS filtering.

### Changed
- 🎨 **Branding Refresh**: Unified **NEBULA/FOREST** motif across CLI, TUI, and documentation.
- 🧩 **Discovery Decoupling**: Refactored cluster discovery into a plugin-based registry to minimize core dependencies.

---

## [0.8.0] - 2026-03-16

### Added
- 🛠️ **Operational Tooling**: 
  - `gekka-cli`: Management tool for cluster membership and diagnostics.
  - `gekka-metrics`: Standalone cluster node for native OTLP/HTTP metric exportation.
- 🔌 **Kubernetes Discovery**: Added `SeedProvider` registry and implementations for Kubernetes API and DNS-based peer discovery.
- ⏱️ **Configurable Handoff**: Added HOCON support for `gekka.cluster.sharding.handoff-timeout`.

### Changed
- 🏗️ **Package Migration**: Reorganized `internal/core` to house the central serialization registry and Artery transport handler.
- 📥 **Typed Spawn Refactor**: Moved `Spawn` from a global function to a method on `ActorSystem` (pre-v0.10 integration).

### Improved
- 🏁 **Handshake Stability**: Verified binary wire compatibility between Akka 2.6.21 and Pekko 1.1.2.
- 🧹 **Concurrency**: Resolved pre-existing data races in actor lifecycle transitions using `atomic` operations and mutex guards.

---

## [0.7.0] - 2026-03-16

### Added
- **Split Brain Resolver (SBR)**: Introduced cluster partition handling strategies including `KeepMajority`, `KeepOldest`, `KeepReferee`, and `StaticQuorum` for enhanced cluster stability.
- **Multi-Data Center (Multi-DC) Support**: Full DC-awareness via `dc-role` configuration. Support for DC-specific singletons and intelligent sharding affinity.
- **Advanced Sharding Features**:
  - **Entity Passivation**: Automatic idle timeout to optimize memory usage by stopping inactive entities.
  - **Remember Entities**: Durable recovery for sharded entities via event sourcing, ensuring entities are restarted after cluster rebalance or node failure.
- **SQL Persistence Backend**: Added a driver-agnostic SQL backend for Actor Persistence. Verified with PostgreSQL and includes automated integration tests via Docker.
- **OpenTelemetry (OTEL) Integration**: Native tracing and metrics infrastructure. Supports W3C TraceContext propagation within Artery envelopes for end-to-end observability.

### Changed
- **API Alignment**: Renamed `ActorSystem.SpawnTyped` to `ActorSystem.Spawn` to better align with Pekko/Akka naming conventions.
- **Documentation Overhaul**: Updated `README.md`, `docs/API.md`, and other documentation files to reflect the v0.7.0 API changes and new features.

### Improved
- **Code Quality**: Conducted a comprehensive project-wide linting and refactoring to ensure adherence to Go best practices and project standards.

---

## [0.6.0] - 2026-03-14

### Added
- **Distributed Pub/Sub**: Full Pekko DistributedPubSub compatibility. Supports `Subscribe`, `Unsubscribe`, `Publish`, and `Send`. Includes GZIP compression for gossip state to optimize bandwidth.
- **Artery TLS Support**: Binary-compatible secure transport using Go's `crypto/tls`. Provides PEM-based certificate management as an alternative to Java JKS.
- **Reliable Delivery (At-Least-Once)**: Implementation of Pekko's Reliable Delivery protocol (Serializer ID 36). Includes `ProducerController` and `ConsumerController` for guaranteed message delivery between Go and Scala.
- **Cluster Singleton Manager & Proxy**: Full distributed lifecycle management. Ensures a single instance of an actor exists on the oldest node with automatic failover during cluster membership changes.
- **Coordinated Shutdown**: Pekko-compatible phased shutdown sequence. Executes tasks in order (`before-service-unbind`, `cluster-leave`, `cluster-shutdown`, etc.) to ensure graceful node departure.
- **Improved CRDTs**: G-Counter and OR-Set now support optimized gossip and GZIP compression.
- **New documentation**: `docs/TLS.md`, `docs/DELIVERY.md`, `docs/ROADMAP.md`.

### Fixed
- **Serialization Registry**: Standardized `internal/core/serialization_registry.go` to avoid fragmentation and ensure consistent ID mappings.
- **Sharding region cleanup**: Registered Sharding regions are now automatically stopped during the `cluster-sharding-shutdown-region` phase of Coordinated Shutdown.
- **Gossip Loop**: Improved robustness of the cluster gossip loop during rapid node joins/leaves.

### Improved
- **Verified Interoperability**: Expanded E2E integration test suite to cover TLS, Singleton failover, and Reliable Delivery against live Pekko 1.0.x nodes.
- **HOCON Configuration**: Improved protocol auto-detection and support for `tls-tcp` transport keys.
- **Location Transparency**: Seamless addressing for Cluster Singletons and Sharded Entities across heterogeneous Go/Scala clusters.

---

## [0.5.0] - 2026-03-13

### Added
- **Cluster Sharding**: Introduced the `sharding` package providing `ShardRegion`, `ShardCoordinator`, `ShardCoordinatorProxy`, `ExtractEntityId`, and `EntityRef[T]`. Entities are automatically distributed across nodes, created on demand, and recovered after failures via `StartSharding` / `GetEntityRef` in the root package.
- **Typed Actors**: Public API in `typed.go` — `Spawn[T]`, `SpawnPersistent[C,E,S]`, `TypedActorRef[T]`, and `Ask[T,R]` provide compile-time type safety for actor messaging via Go generics.
- **Actor Persistence (Event Sourcing)**: `actor.EventSourcedBehavior[C,E,S]` with `Persist`, `PersistThen`, and `None` effects. `persistence.Journal` and `persistence.SnapshotStore` interfaces with `InMemoryJournal` and `InMemorySnapshotStore` built-in backends.
- **Public CRDT Package**: `GCounter`, `ORSet`, and `Replicator` promoted to the public `crdt/` package with `WriteLocal` / `WriteAll` consistency levels.
- **Pekko Interoperability Tests**: `actor/remoting_compatibility_test.go` (package `actor_test`) verifies binary wire format correctness — System Message round-trips (Watch, Terminated, Envelope), manifest mapping for Java/Go types, and SerializerID verification for IDs 2, 4, 5, 9, and 17.
- **Project Structure Finalization**: Internal engine code encapsulated in `internal/core/`; protobuf types moved to `internal/proto/remote/` and `internal/proto/cluster/`.
- **`cluster.IsUp()`**: Returns `true` once the cluster Welcome message has been received.
- **New documentation**: `docs/SERIALIZATION.md`, `docs/SHARDING.md`, `docs/PERSISTENCE.md`.

### Changed
- **API Rename**: `GekkaNode` → `Cluster`, `NodeConfig` → `ClusterConfig`, `Spawn` → `NewCluster`.
- **`ActorSystem` interface extended**: Added `Serialization()`, `RegisterType()`, `GetTypeByManifest()`, and `ActorSelection()`.
- **`Cluster.Metrics()`**: Now returns `*core.NodeMetrics` (live atomic counters).
- **Typed actor files reorganized**: Engine in `actor/typed_actor.go` + `actor/typed_persistence.go`; public re-exports in `typed.go`.
- **Updated docs**: `docs/API.md`, `docs/PROTOCOL.md`, and `docs/ROUTING.md` updated for v0.5.0.

### Fixed
- **Cluster Singleton Proxy**: `WithSingletonName("")` now correctly routes to `ClusterSingletonManager`-managed actors.
- **Sharding coordinator handover**: `ShardCoordinatorProxy` stashes and replays messages during coordinator migration.

---

## [0.4.0] - 2026-03-09

### Added
- **Pool Routers**: `actor.PoolRouter` spawns and supervises a fixed-size pool of routees. Supports `AdjustPoolSize` (delta) and `Broadcast` management messages.
- **Group Routers**: `actor.GroupRouter` routes to pre-existing actors resolved by path at `PreStart` via `ActorContext.Resolve`.
- **Routing Logics**: `RoundRobinRoutingLogic` (atomic counter, lock-free) and `RandomRoutingLogic` (uniform distribution).
- **HOCON Deployment Configuration**: `pekko.actor.deployment` / `akka.actor.deployment` blocks in HOCON are parsed and stored in `ClusterConfig.Deployments`. `ActorSystem.ActorOf` transparently provisions the appropriate router when the actor path matches a deployment entry.
- **`DeploymentConfig`**: Struct carrying `Router`, `NrOfInstances`, and `RouteesPaths` for programmatic router configuration.
- **`LookupDeployment`**: Searches HOCON config for a router deployment matching a given actor path (supports full and short path forms, and both `pekko.*` and `akka.*` prefixes).

### Changed
- **`ActorContext` interface extended**: Added `Resolve(path string) (Ref, error)` to allow `GroupRouter.PreStart` to look up actors by path without importing the root package.
- **`ActorOf` auto-provisioning**: When a deployment entry exists, `ActorOf` returns a router `ActorRef` instead of a plain actor.

### Fixed
- **`PoolRouter` nil watcher panic**: Removed a nil-pointer dereference in `Watch` when `watcher` is nil.
- **HOCON unquoted slash keys**: Actor-path keys in HOCON deployment blocks must be quoted (e.g., `"/user/myRouter"`).

---

## [0.3.0] - 2026-03-09

### Added
- **Hierarchical Actor System**: Implemented robust parent-child relationships for reliable actor lifecycle management and name uniqueness within the `/user/` namespace.
- **Actor Supervision**: Introduced `OneForOneStrategy` with support for `Restart`, `Resume`, `Stop`, and `Escalate` directives to handle child actor failures.
- **Pekko Remote Support**: Added direct messaging compatibility for `pekko://` and `akka://` URIs via `ActorSelection`, enabling communication without mandatory cluster membership.
- **Actor-Aware Logging**: Integrated structured logging with `a.Log()` (via `log/slog`) that automatically includes actor paths, system names, and sender context.
- **Advanced Serialization**: Built-in support for **Protobuf** (ID 2), **Raw Bytes** (ID 4), and **JSON/Jackson-compatible** (ID 9) serialization.
- **GitHub Actions CI**: Automated CI pipeline for Go 1.24, including unit testing, coverage reporting, and linting.

### Changed
- **Config Engine Migration**: Replaced `go-akka/configuration` with the high-performance [`gekka-config`](https://github.com/sopranoworks/gekka-config) engine.
- **Location Transparency**: Refactored `ActorSelection` and `ArteryTransport` to support seamless addressing across local and remote nodes.
- **Namespace Enforcement**: `ActorOf` now strictly enforces the `/user/` path for all user-created actors.

### Fixed
- **Handshake Stability**: Improved Artery association handshake and termination signal delivery for more reliable remote connections.
- **Concurrency Fixes**: Resolved various race conditions in actor spawning and lifecycle transition hooks.
- **Message Dispatch**: Fixed a critical bug where messages were not correctly routed to registered actors by default when incoming envelopes contained full URIs.


[Unreleased]: https://github.com/sopranoworks/gekka/compare/v0.15.0...HEAD
[0.15.0]: https://github.com/sopranoworks/gekka/compare/v0.14.0...v0.15.0
[0.14.0]: https://github.com/sopranoworks/gekka/compare/v0.13.0...v0.14.0
[0.13.0]: https://github.com/sopranoworks/gekka/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/sopranoworks/gekka/compare/v0.11.0...v0.12.0
[0.10.0]: https://github.com/sopranoworks/gekka/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/sopranoworks/gekka/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/sopranoworks/gekka/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/sopranoworks/gekka/releases/tag/v0.7.0
[0.6.0]: https://github.com/sopranoworks/gekka/releases/tag/v0.6.0
[0.5.0]: https://github.com/sopranoworks/gekka/releases/tag/v0.5.0
[0.4.0]: https://github.com/sopranoworks/gekka/releases/tag/v0.4.0
[0.3.0]: https://github.com/sopranoworks/gekka/releases/tag/v0.3.0
