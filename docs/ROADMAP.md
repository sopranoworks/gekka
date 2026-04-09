<!--
  ROADMAP.md — Gekka Project Development Roadmap
  Copyright (c) 2026 Sopranoworks, Osamu Takahashi
  SPDX-License-Identifier: MIT
-->

# Gekka Roadmap

## Released

### v0.9.0 (2026-03-18)
- **Akka Typed API Refinement**: Integrated **Timers** (`TimerScheduler`) and **Stash** (`StashBuffer`) for full functional parity with Pekko/Akka's typed actor model.
- **Gekka Streams**: Reactive streams implementation aligned with the Akka Streams programming model, featuring backpressure-aware async stages and actor integration.
- **Kubernetes-native Discovery**: Automated cluster formation using the **Kubernetes API** or **DNS SRV** (headless services) for dynamic seed node resolution.
- **Zero-copy Serialization**: High-performance Artery transport framing using `net.Buffers`, delivering **8.5x faster** throughput and zero allocations on the hot path.
- **`gekka-cli` / `gekka-metrics` TUI**: Enhanced operational tools with interactive terminal dashboards and unified Nebula/Forest branding.

### v0.8.0 (2026-03-17)
- **Cluster HTTP Management API**: REST endpoints for `/cluster/members`, `/health/alive`, `/health/ready` (Kubernetes probe compatible)
- **`gekka-cli`**: Command-line cluster management tool (`members` command, HOCON config loading)
- **`gekka-metrics`**: Dedicated metrics node with native OpenTelemetry export
- **Coordinated Shutdown refinement**: Phased exit sequence with readiness gate and shard handoff timeout (configurable via HOCON)
- **Rolling Update Optimization**: Operational suite for zero-downtime cluster upgrades
- **Pekko 1.1.2 Upgrade**: Verified interoperability; DC-scoped leader election for multi-DC Go nodes

### v0.7.0 (2026-03-16)
- Split Brain Resolver: `keep-majority`, `keep-oldest`, `keep-referee`, `static-quorum`
- Multi-Data Center support: DC-role gossip, DC-scoped singletons, DC-aware sharding
- Advanced Sharding: Entity Passivation (`PassivationIdleTimeout`) and Remember Entities
- SQL Persistence Backend (PostgreSQL, driver-agnostic)
- OpenTelemetry integration: tracing and metrics with W3C TraceContext propagation

### v0.6.0 (2026-03-14)
- Distributed Pub/Sub (Full Pekko compatibility, GZIP support)
- Artery TLS Transport (Secure encrypted cluster traffic)
- Reliable Delivery (Serializer ID 36, at-least-once delivery)
- Cluster Singleton Manager & Proxy (Failover support)
- Coordinated Shutdown (Phased node exit sequence)

### v0.10.0 — Finite State Machines & Structural Alignment (2026-03-19)
- **Typed FSM DSL**: Integrated behavior-based FSM for Typed Actors with full lifecycle and timer support.
- **Classic FSM Parity**: Ported the FSM DSL to Classic Actors (`BaseFSM`) for managing complex state transitions.
- **Structural Refactoring**: Relocated Sharding, Cluster Singleton, Distributed Data, and Reliable Delivery into specialized subpackages (`cluster/sharding`, `cluster/singleton`, etc.) for architectural parity with Pekko/Akka.
- **Durable State Persistence**: Added state-based persistence (`DurableStateBehavior`) as an alternative to event sourcing.
- **Advanced Routing**: Implemented Scatter-Gather, Tail-Chopping, and Consistent Hashing routing logics for both classic and typed systems.
- **Message Adaptation (Ask)**: Implemented the `TypedContext.Ask` pattern for asynchronous response handling.

### v0.11.0 (2026-03-20)
- **Distributed Data (CRDTs)**: Implemented `PNCounter`, `ORSet`, and `LWWRegister` with full serializer support.
- **Delta-CRDT Gossip**: Bandwidth-efficient state synchronization using delta-propagation for massive clusters.
- **Kubernetes-aware Self-Healing**: Smart Split Brain Resolver that monitors Pod lifecycle via the K8s API to accelerate recovery.

### v0.12.0 (2026-03-22)
- **Cloud Spanner Native Persistence**: Highly optimized backend using Spanner Mutations and Streaming Reads for low-latency event sourcing.
- **Advanced Sharding**: Adaptive shard allocation based on node pressure and Manual Rebalance API for ops control.
- **Exactly-once Reliable Delivery**: Deep integration between Sharding and Reliable Delivery to ensure zero message loss during failovers.
- **End-to-End Distributed Tracing**: Full OpenTelemetry instrumentation across Sharding, Persistence, and Projections.
- **Performance Benchmarking Suite**: Comprehensive suite for measuring Scale, Throughput, and Recovery metrics.

### v0.13.0 (2026-03-24)
- **Artery TCP Wire Compatibility**: Full protocol alignment with Akka 2.6.x and Pekko 1.x, featuring automated preamble detection and manifest-based routing.
- **Enhanced Operational Suite**: Standardized TUI behaviors across `gekka-cli` and `gekka-metrics`, including interactive dashboards with auto-scrolling metadata and graceful exit confirmation.
- **Management API Auto-Bootstrap**: Intelligent configuration-driven activation of the Cluster Management HTTP server.
- **Thin-Core Refactoring**: Strategic extraction of heavy cloud and telemetry SDKs into independent extension modules, keeping the core dependency tree lean.
- **Plugin-based Persistence**: Unified standard-library interfaces for Journal and Snapshot storage with support for dependency-injected backends (Spanner, SQL, Redis).
- **Structured Logging (slog)**: Integrated granular log level control for high-frequency protocol events, significantly reducing default terminal noise.

### v0.14.0 (2026-03-28) ✅ Released
- **Native Aeron UDP Transport**: Wire-level Go implementation of the Aeron 1.30.0 framing protocol. Enables hybrid Go/JVM clusters over `aeron-udp` without a JVM Media Driver. Three Artery logical streams (Control=1, Ordinary=2, Large=3) are multiplexed over a single UDP port. Reliability via NACK-based retransmission and SM flow control. Verified by full `sbt multi-jvm:test` with 60-second stability window against Akka 2.6.21.
- **GraphDSL Builder API**: Explicit graph wiring DSL (`NewBuilder`, `Add`, `Connect`) for constructing non-linear stream topologies such as diamond graphs and multi-branch fan-out/fan-in pipelines.
- **Junction Stages** (`stream` package): `NewBroadcast[T](n)`, `NewMerge[T](n)`, `NewZip[A, B]()` — first-class `Graph` components with full back-pressure semantics, verified through the complete test suite.
- **PersistenceId Discovery**: `ReadJournal` DSL with `CurrentPersistenceIds()` and `EventsByPersistenceId()` backed by Spanner and SQL stores for CQRS projection support.
- **Custom Shard Allocation DSL**: `ShardAllocationStrategy` interface for external placement strategies (geo-aware, latency-weighted).
- **Adaptive Cluster Rebalancing**: Automatic shard migration driven by node-level pressure scores (CPU, Memory, Mailbox).
- **Ultra Thin Core (CBOR removal)**: Removed `fxamacker/cbor` from core to comply with the zero-non-stdlib-dependency policy for transport and serialization primitives.

### v0.15.0 (2026-04-05) ✅ Released
- **Actor System Enhancements**: `AllForOneStrategy` supervisor; `EventStream` system-wide pub/sub bus; `DeadLetter` events; custom mailbox types (`BoundedMailbox`, `UnboundedPriorityMailbox`) with pluggable `MailboxFactory`.
- **Typed Actor Enhancements**: `Behaviors.intercept` API with `BehaviorInterceptor[T]` and `LogMessages[T]`; typed `Topic[T]` pub/sub with `LocalMediator`; `TypedReplicatorAdapter` for typed-actor DData integration.
- **Persistence Enhancements**: `PersistentFSM` for state-machine event sourcing; `EventAdapter` schema evolution hooks; `snapshotWhen` predicate; `persistAsync`/`persistAllAsync`; `DurableProducerQueue`; `EventsByTag` query DSL; snapshot lifecycle management with `RetentionCriteria`.
- **Streams — Operators**: `WireTap`, `Interleave`, `TakeWhile`, `DropWhile`, `FlatMapConcat`, `MapAsyncUnordered`, `Scan`, `MergePrioritized`, `UnzipWith2`, `RetryFlowWithBackoff`, `FlowWithContext`, `SourceWithContext`.
- **Streams — Hubs, Compression & Framing**: `MergeHub`, `BroadcastHub`, `PartitionHub`; `Gzip`/`Gunzip`/`Deflate`/`Inflate`; length-field and delimiter framing flows; `GroupBy` sub-streams; `BidiFlow` protocol stacking.
- **Cluster Extensions**: Cluster Client; Artery quarantine protocol parity; `LeaseMajority`, `DownAll`, `DownAllNodesInDataCenter` SBR strategies; `TestLease`/`KubernetesLease`; Consul and AWS EC2 discovery.
- **Testing Framework**: `Actor TestKit` with `TestProbe` and assertion helpers; `Stream TestKit` with reactive source/sink probes.
- **Distributed Data**: `PNCounterMap` and `ORMultiMap` typed accessor API; configurable cross-DC gossip probability; exactly-once projection delivery.
- **Extensible Serialization**: HOCON-based user-defined serializer registration via `gekka.serialization.bindings`.

### v0.16.0 (2026-04-09) ✅ Released
- **Actor System — Dispatchers**: `PinnedDispatcher` (OS-thread pinning), `CallingThreadDispatcher` (synchronous for tests), `LoggingMailbox` decorator; HOCON dispatcher configuration via `Props.DispatcherKey`.
- **Classic Actor Completeness**: `become`/`unbecome` behavior stack; `PoisonPill`/`Kill` lifecycle messages; `Identify`/`ActorIdentity` protocol; `GracefulStop` pattern; `SetReceiveTimeout`/`CancelReceiveTimeout` for classic actors.
- **Typed Actor Completeness**: `ctx.PipeToSelf`, `MessageAdapter`, `AskWithStatus`/`StatusReply`, `WatchWith`; `SetReceiveTimeout` and `CancelReceiveTimeout` for typed actors; `Behaviors.empty`, `Behaviors.ignore`, `Behaviors.unhandled`; `WithMdc` structured logging context; `Behaviors.supervise` wrapper (Restart/Stop/Resume/RestartWithBackoff); `SpawnProtocol`; `TransformMessages`; `StoppedWithPostStop`; `ReceiveSignal` (Terminated/PreRestart/PostStop); `ReceivePartial`.
- **Cluster — Bootstrap & Resilience**: `ClusterBootstrap` coordinator with discovery polling, quorum logic, and leader-election by address sort; `ClusterMetricsRoutingLogic` for pressure-driven routing; Singleton lease coordination; `AppVersion` in handshake for rolling updates with `AppVersionChanged` events.
- **Distributed Data**: `GSet[T]` grow-only set CRDT with delta propagation.
- **Persistence — Typed Completeness**: `RetentionCriteria` (snapshot-every-N, keep-N, delete-events-on-snapshot); `RecoveryStrategy` (Disabled, SnapshotSelectionCriteria, ReplayFilter); `RecoveryCompleted` signal; `ReplicatedEventSourcing` for multi-DC event sourcing with cross-replica streaming.
- **Streams — Source constructors**: `Single`, `Empty`, `Range`, `Tick`, `FromFuture`, `Queue` (with overflow strategies), `Unfold`, `UnfoldAsync`, `Cycle`, `ZipWithIndex`, `Combine` (Merge/Concat strategies).
- **Streams — Flow operators**: Token-bucket `Throttle` with `costCalculation`; `MapConcat`, `FlatMapMerge`; `TakeWithin`, `DropWithin`; `DivertTo`, `AlsoTo`; `InitialTimeout`, `CompletionTimeout`, `IdleTimeout`, `KeepAlive`; `DelayWith`, `OrElse`, `Prepend`; `WatchTermination`, `Monitor`; `Intersperse`, `RecoverWithRetries`, `MapError`, `StatefulMap`, `BatchWeighted`.
- **Streams — Sink & buffering**: `Fold`, `Reduce`, `Last`, `LastOption`, `HeadOption`, `Cancelled`; `ActorRefWithBackpressure`; `RestartSink`; `Batch`, `Expand`, `Extrapolate`, `Sliding`, `SplitWhen`, `SplitAfter`, `ScanAsync`.
- **Streams — Custom GraphStage API**: `GraphStage[S]`/`GraphStageLogic` with `InHandler`/`OutHandler` for fully custom stages; `LazySource`, `LazySink`, `LazyFlow`; Reactive Streams `Publisher`/`Subscriber`/`Processor` interop.
- **Streams — Sink Integration**: `ConsistentHashRouter` with 100-replica virtual-node ring; `BalancingPool` (shared-channel work-stealing); `SmallestMailboxPool`; `Behaviors.monitor` (typed DeathWatch with custom message synthesis).
- **Serialization**: CBOR serializer (`serialization/cbor`) using `fxamacker/cbor/v2`; Remote actor deployment via `RemoteScope`/`RemoteDeploy`.
- **Testing**: `BehaviorTestKit` (synchronous typed behavior unit tests); `SerializationTestKit` (round-trip verification); `ManualTime` (deterministic timer control); `PersistenceTestKit` with failure injection; `LoggingTestKit` (log assertion); `MultiNodeTestKit` (in-process cluster formation with `Barrier`/`RunOn`).
- **Extension Framework**: `ExtensionId`/`Extension` API for lazy singleton extensions on `ActorSystem`; `AggregateProvider` for multi-source seed discovery.
- **Bug Fixes**: Resolved 11 integration test failures in Artery delivery, `ProducerController` type switch, and serialization registry dispatch.
- **Multi-JVM Testing**: 4-node compatibility test (2 Akka + 2 Go nodes) with full cluster formation verification.

---

## Upcoming

### v1.0.0 — Production Readiness & Stability

**Target:** 2027

#### 1. API Stabilization
Finalize public interfaces and package structures for the first stable 1.0 release.

#### 2. Performance Tuning
Exhaustive benchmarking and optimization of the mailbox processing loop and gossip propagation.

#### 3. Documentation & Guides
Comprehensive user manual, architectural deep-dives, and production deployment best practices.

---

## Compatibility Commitment

All v0.x releases maintain binary wire compatibility with **Apache Pekko 1.x** and
**Lightbend Akka 2.6+** for the Artery TCP transport layer.

> **MANDATORY RULE (all future features):** Any network-related feature MUST include
> binary compatibility tests with Scala (Pekko/Akka) from the initial implementation phase.
> No network feature may be merged without a passing `//go:build integration` test that
> exercises the wire format against a live Scala node.
