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

## Release Candidates — v1.0.0

The 1.0.0 line is being stabilized through a release-candidate series. No public
API breakage is expected across the candidates.

### v1.0.0-rc1 (2026-04-19)
- HTTP server & routing DSL, Cassandra persistence plugin, stream connectors
  (Kafka/Kinesis/AMQP/S3/SQS), Flight Recorder, and the extraction of
  `gekka-cli` / `gekka-metrics` / compat-test binaries into independent
  repositories/modules.

### v1.0.0-rc2 (2026-05-10)
- Pekko config-completeness honesty pass; slog-based logging foundation
  (`pekko.loglevel` / `pekko.stdout-loglevel`); Artery multi-TCP outbound
  transport (`outbound-lanes`); Artery TLS cipher suites; Pekko-compatible
  cluster bootstrap/config; integration-test runtime reliability rework.

### v1.0.0-rc3 (2026-07-12)
Cross-language interoperability and reliability hardening (104 commits since
rc2). Highlights:
- **TLS / mTLS — substantially addressed.** The `RawTLSConfig` escape hatch
  (Go-native analog of Pekko's `SSLEngineProviderSetup`) enables CA-less
  public-key/fingerprint pinning over `tls-tcp`, complementing rc2's HOCON
  cipher-suite support. Verified end-to-end (matching vs. mismatched pin).
- Pekko-compatible **jackson-cbor** serializer for cross-language messaging.
- **Sharding ↔ Pekko interop** (`ShardingSerializer` id 13 + `PekkoCoordinatorShim`).
- Real-Akka-cluster **join hardening** (OUTBOUND control-lane routing,
  heartbeat sender, `UpNumber`-at-Join) and Artery wire-protocol correctness
  (manifests, reincarnation, dashboard self-down).
- Reliability-gate campaign (transport/cluster/deathwatch correctness + log
  noise) and a cross-language **showcase harness** + Go-seed integration suite.

### v1.0.0-rc4 (2026-07-15)
Cluster-convergence correctness and concurrency hardening (15 commits since
rc3, no new public API, no API breakage). This candidate closes the three
cluster bugs rc3 shipped with as open issues and makes `go test -race ./...`
clean. The full cross-language 8-node showcase (Gate 1 + the 600 s
zero-WARN/ERROR Gate 2) now passes. Highlights:
- **Clean-boot convergence deadlock resolved** (`5def7e1`): Pekko-correct
  address-ordered leader election (was `upNumber`-ordered) + a pre-Welcome
  leader-action gate.
- **Gate-2 cross-language steady-state resolved** (`c56a27c`): fixed a
  5-minute idle-sweep that closed write-only Artery connections, a 1 Hz
  inbound kill/reconnect loop, and permanent quarantine on Pekko's harmless
  boot-race `Quarantined` frames.
- **Join-time member flicker resolved** (`991565d`): stopped adopting other
  nodes' vector-clock identities via a positional misread of `allHashes`.
- **Gossip/upNumber/failure-detector Pekko alignment** (`9e7d751`,
  `b732736`): clock pruning with tombstones, version-scoped `Seen`, per-pass
  `1 + youngest` upNumber, heartbeat-response-only failure detection with
  `acceptable-heartbeat-pause` in φ — plus four transport defects the
  verification unmasked.
- **Data-race-clean** (`351c614`): `atomic.Pointer` telemetry provider and a
  serialized `BaseActor` mailbox send/close path; `go test -race ./...` clean.
- **Cross-language CI enforcement**: a scheduled Multi-JVM Compat workflow
  (`de217a0`), all 16 `go.work` modules gated with gofmt (`4490295`), and the
  sbt toolchain on JDK 25 (`ed33321`).

### v1.0.0-rc5 (2026-07-15)
Persistence — a new embedded backend, an actor-messaging blocking fix, and
real-process-boundary validation of cross-process forwarding (4 commits since
rc4, no API breakage). Highlights:
- **bbolt `DurableStateStore` backend** (`41e92b2`): embedded, file-based
  durable-state provider with no external database process, registered as the
  `"bbolt"` plugin (HOCON-selectable like in-memory / SQL / Redis); per-`Upsert`
  / `Delete` fsync durability proven by a genuine child-process restart test.
- **System-level `Send` no longer blocks on a full mailbox** (`166dcde`):
  `ActorSystem.Send` / `SendWithSender` now drop + `DeadLetter` + return an
  error instead of blocking indefinitely, matching `Tell` and the remote path.
- **Cross-process journal + snapshot-store forwarding proven across two real
  OS processes** (`80ee8c4`, `336b4d2`): genuine child-process tests where the
  store lives in a separate process and a correct round-trip is only reachable
  via real Artery forwarding, never local state. The forwarding implementation
  itself predates this candidate.
- **Persistence backend dependency isolation verified**: `go list -deps`
  confirms bbolt-only and sql-only consumers do not cross-contaminate their
  driver dependencies (only shared dependency is stdlib `database/sql`).

## Known Open Issues

*None currently tracked as release-blocking.* The three cluster issues open
when rc3 shipped are resolved in rc4:

- **Clean-boot cluster convergence gap** — RESOLVED (`5def7e1`): leader
  candidates were ordered by `upNumber` instead of Pekko's address ordering,
  so in a mixed cluster no leader promoted joiners and membership deadlocked;
  a slow-Welcome joiner could also self-promote off its single-node view. Both
  fixed and verified by 3/3 clean-boot Gate-1 passes.
- **Gate-2 cross-language traffic failures** — RESOLVED (`c56a27c`): three
  transport time-bombs (inbound-only idle sweep, inbound kill/reconnect loop,
  permanent received-`Quarantined` blacklisting) fixed; the full 600 s
  zero-WARN/ERROR Gate-2 window now passes.
- **Join-time member flicker** — RESOLVED (`991565d`): the positional
  `allHashes` misread that fabricated causal history and produced spurious
  `MemberRemoved`/"new incarnation joined" quarantine is fixed.

Follow-up (non-blocking, tracked for a later phase): a Phase 4 external
log-server / logger-gate ticket (deferred per the current cycle scope).

## Upcoming

### v1.0.0 (stable) — Production Readiness & Stability

**Target:** 2027

#### 1. API Stabilization
Finalize public interfaces and package structures for the first stable 1.0 release.

#### 2. Cluster Convergence Robustness
The clean-boot convergence gap is closed as of rc4 (`5def7e1`); remaining work
is to add convergence-timing coverage to the multi-JVM gate so regressions are
caught automatically.

#### 3. Performance Tuning
Exhaustive benchmarking and optimization of the mailbox processing loop and gossip propagation.

#### 4. Documentation & Guides
Comprehensive user manual, architectural deep-dives, and production deployment best practices.

---

## Compatibility Commitment

All v0.x releases maintain binary wire compatibility with **Apache Pekko 1.x** and
**Lightbend Akka 2.6+** for the Artery TCP transport layer.

> **MANDATORY RULE (all future features):** Any network-related feature MUST include
> binary compatibility tests with Scala (Pekko/Akka) from the initial implementation phase.
> No network feature may be merged without a passing `//go:build integration` test that
> exercises the wire format against a live Scala node.
