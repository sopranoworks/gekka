# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
