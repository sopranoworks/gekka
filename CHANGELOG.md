# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] - 2026-03-13

### Added
- **Cluster Sharding**: Introduced the `sharding` package providing `ShardRegion`, `ShardCoordinator`, `ShardCoordinatorProxy`, `ExtractEntityId`, and `EntityRef[T]`. Entities are automatically distributed across nodes, created on demand, and recovered after failures via `StartSharding` / `GetEntityRef` in the root package.
- **Typed Actors**: Public API in `typed.go` — `SpawnTyped[T]`, `SpawnPersistent[C,E,S]`, `TypedActorRef[T]`, and `Ask[T,R]` provide compile-time type safety for actor messaging via Go generics.
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


[0.5.0]: https://github.com/sopranoworks/gekka/releases/tag/v0.5.0
[0.4.0]: https://github.com/sopranoworks/gekka/releases/tag/v0.4.0
[0.3.0]: https://github.com/sopranoworks/gekka/releases/tag/v0.3.0
