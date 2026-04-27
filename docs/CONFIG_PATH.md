# Configuration Path Compatibility: Pekko reference.conf vs Gekka

This document provides a complete comparison of all Pekko reference.conf configuration paths
against what gekka currently parses. It covers every module in the Pekko source tree.

Legend:
- ✅ = Gekka parses this path correctly
- ⚠️ = Gekka parses a different/wrong path for the same feature
- ❌ = No equivalent feature in gekka (not parsed)

---

## Module: `pekko/actor` (pekko-actor)

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `pekko.loglevel` | `"INFO"` | ✅ | |
| `pekko.stdout-loglevel` | `"WARNING"` | ❌ | No feature |
| `pekko.log-config-on-start` | `off` | ✅ | When on, NewCluster emits the resolved ClusterConfig at INFO via slog |
| `pekko.log-dead-letters` | `10` | ❌ | No feature |
| `pekko.log-dead-letters-during-shutdown` | `off` | ❌ | No feature |
| `pekko.actor.debug.receive` | `off` | ✅ | DEBUG slog via `ActorDebugConfig.LogActorReceive` |
| `pekko.actor.debug.autoreceive` | `off` | ✅ | DEBUG slog via `ActorDebugConfig.LogActorAutoreceive` (PoisonPill/Kill/Terminate) |
| `pekko.actor.debug.lifecycle` | `off` | ✅ | DEBUG slog via `ActorDebugConfig.LogActorLifecycle` (started/stopped/restarted) |
| `pekko.actor.debug.fsm` | `off` | ✅ | DEBUG slog via `ActorDebugConfig.LogActorFSM` |
| `pekko.actor.debug.event-stream` | `off` | ✅ | DEBUG slog via `ActorDebugConfig.LogActorEventStream` |
| `pekko.actor.debug.unhandled` | `off` | ✅ | DEBUG slog via `ActorDebugConfig.LogActorUnhandled` |
| `pekko.actor.debug.router-misconfiguration` | `off` | ✅ | WARN slog via `ActorDebugConfig.LogRouterMisconfiguration` (matches Pekko severity) |
| `pekko.actor.provider` | `"local"` | ✅ | Used for protocol detection |
| `pekko.actor.default-dispatcher.*` | (complex) | ❌ | Gekka uses `pekko.dispatchers.*` instead |
| `pekko.actor.internal-dispatcher.*` | (complex) | ❌ | No feature |
| `pekko.actor.deployment.{path}.*` | (various) | ✅ | Router deployment |
| `pekko.actor.deployment.{path}.cluster.max-nr-of-instances-per-node` | `1` | ✅ | Caps local routees on a cluster pool router (Round-2 session 11) |
| `pekko.actor.serializers.*` | (registry) | ✅ | Via LoadFromConfig |
| `pekko.actor.serialization-bindings.*` | (registry) | ✅ | Via LoadFromConfig |
| `pekko.actor.default-resizer.*` | (various) | ✅ | In reference.conf |

---

## Module: `pekko/remote` (pekko-remote)

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `pekko.remote.artery.canonical.hostname` | `"<getHostAddress>"` | ✅ | |
| `pekko.remote.artery.canonical.port` | `17355` | ✅ | |
| `pekko.remote.artery.transport` | `tcp` | ✅ | |
| `pekko.remote.artery.bind.hostname` | `""` | ✅ | NAT/Docker bind support |
| `pekko.remote.artery.bind.port` | `""` | ✅ | NAT/Docker bind support |
| `pekko.remote.artery.bind.bind-timeout` | `3s` | ✅ | Plumbed into `TcpServerConfig.BindTimeout`; wraps `net.Listen` with a context deadline |
| `pekko.remote.artery.log-received-messages` | `off` | ✅ | DEBUG-level inbound logging in `handleUserMessage` |
| `pekko.remote.artery.log-sent-messages` | `off` | ✅ | DEBUG-level outbound logging in `Send` |
| `pekko.remote.artery.log-frame-size-exceeding` | `off` | ✅ | Warns once per (serializerId, manifest) when payload exceeds threshold; +10% growth re-logs |
| `pekko.remote.artery.propagate-harmless-quarantine-events` | `off` | ✅ | Toggles severity (DEBUG vs WARN) of `EmitHarmlessQuarantineEvent` (legacy Pekko 1.x WARN behavior) |
| `pekko.remote.artery.large-message-destinations` | `[]` | ❌ | No feature |
| `pekko.remote.artery.advanced.maximum-frame-size` | `256 KiB` | ✅ | Configurable via HOCON |
| `pekko.remote.artery.advanced.buffer-pool-size` | `128` | ✅ | Recorded on NodeManager (`EffectiveBufferPoolSize`) for future receive-buffer-pool consumer |
| `pekko.remote.artery.advanced.maximum-large-frame-size` | `2 MiB` | ✅ | Recorded on NodeManager (`EffectiveMaximumLargeFrameSize`) for the large-message stream |
| `pekko.remote.artery.advanced.large-buffer-pool-size` | `32` | ✅ | Recorded on NodeManager (`EffectiveLargeBufferPoolSize`) for future large-stream buffer-pool consumer |
| `pekko.remote.artery.advanced.outbound-large-message-queue-size` | `256` | ✅ | Recorded on NodeManager (`EffectiveOutboundLargeMessageQueueSize`) for the large-stream outbox |
| `pekko.remote.artery.advanced.compression.actor-refs.max` | `256` | ✅ | Cap enforced by `CompressionTableManager.UpdateActorRefTable` — oversize advertisements are rejected |
| `pekko.remote.artery.advanced.compression.actor-refs.advertisement-interval` | `1m` | ✅ | Drives `CompressionTableManager.StartAdvertisementScheduler` actor-ref ticker |
| `pekko.remote.artery.advanced.compression.manifests.max` | `256` | ✅ | Cap enforced by `CompressionTableManager.UpdateManifestTable` — oversize advertisements are rejected |
| `pekko.remote.artery.advanced.compression.manifests.advertisement-interval` | `1m` | ✅ | Drives `CompressionTableManager.StartAdvertisementScheduler` manifest ticker |
| `pekko.remote.artery.advanced.tcp.connection-timeout` | `5s` | ✅ | Threaded into `TcpClient.DialTimeout` and `DialRemote`'s association poll |
| `pekko.remote.artery.advanced.tcp.outbound-client-hostname` | `""` | ✅ | Sets the local source address for outbound dials (`net.Dialer.LocalAddr`) |
| `pekko.remote.artery.advanced.inbound-lanes` | `4` | ✅ | Exposed via `NodeManager.EffectiveInboundLanes()` |
| `pekko.remote.artery.advanced.outbound-lanes` | `1` | ✅ | Exposed via `NodeManager.EffectiveOutboundLanes()` |
| `pekko.remote.artery.advanced.outbound-message-queue-size` | `3072` | ✅ | Sizes each association's outbox channel |
| `pekko.remote.artery.advanced.system-message-buffer-size` | `20000` | ✅ | Recorded on NodeManager for future sender-side redelivery |
| `pekko.remote.artery.advanced.outbound-control-queue-size` | `20000` | ✅ | Sizes each outbound control-stream (streamId=1) association's outbox |
| `pekko.remote.artery.advanced.handshake-timeout` | `20s` | ✅ | Outbound association gives up after this deadline |
| `pekko.remote.artery.advanced.handshake-retry-interval` | `1s` | ✅ | Re-sends HandshakeReq at this cadence until ASSOCIATED |
| `pekko.remote.artery.advanced.system-message-resend-interval` | `1s` | ✅ | Recorded on NodeManager for future sender-side redelivery |
| `pekko.remote.artery.advanced.give-up-system-message-after` | `6h` | ✅ | Recorded on NodeManager for future sender-side redelivery |
| `pekko.remote.artery.advanced.stop-idle-outbound-after` | `5m` | ✅ | Recorded on NodeManager (`EffectiveStopIdleOutboundAfter`) for the idle-sweep consumer |
| `pekko.remote.artery.advanced.quarantine-idle-outbound-after` | `6h` | ✅ | Drives `NodeManager.SweepIdleOutboundQuarantine` — idle outbound associations are quarantined and removed |
| `pekko.remote.artery.advanced.stop-quarantined-after-idle` | `3s` | ✅ | Recorded on NodeManager (`EffectiveStopQuarantinedAfterIdle`) for the idle-sweep consumer |
| `pekko.remote.artery.advanced.remove-quarantined-association-after` | `1h` | ✅ | Recorded on NodeManager (`EffectiveRemoveQuarantinedAssociationAfter`) for the idle-sweep consumer |
| `pekko.remote.artery.advanced.shutdown-flush-timeout` | `1s` | ✅ | Recorded on NodeManager (`EffectiveShutdownFlushTimeout`) for the coordinated-shutdown consumer |
| `pekko.remote.artery.advanced.death-watch-notification-flush-timeout` | `3s` | ✅ | Recorded on NodeManager (`EffectiveDeathWatchNotificationFlushTimeout`) for the death-watch consumer |
| `pekko.remote.artery.advanced.inbound-restart-timeout` | `5s` | ✅ | Drives `NodeManager.TryRecordInboundRestart` rolling window |
| `pekko.remote.artery.advanced.inbound-max-restarts` | `5` | ✅ | Cap enforced by `NodeManager.TryRecordInboundRestart` |
| `pekko.remote.artery.advanced.outbound-restart-backoff` | `1s` | ✅ | Recorded on NodeManager (`EffectiveOutboundRestartBackoff`) for the dialer consumer |
| `pekko.remote.artery.advanced.outbound-restart-timeout` | `5s` | ✅ | Drives `NodeManager.TryRecordOutboundRestart` rolling window |
| `pekko.remote.artery.advanced.outbound-max-restarts` | `5` | ✅ | Cap enforced by `NodeManager.TryRecordOutboundRestart` |
| `pekko.remote.artery.ssl.*` (TLS) | (various) | ✅ | Gekka uses `artery.tls.*` |
| `pekko.remote.watch-failure-detector.*` | (various) | ❌ | Remote watch FD — no feature |
| `pekko.remote.accept-protocol-names` | `["pekko"]` | ❌ | No feature (hardcoded) |

---

## Module: `pekko/cluster` (pekko-cluster)

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `pekko.cluster.seed-nodes` | `[]` | ✅ | |
| `pekko.cluster.seed-node-timeout` | `5s` | ✅ | Warns on seed timeout |
| `pekko.cluster.retry-unsuccessful-join-after` | `10s` | ✅ | |
| `pekko.cluster.shutdown-after-unsuccessful-join-seed-nodes` | `off` | ✅ | Triggers ShutdownCallback |
| `pekko.cluster.down-removal-margin` | `off` | ✅ | Delays Down→Removed |
| `pekko.cluster.downing-provider-class` | `""` | ✅ | Used in InitJoin |
| `pekko.cluster.quarantine-removed-node-after` | `5s` | ✅ | Schedules UID quarantine |
| `pekko.cluster.allow-weakly-up-members` | `7s` | ✅ | WeaklyUp promotion logic |
| `pekko.cluster.roles` | `[]` | ✅ | |
| `pekko.cluster.run-coordinated-shutdown-when-down` | `on` | ✅ | Triggers CoordinatedShutdown |
| `pekko.cluster.role.{name}.min-nr-of-members` | — | ✅ | Per-role gating in leader actions |
| `pekko.cluster.app-version` | `"0.0.0"` | ✅ | Wired to SetLocalAppVersion |
| `pekko.cluster.min-nr-of-members` | `1` | ✅ | |
| `pekko.cluster.log-info` | `on` | ✅ | Gates info-level messages |
| `pekko.cluster.log-info-verbose` | `off` | ✅ | Verbose heartbeat/phi/gossip logging |
| `pekko.cluster.periodic-tasks-initial-delay` | `1s` | ✅ | Applied to all periodic tasks |
| `pekko.cluster.gossip-interval` | `1s` | ✅ | |
| `pekko.cluster.gossip-time-to-live` | `2s` | ✅ | Discards stale gossip |
| `pekko.cluster.leader-actions-interval` | `1s` | ✅ | Independent leader ticker |
| `pekko.cluster.unreachable-nodes-reaper-interval` | `1s` | ✅ | Periodic phi re-evaluation |
| `pekko.cluster.publish-stats-interval` | `off` | ✅ | `StartPublishStatsLoop` ticks at configured cadence and emits `CurrentClusterStats` to subscribers |
| `pekko.cluster.gossip-different-view-probability` | `0.8` | ✅ | Prefers different-view targets |
| `pekko.cluster.reduce-gossip-different-view-probability` | `400` | ✅ | Halves probability at scale |
| `pekko.cluster.prune-gossip-tombstones-after` | `24h` | ✅ | Prunes removed-member tombstones |
| `pekko.cluster.configuration-compatibility-check.enforce-on-join` | `on` | ✅ | Validates incoming InitJoin config |
| `pekko.cluster.configuration-compatibility-check.sensitive-config-paths.<group>` | (built-in) | ✅ | User groups append (deduped) to `DefaultSensitiveConfigPaths`; matched by prefix in `IsSensitiveConfigPath` |

### pekko.cluster.failure-detector

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `implementation-class` | (FQCN) | ❌ | N/A (Go implementation) |
| `heartbeat-interval` | `1s` | ✅ | |
| `threshold` | `8.0` | ✅ | |
| `max-sample-size` | `1000` | ✅ | |
| `min-std-deviation` | `100ms` | ✅ | |
| `acceptable-heartbeat-pause` | `3s` | ✅ | |
| `monitored-by-nr-of-members` | `9` | ✅ | Limits heartbeat targets |
| `expected-response-after` | `1s` | ✅ | |

### pekko.cluster.multi-data-center

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `self-data-center` | `"default"` | ✅ | |
| `cross-data-center-connections` | `5` | ❌ | No feature |
| `cross-data-center-gossip-probability` | `0.2` | ✅ | |
| `failure-detector.heartbeat-interval` | `3s` | ✅ | Cross-DC HB cadence; `EffectiveHeartbeatInterval` returns this for cross-DC targets, intra-DC default otherwise (Round-2 session 12) |
| `failure-detector.acceptable-heartbeat-pause` | `10s` | ✅ | Plumbed via `MultiDCFailureDetectorConfig`; consulted by future cross-DC reachability margin (Round-2 session 12) |
| `failure-detector.expected-response-after` | `1s` | ✅ | Plumbed via `MultiDCFailureDetectorConfig` (Round-2 session 12) |

### pekko.cluster.split-brain-resolver

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `active-strategy` | `keep-majority` | ✅ | |
| `stable-after` | `20s` | ✅ | |
| `down-all-when-unstable` | `on` | ✅ | Downs all after instability timeout |
| `static-quorum.quorum-size` | `undefined` | ✅ | |
| `static-quorum.role` | `""` | ❌ | No feature |
| `keep-majority.role` | `""` | ✅ | |
| `keep-oldest.down-if-alone` | `on` | ✅ | |
| `keep-oldest.role` | `""` | ✅ | |
| `lease-majority.lease-implementation` | `""` | ✅ | `SBRConfig.LeaseImplementation` → resolves a `LeaseProvider` via `cfg.CoordinationLease.LeaseManager`; defaults to `lease.MemoryProviderName` when active-strategy is `lease-majority` and the field is empty (Round-2 session 19) |
| `lease-majority.acquire-lease-delay-for-minority` | `2s` | ✅ | `SBRConfig.AcquireLeaseDelayForMinority` → `LeaseMajority.AcquireDelay`; minority side waits this long before attempting `Acquire` (Round-2 session 19) |
| `lease-majority.role` | `""` | ✅ | `SBRConfig.LeaseMajorityRole` → `LeaseMajority.Role`; falls back to `cfg.SBR.Role` when empty (Round-2 session 19) |

---

## Module: `pekko/distributed-data` (pekko-distributed-data)

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `pekko.cluster.distributed-data.gossip-interval` | `2s` | ✅ | |
| `pekko.cluster.distributed-data.name` | `ddataReplicator` | ❌ | No feature (hardcoded) |
| `pekko.cluster.distributed-data.role` | `""` | ❌ | No feature |
| `pekko.cluster.distributed-data.notify-subscribers-interval` | `500ms` | ❌ | No feature |
| `pekko.cluster.distributed-data.max-delta-elements` | `500` | ❌ | No feature |
| `pekko.cluster.distributed-data.pruning-interval` | `120s` | ❌ | No feature |
| `pekko.cluster.distributed-data.max-pruning-dissemination` | `300s` | ❌ | No feature |
| `pekko.cluster.distributed-data.delta-crdt.enabled` | `on` | ❌ | No feature |
| `pekko.cluster.distributed-data.delta-crdt.max-delta-size` | `50` | ❌ | No feature |
| `pekko.cluster.distributed-data.durable.*` | (various) | ❌ | No feature |
| `pekko.cluster.distributed-data.prefer-oldest` | `off` | ❌ | No feature |
| `pekko.cluster.distributed-data.pruning-marker-time-to-live` | `6h` | ✅ | `DistributedDataConfig.PruningMarkerTimeToLive` → `PruningManager.SetPruningMarkerTimeToLive` retains tombstones in PruningComplete phase for the TTL (Round-2 session 16) |
| `pekko.cluster.distributed-data.log-data-size-exceeding` | `10 KiB` | ✅ | `DistributedDataConfig.LogDataSizeExceeding` → `Replicator.LogDataSizeExceeding`; `sendToPeers` emits a slog.Warn when serialized payload exceeds the threshold (Round-2 session 16) |
| `pekko.cluster.distributed-data.recovery-timeout` | `10s` | ✅ | `DistributedDataConfig.RecoveryTimeout` → `Replicator.WaitForRecovery` blocks until at least one peer is registered or the timeout elapses (Round-2 session 16) |
| `pekko.cluster.distributed-data.serializer-cache-time-to-live` | `10s` | ✅ | `DistributedDataConfig.SerializerCacheTimeToLive` → `Replicator.SerializerCacheTimeToLive`; surface field reserved for future per-CRDT serialization cache (Round-2 session 16) |

---

## Module: `pekko/cluster-sharding` (pekko-cluster-sharding)

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `pekko.cluster.sharding.remember-entities` | `off` | ✅ | |
| `pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.timeout` | `120s` | ⚠️ | **Wrong path**: gekka uses `.passivation.idle-timeout` |
| `pekko.cluster.sharding.passivation.strategy` | `"default-idle-strategy"` | ❌ | No feature |
| `pekko.cluster.sharding.guardian-name` | `"sharding"` | ❌ | No feature |
| `pekko.cluster.sharding.role` | `""` | ✅ | Filters shard allocation by role |
| `pekko.cluster.sharding.remember-entities-store` | `"ddata"` | ❌ | No feature |
| `pekko.cluster.sharding.passivate-idle-entity-after` | `null` | ❌ | Deprecated in Pekko |
| `pekko.cluster.sharding.number-of-shards` | `1000` | ✅ | Wired to coordinator/region |
| `pekko.cluster.sharding.rebalance-interval` | `10s` | ✅ | Applied to ShardCoordinator.RebalanceInterval |
| `pekko.cluster.sharding.least-shard-allocation-strategy.rebalance-threshold` | `1` | ✅ | Applied to NewLeastShardAllocationStrategy(threshold) |
| `pekko.cluster.sharding.least-shard-allocation-strategy.max-simultaneous-rebalance` | `3` | ✅ | Applied to NewLeastShardAllocationStrategy(maxSimultaneous) |
| `pekko.cluster.sharding.distributed-data.majority-min-cap` | `5` | ⚠️ | Parsed; gekka uses shared replicator (not yet routed) |
| `pekko.cluster.sharding.distributed-data.max-delta-elements` | `5` | ⚠️ | Parsed; gekka uses shared replicator (not yet routed) |
| `pekko.cluster.sharding.distributed-data.prefer-oldest` | `on` | ⚠️ | Parsed; gekka uses shared replicator (not yet routed) |
| `pekko.cluster.sharding.distributed-data.durable.keys` | `["shard-*"]` | ⚠️ | Parsed; durable storage is roadmap F2 (sessions 21-23) |
| `pekko.cluster.sharding.coordinator-singleton.role` | `""` | ✅ | Applied to coordinator singleton-proxy when override = off |
| `pekko.cluster.sharding.coordinator-singleton.singleton-name` | `"singleton"` | ✅ | Parsed (gekka uses fixed `<typeName>Coordinator` path) |
| `pekko.cluster.sharding.coordinator-singleton.hand-over-retry-interval` | `1s` | ✅ | Parsed; routed via SingletonConfig |
| `pekko.cluster.sharding.coordinator-singleton.min-number-of-hand-over-retries` | `15` | ✅ | Parsed; routed via SingletonConfig |
| `pekko.cluster.sharding.coordinator-singleton-role-override` | `on` | ✅ | When `on`, sharding.role wins over coordinator-singleton.role |
| `pekko.cluster.sharding.retry-interval` | `2s` | ✅ | ShardRegion ticker re-tells GetShardHome for shards with unknown home (Round-2 session 13) |
| `pekko.cluster.sharding.buffer-size` | `100000` | ✅ | Caps per-shard pendingMessages while awaiting ShardHome; further messages are dropped (Round-2 session 13) |
| `pekko.cluster.sharding.shard-start-timeout` | `10s` | ✅ | Plumbed onto ShardSettings; Shard-startup consumer wires in part 2 (Round-2 session 13) |
| `pekko.cluster.sharding.shard-failure-backoff` | `10s` | ✅ | Region delays clearing the cached home after a Shard terminates (Round-2 session 13) |
| `pekko.cluster.sharding.entity-restart-backoff` | `10s` | ✅ | Plumbed onto ShardSettings; entity-restart consumer wires in part 2 (Round-2 session 13) |
| `pekko.cluster.sharding.coordinator-failure-backoff` | `5s` | ✅ | Region delays re-registration with the coordinator after termination (Round-2 session 13) |
| `pekko.cluster.sharding.waiting-for-state-timeout` | `2s` | ⚠️ | Plumbed onto ShardSettings; consumed by the DData coordinator-state path when present (Round-2 session 14) |
| `pekko.cluster.sharding.updating-state-timeout` | `5s` | ⚠️ | Plumbed onto ShardSettings; consumed by DData updates / remember-entities writes when present (Round-2 session 14) |
| `pekko.cluster.sharding.shard-region-query-timeout` | `3s` | ⚠️ | Plumbed onto ShardSettings; consumed by region-level query handlers as added (Round-2 session 14) |
| `pekko.cluster.sharding.entity-recovery-strategy` | `"all"` | ✅ | `"all"` spawns all remembered entities at once; `"constant"` paces recovery in batches (Round-2 session 14) |
| `pekko.cluster.sharding.entity-recovery-constant-rate-strategy.frequency` | `100ms` | ✅ | Delay between batches under the `"constant"` strategy (Round-2 session 14) |
| `pekko.cluster.sharding.entity-recovery-constant-rate-strategy.number-of-entities` | `5` | ✅ | Batch size under the `"constant"` strategy (Round-2 session 14) |
| `pekko.cluster.sharding.coordinator-state.write-majority-plus` | `3` | ⚠️ | Plumbed onto ShardSettings; `"all"` maps to math.MaxInt sentinel; consumer wires when DData write-majority lands (Round-2 session 14) |
| `pekko.cluster.sharding.coordinator-state.read-majority-plus` | `5` | ⚠️ | Plumbed onto ShardSettings; `"all"` maps to math.MaxInt sentinel; consumer wires when DData read-majority lands (Round-2 session 14) |
| `pekko.cluster.sharding.verbose-debug-logging` | `off` | ✅ | Gates fine-grained per-message DEBUG log lines via Shard.vdebug (Round-2 session 15) |
| `pekko.cluster.sharding.fail-on-invalid-entity-state-transition` | `off` | ✅ | When `on`, Shard panics on invalid handoff transitions; otherwise logs WARN (Round-2 session 15) |
| `pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.interval` | `default` (= timeout/2) | ✅ | Overrides idle-entity scan cadence; `"default"` leaves the timeout/2 fallback (Round-2 session 15) |
| `pekko.cluster.sharding.healthcheck.names` | `[]` | ✅ | List of sharding type names that ClusterShardingHealthCheck must find registered to pass (Round-2 session 15) |
| `pekko.cluster.sharding.healthcheck.timeout` | `5s` | ✅ | Caps how long ClusterShardingHealthCheck is allowed to run before returning ErrHealthCheckTimeout (Round-2 session 15) |

---

## Module: `pekko/cluster-tools` (pekko-cluster-tools)

### pekko.cluster.pub-sub

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `name` | `distributedPubSubMediator` | ❌ | No feature (hardcoded) |
| `role` | `""` | ❌ | No feature |
| `routing-logic` | `random` | ❌ | No feature |
| `gossip-interval` | `1s` | ✅ | ClusterMediator gossip interval |
| `removed-time-to-live` | `120s` | ❌ | No feature |
| `max-delta-elements` | `3000` | ❌ | No feature |
| `send-to-dead-letters-when-no-subscribers` | `on` | ❌ | No feature |

### pekko.cluster.singleton

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `singleton-name` | `"singleton"` | ❌ | No feature (hardcoded) |
| `role` | `""` | ✅ | Applied via SingletonManager factory |
| `hand-over-retry-interval` | `1s` | ✅ | Applied via WithHandOverRetryInterval |
| `min-number-of-hand-over-retries` | `15` | ❌ | No feature |
| `use-lease` | `""` | ❌ | No feature |
| `lease-retry-interval` | `5s` | ❌ | No feature |

### pekko.cluster.singleton-proxy

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `singleton-name` | (ref singleton) | ❌ | No feature |
| `role` | `""` | ❌ | No feature |
| `singleton-identification-interval` | `1s` | ✅ | Applied via SingletonProxy factory |
| `buffer-size` | `1000` | ✅ | Drop-oldest with warning on overflow |

### pekko.cluster.client

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `initial-contacts` | `[]` | ✅ | Loaded into `ClusterConfig.ClusterClient.InitialContacts` (hocon_config.go) |
| `establishing-get-contacts-interval` | `3s` | ✅ | `ClusterConfig.ClusterClient.EstablishingGetContactsInterval` |
| `refresh-contacts-interval` | `60s` | ✅ | `ClusterConfig.ClusterClient.RefreshContactsInterval` |
| `heartbeat-interval` | `2s` | ✅ | `ClusterConfig.ClusterClient.HeartbeatInterval` (drives ticker cadence) |
| `acceptable-heartbeat-pause` | `13s` | ✅ | `ClusterConfig.ClusterClient.AcceptableHeartbeatPause` |
| `buffer-size` | `1000` | ✅ | `ClusterConfig.ClusterClient.BufferSize` |
| `reconnect-timeout` | `off` | ✅ | `ClusterConfig.ClusterClient.ReconnectTimeout` (`off`=0=retry forever) |

### pekko.cluster.client.receptionist

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `name` | `receptionist` | ✅ | `ClusterConfig.ClusterReceptionist.Name` (hocon_config.go) — surfaces in receptionist contact paths |
| `role` | `""` | ✅ | `ClusterConfig.ClusterReceptionist.Role` |
| `number-of-contacts` | `3` | ✅ | `ClusterConfig.ClusterReceptionist.NumberOfContacts` — caps `Contacts.Paths` length (cluster/client/receptionist.go) |
| `heartbeat-interval` | `2s` | ✅ | `ClusterConfig.ClusterReceptionist.HeartbeatInterval` — drives stale-client checker cadence |
| `acceptable-heartbeat-pause` | `13s` | ✅ | `ClusterConfig.ClusterReceptionist.AcceptableHeartbeatPause` |
| `response-tunnel-receive-timeout` | `30s` | ✅ | `ClusterConfig.ClusterReceptionist.ResponseTunnelReceiveTimeout` — bounds forwarded Send/SendToAll deliveries (Round-2 session 16) |
| `failure-detection-interval` | `2s` | ✅ | `ClusterConfig.ClusterReceptionist.FailureDetectionInterval` — drives the receptionist's stale-client sweep cadence (Round-2 session 16) |

---

## Module: `pekko/cluster-typed` (pekko-cluster-typed)

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `pekko.cluster.typed.receptionist.write-consistency` | `local` | ❌ | No feature |
| `pekko.cluster.typed.receptionist.pruning-interval` | `3s` | ❌ | No feature |
| `pekko.cluster.typed.receptionist.distributed-key-count` | `5` | ❌ | No feature |
| `pekko.cluster.ddata.typed.replicator-message-adapter-unexpected-ask-timeout` | `20s` | ❌ | No feature |

---

## Module: `pekko/persistence` (pekko-persistence)

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `pekko.persistence.journal.plugin` | `""` | ✅ | |
| `pekko.persistence.journal.auto-start-journals` | `[]` | ✅ | Eagerly instantiates journal providers via `persistence.AutoStartJournals` |
| `pekko.persistence.snapshot-store.plugin` | `""` | ✅ | |
| `pekko.persistence.snapshot-store.auto-start-snapshot-stores` | `[]` | ✅ | Eagerly instantiates snapshot-store providers |
| `pekko.persistence.snapshot-store.auto-migrate-manifest` | `"pekko"` | ✅ | Manifest used when migrating legacy snapshot envelopes |
| `pekko.persistence.state-plugin-fallback.recovery-timeout` | `30s` | ✅ | Cap for durable-state plugin fallback recovery |
| `pekko.persistence.max-concurrent-recoveries` | `50` | ✅ | Global semaphore for recoveries |
| `pekko.persistence.fsm.snapshot-after` | `off` | ✅ | Per-FSM opt-in via `WithSnapshotStore`+`SetSnapshotAfter`; save-side wired |
| `pekko.persistence.at-least-once-delivery.redeliver-interval` | `5s` | ❌ | No feature |
| `pekko.persistence.at-least-once-delivery.redelivery-burst-limit` | `10000` | ❌ | No feature |
| `pekko.persistence.at-least-once-delivery.max-unconfirmed-messages` | `100000` | ❌ | No feature |

---

## Module: `pekko/persistence-typed` (pekko-persistence-typed)

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `pekko.persistence.typed.stash-capacity` | `4096` | ✅ | Default for typed persistent actor recovery stash |
| `pekko.persistence.typed.stash-overflow-strategy` | `"drop"` | ✅ | Honors `drop` and `fail` |
| `pekko.persistence.typed.snapshot-on-recovery` | `false` | ✅ | Saves a snapshot at end of recovery |
| `pekko.persistence.typed.log-stashing` | `off` | ❌ | No feature |

---

## Module: `pekko/discovery` (pekko-discovery)

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `pekko.discovery.method` | `"<method>"` | ✅ | |
| `pekko.discovery.config.*` | (service map) | ❌ | No feature (config-based discovery) |
| `pekko.discovery.aggregate.*` | (multi-method) | ❌ | No feature |
| `pekko.discovery.pekko-dns.*` | (DNS) | ❌ | No feature |

---

## Module: `pekko/cluster-metrics` (pekko-cluster-metrics)

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `pekko.cluster.metrics.*` | (all) | ❌ | No feature (JVM-specific: Sigar/JMX) |

---

## Module: `pekko/actor-typed` (pekko-actor-typed)

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `pekko.actor.typed.restart-stash-capacity` | `1000` | ❌ | No feature |
| `pekko.reliable-delivery.*` | (all) | ❌ | No feature |

---

## Module: `pekko/cluster-sharding-typed`

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `pekko.cluster.sharding.number-of-shards` | `1000` | ✅ | Wired to coordinator/region |
| `pekko.cluster.sharded-daemon-process.*` | (all) | ❌ | No feature |

---

## Module: `pekko/coordination` (pekko-coordination)

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `pekko.coordination.lease.lease-class` | `""` | ✅ | Implementation name resolved by `cluster/lease.LeaseManager`; `""` falls back to `"memory"` |
| `pekko.coordination.lease.heartbeat-timeout` | `120s` | ✅ | TTL after which an unrenewed lease becomes available |
| `pekko.coordination.lease.heartbeat-interval` | `12s` | ✅ | Recommended cadence for holders to renew |
| `pekko.coordination.lease.lease-operation-timeout` | `5s` | ✅ | Bound on individual Acquire/Release calls |

The public coordination-lease API is provided by package
`github.com/sopranoworks/gekka/cluster/lease`.  Round-2 session 18 ships the
in-memory reference provider (registered under name `"memory"` by
`lease.NewDefaultManager`); SBR `lease-majority` and Singleton/Sharding
`use-lease` wiring is delivered by sessions 19 and 20.

---

## Summary

### Correctly Parsed (✅): 75+ paths

All core cluster formation, failure detection, SBR (including down-all-when-unstable),
multi-DC, sharding (number-of-shards, role), singleton (role, hand-over-retry-interval),
singleton-proxy (identification-interval, buffer-size), pub-sub (gossip-interval),
persistence (max-concurrent-recoveries, plugin selection), advanced gossip tuning
(probability, TTL, tombstones, reaper), WeaklyUp promotion, app-version, NAT/Docker bind,
discovery, cluster client, management, and remote transport paths.

### Wrong Path (⚠️): 1 path

| Gekka Path | Correct Pekko Path |
|---|---|
| `pekko.cluster.sharding.passivation.idle-timeout` | `pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.timeout` |

### Not Parsed (❌): ~50+ paths

These are paths for features that do not exist in gekka. They fall into categories:

1. **JVM-specific** — class loading, dispatchers, JMX, Sigar metrics
2. **Lease-based SBR** — coordination-lease API ships in `cluster/lease` (round-2 session 18, in-memory ref); SBR/Singleton/Sharding `use-lease` wiring lands in sessions 19 and 20
3. **Reliable delivery** — producer/consumer controller not implemented
4. **Advanced passivation strategies** — only idle timeout supported
5. **Durable distributed data** — persistence-backed DData not implemented
6. **Sharded daemon process** — not implemented
7. **Typed receptionist** — write-consistency, pruning, distributed-key-count
8. **Advanced DData tuning** — delta-crdt, pruning-interval, prefer-oldest
