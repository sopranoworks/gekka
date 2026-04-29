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
| `pekko.remote.artery.large-message-destinations` | `[]` | ✅ | Round-2 session 29 — `LargeMessageRouter` glob-matches recipient paths; matching outbound user messages route via `udpLargeOutbox` onto Aeron stream 3 (`AeronStreamLarge`). TCP separate stream-3 connection deferred to session 30; until then matching TCP recipients fall back to stream 1 |
| `pekko.remote.artery.untrusted-mode` | `off` | ✅ | Round-2 session 31 — when on, drops `Watch`/`Unwatch`/PossiblyHarmful system messages and `PoisonPill`/`Kill` MiscMessage payloads from remote (top-level and nested in `ActorSelection`); cluster traffic bypasses the gate so gossip/heartbeats are unaffected. First drop per (serializerId, manifest) emits WARN; subsequent drops downgrade to DEBUG |
| `pekko.remote.artery.trusted-selection-paths` | `[]` | ✅ | Round-2 session 32 — when `untrusted-mode = on`, inbound `ActorSelection` is delivered only when its resolved path is verbatim in this allowlist (URI scheme/authority stripped before comparison). Empty list under untrusted-mode blocks every selection, including `Identify` |
| `pekko.remote.artery.advanced.maximum-frame-size` | `256 KiB` | ✅ | Configurable via HOCON |
| `pekko.remote.artery.advanced.buffer-pool-size` | `128` | ✅ | Recorded on NodeManager (`EffectiveBufferPoolSize`) for future receive-buffer-pool consumer |
| `pekko.remote.artery.advanced.maximum-large-frame-size` | `2 MiB` | ✅ | Round-2 session 30 — applied to the inbound read loop when `assoc.streamId == AeronStreamLarge` (stream 3) via `effectiveStreamFrameSizeCap`; streams 1/2 keep `maximum-frame-size` |
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
| `pekko.cluster.downing-provider-class` | `""` | ✅ | Round-2 session 27. HOCON parsed into `ClusterConfig.DowningProviderClass`; Pekko / Akka FQCNs ending in `SplitBrainResolverProvider` are normalised to the gekka short name `"split-brain-resolver"` and looked up in `cluster.DowningProviderRegistry` during `gekka.NewCluster`. Unknown names log a warning and fall back to the bundled SBR provider. Empty value defaults to SBR. Also feeds the InitJoin compatibility check. |
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
| `pekko.cluster.distributed-data.enabled` | `on` | ✅ | `DistributedDataConfig.Enabled` — gates Replicator startup |
| `pekko.cluster.distributed-data.gossip-interval` | `2s` | ✅ | |
| `pekko.cluster.distributed-data.name` | `ddataReplicator` | ❌ | No feature (hardcoded) |
| `pekko.cluster.distributed-data.role` | `""` | ❌ | No feature |
| `pekko.cluster.distributed-data.notify-subscribers-interval` | `500ms` | ❌ | No feature |
| `pekko.cluster.distributed-data.max-delta-elements` | `500` | ❌ | No feature |
| `pekko.cluster.distributed-data.pruning-interval` | `120s` | ❌ | No feature |
| `pekko.cluster.distributed-data.max-pruning-dissemination` | `300s` | ❌ | No feature |
| `pekko.cluster.distributed-data.delta-crdt.enabled` | `on` | ❌ | No feature |
| `pekko.cluster.distributed-data.delta-crdt.max-delta-size` | `50` | ❌ | No feature |
| `pekko.cluster.distributed-data.durable.enabled` | `off` | ✅ | Round-2 session 23. `DistributedDataConfig.DurableEnabled` — implicitly true when `durable.keys` is non-empty; explicit `on` lights up the bbolt backend before any keys are configured. |
| `pekko.cluster.distributed-data.durable.keys` | `[]` | ✅ | Round-2 session 23. `DistributedDataConfig.DurableKeys` filters which CRDT keys are persisted (prefix glob via trailing `*`). |
| `pekko.cluster.distributed-data.durable.pruning-marker-time-to-live` | `10d` | ✅ | Round-2 session 23. `DistributedDataConfig.DurablePruningMarkerTimeToLive` — when DurableEnabled, applied as the marker TTL whenever it exceeds the non-durable `pruning-marker-time-to-live`, so durable replicas can rejoin without resurrecting stale state. |
| `pekko.cluster.distributed-data.durable.lmdb.dir` | `ddata` | ✅ | Round-2 session 23. `DistributedDataConfig.DurableLmdbDir` → `BoltDurableStoreOptions.Dir`. |
| `pekko.cluster.distributed-data.durable.lmdb.map-size` | `100 MiB` | ✅ | Round-2 session 23. `DistributedDataConfig.DurableLmdbMapSize` → `BoltDurableStoreOptions.MapSize` (hard cap enforced via pre-flight file-size check). |
| `pekko.cluster.distributed-data.durable.lmdb.write-behind-interval` | `off` | ✅ | Round-2 session 23. `DistributedDataConfig.DurableLmdbWriteBehindInterval` → `BoltDurableStoreOptions.WriteBehindInterval`; coalesces same-key writes per flush tick. |
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
| `pekko.cluster.sharding.passivation.strategy` | `"default-idle-strategy"` | ✅ | Round-2 session 24. Recognises `default-idle-strategy`, `least-recently-used` (Pekko canonical) and `custom-lru-strategy` (legacy alias normalised at parse time). |
| `pekko.cluster.sharding.passivation.least-recently-used-strategy.active-entity-limit` | `100000` | ✅ | Round-2 session 24. `ShardingConfig.PassivationActiveEntityLimit`; legacy `custom-lru-strategy.active-entity-limit` still parsed as a fallback. |
| `pekko.cluster.sharding.passivation.least-recently-used-strategy.replacement.policy` | `"least-recently-used"` | ✅ | Round-2 session 24. `ShardingConfig.PassivationReplacementPolicy`. |
| `pekko.cluster.sharding.passivation.most-recently-used-strategy.active-entity-limit` | `100000` | ✅ | Round-2 session 25. Active strategy limit reaches `ShardingConfig.PassivationActiveEntityLimit`; the eviction loop picks the freshest entity. |
| `pekko.cluster.sharding.passivation.least-frequently-used-strategy.active-entity-limit` | `100000` | ✅ | Round-2 session 25. Active strategy limit reaches `ShardingConfig.PassivationActiveEntityLimit`; the eviction loop picks the entity with the lowest cumulative access count, ties broken by oldest activity. |
| `pekko.cluster.sharding.passivation.least-frequently-used-strategy.dynamic-aging` | `off` | ⚠️ | Round-2 session 25. `ShardingConfig.PassivationLFUDynamicAging` is parsed for forward-compat; the actual frequency-aging loop will land in a later session. |
| `pekko.cluster.sharding.passivation.strategy = default-strategy` | `"default-idle-strategy"` | ✅ | Round-2 session 26. Selects the W-TinyLFU composite strategy implemented in `cluster/sharding/passivation_composite.go`. The plan-internal alias `"composite-strategy"` is normalised at parse time so configs that use either convention reach the same code path. |
| `pekko.cluster.sharding.passivation.default-strategy.active-entity-limit` | `100000` | ✅ | Round-2 session 26. Active strategy limit reaches `ShardingConfig.PassivationActiveEntityLimit`; the composite strategy splits it between admission-window and main areas via `PassivationWindowProportion`. |
| `pekko.cluster.sharding.passivation.default-strategy.admission.window.policy` | `"least-recently-used"` | ✅ | Round-2 session 26. `ShardingConfig.PassivationWindowPolicy`. Currently only `"least-recently-used"` is honoured at runtime; non-empty values activate the admission window. |
| `pekko.cluster.sharding.passivation.default-strategy.admission.window.optimizer` | `"hill-climbing"` | ⚠️ | Round-2 session 26. Parsed for forward-compat with Pekko configs; the adaptive resizing loop lands in a later session. |
| `pekko.cluster.sharding.passivation.default-strategy.admission.filter` | `"frequency-sketch"` | ✅ | Round-2 session 26. `ShardingConfig.PassivationFilter`. Recognised values: `"frequency-sketch"` (admission filter active), `"off"` / `"none"` (composite degrades to LRU window + LRU main with no admission gate). |
| `pekko.cluster.sharding.passivation.default-strategy.replacement.policy` | `"least-recently-used"` | ✅ | Round-2 session 26. Per-strategy override for `ShardingConfig.PassivationReplacementPolicy`; wins over the LRU-strategy block when both are present. |
| `pekko.cluster.sharding.passivation.strategy-defaults.admission.window.proportion` | `0.01` | ✅ | Round-2 session 26. Admission-window size as a fraction of the active-entity-limit. `default-strategy.admission.window.proportion` overrides per-strategy. |
| `pekko.cluster.sharding.passivation.strategy-defaults.admission.window.minimum-proportion` | `0.01` | ⚠️ | Round-2 session 26. Parsed; consumed once the hill-climbing optimizer lands. |
| `pekko.cluster.sharding.passivation.strategy-defaults.admission.window.maximum-proportion` | `1.0` | ⚠️ | Round-2 session 26. Parsed; consumed once the hill-climbing optimizer lands. |
| `pekko.cluster.sharding.passivation.strategy-defaults.admission.frequency-sketch.depth` | `4` | ✅ | Round-2 session 26. Count-min-sketch row count; threaded into `cluster/sharding/passivation_admission.go`. |
| `pekko.cluster.sharding.passivation.strategy-defaults.admission.frequency-sketch.counter-bits` | `4` | ⚠️ | Round-2 session 26. Pekko documents 2/4/8/16/32/64; gekka stores 4-bit counters regardless. Parsed for forward-compat. |
| `pekko.cluster.sharding.passivation.strategy-defaults.admission.frequency-sketch.width-multiplier` | `4` | ✅ | Round-2 session 26. Sketch width = active-entity-limit × multiplier (rounded up to a power of two, clamped to 64). |
| `pekko.cluster.sharding.passivation.strategy-defaults.admission.frequency-sketch.reset-multiplier` | `10.0` | ✅ | Round-2 session 26. Triggers the sketch's halving reset every (active-entity-limit × multiplier) accesses. |
| `pekko.cluster.sharding.guardian-name` | `"sharding"` | ❌ | No feature |
| `pekko.cluster.sharding.role` | `""` | ✅ | Filters shard allocation by role |
| `pekko.cluster.sharding.remember-entities-store` | `"ddata"` | ✅ | Round-2 session 35. `"ddata"` wires `DDataEntityStore` (existing); `"eventsourced"` wires `EventSourcedEntityStore` (new) — both implement `ShardStore` and are selected in `sharding.go` from `cluster.cfg.Sharding.RememberEntitiesStore`. |
| `pekko.cluster.sharding.passivate-idle-entity-after` | `null` | ❌ | Deprecated in Pekko |
| `pekko.cluster.sharding.number-of-shards` | `1000` | ✅ | Wired to coordinator/region |
| `pekko.cluster.sharding.rebalance-interval` | `10s` | ✅ | Applied to ShardCoordinator.RebalanceInterval |
| `pekko.cluster.sharding.least-shard-allocation-strategy.rebalance-threshold` | `1` | ✅ | Applied to NewLeastShardAllocationStrategy(threshold) |
| `pekko.cluster.sharding.least-shard-allocation-strategy.max-simultaneous-rebalance` | `3` | ✅ | Applied to NewLeastShardAllocationStrategy(maxSimultaneous) |
| `pekko.cluster.sharding.least-shard-allocation-strategy.rebalance-absolute-limit` | `0` | ✅ | Round-2 session 33 — when `> 0` selects the Pekko 1.0+ two-phase `LeastShardAllocationStrategyV2` (`cluster/sharding/strategy.go`); `0` keeps the legacy threshold-based strategy. |
| `pekko.cluster.sharding.least-shard-allocation-strategy.rebalance-relative-limit` | `0.1` | ✅ | Round-2 session 33 — fraction of total shards capping each rebalance round under V2 (`max(1, min(int(rel*total), absolute))`). |
| `pekko.cluster.sharding.external-shard-allocation-strategy.client-timeout` | `5s` | ✅ | `cluster/sharding/strategy.go` — bound on external strategy RPC calls. |
| `pekko.cluster.sharding.event-sourced-remember-entities-store.max-updates-per-write` | `100` | ✅ | Round-2 session 34 — Shard buffers EntityStarted/EntityStopped events; the buffer is flushed in a single AsyncWriteMessages call once it hits the cap, with a final flush in PostStop. Cap of `0` keeps the legacy one-event-per-write path. |
| `pekko.cluster.sharding.state-store-mode` | `"ddata"` | ☕ | JVM-only — gekka commits to `ddata` exclusively for sharding state; the `persistence` mode and its tunables (`snapshot-after`, `keep-nr-of-batches`, `journal-plugin-id`, `snapshot-plugin-id`) are N/A in gekka. |
| `pekko.cluster.sharding.snapshot-after` | `1000` | ☕ | JVM-only — `state-store-mode = persistence` subset; gekka uses `ddata`. |
| `pekko.cluster.sharding.keep-nr-of-batches` | `2` | ☕ | JVM-only — `state-store-mode = persistence` subset; gekka uses `ddata`. |
| `pekko.cluster.sharding.journal-plugin-id` | `""` | ☕ | JVM-only — `state-store-mode = persistence` subset; gekka uses `ddata`. |
| `pekko.cluster.sharding.snapshot-plugin-id` | `""` | ☕ | JVM-only — `state-store-mode = persistence` subset; gekka uses `ddata`. |
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
| `pekko.cluster.sharding.use-lease` | `""` | ✅ | Resolves a Lease from Cluster.LeaseManager; every Shard acquires before becoming active and releases on handoff/stop (Round-2 session 20) |
| `pekko.cluster.sharding.lease-retry-interval` | `5s` | ✅ | Backoff between Shard Acquire retries when a prior call returned false (Round-2 session 20) |

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
| `use-lease` | `""` | ✅ | Resolves a Lease from Cluster.LeaseManager; manager acquires before spawn, releases on handoff |
| `lease-retry-interval` | `5s` | ✅ | Backoff between Acquire retries when prior call returned false |

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
| `pekko.persistence.journal.proxy.target-journal-plugin-id` | (required) | ✅ | Round-2 session 36 — registry name of the real journal the proxy delegates to (`persistence/proxy_journal.go`). |
| `pekko.persistence.journal.proxy.init-timeout` | `10s` | ✅ | Round-2 session 36 — first-use resolution retry budget (100 ms tick). |
| `pekko.persistence.journal.proxy.start-target-journal` | `on` | ⚠️ | Round-2 session 36 — only `on` (in-process) is implemented; `off` is rejected at construction time pending Artery cross-process transport. |
| `pekko.persistence.snapshot-store.plugin` | `""` | ✅ | |
| `pekko.persistence.snapshot-store.auto-start-snapshot-stores` | `[]` | ✅ | Eagerly instantiates snapshot-store providers |
| `pekko.persistence.snapshot-store.auto-migrate-manifest` | `"pekko"` | ✅ | Manifest used when migrating legacy snapshot envelopes |
| `pekko.persistence.snapshot-store.proxy.target-snapshot-store-plugin-id` | (required) | ✅ | Round-2 session 37 — registry name of the real snapshot store the proxy delegates to (`persistence/proxy_snapshot.go`). |
| `pekko.persistence.snapshot-store.proxy.init-timeout` | `10s` | ✅ | Round-2 session 37 — first-use resolution retry budget (100 ms tick). |
| `pekko.persistence.snapshot-store.proxy.start-target-snapshot-store` | `on` | ⚠️ | Round-2 session 37 — only `on` (in-process) is implemented; `off` is rejected at construction time pending Artery cross-process transport. |
| `pekko.persistence.state-plugin-fallback.recovery-timeout` | `30s` | ✅ | Cap for durable-state plugin fallback recovery |
| `pekko.persistence.journal-plugin-fallback.circuit-breaker.max-failures` | `10` | ✅ | Round-2 session 38 — consecutive failures that trip the journal breaker (`persistence/circuit_breaker.go`). |
| `pekko.persistence.journal-plugin-fallback.circuit-breaker.call-timeout` | `10s` | ✅ | Round-2 session 38 — per-call deadline; expiries count as failures. |
| `pekko.persistence.journal-plugin-fallback.circuit-breaker.reset-timeout` | `30s` | ✅ | Round-2 session 38 — open→half-open dwell time. |
| `pekko.persistence.journal-plugin-fallback.replay-filter.mode` | `repair-by-discard-old` | ✅ | Round-2 session 38 — `off`, `warn`, `fail`, or `repair-by-discard-old` (`persistence/replay_filter.go`). |
| `pekko.persistence.journal-plugin-fallback.replay-filter.window-size` | `100` | ✅ | Round-2 session 38 — look-ahead window in events for writer disambiguation. |
| `pekko.persistence.journal-plugin-fallback.replay-filter.max-old-writers` | `10` | ✅ | Round-2 session 38 — bounded LRU of stale writer UUIDs to suppress. |
| `pekko.persistence.journal-plugin-fallback.replay-filter.debug` | `off` | ✅ | Round-2 session 38 — verbose per-event log line during recovery. |
| `pekko.persistence.journal-plugin-fallback.recovery-event-timeout` | `30s` | ✅ | Round-2 session 38 — inter-event silence ceiling during replay (`RecoveryEventTimeoutJournal`). |
| `pekko.persistence.snapshot-store-plugin-fallback.circuit-breaker.max-failures` | `5` | ✅ | Round-2 session 38 — snapshot-store breaker trip count. |
| `pekko.persistence.snapshot-store-plugin-fallback.circuit-breaker.call-timeout` | `20s` | ✅ | Round-2 session 38 — snapshot-store per-call deadline. |
| `pekko.persistence.snapshot-store-plugin-fallback.circuit-breaker.reset-timeout` | `60s` | ✅ | Round-2 session 38 — snapshot-store open→half-open dwell time. |
| `pekko.persistence.max-concurrent-recoveries` | `50` | ✅ | Global semaphore for recoveries |
| `pekko.persistence.fsm.snapshot-after` | `off` | ✅ | Per-FSM opt-in via `WithSnapshotStore`+`SetSnapshotAfter`; save-side wired |
| `pekko.persistence.at-least-once-delivery.redeliver-interval` | `5s` | ✅ | Round-2 session 39 — `AtLeastOnceDelivery` redelivery period. Wired through `persistence.SetDefaultAtLeastOnceConfig`. Round-2 session 40 — driven by `persistence.Scheduler` (`aald_scheduler.go`) so persistent actors can wire redelivery + warn callbacks via a single object. |
| `pekko.persistence.at-least-once-delivery.redelivery-burst-limit` | `10000` | ✅ | Round-2 session 39 — caps redeliveries fired per redeliver tick. Integration test (`persistence/aald_integration_test.go`) exercises the loop end-to-end against a flaky-receiver harness. |
| `pekko.persistence.at-least-once-delivery.warn-after-number-of-unconfirmed-attempts` | `5` | ✅ | Round-2 session 39 — per-message attempt threshold surfaced via `MaxAttempts()`. Round-2 session 40 wires the threshold into a once-per-delivery `WarnFunc` callback fired by the redelivery loop; `SetDeliverySnapshot` pre-marks restored entries above the threshold so recovery does not re-warn. |
| `pekko.persistence.at-least-once-delivery.max-unconfirmed-messages` | `100000` | ✅ | Round-2 session 39 — `Deliver` returns `ErrMaxUnconfirmedMessagesExceeded` once the ceiling is reached. Round-2 session 40 — integration test verifies pending ids survive a snapshot-restore cycle and are still confirmable by a late ack. |

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
`lease.NewDefaultManager`).  SBR `lease-majority` (session 19) and
Singleton/Sharding `use-lease` (session 20) wiring is now in place: each
consumer resolves its lease through `Cluster.LeaseManager()` so operators can
register additional providers (e.g. a Kubernetes-leases extension) without
touching the consumer code.

---

## Summary

### Correctly Parsed (✅): ~250 paths

Round-2 sessions 01–40 closed the entire portable Pekko surface.  Coverage now
includes core cluster formation, failure detection (incl. cross-DC), SBR
(`down-all-when-unstable` + `lease-majority`), full advanced gossip tuning
(probability, TTL, tombstones, reaper, prune timers), Artery transport
(handshake budgets, idle/quarantine sweeps, restart caps, compression tables,
TCP timeouts, `untrusted-mode`/`trusted-selection-paths`,
`large-message-destinations` + UDP stream-3 routing,
`maximum-large-frame-size` cap), full sharding (every retry/backoff path,
`use-lease`, advanced passivation including W-TinyLFU composite, allocation V1
& V2, EventSourced remember-entities store with batched writes), full
distributed-data (durable bbolt store, `prune-marker-time-to-live`,
`log-data-size-exceeding`, `recovery-timeout`), full singleton/proxy/pub-sub
(incl. `use-lease`, `send-to-dead-letters-when-no-subscribers`), full cluster
client + receptionist (`response-tunnel-receive-timeout`,
`failure-detection-interval`), full persistence (proxy journal/snapshot,
plugin-fallback circuit-breaker + replay-filter + recovery-event-timeout, FSM
snapshots, AtLeastOnceDelivery scheduler + warn callback), `pekko.actor.debug`
sub-flags, `log-config-on-start`, NAT/Docker bind, discovery, management.

### Wrong Path (⚠️): 1 path

| Gekka Path | Correct Pekko Path |
|---|---|
| `pekko.cluster.sharding.passivation.idle-timeout` | `pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.timeout` |

### Not Parsed: residual JVM-only paths (☕)

After Round-2 session 41 the only remaining unwired paths on the Pekko surface
are JVM-only ones — see `docs/LEFTWORKS.md` §9 for the full list. Highlights:

1. **JVM-specific** — class loading, dispatchers, JMX, Sigar metrics, scheduler internals
2. **Reliable delivery typed protocol** — producer/consumer controller not implemented (Pekko's `pekko.reliable-delivery.*` is distinct from `at-least-once-delivery.*`, which is ✅ DONE)
3. **Sharded daemon process** — not implemented (Pekko-typed feature)
4. **Typed receptionist** — write-consistency, pruning, distributed-key-count
5. **Sharding `state-store-mode = persistence` subset** — gekka commits to `ddata`; `state-store-mode`, `snapshot-after`, `keep-nr-of-batches`, `journal-plugin-id`, `snapshot-plugin-id`
6. **Discovery aggregate / pekko-dns** — not implemented (out of scope; gekka uses k8s-extension)
7. **Cluster metrics (Sigar/JMX)** — JVM-only

### Round-2 close-out

Session 41 (2026-04-30) marks Round-2 complete: 0 HARDCODED + 0 NO FEATURE
on the portable surface. Full test gate (unit + integration + sbt
multi-jvm:test 3/3) passes. See
`docs/superpowers/plans/2026-04-24-config-completeness-round2.md` for the
session ledger.
