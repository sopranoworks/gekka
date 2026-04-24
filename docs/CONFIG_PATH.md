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
| `pekko.log-config-on-start` | `off` | ❌ | No feature |
| `pekko.log-dead-letters` | `10` | ❌ | No feature |
| `pekko.log-dead-letters-during-shutdown` | `off` | ❌ | No feature |
| `pekko.actor.provider` | `"local"` | ✅ | Used for protocol detection |
| `pekko.actor.default-dispatcher.*` | (complex) | ❌ | Gekka uses `pekko.dispatchers.*` instead |
| `pekko.actor.internal-dispatcher.*` | (complex) | ❌ | No feature |
| `pekko.actor.deployment.{path}.*` | (various) | ✅ | Router deployment |
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
| `pekko.cluster.publish-stats-interval` | `off` | ❌ | No feature |
| `pekko.cluster.gossip-different-view-probability` | `0.8` | ✅ | Prefers different-view targets |
| `pekko.cluster.reduce-gossip-different-view-probability` | `400` | ✅ | Halves probability at scale |
| `pekko.cluster.prune-gossip-tombstones-after` | `24h` | ✅ | Prunes removed-member tombstones |
| `pekko.cluster.configuration-compatibility-check.enforce-on-join` | `on` | ✅ | Validates incoming InitJoin config |

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
| `failure-detector.*` | (various) | ❌ | No feature (cross-DC FD) |

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
| `lease-majority.*` | (various) | ❌ | No feature |

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
| `initial-contacts` | `[]` | ✅ | Via cluster/client/ |
| `establishing-get-contacts-interval` | `3s` | ✅ | |
| `refresh-contacts-interval` | `60s` | ✅ | |
| `heartbeat-interval` | `2s` | ✅ | |
| `acceptable-heartbeat-pause` | `13s` | ✅ | |
| `buffer-size` | `1000` | ✅ | |
| `reconnect-timeout` | `off` | ✅ | |

### pekko.cluster.client.receptionist

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `name` | `receptionist` | ✅ | |
| `role` | `""` | ✅ | |
| `number-of-contacts` | `3` | ✅ | |
| `heartbeat-interval` | `2s` | ✅ | |
| `acceptable-heartbeat-pause` | `13s` | ✅ | |
| `failure-detection-interval` | `2s` | ❌ | No feature |

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
| `pekko.persistence.journal.auto-start-journals` | `[]` | ❌ | No feature |
| `pekko.persistence.snapshot-store.plugin` | `""` | ✅ | |
| `pekko.persistence.snapshot-store.auto-start-snapshot-stores` | `[]` | ❌ | No feature |
| `pekko.persistence.max-concurrent-recoveries` | `50` | ✅ | Global semaphore for recoveries |
| `pekko.persistence.at-least-once-delivery.redeliver-interval` | `5s` | ❌ | No feature |
| `pekko.persistence.at-least-once-delivery.redelivery-burst-limit` | `10000` | ❌ | No feature |
| `pekko.persistence.at-least-once-delivery.max-unconfirmed-messages` | `100000` | ❌ | No feature |

---

## Module: `pekko/persistence-typed` (pekko-persistence-typed)

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `pekko.persistence.typed.stash-capacity` | `4096` | ❌ | No feature |
| `pekko.persistence.typed.stash-overflow-strategy` | `"drop"` | ❌ | No feature |
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
2. **Lease-based SBR** — requires coordination lease (no Go impl)
3. **Reliable delivery** — producer/consumer controller not implemented
4. **Advanced passivation strategies** — only idle timeout supported
5. **Durable distributed data** — persistence-backed DData not implemented
6. **Sharded daemon process** — not implemented
7. **Typed receptionist** — write-consistency, pruning, distributed-key-count
8. **Advanced DData tuning** — delta-crdt, pruning-interval, prefer-oldest
