# Configuration Path Compatibility: Pekko reference.conf vs Gekka

Gekka uses HOCON for layered configuration via [`gekka-config`](https://github.com/sopranoworks/gekka-config). Standard `pekko.*` and `akka.*` keys are accepted directly — existing Pekko/Akka HOCON works without translation.

This document is the comprehensive Pekko-vs-Gekka audit: every leaf path in
`pekko/{actor,remote,cluster,cluster-sharding,cluster-tools,distributed-data,persistence,discovery,coordination,reliable-delivery}/reference.conf`
classified by what gekka does with it at runtime.

**For end users**, the [Quick Reference — Common Settings](#quick-reference--common-settings) section below covers the keys most workloads need to tune. The per-module audit tables that follow are the canonical source of truth for compatibility, deferrals, and Go/JVM gaps.

---

## Legend

- ✅ = Parsed AND consumed in gekka
- ⚠️ = Parsed for forward-compat at the canonical Pekko path; consumer deferred (Note states what's deferred)
- ☕ = JVM-only — no equivalent capability exists in the Go runtime (e.g., dispatcher hierarchy, JMX/Sigar metrics, Java scheduler internals)
- 🚫 = Go/JVM API-shape incompatibility — the HOCON value or path semantics doesn't transfer to Go (e.g., the value is a Java FQCN to be loaded via reflection; gekka has its own Go impl wired directly)
- ❌ = Not implemented in gekka — portable in principle, no current bridge (tracked in `docs/LEFTWORKS.md` §11)

---

## Quick Reference — Common Settings

The keys most users actually tune. All are Pekko-compatible (drop them into your existing `pekko.*` HOCON).

### Cluster

| Key | Default | Description |
|---|---|---|
| `pekko.cluster.min-nr-of-members` | `1` | Min members before leader promotes Joining → Up |
| `pekko.cluster.retry-unsuccessful-join-after` | `10s` | InitJoin retry interval |
| `pekko.cluster.gossip-interval` | `1s` | Duration between gossip rounds |
| `pekko.cluster.failure-detector.threshold` | `8.0` | Phi threshold for unreachable declaration |
| `pekko.cluster.failure-detector.heartbeat-interval` | `1s` | Heartbeat send interval |
| `pekko.cluster.failure-detector.acceptable-heartbeat-pause` | `3s` | Tolerable heartbeat gap |
| `pekko.cluster.failure-detector.max-sample-size` | `1000` | Heartbeat history window |
| `pekko.cluster.failure-detector.monitored-by-nr-of-members` | `9` | Max heartbeat targets per node |
| `pekko.cluster.leader-actions-interval` | `1s` | Independent leader action ticker |
| `pekko.cluster.shutdown-after-unsuccessful-join-seed-nodes` | `off` | Shutdown on join timeout |
| `pekko.cluster.allow-weakly-up-members` | `7s` | WeaklyUp promotion timeout (`off` = disabled) |
| `pekko.cluster.app-version` | `"0.0.0"` | Application version advertised in handshake |
| `pekko.cluster.gossip-different-view-probability` | `0.8` | Prefer different-view gossip targets |
| `pekko.cluster.down-removal-margin` | `off` | Delay Down → Removed transition |
| `pekko.cluster.run-coordinated-shutdown-when-down` | `on` | Trigger shutdown when self is downed |
| `pekko.cluster.role.{name}.min-nr-of-members` | — | Per-role gating before leader promotes |

### Singleton & Singleton Proxy

| Key | Default | Description |
|---|---|---|
| `pekko.cluster.singleton.role` | `""` | Restrict singleton to nodes with this role |
| `pekko.cluster.singleton.hand-over-retry-interval` | `1s` | Retry interval during leadership transfer |
| `pekko.cluster.singleton-proxy.singleton-identification-interval` | `1s` | Periodic re-resolution of singleton location |
| `pekko.cluster.singleton-proxy.buffer-size` | `1000` | Buffer messages while singleton is unknown |

### Sharding

| Key | Default | Description |
|---|---|---|
| `pekko.cluster.sharding.number-of-shards` | `1000` | Total shard count (immutable after start) |
| `pekko.cluster.sharding.role` | `""` | Restrict sharding to nodes with this role |
| `pekko.cluster.sharding.remember-entities` | `off` | Persist entity lifecycle for auto-respawn |
| `pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.timeout` | `120s` | Stop idle entities after this duration |

### Split Brain Resolver

| Key | Default | Description |
|---|---|---|
| `pekko.cluster.split-brain-resolver.active-strategy` | `""` | `keep-majority`, `keep-oldest`, `static-quorum`, `keep-referee`, `lease-majority`, `down-all` |
| `pekko.cluster.split-brain-resolver.stable-after` | `20s` | Stability window before decision |
| `pekko.cluster.split-brain-resolver.down-all-when-unstable` | `on` | Down all nodes if instability persists |

### Distributed Data & Pub-Sub

| Key | Default | Description |
|---|---|---|
| `pekko.cluster.distributed-data.enabled` | `false` | Enable the CRDT replicator |
| `pekko.cluster.distributed-data.gossip-interval` | `2s` | Interval between DData gossip rounds |
| `pekko.cluster.pub-sub.gossip-interval` | `1s` | Subscription gossip interval |

### Persistence

| Key | Default | Description |
|---|---|---|
| `pekko.persistence.max-concurrent-recoveries` | `50` | Global semaphore for concurrent actor recoveries |

### Remote Transport (Artery)

| Key | Default | Description |
|---|---|---|
| `pekko.remote.artery.canonical.hostname` | `<getHostAddress>` | Advertised hostname |
| `pekko.remote.artery.canonical.port` | `17355` | Advertised port |
| `pekko.remote.artery.bind.hostname` | `""` | NAT/Docker: bind address (advertise canonical) |
| `pekko.remote.artery.bind.port` | `""` | NAT/Docker: bind port (advertise canonical) |
| `pekko.remote.artery.advanced.maximum-frame-size` | `256 KiB` | Maximum Artery frame payload size |
| `pekko.remote.artery.advanced.maximum-large-frame-size` | `2 MiB` | Frame cap on stream 3 (large) |
| `pekko.remote.artery.advanced.outbound-lanes` | `1` | Per-peer parallel `streamId=2` TCP sockets (sub-plan 8f) |
| `pekko.remote.artery.advanced.inbound-lanes` | `4` | Per-association inbound dispatch fan-out (sub-plan 8f) |
| `pekko.remote.artery.large-message-destinations` | `[]` | Glob patterns routed to stream 3 |

### Management (Pekko Management-compatible)

| Key | Default | Description |
|---|---|---|
| `pekko.management.http.port` | `8558` | TCP port for the HTTP Management API |
| `pekko.management.http.hostname` | `127.0.0.1` | Binding interface for the Management API |
| `pekko.management.http.enabled` | `false` | Explicitly enable/disable the Management API |

**Auto-Enable Logic.** If either `pekko.management.http.hostname` or `pekko.management.http.port` is explicitly defined, the Management Server is enabled automatically (`enabled = true`).

### Other

| Key | Default | Description |
|---|---|---|
| `pekko.loglevel` | `INFO` | Minimum log level (`DEBUG`, `INFO`, `WARN`, `ERROR`) — *parsed only; consumer deferred to the external-log-server cycle* |
| `gekka.telemetry.exporter.otlp.endpoint` | `""` | OTLP/HTTP collector endpoint for metrics/traces |

---

## Module: `pekko/actor` (pekko-actor)

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `pekko.loglevel` | `"INFO"` | ❌ | Not implemented — parsed into `NodeConfig.LogLevel` (cluster.go:223) but no consumer; deferred to the future external-log-server logging cycle (gekka's logger is being replaced). Tracked in `docs/LEFTWORKS.md` §11 |
| `pekko.stdout-loglevel` | `"WARNING"` | ❌ | Not implemented — deferred to the future external-log-server logging cycle (separate from the immediate post-audit roadmap). Tracked in `docs/LEFTWORKS.md` §11 |
| `pekko.log-config-on-start` | `off` | ✅ | When on, NewCluster emits the resolved ClusterConfig at INFO via slog |
| `pekko.log-dead-letters` | `10` | ✅ | `ClusterConfig.LogDeadLetters` — cap on dead-letter log spam (off=disable, N=log first N occurrences) |
| `pekko.log-dead-letters-during-shutdown` | `on` | ✅ | `ClusterConfig.LogDeadLettersDuringShutdown` — when off, suppresses dead-letter logging during coordinated shutdown |
| `pekko.actor.debug.receive` | `off` | ✅ | DEBUG slog via `ActorDebugConfig.LogActorReceive` |
| `pekko.actor.debug.autoreceive` | `off` | ✅ | DEBUG slog via `ActorDebugConfig.LogActorAutoreceive` (PoisonPill/Kill/Terminate) |
| `pekko.actor.debug.lifecycle` | `off` | ✅ | DEBUG slog via `ActorDebugConfig.LogActorLifecycle` (started/stopped/restarted) |
| `pekko.actor.debug.fsm` | `off` | ✅ | DEBUG slog via `ActorDebugConfig.LogActorFSM` |
| `pekko.actor.debug.event-stream` | `off` | ✅ | DEBUG slog via `ActorDebugConfig.LogActorEventStream` |
| `pekko.actor.debug.unhandled` | `off` | ✅ | DEBUG slog via `ActorDebugConfig.LogActorUnhandled` |
| `pekko.actor.debug.router-misconfiguration` | `off` | ✅ | WARN slog via `ActorDebugConfig.LogRouterMisconfiguration` (matches Pekko severity) |
| `pekko.actor.provider` | `"local"` | ✅ | Used for protocol detection |
| `pekko.actor.default-dispatcher.*` | (complex) | ☕ | Go has no dispatcher hierarchy; gekka uses goroutines + per-actor mailboxes |
| `pekko.actor.internal-dispatcher.*` | (complex) | ☕ | Same — Go runtime has no dispatcher hierarchy |
| `pekko.actor.deployment.{path}.*` | (various) | ✅ | Router deployment |
| `pekko.actor.deployment.{path}.cluster.max-nr-of-instances-per-node` | `1` | ✅ | Caps local routees on a cluster pool router (Round-2 session 11) |
| `pekko.actor.serializers.*` | (registry) | ✅ | Via LoadFromConfig |
| `pekko.actor.serialization-bindings.*` | (registry) | ✅ | Via LoadFromConfig |
| `pekko.actor.default-resizer.*` | (various) | ✅ | In reference.conf |

### pekko.actor — mailboxes

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `pekko.actor.default-mailbox.mailbox-type` | `UnboundedMailbox` | ✅ | Parsed at `hocon_config.go:extractMailboxConfig` into `MailboxConfig.DefaultType`; consumed by `localActorSystem.resolvePhase1Mailbox` and `Cluster.resolvePhase1Mailbox` to select the factory from the `actor/mailbox` registry when no `Props.MailboxName` is supplied. Both short ids ("unbounded", "bounded", "unbounded-control-aware", "bounded-control-aware") and Pekko/Akka FQCNs are accepted. |
| `pekko.actor.default-mailbox.mailbox-capacity` | `1000` | ✅ | Parsed into `MailboxConfig.DefaultCapacity` and forwarded to `mailbox.GlobalDefaults().Capacity`; applied to `BoundedMailbox` and `BoundedControlAwareMailbox` factories at construction time. |
| `pekko.actor.default-mailbox.mailbox-push-timeout-time` | `10s` | ✅ | Parsed into `MailboxConfig.DefaultPushTimeout` and forwarded to `mailbox.GlobalDefaults().PushTimeout`; applied to `BoundedMailbox` and `BoundedControlAwareMailbox` per-Enqueue timeout protocol. Zero ("0s") means block-forever (Pekko parity). |
| `pekko.actor.mailbox.requirements.*` | (binding map) | ✅ | Parsed at `hocon_config.go:extractMailboxConfig` into `MailboxConfig.Requirements` (map of requirement-FQCN → mailbox factory id); seeded by `pekko.dispatch.{Unbounded,Bounded}ControlAwareMailbox` blocks. Consumed by `mailbox.ResolveForActor` at SpawnActor time. Actors declare a requirement by implementing `mailbox.RequiresControlAwareMessageQueueSemantics`; mismatch with the bound factory is a HARD start failure. |
| `pekko.dispatch.UnboundedControlAwareMailbox` (binding) | — | ✅ | Block parsed at `hocon_config.go:extractMailboxConfig`; its `mailbox-type` value is registered into `MailboxConfig.Requirements` for the four ControlAware FQCN keys (Pekko + Akka spellings, plus the broader `ControlAwareMessageQueueSemantics` superclass). Resolves to the gekka `unbounded-control-aware` factory (sub-commit 1.4) at SpawnActor time. |
| `pekko.dispatch.BoundedControlAwareMailbox` (binding) | — | ✅ | Block parsed at `hocon_config.go:extractMailboxConfig`; its `mailbox-type` value is registered into `MailboxConfig.Requirements` for the BoundedControlAware FQCN keys (Pekko + Akka spellings). Resolves to the gekka `bounded-control-aware` factory (sub-commit 1.5) at SpawnActor time. |

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
| `pekko.remote.artery.advanced.buffer-pool-size` | `128` | ✅ | Sizes the regular receive buffer pool (`NodeManager.bufferPool`, lazily built by `streamBufferPool` for streams 1/2). Each pool buffer is pre-sized to `maximum-frame-size`. The Artery TCP read loop (`tcpArteryReadLoop`) checks out one buffer on entry and returns it on exit, eliminating per-frame allocations and bounding the steady-state heap footprint. Wired in Phase 3.2 (sub-commit 3add8d0) |
| `pekko.remote.artery.advanced.maximum-large-frame-size` | `2 MiB` | ✅ | Round-2 session 30 — applied to the inbound read loop when `assoc.streamId == AeronStreamLarge` (stream 3) via `effectiveStreamFrameSizeCap`; streams 1/2 keep `maximum-frame-size` |
| `pekko.remote.artery.advanced.large-buffer-pool-size` | `32` | ✅ | Sizes the large-stream receive buffer pool (`NodeManager.largeBufferPool`, lazily built by `streamBufferPool` for stream 3). Each pool buffer is pre-sized to `maximum-large-frame-size`. The pool is fully independent from the regular pool — a burst of large traffic never drains the ordinary buffers and vice versa. Wired in Phase 3.2 (sub-commit 3add8d0) |
| `pekko.remote.artery.advanced.outbound-large-message-queue-size` | `256` | ✅ | Recorded on NodeManager (`EffectiveOutboundLargeMessageQueueSize`) for the large-stream outbox |
| `pekko.remote.artery.advanced.compression.actor-refs.max` | `256` | ✅ | Consumed by `CompressionTableManager.UpdateActorRefTable`; CTM is constructed in production via `core.StartCompressionTableManager` (called from `cluster.NewCluster`) and reachable from the inbound-advert handler at `association.go:2802` (sub-plan 8g) |
| `pekko.remote.artery.advanced.compression.actor-refs.advertisement-interval` | `1m` | ✅ | Consumed by `CompressionTableManager.StartAdvertisementScheduler` invoked from `core.StartCompressionTableManager` (called from `cluster.NewCluster`); the production advertisement callback fires at this cadence and emits `CompressionTableAdvertisement` to each associated peer (sub-plan 8g) |
| `pekko.remote.artery.advanced.compression.manifests.max` | `256` | ✅ | Consumed by `CompressionTableManager.UpdateManifestTable`; same production construction path as the row above (sub-plan 8g) |
| `pekko.remote.artery.advanced.compression.manifests.advertisement-interval` | `1m` | ✅ | Same production wiring as the actor-refs row above; the production advertisement callback fires at this cadence (sub-plan 8g) |
| `pekko.remote.artery.advanced.tcp.connection-timeout` | `5s` | ✅ | Threaded into `TcpClient.DialTimeout` and `DialRemote`'s association poll |
| `pekko.remote.artery.advanced.tcp.outbound-client-hostname` | `""` | ✅ | Sets the local source address for outbound dials (`net.Dialer.LocalAddr`) |
| `pekko.remote.artery.advanced.inbound-lanes` | `4` | ✅ | Consumed by `ProcessConnection` at association construction; allocates a per-association inbound-lane fan-out (channels + N drain goroutines hashed by recipient path via `dispatchSharded`/`laneIndex`) when the value is > 1 and `streamId != 1`; control-stream traffic and recipient-less frames bypass the lanes for ordering; lane-full saturation surfaces as `CatInboundLaneFull` flight events with inline-dispatch fallback (sub-plan 8f) |
| `pekko.remote.artery.advanced.outbound-lanes` | `1` | ✅ | Consumed by `EnsureOrdinarySibling` at control-handshake completion; opens N parallel TCP connections per peer for streamId=2 (Ordinary) via `DialRemoteOrdinaryLanes`, each lane with its own outboundLane (conn + outbox + writer goroutine + per-lane handshake state); `outboxFor` pivots streamId=1 user-message traffic onto `sibling.lanes[fnv32(recipient) % N].outbox` once the sibling is registered; siblings linked at control-handshake completion via EnsureOrdinarySibling, outbound default opens 2 TCPs per peer (1 control + 1 ordinary) matching Pekko's outbound-lanes=1 default; control-stream and large-stream remain single-conn; inbound coalescence attaches multiple inbound TCPs from the same peer UID as additional lanes of one logical association via the assoc.delegate dispatch redirect (sub-plan 8f outbound half) |
| `pekko.remote.artery.advanced.outbound-message-queue-size` | `3072` | ✅ | Sizes each association's outbox channel |
| `pekko.remote.artery.advanced.system-message-buffer-size` | `20000` | ✅ | Sizes the per-association `SystemMessageOutbox` allocated by `ProcessConnection` for streamId=1 control associations (Phase 2). `(*GekkaAssociation).SendSystem` enqueues unacked system-message frames into this buffer; `runSystemRedelivery` replays them on the resend ticker; `handleControlMessage` "h" prunes them on incoming `SystemMessageDeliveryAck`. When the buffer fills, `quarantineForSystemRedelivery` escalates the association to QUARANTINED |
| `pekko.remote.artery.advanced.outbound-control-queue-size` | `20000` | ✅ | Sizes each outbound control-stream (streamId=1) association's outbox |
| `pekko.remote.artery.advanced.handshake-timeout` | `20s` | ✅ | Outbound association gives up after this deadline |
| `pekko.remote.artery.advanced.handshake-retry-interval` | `1s` | ✅ | Re-sends HandshakeReq at this cadence until ASSOCIATED |
| `pekko.remote.artery.advanced.system-message-resend-interval` | `1s` | ✅ | Cadence of the per-association `runSystemRedelivery` goroutine (Phase 2). On each tick the loop walks `SystemMessageOutbox.Snapshot()` and re-emits any entry whose `lastAttempt` is older than the interval; entries pruned by an inbound `SystemMessageDeliveryAck` are skipped. Receiver-side dedupe via `lastDeliveredSystemSeq` keeps replays idempotent across genuine duplicates |
| `pekko.remote.artery.advanced.give-up-system-message-after` | `6h` | ✅ | Deadline enforced by `runSystemRedelivery` (Phase 2). Each tick checks `SystemMessageOutbox.OldestFirstAttempt()` (the longest-pending unacked entry's first-send timestamp) against `now - give-up-system-message-after`; on exceedance `quarantineForSystemRedelivery` is invoked with reason `give-up-system-message-after` to drain the buffer, transition state to QUARANTINED, close the conn, and emit a flight-recorder dump |
| `pekko.remote.artery.advanced.stop-idle-outbound-after` | `5m` | ✅ | Consumed by `NodeManager.SweepIdleOutboundStop` invoked by `core.StartLifecycleSweepers` (called from `cluster.NewCluster`). Idle ASSOCIATED outbound associations are gracefully closed and removed from the registry without permanent quarantine (sub-plan 8h) |
| `pekko.remote.artery.advanced.quarantine-idle-outbound-after` | `6h` | ✅ | Consumed by `NodeManager.SweepIdleOutboundQuarantine` invoked by `core.StartLifecycleSweepers`. Idle outbound associations transition to QUARANTINED, register the UID with timestamp, and remain in the registry awaiting the stop/remove sweeps (sub-plan 8h) |
| `pekko.remote.artery.advanced.stop-quarantined-after-idle` | `3s` | ✅ | Consumed by `NodeManager.SweepStopQuarantinedAfterIdle` invoked by `core.StartLifecycleSweepers`. The conn of QUARANTINED idle outbound associations is closed without deleting the entry (sub-plan 8h) |
| `pekko.remote.artery.advanced.remove-quarantined-association-after` | `1h` | ✅ | Consumed by `NodeManager.SweepRemoveQuarantinedAssociation` invoked by `core.StartLifecycleSweepers`. UIDs older than the threshold are dropped from `quarantinedUIDs` and the matching QUARANTINED associations are removed from the registry, allowing fresh handshakes (sub-plan 8h) |
| `pekko.remote.artery.advanced.shutdown-flush-timeout` | `1s` | ✅ | Consumed by `core.WaitForOutboundFlush` invoked from the `before-actor-system-terminate` task registered by `Cluster.registerBuiltinShutdownTasks`. The flush phase polls every association outbox (primary, lane, UDP-ordinary, UDP-large) until empty or the timeout elapses, then yields to `actor-system-terminate` which closes the transport (sub-plan 8i) |
| `pekko.remote.artery.advanced.death-watch-notification-flush-timeout` | `3s` | ✅ | Consumed by `core.DeathWatchNotificationFlushDelay` invoked from the goroutine that `Cluster.triggerLocalActorDeath` spawns before emitting remote `DeathWatchNotification` frames. The local `Terminated` dispatch path is unchanged; only the cross-network notification is delayed (sub-plan 8i) |
| `pekko.remote.artery.advanced.inbound-restart-timeout` | `5s` | ✅ | Consumed by `NodeManager.TryRecordInboundRestart`, which `TcpArteryHandlerWithNodeManager` invokes whenever `ProcessConnection(INBOUND, …)` returns an error; saturation events surface via flight-recorder + `slog.Warn` (sub-plan 8e) |
| `pekko.remote.artery.advanced.inbound-max-restarts` | `5` | ✅ | Same call site as the row above; the parsed cap gates the rolling window enforced by `TryRecordInboundRestart` (sub-plan 8e) |
| `pekko.remote.artery.advanced.outbound-restart-backoff` | `1s` | ✅ | Consumed by `NodeManager.DialRemoteWithRestart` between retry attempts; the production callers are `Router.Send`, `Router.SendWithSender`, `Cluster.DeliverSelection`, and the post-handshake re-dial in `association.go` (sub-plan 8e) |
| `pekko.remote.artery.advanced.outbound-restart-timeout` | `5s` | ✅ | Same call site as the row above; the parsed window scopes `TryRecordOutboundRestart`'s count of retries (sub-plan 8e) |
| `pekko.remote.artery.advanced.outbound-max-restarts` | `5` | ✅ | Same call site as the row above; once the cap is exceeded `DialRemoteWithRestart` returns "outbound restart cap exceeded" (sub-plan 8e) |
| `pekko.remote.artery.tls.certificate` | `""` | ✅ | Gekka-native — PEM server certificate (`hocon_config.go` → `core.TLSConfig.CertFile`). Replaces Pekko's `ssl.config-ssl-engine.key-store`. |
| `pekko.remote.artery.tls.private-key` | `""` | ✅ | Gekka-native — PEM private key paired with `tls.certificate`. Replaces Pekko's `ssl.config-ssl-engine.key-store` private-key entry. |
| `pekko.remote.artery.tls.ca-certificates` | `""` | ✅ | Gekka-native — PEM CA bundle for peer verification (loaded into `crypto/x509.CertPool`; serves as both `ClientCAs` and `RootCAs`). Replaces Pekko's `ssl.config-ssl-engine.trust-store`. |
| `pekko.remote.artery.tls.min-version` | `TLSv1.2` | ✅ | Gekka-native — accepts `TLSv1.2`/`TLS1.2` or `TLSv1.3`/`TLS1.3`; mapped to Go `tls.VersionTLS12`/`VersionTLS13`. Covers Pekko's `ssl.config-ssl-engine.protocol`. |
| `pekko.remote.artery.tls.require-client-auth` | `false` | ✅ | Gekka-native — when `true`, sets `tls.RequireAndVerifyClientCert`. Alias of Pekko's `ssl.config-ssl-engine.require-mutual-authentication`. |
| `pekko.remote.artery.tls.server-name` | `""` | ✅ | Gekka-native — SNI hostname for client handshake; non-empty value enables Pekko-style hostname verification. Covers `ssl.config-ssl-engine.hostname-verification`. |
| `pekko.remote.artery.tls.cipher-suites` | `[]` | ✅ | Gekka-native — explicit IANA cipher allow-list (e.g. `["TLS_AES_128_GCM_SHA256"]`). Mapped via `core.ParseCipherSuiteNames` to Go `crypto/tls` IDs and passed to `tls.Config.CipherSuites`. Note: only TLS 1.2 honors this list — TLS 1.3 ciphers are spec-fixed in Go. Pekko's `ssl.config-ssl-engine.enabled-algorithms` is accepted as an alias (gekka-native path wins when both are set). |
| `pekko.remote.artery.ssl.config-ssl-engine.key-store[-password]` | — | ✅ | Pekko-compatible alias — gekka uses PEM, so configure via `tls.certificate` + `tls.private-key`. No JKS password needed. |
| `pekko.remote.artery.ssl.config-ssl-engine.trust-store[-password]` | — | ✅ | Pekko-compatible alias — gekka uses PEM, so configure via `tls.ca-certificates`. No JKS password needed. |
| `pekko.remote.artery.ssl.config-ssl-engine.protocol` | `TLSv1.3` | ✅ | Pekko-compatible alias — covered by gekka-native `tls.min-version` (accepts `TLSv1.2` or `TLSv1.3`). |
| `pekko.remote.artery.ssl.config-ssl-engine.enabled-algorithms` | (list) | ✅ | Pekko-compatible alias of gekka-native `tls.cipher-suites`. Same parser; gekka-native path wins when both are set. |
| `pekko.remote.artery.ssl.config-ssl-engine.require-mutual-authentication` | `on` | ✅ | Pekko-compatible alias — same effect as gekka-native `tls.require-client-auth`. |
| `pekko.remote.artery.ssl.config-ssl-engine.hostname-verification` | `off` | ✅ | Pekko-compatible alias — enable by setting gekka-native `tls.server-name` (Go `crypto/tls` SNI). |
| `pekko.remote.artery.ssl.ssl-engine-provider` | (Pekko FQCN) | 🚫 | JVM-only API-shape incompatibility — value is a Java FQCN selecting an `SSLEngineProvider` plug-in via reflection. Gekka has a fixed Go `crypto/tls` engine wired directly; no class-loader bridge. Configure certificates via the `tls.*` paths above. |
| `pekko.remote.artery.ssl.config-ssl-engine.random-number-generator` | `""` | 🚫 | JVM-only API-shape incompatibility — value names a JCA SecureRandom algorithm strung through Java's `Provider` SPI. Gekka uses `crypto/rand.Reader` (Go's `tls.Config.Rand` left at default); the JCA name does not transfer. |
| `pekko.remote.artery.ssl.rotating-keys-engine.*` | — | 🚫 | JVM-only API-shape incompatibility — Pekko's `RotatingKeysSSLEngineProvider` re-loads JKS/PKCS#12 key-stores from disk on a JVM filesystem watcher. Gekka has no equivalent rotation mechanism in Go's `crypto/tls`; operators must reload via process restart or an external SIGHUP path. |
| `pekko.remote.watch-failure-detector.*` | (various) | ✅ | `cluster.WatchFailureDetector` (`cluster/watch_failure_detector.go`) — separate Phi-Accrual instance fed heartbeats by `ClusterManager.handleHeartbeat`/`handleHeartbeatRsp`; reaper goroutine in `cluster_watch.go` consults `IsAvailable` per `unreachable-nodes-reaper-interval` and drives `triggerRemoteNodeDeath` when a watched node's `acceptable-heartbeat-pause` window expires. Distinct from cluster membership FD (`cluster/failure_detector.go`). |
| `pekko.remote.accept-protocol-names` | `["pekko"]` | ✅ | `ClusterConfig.AcceptProtocolNames` — list of protocols (e.g., `pekko`/`akka`) accepted on inbound handshake |

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
| `implementation-class` | (FQCN) | 🚫 | Value is a Java FQCN; Go has no class loader. Gekka's Phi-Accrual FD is wired directly in `cluster/failure_detector.go`. |
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
| `cross-data-center-connections` | `5` | ✅ | `ClusterConfig.MultiDataCenter.CrossDataCenterConnections` — caps gossip-target nodes per remote DC |
| `cross-data-center-gossip-probability` | `0.2` | ✅ | |
| `failure-detector.heartbeat-interval` | `3s` | ✅ | Cross-DC HB cadence; `EffectiveHeartbeatInterval` returns this for cross-DC targets, intra-DC default otherwise (Round-2 session 12) |
| `failure-detector.acceptable-heartbeat-pause` | `10s` | ✅ | `EffectiveAcceptableHeartbeatPause(target)` returns `cm.CrossDCAcceptableHeartbeatPause` for cross-DC targets and 0 otherwise; `IsTargetAvailable` routes cross-DC reachability through `Fd.IsAvailableWithMargin`, granting a φ-above-threshold target an additive grace window equal to this margin before flipping unreachable. Intra-DC behaviour unchanged (perfect-pekko Phase 4) |
| `failure-detector.expected-response-after` | `1s` | ✅ | `EffectiveExpectedResponseAfter(target)` on `ClusterManager` returns this for cross-DC targets and the intra-DC `ExpectedResponseAfter` otherwise; consumed by `handleHeartbeat`/`handleHeartbeatRsp` via `Fd.HeartbeatWithEstimate`, which seeds the per-node Phi detector's `firstHeartbeatEstimate` (sub-plan 8 group A) |

### pekko.cluster.split-brain-resolver

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `active-strategy` | `keep-majority` | ✅ | |
| `stable-after` | `20s` | ✅ | |
| `down-all-when-unstable` | `on` | ✅ | Downs all after instability timeout |
| `static-quorum.quorum-size` | `undefined` | ✅ | |
| `static-quorum.role` | `""` | ✅ | `ClusterConfig.SBR.StaticQuorum.Role` — when set, only members with this role count toward quorum |
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
| `pekko.cluster.distributed-data.name` | `ddataReplicator` | ✅ | `DistributedDataConfig.Name` — replicator actor name |
| `pekko.cluster.distributed-data.role` | `""` | ✅ | `DistributedDataConfig.Role` — when set, only members in this role host a replicator |
| `pekko.cluster.distributed-data.notify-subscribers-interval` | `500ms` | ✅ | `DistributedDataConfig.NotifySubscribersInterval` — batched subscriber-change notifications |
| `pekko.cluster.distributed-data.max-delta-elements` | `500` | ✅ | `DistributedDataConfig.MaxDeltaElements` — cap on per-gossip delta batch size |
| `pekko.cluster.distributed-data.pruning-interval` | `120s` | ✅ | `DistributedDataConfig.PruningInterval` — how often the replicator scans for prunable tombstones |
| `pekko.cluster.distributed-data.max-pruning-dissemination` | `300s` | ✅ | `DistributedDataConfig.MaxPruningDissemination` — upper bound on dissemination time before tombstones are removed |
| `pekko.cluster.distributed-data.delta-crdt.enabled` | `on` | ✅ | `DistributedDataConfig.DeltaCRDTEnabled` — toggle delta-propagation gossip path |
| `pekko.cluster.distributed-data.delta-crdt.max-delta-size` | `50` | ✅ | `DistributedDataConfig.DeltaCRDTMaxDeltaSize` — cap on delta batch elements before falling back to full-state gossip |
| `pekko.cluster.distributed-data.durable.enabled` | `off` | ✅ | Round-2 session 23. `DistributedDataConfig.DurableEnabled` — implicitly true when `durable.keys` is non-empty; explicit `on` lights up the bbolt backend before any keys are configured. |
| `pekko.cluster.distributed-data.durable.keys` | `[]` | ✅ | Round-2 session 23. `DistributedDataConfig.DurableKeys` filters which CRDT keys are persisted (prefix glob via trailing `*`). |
| `pekko.cluster.distributed-data.durable.pruning-marker-time-to-live` | `10d` | ✅ | Round-2 session 23. `DistributedDataConfig.DurablePruningMarkerTimeToLive` — when DurableEnabled, applied as the marker TTL whenever it exceeds the non-durable `pruning-marker-time-to-live`, so durable replicas can rejoin without resurrecting stale state. |
| `pekko.cluster.distributed-data.durable.lmdb.dir` | `ddata` | ✅ | Round-2 session 23. `DistributedDataConfig.DurableLmdbDir` → `BoltDurableStoreOptions.Dir`. |
| `pekko.cluster.distributed-data.durable.lmdb.map-size` | `100 MiB` | ✅ | Round-2 session 23. `DistributedDataConfig.DurableLmdbMapSize` → `BoltDurableStoreOptions.MapSize` (hard cap enforced via pre-flight file-size check). |
| `pekko.cluster.distributed-data.durable.lmdb.write-behind-interval` | `off` | ✅ | Round-2 session 23. `DistributedDataConfig.DurableLmdbWriteBehindInterval` → `BoltDurableStoreOptions.WriteBehindInterval`; coalesces same-key writes per flush tick. |
| `pekko.cluster.distributed-data.prefer-oldest` | `off` | ✅ | `DistributedDataConfig.PreferOldest` — when on, prefers the oldest member as gossip target for stickier replicator placement |
| `pekko.cluster.distributed-data.pruning-marker-time-to-live` | `6h` | ✅ | `DistributedDataConfig.PruningMarkerTimeToLive` → `PruningManager.SetPruningMarkerTimeToLive` retains tombstones in PruningComplete phase for the TTL (Round-2 session 16) |
| `pekko.cluster.distributed-data.log-data-size-exceeding` | `10 KiB` | ✅ | `DistributedDataConfig.LogDataSizeExceeding` → `Replicator.LogDataSizeExceeding`; `sendToPeers` emits a slog.Warn when serialized payload exceeds the threshold (Round-2 session 16) |
| `pekko.cluster.distributed-data.recovery-timeout` | `10s` | ✅ | `DistributedDataConfig.RecoveryTimeout` → `Replicator.WaitForRecovery` blocks until at least one peer is registered or the timeout elapses (Round-2 session 16) |
| `pekko.cluster.distributed-data.serializer-cache-time-to-live` | `10s` | ✅ | Parsed into `DistributedDataConfig.SerializerCacheTimeToLive` and set on `Replicator.SerializerCacheTimeToLive`. Consumed by `cluster/ddata/serializer_cache.go`: a TTL cache keyed by `(crdt-key, snapshot-fingerprint)` memoizes the JSON-serialized full-state gossip payload across rounds. The six gossip entry points (`gossipCounter` / `gossipSet` / `gossipMap` / `gossipPNCounter` / `gossipORFlag` / `gossipLWWRegister`) route through `Replicator.cachedMarshal`; identical snapshots within the TTL window reuse cached bytes instead of re-marshalling. `SerializerCacheStats()` exposes hit/miss counters; setting the TTL to `0` disables caching (Store becomes a no-op) |

---

## Module: `pekko/cluster-sharding` (pekko-cluster-sharding)

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `pekko.cluster.sharding.remember-entities` | `off` | ✅ | |
| `pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.timeout` | `120s` | ✅ | `ShardingConfig.PassivationIdleTimeout` parsed at the correct Pekko path (hocon_config.go:526). The legacy gekka path `.passivation.idle-timeout` was removed — only the Pekko-canonical path is honored. |
| `pekko.cluster.sharding.passivation.strategy` | `"default-idle-strategy"` | ✅ | Round-2 session 24. Recognises `default-idle-strategy`, `least-recently-used` (Pekko canonical) and `custom-lru-strategy` (legacy alias normalised at parse time). |
| `pekko.cluster.sharding.passivation.least-recently-used-strategy.active-entity-limit` | `100000` | ✅ | Round-2 session 24. `ShardingConfig.PassivationActiveEntityLimit`; legacy `custom-lru-strategy.active-entity-limit` still parsed as a fallback. |
| `pekko.cluster.sharding.passivation.least-recently-used-strategy.replacement.policy` | `"least-recently-used"` | ✅ | Round-2 session 24. `ShardingConfig.PassivationReplacementPolicy`. |
| `pekko.cluster.sharding.passivation.most-recently-used-strategy.active-entity-limit` | `100000` | ✅ | Round-2 session 25. Active strategy limit reaches `ShardingConfig.PassivationActiveEntityLimit`; the eviction loop picks the freshest entity. |
| `pekko.cluster.sharding.passivation.least-frequently-used-strategy.active-entity-limit` | `100000` | ✅ | Round-2 session 25. Active strategy limit reaches `ShardingConfig.PassivationActiveEntityLimit`; the eviction loop picks the entity with the lowest cumulative access count, ties broken by oldest activity. |
| `pekko.cluster.sharding.passivation.least-frequently-used-strategy.dynamic-aging` | `off` | ✅ | Phase 6.2 wires the flag through `ShardingSettings`/`ShardSettings` into `Shard.lfuAccessesSinceAging`. When true, `Shard.ageLFUFrequencies()` halves every per-entity counter every `lfuAgingThreshold()` LFU increments (anchored to `active-entity-limit`, floor 64). |
| `pekko.cluster.sharding.passivation.strategy = default-strategy` | `"default-idle-strategy"` | ✅ | Round-2 session 26. Selects the W-TinyLFU composite strategy implemented in `cluster/sharding/passivation_composite.go`. The plan-internal alias `"composite-strategy"` is normalised at parse time so configs that use either convention reach the same code path. |
| `pekko.cluster.sharding.passivation.default-strategy.active-entity-limit` | `100000` | ✅ | Round-2 session 26. Active strategy limit reaches `ShardingConfig.PassivationActiveEntityLimit`; the composite strategy splits it between admission-window and main areas via `PassivationWindowProportion`. |
| `pekko.cluster.sharding.passivation.default-strategy.admission.window.policy` | `"least-recently-used"` | ✅ | Round-2 session 26. `ShardingConfig.PassivationWindowPolicy`. Currently only `"least-recently-used"` is honoured at runtime; non-empty values activate the admission window. |
| `pekko.cluster.sharding.passivation.default-strategy.admission.window.optimizer` | `"hill-climbing"` | ✅ | Phase 6.3 wires the optimizer through `ShardingSettings.PassivationWindowOptimizer` into `compositeStrategy`. `"hill-climbing"` activates the adaptive resizer (instrumented in `OnAccess`, fires every `optimizerInterval` accesses); any other value freezes `windowProportion` at the seed. |
| `pekko.cluster.sharding.passivation.default-strategy.admission.filter` | `"frequency-sketch"` | ✅ | Round-2 session 26. `ShardingConfig.PassivationFilter`. Recognised values: `"frequency-sketch"` (admission filter active), `"off"` / `"none"` (composite degrades to LRU window + LRU main with no admission gate). |
| `pekko.cluster.sharding.passivation.default-strategy.replacement.policy` | `"least-recently-used"` | ✅ | Round-2 session 26. Per-strategy override for `ShardingConfig.PassivationReplacementPolicy`; wins over the LRU-strategy block when both are present. |
| `pekko.cluster.sharding.passivation.strategy-defaults.admission.window.proportion` | `0.01` | ✅ | Round-2 session 26. Admission-window size as a fraction of the active-entity-limit. `default-strategy.admission.window.proportion` overrides per-strategy. |
| `pekko.cluster.sharding.passivation.strategy-defaults.admission.window.minimum-proportion` | `0.01` | ✅ | Phase 6.3 wires this into `compositeStrategy.windowMinProp` via `ShardingConfig.PassivationWindowMinimumProportion`; the hill-climbing optimizer reverses direction at this floor. |
| `pekko.cluster.sharding.passivation.strategy-defaults.admission.window.maximum-proportion` | `1.0` | ✅ | Phase 6.3 wires this into `compositeStrategy.windowMaxProp` via `ShardingConfig.PassivationWindowMaximumProportion`; the hill-climbing optimizer reverses direction at this ceiling. |
| `pekko.cluster.sharding.passivation.strategy-defaults.admission.frequency-sketch.depth` | `4` | ✅ | Round-2 session 26. Count-min-sketch row count; threaded into `cluster/sharding/passivation_admission.go`. |
| `pekko.cluster.sharding.passivation.strategy-defaults.admission.frequency-sketch.counter-bits` | `4` | ✅ | Phase 6.1 wires `cfg.frequencySketchCounterBits` into `countMinSketch.maxCount` via `resolveCounterBits` (clamps to [2,8]; values >8 fold down because cells are uint8). Saturation cap = `(1 << bits) - 1`; default 4 → 0x0F preserves the prior behaviour. |
| `pekko.cluster.sharding.passivation.strategy-defaults.admission.frequency-sketch.width-multiplier` | `4` | ✅ | Round-2 session 26. Sketch width = active-entity-limit × multiplier (rounded up to a power of two, clamped to 64). |
| `pekko.cluster.sharding.passivation.strategy-defaults.admission.frequency-sketch.reset-multiplier` | `10.0` | ✅ | Round-2 session 26. Triggers the sketch's halving reset every (active-entity-limit × multiplier) accesses. |
| `pekko.cluster.sharding.guardian-name` | `"sharding"` | ✅ | `ShardingConfig.GuardianName` — top-level guardian actor under which all sharding regions are spawned |
| `pekko.cluster.sharding.role` | `""` | ✅ | Filters shard allocation by role |
| `pekko.cluster.sharding.remember-entities-store` | `"ddata"` | ✅ | Round-2 session 35. `"ddata"` wires `DDataEntityStore` (existing); `"eventsourced"` wires `EventSourcedEntityStore` (new) — both implement `ShardStore` and are selected in `sharding.go` from `cluster.cfg.Sharding.RememberEntitiesStore`. |
| `pekko.cluster.sharding.passivate-idle-entity-after` | `null` | ✅ | Deprecated alias — Phase 1.6 wires it as a fallback at `hocon_config.go` next to the canonical `passivation.default-idle-strategy.idle-entity.timeout` parse: when only the alias is set it populates the same `Sharding.PassivationIdleTimeout`; when both are set, the canonical key wins. The literal "off" / "false" / "0" disable passivation. |
| `pekko.cluster.sharding.number-of-shards` | `1000` | ✅ | Wired to coordinator/region |
| `pekko.cluster.sharding.rebalance-interval` | `10s` | ✅ | Applied to ShardCoordinator.RebalanceInterval |
| `pekko.cluster.sharding.least-shard-allocation-strategy.rebalance-threshold` | `1` | ✅ | Applied to NewLeastShardAllocationStrategy(threshold) |
| `pekko.cluster.sharding.least-shard-allocation-strategy.max-simultaneous-rebalance` | `3` | ✅ | Applied to NewLeastShardAllocationStrategy(maxSimultaneous) |
| `pekko.cluster.sharding.least-shard-allocation-strategy.rebalance-absolute-limit` | `0` | ✅ | Round-2 session 33 — when `> 0` selects the Pekko 1.0+ two-phase `LeastShardAllocationStrategyV2` (`cluster/sharding/strategy.go`); `0` keeps the legacy threshold-based strategy. |
| `pekko.cluster.sharding.least-shard-allocation-strategy.rebalance-relative-limit` | `0.1` | ✅ | Round-2 session 33 — fraction of total shards capping each rebalance round under V2 (`max(1, min(int(rel*total), absolute))`). |
| `pekko.cluster.sharding.external-shard-allocation-strategy.client-timeout` | `5s` | ✅ | Sub-plan 7 — parsed in `hocon_config.go` (`Sharding.ExternalShardAllocation.ClientTimeout`); consumed by `LoadAllocationStrategy` in `cluster/sharding/strategy.go` when the `external` allocation strategy is selected. Precedence: gekka-native `gekka.cluster.sharding.allocation-strategy.external.timeout` (legacy) > canonical Pekko `client-timeout` > 5s default. |
| `pekko.cluster.sharding.event-sourced-remember-entities-store.max-updates-per-write` | `100` | ✅ | Round-2 session 34 — Shard buffers EntityStarted/EntityStopped events; the buffer is flushed in a single AsyncWriteMessages call once it hits the cap, with a final flush in PostStop. Cap of `0` keeps the legacy one-event-per-write path. |
| `pekko.cluster.sharding.state-store-mode` | `"ddata"` | ☕ | JVM-only — gekka commits to `ddata` exclusively for sharding state; the `persistence` mode and its tunables (`snapshot-after`, `keep-nr-of-batches`, `journal-plugin-id`, `snapshot-plugin-id`) are N/A in gekka. |
| `pekko.cluster.sharding.snapshot-after` | `1000` | ☕ | JVM-only — `state-store-mode = persistence` subset; gekka uses `ddata`. |
| `pekko.cluster.sharding.keep-nr-of-batches` | `2` | ☕ | JVM-only — `state-store-mode = persistence` subset; gekka uses `ddata`. |
| `pekko.cluster.sharding.journal-plugin-id` | `""` | ☕ | JVM-only — `state-store-mode = persistence` subset; gekka uses `ddata`. |
| `pekko.cluster.sharding.snapshot-plugin-id` | `""` | ☕ | JVM-only — `state-store-mode = persistence` subset; gekka uses `ddata`. |
| `pekko.cluster.sharding.distributed-data.majority-min-cap` | `5` | ✅ | Sharding override applied to `cluster.repl.MajorityMinCap`; consumed by `Replicator.sendToPeers` via `EffectiveMajorityQuorum` to size WriteMajority gossip targets (sub-plan 8b) |
| `pekko.cluster.sharding.distributed-data.max-delta-elements` | `5` | ✅ | Sharding override merged onto `cluster.repl.MaxDeltaElements` (smaller-wins); consumed by `Replicator.gossipAll` periodic-gossip path as the per-round delta-batch cap (sub-plan 8b) |
| `pekko.cluster.sharding.distributed-data.prefer-oldest` | `on` | ✅ | Sharding override OR-merged onto `cluster.repl.PreferOldest`; consumed by `Replicator.selectGossipTargets` to order WriteMajority/WriteAll gossip targets (sub-plan 8b) |
| `pekko.cluster.sharding.distributed-data.durable.keys` | `["shard-*"]` | ⚠️ | Parsed into `ShardingConfig.DistributedData.DurableKeys`; the parent `distributed-data.durable.keys` is fully ✅, but the sharding-specific filter is not yet wired into the durable store |
| `pekko.cluster.sharding.coordinator-singleton.role` | `""` | ✅ | Applied to coordinator singleton-proxy when override = off |
| `pekko.cluster.sharding.coordinator-singleton.singleton-name` | `"singleton"` | ✅ | Sharding override applied to `ClusterSingletonManager.WithSingletonName` at the coordinator manager (`sharding.go` top-level + `cluster/sharding/cluster_sharding.go`); also applied to `ClusterSingletonProxy.WithSingletonName` so the proxy resolves the renamed singleton child (sub-plan 8c) |
| `pekko.cluster.sharding.coordinator-singleton.hand-over-retry-interval` | `1s` | ✅ | Sharding override applied to `ClusterSingletonManager.WithHandOverRetryInterval` at the coordinator manager; consumed by `manager.acquireLease` retry path (`cluster/singleton/manager.go:247`) (sub-plan 8c) |
| `pekko.cluster.sharding.coordinator-singleton.min-number-of-hand-over-retries` | `15` | ✅ | Sharding override applied to `ClusterSingletonManager.WithMinHandOverRetries` at the coordinator manager (sub-plan 8c) |
| `pekko.cluster.sharding.coordinator-singleton-role-override` | `on` | ✅ | When `on`, sharding.role wins over coordinator-singleton.role |
| `pekko.cluster.sharding.retry-interval` | `2s` | ✅ | ShardRegion ticker re-tells GetShardHome for shards with unknown home (Round-2 session 13) |
| `pekko.cluster.sharding.buffer-size` | `100000` | ✅ | Caps per-shard pendingMessages while awaiting ShardHome; further messages are dropped (Round-2 session 13) |
| `pekko.cluster.sharding.shard-start-timeout` | `10s` | ⚠️ | Plumbed onto `ShardSettings.ShardStartTimeout`; Shard-startup timeout consumer not yet implemented (field is declared but unread) |
| `pekko.cluster.sharding.shard-failure-backoff` | `10s` | ✅ | Region delays clearing the cached home after a Shard terminates (Round-2 session 13) |
| `pekko.cluster.sharding.entity-restart-backoff` | `10s` | ⚠️ | Plumbed onto `ShardSettings.EntityRestartBackoff`; entity-restart-backoff consumer not yet implemented (field is declared but unread) |
| `pekko.cluster.sharding.coordinator-failure-backoff` | `5s` | ✅ | Region delays re-registration with the coordinator after termination (Round-2 session 13) |
| `pekko.cluster.sharding.waiting-for-state-timeout` | `2s` | ⚠️ | Plumbed onto ShardSettings; consumed by the DData coordinator-state path when present (Round-2 session 14) |
| `pekko.cluster.sharding.updating-state-timeout` | `5s` | ⚠️ | Plumbed onto ShardSettings; consumed by DData updates / remember-entities writes when present (Round-2 session 14) |
| `pekko.cluster.sharding.shard-region-query-timeout` | `3s` | ⚠️ | Plumbed onto ShardSettings; consumed by region-level query handlers as added (Round-2 session 14) |
| `pekko.cluster.sharding.entity-recovery-strategy` | `"all"` | ✅ | `"all"` spawns all remembered entities at once; `"constant"` paces recovery in batches (Round-2 session 14) |
| `pekko.cluster.sharding.entity-recovery-constant-rate-strategy.frequency` | `100ms` | ✅ | Delay between batches under the `"constant"` strategy (Round-2 session 14) |
| `pekko.cluster.sharding.entity-recovery-constant-rate-strategy.number-of-entities` | `5` | ✅ | Batch size under the `"constant"` strategy (Round-2 session 14) |
| `pekko.cluster.sharding.coordinator-state.write-majority-plus` | `3` | ⚠️ | Plumbed onto `ShardSettings.CoordinatorWriteMajorityPlus` (`"all"` maps to `math.MaxInt`); DData write-majority consumer not yet implemented |
| `pekko.cluster.sharding.coordinator-state.read-majority-plus` | `5` | ⚠️ | Plumbed onto `ShardSettings.CoordinatorReadMajorityPlus` (`"all"` maps to `math.MaxInt`); DData read-majority consumer not yet implemented |
| `pekko.cluster.sharding.verbose-debug-logging` | `off` | ✅ | Gates fine-grained per-message DEBUG log lines via Shard.vdebug (Round-2 session 15) |
| `pekko.cluster.sharding.fail-on-invalid-entity-state-transition` | `off` | ✅ | When `on`, Shard panics on invalid handoff transitions; otherwise logs WARN (Round-2 session 15) |
| `pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.interval` | `default` (= timeout/2) | ✅ | Overrides idle-entity scan cadence; `"default"` leaves the timeout/2 fallback (Round-2 session 15) |
| `pekko.cluster.sharding.healthcheck.names` | `[]` | ✅ | Consumed by `Cluster.ShardingHealthCheckReady` (cluster_management.go), which the management server's `/health/ready` probe consults as a readiness gate; the function delegates to `sharding.ClusterShardingHealthCheck` (cluster/sharding/healthcheck.go) (sub-plan 8d) |
| `pekko.cluster.sharding.healthcheck.timeout` | `5s` | ✅ | Same consumer site as the row above; passed through to `sharding.ClusterShardingHealthCheck` as `HealthCheckConfig.Timeout`, which caps its lookup deadline (sub-plan 8d) |
| `pekko.cluster.sharding.use-lease` | `""` | ✅ | Resolves a Lease from Cluster.LeaseManager; every Shard acquires before becoming active and releases on handoff/stop (Round-2 session 20) |
| `pekko.cluster.sharding.lease-retry-interval` | `5s` | ✅ | Backoff between Shard Acquire retries when a prior call returned false (Round-2 session 20) |

---

## Module: `pekko/cluster-tools` (pekko-cluster-tools)

### pekko.cluster.pub-sub

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `name` | `distributedPubSubMediator` | ✅ | `ClusterConfig.PubSub.Name` (hocon_config.go) — mediator actor name |
| `role` | `""` | ✅ | `ClusterConfig.PubSub.Role` — when set, only members in this role host the mediator |
| `routing-logic` | `random` | ✅ | `ClusterConfig.PubSub.RoutingLogic` — `Send` routing (random/round-robin/broadcast) |
| `gossip-interval` | `1s` | ✅ | ClusterMediator gossip interval |
| `removed-time-to-live` | `120s` | ✅ | `ClusterConfig.PubSub.RemovedTimeToLive` — TTL after which removed-mediator state is pruned |
| `max-delta-elements` | `3000` | ✅ | `ClusterConfig.PubSub.MaxDeltaElements` — cap on delta batch size before falling back to full state |
| `send-to-dead-letters-when-no-subscribers` | `on` | ✅ | `ClusterConfig.PubSub.SendToDeadLettersWhenNoSubscribers` — when off, silently drops messages with no subscribers |

### pekko.cluster.singleton

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `singleton-name` | `"singleton"` | ✅ | `ClusterConfig.Singleton.SingletonName` — child actor name under the singleton manager |
| `role` | `""` | ✅ | Applied via SingletonManager factory |
| `hand-over-retry-interval` | `1s` | ✅ | Applied via WithHandOverRetryInterval |
| `min-number-of-hand-over-retries` | `15` | ✅ | `ClusterConfig.Singleton.MinNumberOfHandOverRetries` — minimum retry count before giving up handover |
| `use-lease` | `""` | ✅ | Resolves a Lease from Cluster.LeaseManager; manager acquires before spawn, releases on handoff |
| `lease-retry-interval` | `5s` | ✅ | Backoff between Acquire retries when prior call returned false |

### pekko.cluster.singleton-proxy

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `singleton-name` | (ref singleton) | ✅ | `ClusterConfig.SingletonProxy.SingletonName` — name of the manager-hosted singleton the proxy targets |
| `role` | `""` | ✅ | `ClusterConfig.SingletonProxy.Role` — restrict proxy resolution to a specific role |
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
| `pekko.cluster.typed.receptionist.write-consistency` | `local` | ✅ | Consumed by `receptionist.Behavior`: `Register.handle`/`prune` pass the configured `ddata.WriteConsistency` to `replicator.AddToSet`/`RemoveFromSet` in `actor/typed/receptionist/receptionist.go`. `majority` is best-effort (same gossip behaviour as `all` until quorum-acked writes ship). |
| `pekko.cluster.typed.receptionist.pruning-interval` | `3s` | ✅ | Consumed by `receptionist.Behavior` periodic ticker (`actor/typed/receptionist/receptionist.go`): on each tick, `prune` walks tracked services and `RemoveFromSet`s any path whose `ctx.System().Resolve` errors. |
| `pekko.cluster.typed.receptionist.distributed-key-count` | `5` | ✅ | Consumed by `receptionist.ShardKey(id, count)` (`actor/typed/receptionist/receptionist.go`): `fmt.Sprintf("receptionist-%d-%s", fnv1a(id) %% count, id)` is the ddata bucket name used by every register/find/subscribe/prune call, spreading the receptionist keyspace across N ORSets. |
| `pekko.cluster.ddata.typed.replicator-message-adapter-unexpected-ask-timeout` | `20s` | ✅ | Consumed by `TypedReplicatorAdapter.AskGet/AskUpdate` in `cluster/ddata/typed_replicator.go`; bounds the wait before delivering `ErrUnexpectedAskTimeout` |

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
| `pekko.persistence.typed.log-stashing` | `off` | ✅ | Gates DEBUG log lines around stash/unstash in `persistence/typed/event_sourcing.go` (`GetLogStashing()`); HOCON wires through `persistencetyped.SetLogStashing` |

---

## Module: `pekko/discovery` (pekko-discovery)

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `pekko.discovery.method` | `"<method>"` | ✅ | |
| `pekko.discovery.config.*` | (service map) | ✅ | Consumed by `discovery.ConfigProvider` (`discovery/config_provider.go`), driven from `cluster.go:applyDiscoveredSeeds` via `cfg.Discovery.{Type,Config}`; `services-path` indirection honoured. Endpoints accept the gekka string-list form `["host:port", ...]`; the JVM-only `{host, port}` object form is unsupported (gekka-config does not unmarshal lists of objects) and the `class` FQCN field is JVM-only (out of scope; see 🚫 bucket). |
| `pekko.discovery.aggregate.*` | (multi-method) | ✅ | Consumed by the HOCON-driven aggregate factory in `discovery/aggregate.go`, which composes child providers from `pekko.discovery.aggregate.discovery-methods` via the existing `discovery.Get` registry. Empty list rejected (Pekko parity); the existing `AggregateProvider` tolerate-partial-failure semantics are preserved. |
| `pekko.discovery.pekko-dns.*` | (DNS) | ✅ | Consumed by `discovery.PekkoDNSProvider` (`discovery/pekko_dns_provider.go`); reads `pekko.discovery.pekko-dns.{service-name,port}` and performs `net.LookupSRV`. The `class` FQCN field is JVM-only (out of scope; see 🚫 bucket). |

---

## Module: `pekko/cluster-metrics` (pekko-cluster-metrics)

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `pekko.cluster.metrics.*` | (all) | ☕ | Sigar/JMX — JVM-only metrics frameworks; gekka uses OpenTelemetry via `extensions/telemetry/otel` |

---

## Module: `pekko/actor-typed` (pekko-actor-typed)

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `pekko.actor.typed.restart-stash-capacity` | `1000` | ✅ | Sizes `TypedActor`/`genericTypedActor` stash buffer in `actor/typed/actor.go` PreStart via `typed.GetDefaultRestartStashCapacity()` |
| `pekko.reliable-delivery.*` | (all) | ✅ | Wired via `delivery.Config` (actor/typed/delivery/config.go), `ClusterConfig.ReliableDelivery`, and HOCON parsing in `hocon_config.go`. `NewProducerControllerFromConfig` consumes `producer-controller.chunk-large-messages` to drive runtime payload chunking; `NewConsumerControllerFromConfig` consumes `consumer-controller.flow-control-window` (drives `Request.RequestUpToSeqNr`) and `only-flow-control` (suppresses gap-filling Resend); `NewWorkPullingProducerControllerFromConfig` consumes `work-pulling.producer-controller.buffer-size` (caps `pendingWork`). Distinct from `at-least-once-delivery.*` |

---

## Module: `pekko/cluster-sharding-typed`

| Path | Pekko Default | Gekka? | Notes |
|---|---|---|---|
| `pekko.cluster.sharding.number-of-shards` | `1000` | ✅ | Wired to coordinator/region |
| `pekko.cluster.sharded-daemon-process.*` | (all) | ✅ | `keep-alive-interval` and `sharding.role` are honoured: parsed onto `ClusterConfig.ShardedDaemonProcess` and applied via `sharding.SetDefault*KeepAliveInterval`/`sharding.SetDefault*ShardingRole`; `runKeepAliveLoop` in `cluster/sharding/sharded_daemon.go` consumes the cadence; `InitShardedDaemonProcess` reads the role default. Per Pekko's reference.conf, the other `sharding.*` overrides (`remember-entities`, `passivation`, `number-of-shards`) are intentionally not exposed. |

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

### Symbol counts (post 2026-05-07 perfect-pekko Phase 6 closure)

| Symbol | Substantive table rows | Meaning |
|---|---|---|
| ✅ | 286 | Parsed AND consumed |
| ⚠️ | 10 | Forward-compat parsed; consumer deferred (Note states what's deferred) |
| ☕ | 8 | JVM-only — no equivalent capability in Go runtime |
| 🚫 | 4 | Go/JVM API-shape incompatibility (FQCN class loading, JCA Provider, JKS rotation) |
| ❌ | 2 | Not implemented; portable in principle (tracked in `docs/LEFTWORKS.md` §11) |

(Counts exclude the legend lines themselves and this Summary table. `grep -c "| ✅ |"` etc. on the file returns counts +1 because each Summary-table row contributes one match.)

### Correctly Parsed (✅)

Round-2 sessions 01–41 closed the entire portable Pekko surface. Coverage
now includes core cluster formation, failure detection (incl. cross-DC), SBR
(`down-all-when-unstable` + `lease-majority`), full advanced gossip tuning
(probability, TTL, tombstones, reaper, prune timers), Artery transport
(handshake budgets, idle/quarantine sweeps, restart caps, compression
tables, TCP timeouts, `untrusted-mode` / `trusted-selection-paths`,
`large-message-destinations` + UDP stream-3 routing,
`maximum-large-frame-size` cap, TLS cipher-suites + Pekko ssl alias), full
sharding (every retry/backoff path, `use-lease`, advanced passivation
including W-TinyLFU composite, allocation V1 & V2, EventSourced
remember-entities store with batched writes), full distributed-data
(durable bbolt store, `prune-marker-time-to-live`,
`log-data-size-exceeding`, `recovery-timeout`), full singleton / proxy /
pub-sub (incl. `use-lease`, `send-to-dead-letters-when-no-subscribers`),
full cluster client + receptionist (`response-tunnel-receive-timeout`,
`failure-detection-interval`), full persistence (proxy journal / snapshot,
plugin-fallback circuit-breaker + replay-filter + recovery-event-timeout,
FSM snapshots, AtLeastOnceDelivery scheduler + warn callback),
`pekko.actor.debug` sub-flags, `log-config-on-start`, NAT/Docker bind,
discovery, management.

### Forward-compat (⚠️)

Paths in this bucket are parsed at the canonical Pekko location, but the
runtime consumer that reads the value is not yet implemented. Each ⚠️
row's Note states the specific deferred consumer; none reference a stale
roadmap session. The full list (post-audit) is consolidated in
`docs/LEFTWORKS.md` §11 under "Deferred consumers".

### JVM-Only (☕) and API-Shape Incompatible (🚫)

JVM-only paths (☕) cover Go-irrelevant infrastructure (dispatcher
hierarchy, JMX/Sigar metrics, sharding `state-store-mode = persistence`
subset). API-shape incompatible paths (🚫) cover HOCON values that name
Java FQCNs to be loaded via reflection — gekka has the underlying
capability wired directly in Go but cannot bridge the FQCN-swap
semantics. See `docs/LEFTWORKS.md` §9 for the full JVM-only list.

### Not Yet Implemented (❌) — single source of truth

Portable Pekko paths gekka has not bridged. The full list lives in
`docs/LEFTWORKS.md` §11 ("Future Work — Portable"). After Phase 1 closed
the mailbox subsystem (sub-commits 1.1–1.6, 2026-05-06) only the logging
deferrals remain:

1. **Logging** — `pekko.loglevel` (honesty-downgraded ✅ → ❌ on
   2026-04-30 — parsed-only) and `pekko.stdout-loglevel`. Both are
   **deferred to the future external-log-server logging cycle**, not the
   immediate post-audit roadmap; the in-process logger is being replaced.

Mailbox subsystem ❌ rows (control-aware mailbox bindings, `default-mailbox.*`
tunables, `mailbox.requirements.*`, and the deprecated
`passivate-idle-entity-after` alias) closed by perfect-pekko-compat Phase 1
on 2026-05-06.

### Audit history

- **2026-05-07 — Perfect-pekko Phase 6 closure (Sharding passivation
  hill-climbing optimizer):** Closes five ⚠️ rows in cluster-sharding —
  340 (`least-frequently-used-strategy.dynamic-aging`), 344
  (`default-strategy.admission.window.optimizer`), 348/349
  (`strategy-defaults.admission.window.{minimum,maximum}-proportion`),
  351 (`strategy-defaults.admission.frequency-sketch.counter-bits`).
  Sub-commit 6.1 wires `frequency-sketch.counter-bits` into
  `countMinSketch.maxCount` via a new `resolveCounterBits` clamp (2..8;
  zero falls back to the historical 4-bit default; >8 folds down because
  cells are uint8). Sub-commit 6.2 plumbs
  `least-frequently-used-strategy.dynamic-aging` from `ShardingConfig`
  through `ShardingSettings`/`ShardSettings` into `Shard`; when enabled,
  `Shard.ageLFUFrequencies()` halves every per-entity counter every
  `lfuAgingThreshold()` LFU increments (anchored to active-entity-limit
  with a floor of 64 so unit tests using small limits still see decay
  within a small envelope batch).  Sub-commit 6.3 adds the hill-climbing
  window optimizer to `compositeStrategy`: `OnAccess` now tallies hit /
  miss counts; every `optimizerInterval` accesses `runOptimizer` adjusts
  `windowProportion` by `optimizerStepSize` in the current direction,
  reverses on a hit-rate regression, and clamps to
  `[windowMinProp, windowMaxProp]`.  HOCON parses
  `default-strategy.admission.window.{minimum-proportion,maximum-proportion,optimizer}`
  with strategy-defaults.* / default-strategy.* precedence; `WindowProportion()`
  / `OptimizerCycles()` accessors expose the runtime state to tests.
  Tests: 4 sketch tests for counter-bits parameterization (sweep + clamp +
  composite-wired + saturation-default), 3 LFU dynamic-aging tests
  (manual-halve, disabled-by-default, auto-trigger-at-threshold), 4
  optimizer tests (bounds-respected, off-keeps-proportion, adapts-to-workload,
  out-of-range-bounds-sanitised), 2 HOCON round-trip tests
  (strategy-defaults parse + default-strategy precedence override).
  Iron Rule 1 full gate: `go test ./...` green per sub-commit, integration
  green on retry (TestAdaptiveShardingRebalance known flake protocol), sbt
  multi-jvm 3/3 196s.  Summary counts: ✅ 281 → 286, ⚠️ 15 → 10.

- **2026-05-07 — Perfect-pekko Phase 5 closure (DistributedData per-CRDT
  serializer cache):** Closes the lone ⚠️ row 325
  (`pekko.cluster.distributed-data.serializer-cache-time-to-live`) by
  giving `Replicator.SerializerCacheTimeToLive` a real production
  consumer. New `cluster/ddata/serializer_cache.go` adds a thread-safe
  TTL cache keyed by `(crdt-key, snapshot-fingerprint)` returning the
  cached JSON-serialized gossip payload. Each CRDT type has a per-snapshot
  fingerprint helper (FNV-64a over sorted version-vector / dots /
  timestamps) cheap enough that the lookup beats `json.Marshal` on
  unchanged state. The six gossip functions
  (`gossipCounter` / `gossipSet` / `gossipMap` / `gossipPNCounter` /
  `gossipORFlag` / `gossipLWWRegister`) route through a new
  `Replicator.cachedMarshal` chokepoint; on a hit the cached bytes are
  reused, on a miss the marshal closure runs and the result is stored
  with the configured TTL. `SerializerCacheStats()` exposes (hits, misses)
  for tests and operator introspection. Setting
  `SerializerCacheTimeToLive = 0` disables the cache — Lookup still misses
  and Store becomes a no-op so the entries map stays empty. Six new tests
  in `serializer_cache_test.go` cover the primitive (hit/miss/expiry with
  injected clock), every fingerprint helper's order-independence and
  content-sensitivity, the gossip-path hit/miss counter trajectory, the
  TTL eviction path with real time, the version-mutation miss path, the
  zero-TTL disabled path, and the all-six-CRDT coverage sweep.
  Iron Rule 1 full gate: `go test ./...` 4m17s, integration retry green
  (TestAdaptiveShardingRebalance flaked once at 40s, passed in 16s on
  isolation per known-flake protocol), sbt multi-jvm 3/3 195s. Summary
  counts: ✅ 280 → 281, ⚠️ 16 → 15.

- **2026-05-07 — Perfect-pekko Phase 4 closure (Multi-DC FD reachability
  margin):** Closes the lone ⚠️ row 280
  (`pekko.cluster.multi-data-center.failure-detector.acceptable-heartbeat-pause`)
  by giving the cross-DC `acceptable-heartbeat-pause` a real consumer.
  The internal Phi-Accrual detector
  (`internal/cluster/phi_accrual_detector.go`) gains
  `IsAvailableWithMargin(margin)` — same φ-vs-threshold check as
  `IsAvailable`, but additionally returns true when the time since the
  last heartbeat is still inside `margin`, granting an additive grace
  window on top of the threshold-based decision. The cluster-level
  wrapper (`cluster/failure_detector.go`) exposes
  `IsAvailableWithMargin(nodeKey, margin)`. `ClusterManager` adds
  `EffectiveAcceptableHeartbeatPause(target)` (cross-DC →
  `cm.CrossDCAcceptableHeartbeatPause`, intra-DC → 0) and
  `IsTargetAvailable(nodeKey, target)`, which the two existing reach
  ability call sites — `CheckReachability` and `GetClusterStats` —
  now invoke instead of `Fd.IsAvailable`. Intra-DC targets keep
  byte-identical phi-only semantics. Tests
  (`cluster/multi_dc_fd_test.go`) cover (a) the wrapper's three states
  (unseen / fresh / phi-spiked-within-margin / phi-spiked-past-margin),
  (b) `EffectiveAcceptableHeartbeatPause` returning 0 intra-DC,
  configured value cross-DC, and 0 when the cross-DC knob is unset,
  (c) the headline behaviour: under the same physical pause, an
  intra-DC peer flips unreachable while a cross-DC peer in the same
  cluster stays reachable until the margin expires, and (d) intra-DC
  reachability ignores a generous cross-DC margin (parity with plain
  `IsAvailable`). Iron Rule 1 full gate green: unit workspace, integration
  `-tags integration -p 1 -count=1 -timeout 600s` (clean on second
  run; first run hit the previously-documented `TestAdaptiveShardingRebalance`
  cross-suite flake that passes in isolation and on retry, same
  behaviour observed in Phase 2.x / 3.x), sbt multi-jvm 3/3. Summary
  counts: ✅ 279 → 280, ⚠️ 17 → 16. Closes one row: 280.
- **2026-05-07 — Perfect-pekko Phase 3 closure (sub-commits 3.1–3.2,
  Artery receive buffer pools):** Closes the two ⚠️ rows for the
  receive-side buffer pools (`buffer-pool-size` row 177 and
  `large-buffer-pool-size` row 179) by giving them real production
  consumers. Sub-commit 3.1 (commit `0d36b4e`) adds
  `internal/core/buffer_pool.go` — a fixed-cap channel-backed pool
  modelled on Pekko's `EnvelopeBufferPool`. `Get()` returns a buffer
  pre-sized to `bufferSize` (or allocates fresh on a cold pool, with
  an atomic `AllocCount` counter for the regression tests); `Put()`
  drops mis-sized buffers and is non-blocking on a full pool, letting
  the GC reclaim excess buffers without backpressure. The pool is
  nil-receiver-safe so call sites that have not been migrated stay
  byte-for-byte identical to the pre-Phase-3 behaviour. Sub-commit
  3.2 (commit `3add8d0`) wires the pool into the receive path:
  `NodeManager` grows lazily-built `bufferPool` and `largeBufferPool`
  fields plus a `streamBufferPool(streamId)` selector — streams 1
  (control) and 2 (ordinary) share the regular pool sized to
  `EffectiveBufferPoolSize × MaxFrameSize`, stream 3 (large) gets its
  own pool sized to `EffectiveLargeBufferPoolSize ×
  EffectiveMaximumLargeFrameSize`. The two pools are independent —
  Pekko parity. `tcpArteryReadLoop` now takes `*BufferPool`, calls
  `Get()` once on entry and `Put()` once on exit (deferred), and
  `Process` / `runLaneReadLoop` pass the right per-stream pool from
  NodeManager. Test additions
  (`internal/core/buffer_pool_wiring_test.go`) cover (a) per-stream
  pool selection and lazy-init memoisation, (b) Pekko-default fallback
  when NodeManager is unconfigured, (c) end-to-end frame roundtrip
  with `AllocCount` and post-loop pool depth assertions, (d) the
  steady-load regression guard — eight sequential read loops sharing
  a 1-buffer pool keep `AllocCount = 1` after warmup, proving the
  per-frame allocations Phase 3 exists to eliminate are gone, and
  (e) regular-vs-large pool independence under load. Iron Rule 1 full
  gate green for both sub-commits: unit workspace, integration
  `-tags integration -p 1 -count=1 -timeout 600s` (clean on second
  run; first run hit the previously-documented
  `TestCluster_GoDominantMixed` cross-suite flake that passes in
  isolation and on retry, same behaviour observed in Phase 2.x), sbt
  multi-jvm 3/3. Summary counts: ✅ 277 → 279, ⚠️ 19 → 17. Closes
  two rows: 177, 179.
- **2026-05-06 — Perfect-pekko Phase 2 closure (sub-commits 2.1–2.4,
  sender-side system-message redelivery):** Closes the three Artery
  redelivery ⚠️ rows (`system-message-buffer-size`,
  `system-message-resend-interval`, `give-up-system-message-after`)
  by wiring the per-association `SystemMessageOutbox` end-to-end.
  Sub-commit 2.1 (commit `ec032fd`) lands the buffer module — a
  cumulative-ack ring with `OldestFirstAttempt` for the give-up
  deadline and a defensive-copy `Snapshot()` for the resend ticker.
  Sub-commit 2.2 (commit `4834b1d`) wires it into `ProcessConnection`
  for streamId=1 control associations, refactors `cluster_watch.go`
  (WATCH/UNWATCH/Terminated) from raw `Outbox()<-frame` writes to
  `SendSystem(seq, frame)`, starts the `runSystemRedelivery`
  goroutine, and adds receiver-side seq dedupe via
  `lastDeliveredSystemSeq` so resends are idempotent. Sub-commit 2.3
  (commit `df0dbc9`) adds the sender-side ack consumer
  (`handleControlMessage` "h" case decodes
  `SystemMessageDeliveryAck`, looks up the outbound assoc by
  `From.Address`, calls `PruneAcked`). Sub-commit 2.4 (this commit)
  adds the give-up timer (`runSystemRedelivery` checks
  `OldestFirstAttempt` against `give-up-system-message-after` on
  every tick) and the `quarantineForSystemRedelivery` helper that
  drains the buffer, sets state to QUARANTINED, closes the conn, and
  emits a flight-recorder dump. `SendSystem` on a full buffer also
  triggers the same quarantine path (Pekko's "buffer-full ⇒ give
  up" semantics). Iron Rule 1 full gate green: unit workspace,
  integration `-tags integration -p 1` 255s root, sbt multi-jvm
  3/3 197s. Summary counts: ✅ 274 → 277, ⚠️ 22 → 19. Closes three
  rows: 190, 194, 195.
- **2026-05-06 — Perfect-pekko Phase 1 closure (sub-commits 1.1–1.6, mailbox
  foundation):** The `actor/mailbox` package now ships four production-ready
  factories — `UnboundedMailbox`, `BoundedMailbox`, `UnboundedControlAwareMailbox`,
  `BoundedControlAwareMailbox` — and the actor cell (sub-commit 1.2, commit
  `b743cd3`) routes system messages through a dedicated priority channel that
  always drains ahead of the configured user-side mailbox. Sub-commit 1.6
  (this audit entry) closes the wiring: `hocon_config.go:extractMailboxConfig`
  parses `pekko.actor.default-mailbox.{mailbox-type, mailbox-capacity,
  mailbox-push-timeout-time}`, the `pekko.actor.mailbox.requirements.*`
  binding map (against the 18 known Pekko/Akka MessageQueueSemantics FQCNs),
  and the `pekko.dispatch.{Unbounded,Bounded}ControlAwareMailbox` blocks; the
  parsed values are published to a process-level registry via
  `mailbox.SetGlobalConfig` and consumed at SpawnActor time by both
  `localActorSystem.resolvePhase1Mailbox` and `Cluster.resolvePhase1Mailbox`.
  Actors declare requirements via the `mailbox.RequiresControlAwareMessageQueueSemantics`
  marker interface; mismatch with the bound factory is a HARD start failure
  (the actor's dispatch goroutine is not created, an error is logged, and the
  caller receives a non-functional ActorRef). The deprecated
  `pekko.cluster.sharding.passivate-idle-entity-after` alias is honored as
  fallback for `passivation.default-idle-strategy.idle-entity.timeout`
  (canonical wins when both are set; "off" / "false" / "0" disable
  passivation). `Props.WithMailbox(name)` is the new fluent selector.
  Default behaviour is preserved: an actor with no `Props.MailboxName`, no
  `MailboxRequirement`, and no HOCON `default-mailbox.mailbox-type` keeps the
  legacy `BaseActor.mailbox` 256-cap chan, matching the plan's "must remain
  identical to today" constraint. Iron Rule 1 full gate green: unit
  workspace, integration `-tags integration -p 1` 255s root, sbt multi-jvm
  3/3 195s. Summary counts: ✅ 267 → 274, ❌ 9 → 2. Closes seven rows: 150,
  151, 152, 153, 154, 155, 357.
- **2026-05-06 — Perfect-pekko Phase 0 (audit refresh + ssl.* reclassification):**
  Three SSL `ssl.*` rows previously parked at ⚠️ (forward-compat parses with
  deferred consumers) reclassified to 🚫 (Go/JVM API-shape incompatibility),
  matching their long-standing classification in `docs/LEFTWORKS.md` §9. The
  flipped paths are `pekko.remote.artery.ssl.ssl-engine-provider`,
  `ssl.config-ssl-engine.random-number-generator`, and
  `ssl.rotating-keys-engine.*` — each is a JVM-only plug-in / Provider /
  rotation-watcher mechanism with no Go bridge. Notes updated to make the
  JVM-only reason explicit instead of the "deferred consumer" framing.
  Re-grep of upstream Pekko 1.1.5 `reference.conf` confirmed the existing
  audit covers every leaf path that gekka could portably wire; the additional
  upstream paths surfaced by the mechanical diff (~377 of 935) are either (a)
  documented under section-relative table forms (cluster.client / pub-sub /
  singleton / failure-detector / multi-DC sub-tables), (b) wildcard-handled
  (`deployment.{path}.*`, `role.{name}.*`, `serializers.*`, etc.), or (c)
  already classified JVM-only in `docs/LEFTWORKS.md` §9 (classic transport,
  dispatcher hierarchy, JMX, Java NIO, Java serialization, Akka classic,
  scheduler tick wheel). No new audit rows added. Summary counts: ⚠️ 25 → 22,
  🚫 1 → 4. The plan's `passivate-idle-entity-after ❌→✅` flip is deferred
  to Phase 1.6 where the runtime alias actually lands — flipping in Phase 0
  without a runtime consumer would violate the "Parsed AND consumed" honesty
  rule established by the 2026-04-30 ✅-honesty pass.
- **2026-05-05 — Portable-pekko milestone closed (sub-plans 8a–8i):** All
  26 ⚠️ rows surfaced by the 2026-04-30 honesty pass were wired onto real
  production consumers. **8a** multi-DC FD `expected-response-after`
  (commit `694c643`); **8b** sharding ddata replicator overrides; **8c**
  sharding coordinator-singleton manager overrides; **8d** sharding
  healthcheck `/health/ready` endpoint (commit `e46baef`); **8e** Artery
  inbound + outbound restart counters with rolling cap; **8f** Artery
  multi-TCP transport refactor — outbound-lanes opens N parallel
  `streamId=2` TCPs per peer with per-lane handshake/writer/buffer +
  recipient-hash dispatch + inbound coalescence; inbound-lanes adds
  per-association lane fan-out by recipient hash (commits `6763083`
  inbound + `30983b3` outbound); **8g** compression-table-manager
  production wiring — `core.StartCompressionTableManager` replaces the
  test-only construction (commit `401c5b7`); **8h** idle/quarantine sweep
  schedulers wired into the cluster lifetime (commit `b6011c1`); **8i**
  shutdown-flush + death-watch notification flush consumers — new
  before-actor-system-terminate phase task drains every association
  outbox; cross-network DeathWatchNotification deferred via goroutine
  (commit `54df871`). Closes the portable-pekko roadmap (16/16 sub-plans);
  `docs/LEFTWORKS.md` §11 honesty-pass subsection emptied. Summary counts:
  ✅ 248 → 267, ⚠️ 41 → 25.
- **2026-05-05 — Sub-plan 7 (External shard allocation client-timeout):**
  Wired `pekko.cluster.sharding.external-shard-allocation-strategy.client-timeout`
  onto the canonical Pekko path (was reading a different sub-path).
  Summary: ✅ 247 → 248.
- **2026-05-05 — Sub-plan 6 (Reliable delivery) landed:** Wired
  `pekko.reliable-delivery.*` into `ProducerController`,
  `ConsumerController`, and `WorkPullingProducerController`. New
  `delivery.Config` (with Pekko reference defaults) re-exported as
  `ClusterConfig.ReliableDelivery`. `chunk-large-messages` drives payload
  chunking, `flow-control-window` drives `Request.RequestUpToSeqNr`,
  `only-flow-control` suppresses the gap-filling Resend, and
  `buffer-size` caps `WorkPulling.pendingWork` with a dropped-count
  counter. Summary: ✅ 240 → 247.
- **2026-05-05 — Sub-plan 5 (Watch failure detector) landed:** Wired
  `pekko.remote.watch-failure-detector.*` (8 keys: implementation-class,
  heartbeat-interval, threshold, max-sample-size, min-std-deviation,
  acceptable-heartbeat-pause, unreachable-nodes-reaper-interval,
  expected-response-after) into a new `cluster.WatchFailureDetector`
  distinct from the cluster membership FD. The detector is fed
  heartbeats from `ClusterManager.handleHeartbeat` /
  `handleHeartbeatRsp` (using the same `host:port-uid` key the
  cluster FD already uses). On the first `Cluster.watchRemote` call,
  a reaper goroutine lazy-starts and ticks at
  `unreachable-nodes-reaper-interval`; each tick walks watched nodes
  and calls `triggerRemoteNodeDeath` for any whose
  `IsAvailable` flips to false. `acceptable-heartbeat-pause` is
  enforced at the wrapper level so an aggressive watch-FD pause
  fires `Terminated` before the cluster FD's slower threshold would.
  `implementation-class` parses for HOCON parity but only the
  bundled Phi-Accrual implementation is honoured. Summary counts:
  ✅ 237 → 238, ❌ 12 → 11. LEFTWORKS.md §11 "Watch failure detector"
  subsection emptied.
- **2026-05-01 — Sub-plan 4 (Typed receptionist) landed:** Wired
  `pekko.cluster.typed.receptionist.write-consistency`,
  `pruning-interval`, and `distributed-key-count` into the existing
  typed receptionist actor in `actor/typed/receptionist/`. Added
  `ddata.WriteMajority` (best-effort: same gossip semantics as
  `WriteAll` until quorum-acked writes ship), the
  `TypedReceptionistConfig` field on `ClusterConfig`, a HOCON parse
  block at `pekko.cluster.typed.receptionist.*`, and a
  `ReplicatorWriter` interface seam so writes/reads target a
  `ShardKey(id, count) = "receptionist-<n>-<id>"` ddata bucket. The
  receptionist `Behavior` now schedules a periodic `pruneTick` via
  the typed `TimerScheduler` at the configured cadence; each tick
  walks tracked services and `RemoveFromSet`s any path whose
  `ctx.System().Resolve` errors, using the configured
  `ddata.WriteConsistency`. Summary counts: ✅ 234 → 237, ❌ 15 → 12.
  LEFTWORKS.md §11 "Typed-API surface" subsection emptied.
- **2026-05-01 — Sub-plan 3 (Discovery) landed:** Wired
  `pekko.discovery.config.*`, `pekko.discovery.aggregate.*`, and
  `pekko.discovery.pekko-dns.*` into the existing
  `discovery.SeedProvider` registry. Three new providers ship in the
  core `discovery` package (no extension dependency):
  `ConfigProvider` (services map + `services-path` indirection),
  `PekkoDNSProvider` (DNS-SRV via `net.LookupSRV`), and a real
  HOCON-driven factory for the existing `AggregateProvider` (replaces
  the empty-stub factory previously registered). All three plug into
  the `applyDiscoveredSeeds` helper extracted from `cluster.go`'s
  former inline block, which reads `cfg.Discovery.{Type,Config}` and
  feeds `provider.FetchSeedNodes()` into `cfg.SeedNodes`.
  Endpoint shape for `pekko.discovery.config` is the gekka string-list
  form `["host:port", ...]`; the JVM-only `{host, port}` object form
  is unsupported because gekka-config does not unmarshal lists of
  objects. `class` FQCN fields stay 🚫 (JVM-only). Summary counts:
  ✅ 231 → 234, ❌ 18 → 15. LEFTWORKS.md §11 "Discovery" subsection
  removed.
- **2026-04-30 — Sub-plan 2 (Sharded daemon process) landed:** Wired
  `pekko.cluster.sharded-daemon-process.keep-alive-interval` (10s default)
  and `pekko.cluster.sharded-daemon-process.sharding.role` into the
  existing `cluster/sharding/sharded_daemon.go` runtime. The keep-alive
  interval drives a per-process ticker (`runKeepAliveLoop`) that re-sends
  `DaemonStart` to every entity index; the role is consumed at
  `InitShardedDaemonProcess` resolution time via the new
  `GetDefaultShardedDaemonProcessShardingRole()` package-level default.
  Per Pekko's reference.conf comment, other `sharded-daemon-process.sharding.*`
  sub-keys (remember-entities, passivation, number-of-shards) are
  intentionally not exposed. Summary counts: ✅ 230 → 231, ❌ 19 → 18.
  LEFTWORKS.md §11 "Sharded daemon process" subsection emptied.
- **2026-04-30 — Sub-plan 1 (Typed-API misc) landed:** Wired
  `pekko.actor.typed.restart-stash-capacity` (sizes the typed-actor stash
  buffer in `actor/typed/actor.go` PreStart via
  `typed.GetDefaultRestartStashCapacity()`),
  `pekko.persistence.typed.log-stashing` (gates DEBUG lines around
  stash/unstash in `persistence/typed/event_sourcing.go`), and
  `pekko.cluster.ddata.typed.replicator-message-adapter-unexpected-ask-timeout`
  (bounds `TypedReplicatorAdapter.AskGet/AskUpdate` in
  `cluster/ddata/typed_replicator.go` and delivers
  `ErrUnexpectedAskTimeout` on the slow path). Each path is gated by a unit
  test that observes the runtime effect, not just the parse. Summary
  counts: ✅ 227 → 230, ❌ 22 → 19. LEFTWORKS.md §11 "Typed-API surface"
  subsection emptied.
- **2026-04-30 — ✅-honesty pass (post-roadmap spot-check):** Pre-roadmap
  spot-check audited 78 ✅ rows in 6 namespaces (artery-advanced,
  multi-DC FD, sharding, ddata, persistence-typed, AALD) and found 27
  dishonest rows. Downgrades applied: 26 ✅ → ⚠️ and 1 ✅ → ❌. The
  pattern was concentrated: (a) 14 artery-advanced lifecycle knobs added
  during round-2 sessions 29-32 followed the `Effective<X>` test-only
  getter pattern (rows 75-78, 81-82, 90-100); (b) sharding sub-namespace
  duplications (rows 265-267 ddata, 270-272 coordinator-singleton,
  291-292 healthcheck) are parsed onto dedicated structs but never
  threaded onto their consumers; (c) `external-shard-allocation-strategy.client-timeout`
  (row 258) was ✅ but the canonical Pekko path is never parsed — the
  current Go code reads a different sub-path. Honest ✅ rate in the
  audited surface: 50/78 = 64%.
- **2026-04-30 — Logging honesty downgrade:** `pekko.loglevel` row 19
  flipped ✅ → ❌. The field is parsed into `NodeConfig.LogLevel`
  (cluster.go:223) but never read by any non-test code, so the previous
  ✅ violated the post-refactor "Parsed AND consumed" rule. Both
  `pekko.loglevel` and `pekko.stdout-loglevel` are deferred to the future
  external-log-server logging cycle (the in-process logger is being
  replaced); their Notes call this out explicitly. Other log-* paths
  (`log-config-on-start`, `log-dead-letters`,
  `log-dead-letters-during-shutdown`,
  `pekko.remote.artery.log-frame-size-exceeding`) were spot-checked and
  remain honestly ✅.
- **2026-04-30 — Taxonomy refactor:** Replaced 3-symbol legend with
  5-symbol scheme. Reclassified former ❌ rows: 3 → ☕ (dispatcher × 2,
  `cluster.metrics`), 1 → 🚫
  (`failure-detector.implementation-class`). Honest "not implemented"
  notes replaced "No feature" hand-waving on the remaining ❌ rows.
  Added mailbox subsection (control-aware mailbox flagged as critical
  gap). Stale "future X consumer" / "wires in part N" / roadmap-session
  references removed; 7 rows previously claimed ✅ but only exposed via a
  getter were honestly downgraded to ⚠️. New `docs/LEFTWORKS.md` §11
  "Future Work — Portable" centralises the unwired and deferred-consumer
  lists.
- **Round-2 close-out (Session 41, commit `2d72eb2`):** 0 HARDCODED + 0
  NO FEATURE on the portable surface. Full test gate (unit + integration
  + sbt multi-jvm:test 3/3) passes. See
  `docs/superpowers/plans/2026-04-24-config-completeness-round2.md` for
  the session ledger.
