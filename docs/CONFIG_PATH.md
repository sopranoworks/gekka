# Configuration Path Compatibility: Pekko reference.conf vs Gekka

This document provides a complete comparison of all Pekko reference.conf configuration paths
against what gekka currently parses. It covers every module in the Pekko source tree.

Legend:
- ✅ = Parsed AND consumed in gekka
- ⚠️ = Parsed for forward-compat at the canonical Pekko path; consumer deferred (Note must say what's deferred and which session it tracks)
- ☕ = JVM-only — no equivalent capability exists in the Go runtime (e.g., dispatcher hierarchy, JMX/Sigar metrics, Java scheduler internals)
- 🚫 = Go/JVM API-shape incompatibility — the HOCON value or path semantics doesn't transfer to Go (e.g., the value is a Java FQCN to be loaded via reflection; gekka has its own Go impl wired directly)
- ❌ = Not implemented in gekka — portable in principle, no current bridge (tracked in `docs/LEFTWORKS.md` §11)

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
| `pekko.actor.default-mailbox.mailbox-type` | `UnboundedMailbox` | ❌ | Not implemented — gekka uses a fixed Go-channel-backed mailbox per actor; no pluggable mailbox type yet. Tracked in `docs/LEFTWORKS.md` §11 |
| `pekko.actor.default-mailbox.mailbox-capacity` | `1000` | ❌ | Not implemented — applies only to bounded mailboxes (currently absent). Tracked in `docs/LEFTWORKS.md` §11 |
| `pekko.actor.default-mailbox.mailbox-push-timeout-time` | `10s` | ❌ | Not implemented — applies only to bounded mailboxes. Tracked in `docs/LEFTWORKS.md` §11 |
| `pekko.actor.mailbox.requirements.*` | (binding map) | ❌ | Not implemented — type-class-based mailbox requirements have no analog in Go yet. Tracked in `docs/LEFTWORKS.md` §11 |
| `pekko.dispatch.UnboundedControlAwareMailbox` (binding) | — | ❌ | **Critical gap.** Control-aware mailboxes prioritise system messages over user messages; without this, watch/terminate/failure handling cannot guarantee the system-priority semantics Pekko/Akka actors rely on. Tracked in `docs/LEFTWORKS.md` §11 |
| `pekko.dispatch.BoundedControlAwareMailbox` (binding) | — | ❌ | **Critical gap.** Bounded variant of the control-aware mailbox; same priority semantics with a capacity cap. Tracked in `docs/LEFTWORKS.md` §11 |

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
| `pekko.remote.artery.advanced.buffer-pool-size` | `128` | ⚠️ | Parsed and exposed via `NodeManager.EffectiveBufferPoolSize`; receive-buffer-pool consumer not yet implemented |
| `pekko.remote.artery.advanced.maximum-large-frame-size` | `2 MiB` | ✅ | Round-2 session 30 — applied to the inbound read loop when `assoc.streamId == AeronStreamLarge` (stream 3) via `effectiveStreamFrameSizeCap`; streams 1/2 keep `maximum-frame-size` |
| `pekko.remote.artery.advanced.large-buffer-pool-size` | `32` | ⚠️ | Parsed and exposed via `NodeManager.EffectiveLargeBufferPoolSize`; large-stream buffer-pool consumer not yet implemented |
| `pekko.remote.artery.advanced.outbound-large-message-queue-size` | `256` | ✅ | Recorded on NodeManager (`EffectiveOutboundLargeMessageQueueSize`) for the large-stream outbox |
| `pekko.remote.artery.advanced.compression.actor-refs.max` | `256` | ⚠️ | Plumbed onto `NodeManager.CompressionActorRefsMax`; `CompressionTableManager.UpdateActorRefTable` enforces the cap but the CTM is not constructed in production code (test-only) |
| `pekko.remote.artery.advanced.compression.actor-refs.advertisement-interval` | `1m` | ⚠️ | Plumbed onto `NodeManager.CompressionActorRefsAdvertisementInterval`; `CompressionTableManager.StartAdvertisementScheduler` is invoked only from tests |
| `pekko.remote.artery.advanced.compression.manifests.max` | `256` | ⚠️ | Plumbed onto `NodeManager.CompressionManifestsMax`; `CompressionTableManager.UpdateManifestTable` enforces the cap but the CTM is not constructed in production code (test-only) |
| `pekko.remote.artery.advanced.compression.manifests.advertisement-interval` | `1m` | ⚠️ | Plumbed onto `NodeManager.CompressionManifestsAdvertisementInterval`; advertisement scheduler invoked only from tests |
| `pekko.remote.artery.advanced.tcp.connection-timeout` | `5s` | ✅ | Threaded into `TcpClient.DialTimeout` and `DialRemote`'s association poll |
| `pekko.remote.artery.advanced.tcp.outbound-client-hostname` | `""` | ✅ | Sets the local source address for outbound dials (`net.Dialer.LocalAddr`) |
| `pekko.remote.artery.advanced.inbound-lanes` | `4` | ⚠️ | Plumbed onto `NodeManager.InboundLanes` and exposed via `EffectiveInboundLanes()`; no production consumer multiplexes inbound dispatch (getter is test-only) |
| `pekko.remote.artery.advanced.outbound-lanes` | `1` | ⚠️ | Plumbed onto `NodeManager.OutboundLanes` and exposed via `EffectiveOutboundLanes()`; no production consumer multiplexes outbound dispatch (getter is test-only) |
| `pekko.remote.artery.advanced.outbound-message-queue-size` | `3072` | ✅ | Sizes each association's outbox channel |
| `pekko.remote.artery.advanced.system-message-buffer-size` | `20000` | ⚠️ | Parsed and exposed via `NodeManager.EffectiveSystemMessageBufferSize`; sender-side system-message redelivery not yet implemented |
| `pekko.remote.artery.advanced.outbound-control-queue-size` | `20000` | ✅ | Sizes each outbound control-stream (streamId=1) association's outbox |
| `pekko.remote.artery.advanced.handshake-timeout` | `20s` | ✅ | Outbound association gives up after this deadline |
| `pekko.remote.artery.advanced.handshake-retry-interval` | `1s` | ✅ | Re-sends HandshakeReq at this cadence until ASSOCIATED |
| `pekko.remote.artery.advanced.system-message-resend-interval` | `1s` | ⚠️ | Parsed and exposed via `NodeManager.EffectiveSystemMessageResendInterval`; sender-side system-message redelivery loop not yet implemented |
| `pekko.remote.artery.advanced.give-up-system-message-after` | `6h` | ⚠️ | Parsed and exposed via `NodeManager.EffectiveGiveUpSystemMessageAfter`; sender-side give-up timer not yet implemented |
| `pekko.remote.artery.advanced.stop-idle-outbound-after` | `5m` | ⚠️ | Plumbed onto `NodeManager.StopIdleOutboundAfter` and exposed via `EffectiveStopIdleOutboundAfter`; no production idle-sweep consumer reads the getter |
| `pekko.remote.artery.advanced.quarantine-idle-outbound-after` | `6h` | ⚠️ | Plumbed onto `NodeManager.QuarantineIdleOutboundAfter`; `SweepIdleOutboundQuarantine` reads it but no production scheduler invokes the sweep |
| `pekko.remote.artery.advanced.stop-quarantined-after-idle` | `3s` | ⚠️ | Plumbed onto `NodeManager.StopQuarantinedAfterIdle`; `EffectiveStopQuarantinedAfterIdle` getter has only test callers |
| `pekko.remote.artery.advanced.remove-quarantined-association-after` | `1h` | ⚠️ | Plumbed onto `NodeManager.RemoveQuarantinedAssociationAfter`; `EffectiveRemoveQuarantinedAssociationAfter` getter has only test callers |
| `pekko.remote.artery.advanced.shutdown-flush-timeout` | `1s` | ⚠️ | Plumbed onto `NodeManager.ShutdownFlushTimeout`; no production coordinated-shutdown consumer reads it |
| `pekko.remote.artery.advanced.death-watch-notification-flush-timeout` | `3s` | ⚠️ | Plumbed onto `NodeManager.DeathWatchNotificationFlushTimeout`; no production death-watch consumer reads it |
| `pekko.remote.artery.advanced.inbound-restart-timeout` | `5s` | ⚠️ | Plumbed onto `NodeManager.InboundRestartTimeout`; `TryRecordInboundRestart` reads it but is invoked only from tests |
| `pekko.remote.artery.advanced.inbound-max-restarts` | `5` | ⚠️ | Plumbed onto `NodeManager.InboundMaxRestarts`; `TryRecordInboundRestart` enforces the cap but is invoked only from tests |
| `pekko.remote.artery.advanced.outbound-restart-backoff` | `1s` | ⚠️ | Plumbed onto `NodeManager.OutboundRestartBackoff`; no production dialer consumer reads it |
| `pekko.remote.artery.advanced.outbound-restart-timeout` | `5s` | ⚠️ | Plumbed onto `NodeManager.OutboundRestartTimeout`; `TryRecordOutboundRestart` reads it but is invoked only from tests |
| `pekko.remote.artery.advanced.outbound-max-restarts` | `5` | ⚠️ | Plumbed onto `NodeManager.OutboundMaxRestarts`; `TryRecordOutboundRestart` enforces the cap but is invoked only from tests |
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
| `pekko.remote.artery.ssl.ssl-engine-provider` | (Pekko FQCN) | ⚠️ | JVM-only — gekka has a fixed Go `crypto/tls` engine (no FQCN plug-in). Configure certificates via the `tls.*` paths above. |
| `pekko.remote.artery.ssl.config-ssl-engine.random-number-generator` | `""` | ⚠️ | JVM-only — gekka uses `crypto/rand.Reader` (Go `tls.Config.Rand` field is left at default). |
| `pekko.remote.artery.ssl.rotating-keys-engine.*` | — | ⚠️ | JVM-only — Pekko's K8s secret rotation; gekka has no equivalent. |
| `pekko.remote.watch-failure-detector.*` | (various) | ❌ | Not implemented — gekka watches via association quarantine + heartbeat FD (`cluster/failure_detector.go`); tracked in `docs/LEFTWORKS.md` §11 |
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
| `failure-detector.acceptable-heartbeat-pause` | `10s` | ⚠️ | Plumbed via `MultiDCFailureDetectorConfig` onto `cm.CrossDCAcceptableHeartbeatPause`; cross-DC reachability-margin consumer not yet implemented |
| `failure-detector.expected-response-after` | `1s` | ⚠️ | Plumbed via `MultiDCFailureDetectorConfig` onto `cm.CrossDCExpectedResponseAfter`; no consumer reads the field outside the assignment line (parallel deferral to row 174) |

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
| `pekko.cluster.distributed-data.serializer-cache-time-to-live` | `10s` | ⚠️ | Parsed into `DistributedDataConfig.SerializerCacheTimeToLive` and set on `Replicator.SerializerCacheTimeToLive`; per-CRDT serialization cache not yet implemented (field has no readers beyond the setter) |

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
| `pekko.cluster.sharding.passivation.least-frequently-used-strategy.dynamic-aging` | `off` | ⚠️ | Parsed into `ShardingConfig.PassivationLFUDynamicAging` (Round-2 session 25); frequency-aging loop not yet implemented |
| `pekko.cluster.sharding.passivation.strategy = default-strategy` | `"default-idle-strategy"` | ✅ | Round-2 session 26. Selects the W-TinyLFU composite strategy implemented in `cluster/sharding/passivation_composite.go`. The plan-internal alias `"composite-strategy"` is normalised at parse time so configs that use either convention reach the same code path. |
| `pekko.cluster.sharding.passivation.default-strategy.active-entity-limit` | `100000` | ✅ | Round-2 session 26. Active strategy limit reaches `ShardingConfig.PassivationActiveEntityLimit`; the composite strategy splits it between admission-window and main areas via `PassivationWindowProportion`. |
| `pekko.cluster.sharding.passivation.default-strategy.admission.window.policy` | `"least-recently-used"` | ✅ | Round-2 session 26. `ShardingConfig.PassivationWindowPolicy`. Currently only `"least-recently-used"` is honoured at runtime; non-empty values activate the admission window. |
| `pekko.cluster.sharding.passivation.default-strategy.admission.window.optimizer` | `"hill-climbing"` | ⚠️ | Parsed for forward-compat with Pekko configs (Round-2 session 26); adaptive resizing loop not yet implemented |
| `pekko.cluster.sharding.passivation.default-strategy.admission.filter` | `"frequency-sketch"` | ✅ | Round-2 session 26. `ShardingConfig.PassivationFilter`. Recognised values: `"frequency-sketch"` (admission filter active), `"off"` / `"none"` (composite degrades to LRU window + LRU main with no admission gate). |
| `pekko.cluster.sharding.passivation.default-strategy.replacement.policy` | `"least-recently-used"` | ✅ | Round-2 session 26. Per-strategy override for `ShardingConfig.PassivationReplacementPolicy`; wins over the LRU-strategy block when both are present. |
| `pekko.cluster.sharding.passivation.strategy-defaults.admission.window.proportion` | `0.01` | ✅ | Round-2 session 26. Admission-window size as a fraction of the active-entity-limit. `default-strategy.admission.window.proportion` overrides per-strategy. |
| `pekko.cluster.sharding.passivation.strategy-defaults.admission.window.minimum-proportion` | `0.01` | ⚠️ | Parsed (Round-2 session 26); consumer is the hill-climbing optimizer, not yet implemented |
| `pekko.cluster.sharding.passivation.strategy-defaults.admission.window.maximum-proportion` | `1.0` | ⚠️ | Parsed (Round-2 session 26); consumer is the hill-climbing optimizer, not yet implemented |
| `pekko.cluster.sharding.passivation.strategy-defaults.admission.frequency-sketch.depth` | `4` | ✅ | Round-2 session 26. Count-min-sketch row count; threaded into `cluster/sharding/passivation_admission.go`. |
| `pekko.cluster.sharding.passivation.strategy-defaults.admission.frequency-sketch.counter-bits` | `4` | ⚠️ | Round-2 session 26. Pekko documents 2/4/8/16/32/64; gekka stores 4-bit counters regardless. Parsed for forward-compat. |
| `pekko.cluster.sharding.passivation.strategy-defaults.admission.frequency-sketch.width-multiplier` | `4` | ✅ | Round-2 session 26. Sketch width = active-entity-limit × multiplier (rounded up to a power of two, clamped to 64). |
| `pekko.cluster.sharding.passivation.strategy-defaults.admission.frequency-sketch.reset-multiplier` | `10.0` | ✅ | Round-2 session 26. Triggers the sketch's halving reset every (active-entity-limit × multiplier) accesses. |
| `pekko.cluster.sharding.guardian-name` | `"sharding"` | ✅ | `ShardingConfig.GuardianName` — top-level guardian actor under which all sharding regions are spawned |
| `pekko.cluster.sharding.role` | `""` | ✅ | Filters shard allocation by role |
| `pekko.cluster.sharding.remember-entities-store` | `"ddata"` | ✅ | Round-2 session 35. `"ddata"` wires `DDataEntityStore` (existing); `"eventsourced"` wires `EventSourcedEntityStore` (new) — both implement `ShardStore` and are selected in `sharding.go` from `cluster.cfg.Sharding.RememberEntitiesStore`. |
| `pekko.cluster.sharding.passivate-idle-entity-after` | `null` | ❌ | Deprecated in Pekko — replacement `passivation.default-idle-strategy.idle-entity.timeout` is fully ✅ (see sharding section above); gekka does not honor the legacy knob |
| `pekko.cluster.sharding.number-of-shards` | `1000` | ✅ | Wired to coordinator/region |
| `pekko.cluster.sharding.rebalance-interval` | `10s` | ✅ | Applied to ShardCoordinator.RebalanceInterval |
| `pekko.cluster.sharding.least-shard-allocation-strategy.rebalance-threshold` | `1` | ✅ | Applied to NewLeastShardAllocationStrategy(threshold) |
| `pekko.cluster.sharding.least-shard-allocation-strategy.max-simultaneous-rebalance` | `3` | ✅ | Applied to NewLeastShardAllocationStrategy(maxSimultaneous) |
| `pekko.cluster.sharding.least-shard-allocation-strategy.rebalance-absolute-limit` | `0` | ✅ | Round-2 session 33 — when `> 0` selects the Pekko 1.0+ two-phase `LeastShardAllocationStrategyV2` (`cluster/sharding/strategy.go`); `0` keeps the legacy threshold-based strategy. |
| `pekko.cluster.sharding.least-shard-allocation-strategy.rebalance-relative-limit` | `0.1` | ✅ | Round-2 session 33 — fraction of total shards capping each rebalance round under V2 (`max(1, min(int(rel*total), absolute))`). |
| `pekko.cluster.sharding.external-shard-allocation-strategy.client-timeout` | `5s` | ❌ | Not implemented — the canonical Pekko path is never parsed in `hocon_config.go`. The strategy in `cluster/sharding/strategy.go` reads a different sub-path (`external.timeout` of the adaptive-rebalancing block); no Go consumer reads the canonical key. Tracked in `docs/LEFTWORKS.md` §11 |
| `pekko.cluster.sharding.event-sourced-remember-entities-store.max-updates-per-write` | `100` | ✅ | Round-2 session 34 — Shard buffers EntityStarted/EntityStopped events; the buffer is flushed in a single AsyncWriteMessages call once it hits the cap, with a final flush in PostStop. Cap of `0` keeps the legacy one-event-per-write path. |
| `pekko.cluster.sharding.state-store-mode` | `"ddata"` | ☕ | JVM-only — gekka commits to `ddata` exclusively for sharding state; the `persistence` mode and its tunables (`snapshot-after`, `keep-nr-of-batches`, `journal-plugin-id`, `snapshot-plugin-id`) are N/A in gekka. |
| `pekko.cluster.sharding.snapshot-after` | `1000` | ☕ | JVM-only — `state-store-mode = persistence` subset; gekka uses `ddata`. |
| `pekko.cluster.sharding.keep-nr-of-batches` | `2` | ☕ | JVM-only — `state-store-mode = persistence` subset; gekka uses `ddata`. |
| `pekko.cluster.sharding.journal-plugin-id` | `""` | ☕ | JVM-only — `state-store-mode = persistence` subset; gekka uses `ddata`. |
| `pekko.cluster.sharding.snapshot-plugin-id` | `""` | ☕ | JVM-only — `state-store-mode = persistence` subset; gekka uses `ddata`. |
| `pekko.cluster.sharding.distributed-data.majority-min-cap` | `5` | ⚠️ | Parsed into `ShardingConfig.DistributedData.MajorityMinCap`; never threaded onto the shared replicator (sharding-specific override consumer not yet wired) |
| `pekko.cluster.sharding.distributed-data.max-delta-elements` | `5` | ⚠️ | Parsed into `ShardingConfig.DistributedData.MaxDeltaElements`; never threaded onto the shared replicator's `MaxDeltaElements` |
| `pekko.cluster.sharding.distributed-data.prefer-oldest` | `on` | ⚠️ | Parsed into `ShardingConfig.DistributedData.PreferOldest`; never threaded onto the shared replicator's `PreferOldest` |
| `pekko.cluster.sharding.distributed-data.durable.keys` | `["shard-*"]` | ⚠️ | Parsed into `ShardingConfig.DistributedData.DurableKeys`; the parent `distributed-data.durable.keys` is fully ✅, but the sharding-specific filter is not yet wired into the durable store |
| `pekko.cluster.sharding.coordinator-singleton.role` | `""` | ✅ | Applied to coordinator singleton-proxy when override = off |
| `pekko.cluster.sharding.coordinator-singleton.singleton-name` | `"singleton"` | ⚠️ | Parse-only — gekka hard-codes the `<typeName>Coordinator` path; `Sharding.CoordinatorSingleton.SingletonName` is never read |
| `pekko.cluster.sharding.coordinator-singleton.hand-over-retry-interval` | `1s` | ⚠️ | Parse-only — `Sharding.CoordinatorSingleton.HandOverRetryInterval` is never threaded onto SingletonConfig (only the top-level `Singleton.HandOverRetryInterval` is consumed) |
| `pekko.cluster.sharding.coordinator-singleton.min-number-of-hand-over-retries` | `15` | ⚠️ | Parse-only — `Sharding.CoordinatorSingleton.MinNumberOfHandOverRetries` is never threaded onto SingletonConfig |
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
| `pekko.cluster.sharding.healthcheck.names` | `[]` | ⚠️ | Parsed into `Sharding.HealthCheck.Names`; `ClusterShardingHealthCheck` reads it but is never invoked outside tests (no production health-check endpoint) |
| `pekko.cluster.sharding.healthcheck.timeout` | `5s` | ⚠️ | Parsed into `Sharding.HealthCheck.Timeout`; same as row above — `ClusterShardingHealthCheck` is invoked only from tests |
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
| `pekko.cluster.typed.receptionist.write-consistency` | `local` | ❌ | Not implemented — gekka has typed actors (`actor/typed/`) but no receptionist analog yet; tracked in `docs/LEFTWORKS.md` §11 |
| `pekko.cluster.typed.receptionist.pruning-interval` | `3s` | ❌ | Not implemented (typed receptionist absent); tracked in `docs/LEFTWORKS.md` §11 |
| `pekko.cluster.typed.receptionist.distributed-key-count` | `5` | ❌ | Not implemented (typed receptionist absent); tracked in `docs/LEFTWORKS.md` §11 |
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
| `pekko.reliable-delivery.*` | (all) | ❌ | Not implemented — Pekko-typed `ProducerController`/`ConsumerController` API. Distinct from `at-least-once-delivery.*` (✅). Tracked in `docs/LEFTWORKS.md` §11 |

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

### Symbol counts (post 2026-04-30 audit)

| Symbol | Substantive table rows | Meaning |
|---|---|---|
| ✅ | 234 | Parsed AND consumed |
| ⚠️ | 51 | Forward-compat parsed; consumer deferred (Note states what's deferred) |
| ☕ | 8 | JVM-only — no equivalent capability in Go runtime |
| 🚫 | 1 | Go/JVM API-shape incompatibility (FQCN class loading) |
| ❌ | 15 | Not implemented; portable in principle (tracked in `docs/LEFTWORKS.md` §11) |

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
`docs/LEFTWORKS.md` §11 ("Future Work — Portable"). Highlights:

1. **Mailboxes (critical)** — control-aware mailbox bindings,
   `default-mailbox.*` tunables, and `mailbox.requirements.*`. Without
   control-aware mailboxes, system-message priority is not guaranteed and
   the system functions completely differently from Pekko/Akka.
2. **Typed-API surface** — typed receptionist
   (`cluster.typed.receptionist.*`). (Sub-plan 1 closed
   `actor.typed.restart-stash-capacity`, `persistence.typed.log-stashing`,
   and `ddata.typed.replicator-message-adapter-unexpected-ask-timeout` on
   2026-04-30.)
3. **Reliable-delivery (typed)** — Pekko-typed `ProducerController` /
   `ConsumerController`. Distinct from `at-least-once-delivery.*` (✅).
4. **Watch failure detector** — `pekko.remote.watch-failure-detector.*`.
5. **Discovery** — `pekko.discovery.config / aggregate / pekko-dns.*`
   (gekka's k8s extension is the alternative path).
6. **Logging** — `pekko.loglevel` (honesty-downgraded ✅ → ❌ on
   2026-04-30 — parsed-only) and `pekko.stdout-loglevel`. Both are
   **deferred to the future external-log-server logging cycle**, not the
   immediate post-audit roadmap; the in-process logger is being replaced.

### Audit history

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
