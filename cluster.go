/*
 * cluster.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"log"
	"log/slog"
	"net"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	hocon "github.com/sopranoworks/gekka-config"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/actor/typed/delivery"
	"github.com/sopranoworks/gekka/actor/typed/receptionist"
	gcluster "github.com/sopranoworks/gekka/cluster"
	"github.com/sopranoworks/gekka/cluster/client"
	"github.com/sopranoworks/gekka/cluster/ddata"
	ddata_typed "github.com/sopranoworks/gekka/cluster/ddata/typed"
	"github.com/sopranoworks/gekka/cluster/lease"
	"github.com/sopranoworks/gekka/cluster/sharding"
	"github.com/sopranoworks/gekka/cluster/singleton"
	"github.com/sopranoworks/gekka/discovery"
	"github.com/sopranoworks/gekka/internal/core"
	"github.com/sopranoworks/gekka/internal/management"
	"github.com/sopranoworks/gekka/persistence"
	persistencetyped "github.com/sopranoworks/gekka/persistence/typed"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"github.com/sopranoworks/gekka/stream"
	"github.com/sopranoworks/gekka/telemetry"

	"google.golang.org/protobuf/proto"
)

// Provider selects the actor-path protocol prefix used in all Artery messages.
type Provider int

const (
	// ProviderPekko (default) uses "pekko://" actor paths — Apache Pekko ≥ 1.0.
	ProviderPekko Provider = iota
	// ProviderAkka uses "akka://" actor paths — Lightbend Akka.
	// The Artery wire format and serializer IDs are identical; only the
	// protocol string in actor paths differs.
	ProviderAkka
)

// protoString maps a Provider to its actor-path scheme string.
func (p Provider) protoString() string {
	if p == ProviderAkka {
		return "akka"
	}
	return "pekko"
}

// ClusterConfig specifies how to initialize a Cluster.
//
// The simplest form uses flat fields:
//
//	gekka.NewCluster(gekka.ClusterConfig{SystemName: "ClusterSystem", Host: "127.0.0.1", Port: 2553})
//
// You can also supply a typed Address directly:
//
//	addr := actor.Address{Protocol: "pekko", System: "ClusterSystem", Host: "127.0.0.1", Port: 2553}
//	gekka.NewCluster(gekka.ClusterConfig{Address: addr})
//
// When Address is set it takes precedence over SystemName/Host/Port/Provider.
//
// LoadConfig populates all fields from a HOCON application.conf automatically.
type ClusterConfig struct {
	// Address sets the node's own address using the typed actor.Address.
	// When non-zero it overrides SystemName, Host, Port, and Provider.
	Address actor.Address

	// SystemName is the actor system name shared by all cluster members.
	// Ignored when Address is set. Defaults to "GekkaSystem".
	SystemName string `hocon:"pekko.actor.system-name"`

	// Host is the TCP bind/advertised address (e.g. "127.0.0.1").
	// Ignored when Address is set. Defaults to "127.0.0.1".
	Host string `hocon:"pekko.remote.artery.canonical.hostname"`

	// Port is the TCP listen port. 0 lets the OS assign a free port.
	// Ignored when Address is set.
	Port uint32 `hocon:"pekko.remote.artery.canonical.port"`

	// BindHostname is the address the TCP listener actually binds to.
	// When set, the server listens on BindHostname while advertising the
	// canonical Host to the rest of the cluster. This supports NAT/Docker
	// environments where the external (canonical) address differs from the
	// internal (bind) address.
	// Corresponds to pekko.remote.artery.bind.hostname.
	// Default: "" (use canonical hostname).
	BindHostname string

	// BindPort is the port the TCP listener actually binds to.
	// When non-zero, the server listens on BindPort while advertising the
	// canonical Port to the rest of the cluster.
	// Corresponds to pekko.remote.artery.bind.port.
	// Default: 0 (use canonical port).
	BindPort uint32

	// BindTimeout is the maximum time the TCP listener may block while binding
	// to BindHostname:BindPort. Consumed by TcpServer.Start.
	// Corresponds to pekko.remote.artery.bind.bind-timeout.
	// Default: 3s. Zero means use the default.
	BindTimeout time.Duration

	// LogReceivedMessages enables DEBUG-level logging of inbound user messages.
	// Corresponds to pekko.remote.artery.log-received-messages.
	// Default: false (off).
	LogReceivedMessages bool

	// LogSentMessages enables DEBUG-level logging of outbound user messages.
	// Corresponds to pekko.remote.artery.log-sent-messages.
	// Default: false (off).
	LogSentMessages bool

	// LogFrameSizeExceeding logs a warning when an outbound payload exceeds
	// this many bytes. Zero (or "off" in HOCON) disables the check.
	// Corresponds to pekko.remote.artery.log-frame-size-exceeding.
	// Default: 0 (off).
	LogFrameSizeExceeding int64

	// PropagateHarmlessQuarantineEvents controls whether InboundQuarantineCheck
	// propagates harmless quarantine events. This is the legacy Pekko 1.x
	// behavior; defaults to off.
	// Corresponds to pekko.remote.artery.propagate-harmless-quarantine-events.
	PropagateHarmlessQuarantineEvents bool

	// LargeMessageDestinations lists actor-path globs whose recipients route
	// over the dedicated large-message stream (streamId=3). Patterns may use
	// trailing-segment wildcards ("/user/large-*") or per-segment wildcards
	// ("/user/*/big"). Empty list disables large-stream routing.
	// Corresponds to pekko.remote.artery.large-message-destinations.
	// Default: [].
	LargeMessageDestinations []string

	// UntrustedMode rejects PossiblyHarmful and DeathWatch system messages
	// from remote senders, drops messages addressed to /system/* paths, and
	// (paired with TrustedSelectionPaths) restricts inbound ActorSelection.
	// Intended for client/server scenarios where the receiving node should
	// not blindly trust connecting peers. Cluster-internal traffic is not
	// affected.
	// Corresponds to pekko.remote.artery.untrusted-mode.
	// Default: false (off).
	UntrustedMode bool

	// TrustedSelectionPaths is the allowlist of actor-path prefixes that may
	// receive inbound ActorSelection messages when UntrustedMode is on.
	// An empty list under untrusted-mode blocks all selections.
	// Corresponds to pekko.remote.artery.trusted-selection-paths.
	// Default: [].
	TrustedSelectionPaths []string

	// ActorDebug bundles the pekko.actor.debug.* logging toggles.
	// All flags default to false (off). When true, the corresponding
	// actor-framework event is emitted at slog.Debug.
	ActorDebug ActorDebugConfig

	// ActorTypedRestartStashCapacity sizes the per-actor stash buffer used by
	// typed actors while they are restarting. Mirrors
	// pekko.actor.typed.restart-stash-capacity. Default: 1000.
	ActorTypedRestartStashCapacity int

	// LogConfigOnStart, when true, dumps the resolved configuration at INFO
	// level during cluster spawn. Useful when uncertain which config layer
	// is active (e.g., reference.conf vs. application.conf merge order).
	// Corresponds to pekko.log-config-on-start.
	// Default: false.
	LogConfigOnStart bool

	// Provider selects the actor-path protocol prefix.
	// Ignored when Address is set. Defaults to ProviderPekko.
	Provider Provider

	// SeedNodes is the list of cluster seed nodes parsed from HOCON
	// (pekko.gcluster.seed-nodes). Populated by LoadConfig; ignored by Spawn.
	// Use JoinSeeds() to connect to the first reachable seed after Spawn.
	SeedNodes []actor.Address

	// ── Monitoring ────────────────────────────────────────────────────────────

	// EnableMonitoring starts the built-in HTTP monitoring server.
	// MonitoringPort must be > 0.
	EnableMonitoring bool `hocon:"gekka.monitoring.enabled"`

	// MonitoringPort is the TCP port for the HTTP monitoring server.
	// Setting this to a non-zero value implies EnableMonitoring = true.
	// 0 (default) disables monitoring.
	//
	// Endpoints:
	//   /healthz  — readiness probe (200 OK when joined, 503 otherwise)
	//   /metrics  — JSON snapshot of internal counters
	//   /metrics?fmt=prom — Prometheus text exposition format
	MonitoringPort int `hocon:"gekka.monitoring.port"`

	// LogHandler is a custom slog.Handler used for all actor-aware loggers
	// created on this node. When nil the default slog handler is used.
	//
	// Example — JSON logging at DEBUG level:
	//
	//	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})
	//	node, _ := gekka.NewCluster(gekka.ClusterConfig{
	//	    SystemName: "MySystem", Host: "127.0.0.1", Port: 2552,
	//	    LogHandler: h,
	//	})
	LogHandler slog.Handler

	// LogLevel is the minimum logging level for the node.
	// Defaults to "INFO". Can be "DEBUG", "INFO", "WARN", "ERROR".
	// Corresponds to pekko.loglevel (Pekko-compatible).
	// Fallback: gekka.logging.level (deprecated).
	LogLevel string `hocon:"pekko.loglevel"`

	// LogDeadLetters controls how many dead letters are logged.
	// 0 = off, positive N = log first N then suppress, negative = unlimited.
	// Corresponds to pekko.log-dead-letters. Default: 10.
	LogDeadLetters int

	// LogDeadLettersDuringShutdown controls whether dead letters are logged
	// during system shutdown.
	// Corresponds to pekko.log-dead-letters-during-shutdown. Default: false.
	LogDeadLettersDuringShutdown bool

	// AcceptProtocolNames lists protocol names accepted during Artery handshake.
	// Corresponds to pekko.remote.accept-protocol-names.
	// Default: ["pekko", "akka"].
	AcceptProtocolNames []string

	// MaxFrameSize is the maximum Artery frame payload size in bytes.
	// Frames larger than this are rejected by the read loop.
	// Corresponds to pekko.remote.artery.advanced.maximum-frame-size.
	// Default: 256 KiB (Pekko default). Set to 0 to use the default.
	MaxFrameSize int

	// ArteryAdvanced holds pekko.remote.artery.advanced.* knobs that control
	// lane counts, outbox capacities, and system-message buffering.
	// Zero-valued fields fall back to Pekko defaults when NewCluster runs.
	ArteryAdvanced ArteryAdvancedConfig

	// Transport selects the Artery transport: "tcp" (default) or "tls-tcp".
	// When "tls-tcp", the TLS field must be populated with valid PEM paths.
	Transport string `hocon:"pekko.remote.artery.transport"`

	// TLS holds TLS parameters; only used when Transport == "tls-tcp".
	TLS core.TLSConfig

	// FlightRecorder configures the Artery flight recorder.
	// Parse from HOCON:
	//
	//	gekka.remote.artery.advanced.flight-recorder {
	//	    enabled = true
	//	    level   = "lifecycle"   # "lifecycle" | "sampled" | "full"
	//	}
	FlightRecorder FlightRecorderConfig

	// Deployments maps actor paths to their router deployment configurations.
	// When ActorOf is called for a path that has a Deployments entry with a
	// non-empty Router field, the system automatically wraps the supplied Props
	// in the appropriate PoolRouter instead of spawning a plain actor.
	//
	// Populated automatically by LoadConfig / SpawnFromConfig from the
	// HOCON akka.actor.deployment (or pekko.actor.deployment) block.
	// Can also be set directly for programmatic configuration:
	//
	//	gekka.NewCluster(gekka.ClusterConfig{
	//	    Deployments: map[string]gekka.core.DeploymentConfig{
	//	        "/user/myRouter": {Router: "round-robin-pool", NrOfInstances: 5},
	//	    },
	//	})
	Deployments map[string]core.DeploymentConfig

	// SBR configures the Split Brain Resolver. Leave zero-valued to disable SBR.
	//
	// Example — keep-majority with a 10-second stable-after delay:
	//
	//	SBR: gekka.SBRConfig{
	//	    ActiveStrategy: "keep-majority",
	//	    StableAfter:    10 * time.Second,
	//	},
	SBR SBRConfig

	// DowningProviderClass selects the cluster's downing provider via
	// the same HOCON path Pekko uses (`pekko.cluster.downing-provider-class`).
	// Round-2 session 27 introduces a Go-native registry: Pekko / Akka
	// FQCNs are normalised to short names at parse time, and the
	// resolved short name is looked up in cluster.DowningProviderRegistry
	// during cluster startup.
	//
	// Recognised inputs:
	//   - "" (empty) — fall back to the bundled SBR provider.
	//   - "*SplitBrainResolverProvider" (Pekko or Akka FQCN) —
	//     normalised to "split-brain-resolver".
	//   - any other string — preserved verbatim and looked up by name;
	//     when the lookup misses, gekka falls back to SBR with a warn log.
	//
	// Corresponds to pekko.cluster.downing-provider-class.
	DowningProviderClass string

	// FailureDetector tunes the Phi Accrual Failure Detector.
	// Zero values are replaced by safe defaults (threshold=8.0, maxSamples=1000,
	// minStdDeviation=500ms).
	//
	// Parse from HOCON (pekko-compatible namespace takes priority):
	//
	//	pekko.cluster.failure-detector {
	//	    threshold                    = 8.0
	//	    max-sample-size              = 1000
	//	    min-std-deviation            = 100ms
	//	    heartbeat-interval           = 1s
	//	    acceptable-heartbeat-pause   = 3s
	//	    expected-response-after      = 1s
	//	}
	//
	// Fallback: gekka.cluster.failure-detector.*
	FailureDetector FailureDetectorConfig

	// WatchFailureDetector tunes the remote-watch failure detector
	// (Pekko: pekko.remote.watch-failure-detector.*). Distinct from the
	// cluster membership FD above: this detector is consulted only for
	// remote actors registered via Watch. Zero-valued fields fall back to
	// Pekko reference defaults inside cluster.NewWatchFailureDetector.
	//
	// Parse from HOCON:
	//
	//	pekko.remote.watch-failure-detector {
	//	    implementation-class               = "org.apache.pekko.remote.PhiAccrualFailureDetector"
	//	    heartbeat-interval                 = 1s
	//	    threshold                          = 10.0
	//	    max-sample-size                    = 200
	//	    min-std-deviation                  = 100ms
	//	    acceptable-heartbeat-pause         = 10s
	//	    unreachable-nodes-reaper-interval  = 1s
	//	    expected-response-after            = 1s
	//	}
	WatchFailureDetector gcluster.WatchFailureDetectorConfig

	// MinNrOfMembers is the minimum number of members that must join before the
	// leader promotes Joining members to Up status.
	// Corresponds to pekko.cluster.min-nr-of-members.
	// Default: 1 (no gate).
	MinNrOfMembers int

	// RoleMinNrOfMembers maps role names to the minimum number of members
	// with that role required before the leader promotes Joining members to Up.
	// Corresponds to pekko.cluster.role.{name}.min-nr-of-members.
	// When set, both the global MinNrOfMembers AND each per-role minimum must
	// be satisfied before any promotions occur.
	RoleMinNrOfMembers map[string]int

	// RetryUnsuccessfulJoinAfter is how often to retry the initial join (InitJoin)
	// when no Welcome has been received.
	// Corresponds to pekko.cluster.retry-unsuccessful-join-after.
	// Default: 10s (Pekko default).
	RetryUnsuccessfulJoinAfter time.Duration

	// GossipInterval is the duration between gossip rounds.
	// Corresponds to pekko.cluster.gossip-interval.
	// Default: 1s.
	GossipInterval time.Duration

	// LeaderActionsInterval is how often leader actions are evaluated.
	// When zero, leader actions run on every gossip tick (legacy behavior).
	// Corresponds to pekko.cluster.leader-actions-interval.
	// Default: 0 (run on gossip tick).
	LeaderActionsInterval time.Duration

	// PeriodicTasksInitialDelay is the delay before starting gossip and
	// heartbeat loops after cluster initialization.
	// Corresponds to pekko.cluster.periodic-tasks-initial-delay.
	// Default: 1s.
	PeriodicTasksInitialDelay time.Duration

	// ShutdownAfterUnsuccessfulJoinSeedNodes is the maximum time to retry
	// joining seed nodes before aborting. Zero means no timeout (retry forever).
	// Corresponds to pekko.cluster.shutdown-after-unsuccessful-join-seed-nodes.
	// Default: 0 (off).
	ShutdownAfterUnsuccessfulJoinSeedNodes time.Duration

	// LogInfo controls whether informational cluster messages are logged.
	// Corresponds to pekko.cluster.log-info.
	// Default: true.
	LogInfo *bool

	// LogInfoVerbose controls whether verbose cluster messages are logged
	// (gossip details, heartbeat events).
	// Corresponds to pekko.cluster.log-info-verbose.
	// Default: false.
	LogInfoVerbose bool

	// AllowWeaklyUpMembers is the duration after which Joining members are
	// promoted to WeaklyUp without convergence. Zero disables WeaklyUp.
	// Corresponds to pekko.cluster.allow-weakly-up-members.
	// Default: 7s.
	AllowWeaklyUpMembers time.Duration

	// GossipDifferentViewProbability is the probability [0.0, 1.0] that a
	// gossip round targets a node with a different state hash (to speed up
	// convergence). When random selection picks a same-view node, it is
	// re-rolled with this probability to prefer a different-view node.
	// Corresponds to pekko.cluster.gossip-different-view-probability.
	// Default: 0.8.
	GossipDifferentViewProbability float64

	// ReduceGossipDifferentViewProbability is the cluster size above which
	// GossipDifferentViewProbability is reduced to 0.4 (halved).
	// Corresponds to pekko.cluster.reduce-gossip-different-view-probability.
	// Default: 400.
	ReduceGossipDifferentViewProbability int

	// GossipTimeToLive is the maximum acceptable age of incoming gossip.
	// Gossip received after TTL duration since the last local gossip update
	// is discarded. Zero disables TTL checking.
	// Corresponds to pekko.cluster.gossip-time-to-live.
	// Default: 2s.
	GossipTimeToLive time.Duration

	// PruneGossipTombstonesAfter is how long removed-member tombstones are
	// retained in gossip state before being pruned. This prevents indefinite
	// growth of the removed-member list.
	// Corresponds to pekko.cluster.prune-gossip-tombstones-after.
	// Default: 24h.
	PruneGossipTombstonesAfter time.Duration

	// UnreachableNodesReaperInterval is how often the periodic reaper goroutine
	// re-evaluates phi for all nodes and publishes unreachable events.
	// A zero value disables the reaper (relies on gossipTick-driven checks only).
	// Corresponds to pekko.cluster.unreachable-nodes-reaper-interval.
	// Default: 1s.
	UnreachableNodesReaperInterval time.Duration

	// PublishStatsInterval is how often a CurrentClusterStats event is
	// published to event-stream subscribers. Zero ("off") disables it.
	// Corresponds to pekko.cluster.publish-stats-interval.
	// Default: 0 (off).
	PublishStatsInterval time.Duration

	// InternalSBR configures the lightweight internal SBR strategy
	// (icluster.Strategy) that is evaluated inline by CheckReachability.
	// This complements the richer event-driven SBRManager (driven by SBR field).
	//
	// Parse from HOCON:
	//
	//	gekka.cluster.split-brain-resolver {
	//	    active-strategy = "keep-oldest"
	//	    stable-after    = 20s
	//	    keep-oldest {
	//	        role         = ""
	//	        down-if-alone = true
	//	    }
	//	}
	InternalSBR InternalSBRConfig

	// Sharding holds parsed sharding configuration from HOCON.
	// It is used to populate ShardingSettings when StartSharding is called.
	//
	//	pekko.cluster.sharding {
	//	    passivation.idle-timeout = 2m
	//	    remember-entities = on
	//	}
	Sharding ShardingConfig `hocon:"gekka.cluster.sharding"`

	// ShardedDaemonProcess holds the portable surface of
	// pekko.cluster.sharded-daemon-process.* — keep-alive cadence and the
	// optional role override for the underlying ShardRegion.
	ShardedDaemonProcess ShardedDaemonProcessConfig

	// Singleton holds singleton manager configuration parsed from HOCON.
	// Corresponds to pekko.cluster.singleton.*
	Singleton SingletonConfig

	// SingletonProxy holds singleton proxy configuration parsed from HOCON.
	// Corresponds to pekko.cluster.singleton-proxy.*
	SingletonProxy SingletonProxyConfig

	// AppVersion is the application version advertised during the cluster join
	// handshake. Used for rolling update coordination.
	// Corresponds to pekko.cluster.app-version.
	// Default: "0.0.0" (unset).
	AppVersion string

	// RunCoordinatedShutdownWhenDown controls whether the node triggers a
	// coordinated shutdown when it is marked as Down by the cluster.
	// Corresponds to pekko.cluster.run-coordinated-shutdown-when-down.
	// Default: true (nil pointer means on).
	RunCoordinatedShutdownWhenDown *bool

	// QuarantineRemovedNodeAfter is the delay after a member is removed before
	// its UID is quarantined, preventing reconnection.
	// Corresponds to pekko.cluster.quarantine-removed-node-after.
	// Default: 5s.
	QuarantineRemovedNodeAfter time.Duration

	// DownRemovalMargin is the duration to delay the Down → Removed transition
	// after a member is marked as Down. This gives time for the downed node to
	// detect its own downed status and perform coordinated shutdown before it is
	// removed from the cluster. Zero or "off" means immediate removal (legacy).
	// Corresponds to pekko.cluster.down-removal-margin.
	// Default: 0 (off).
	DownRemovalMargin time.Duration

	// SeedNodeTimeout is the maximum time to wait for the first seed node to
	// respond before falling back to self-join (when this node is itself a seed).
	// Corresponds to pekko.cluster.seed-node-timeout.
	// Default: 5s.
	SeedNodeTimeout time.Duration

	// ConfigCompatCheck holds configuration compatibility check settings.
	ConfigCompatCheck ConfigCompatCheckConfig

	// DataCenter identifies which data center this node belongs to.
	// Corresponds to pekko.cluster.multi-data-center.self-data-center.
	// Defaults to "default" when unset or when parsed from HOCON without the key.
	//
	//	pekko.cluster.multi-data-center.self-data-center = "us-east"
	DataCenter string

	// CrossDataCenterGossipProbability is the probability [0.0, 1.0] that a
	// gossip round will target a node in a different data center.  Intra-DC
	// gossip always proceeds; cross-DC gossip is throttled by this value to
	// reduce inter-DC bandwidth.
	// Corresponds to pekko.cluster.multi-data-center.cross-data-center-gossip-probability.
	// Default: 0.1.
	CrossDataCenterGossipProbability float64

	// CrossDataCenterConnections limits the number of distinct foreign-DC nodes
	// used as gossip targets per round. Corresponds to
	// pekko.cluster.multi-data-center.cross-data-center-connections.
	// Default: 5.
	CrossDataCenterConnections int

	// MultiDCFailureDetector tunes the cross-DC failure detector. The values
	// here only apply when sending heartbeats to (or judging reachability of)
	// a node in a different data center than this one; intra-DC nodes still
	// use the FailureDetector field above.
	//
	// Parse from HOCON:
	//
	//	pekko.cluster.multi-data-center.failure-detector {
	//	    heartbeat-interval         = 3s
	//	    acceptable-heartbeat-pause = 10s
	//	    expected-response-after    = 1s
	//	}
	MultiDCFailureDetector MultiDCFailureDetectorConfig

	// Roles is the list of cluster roles this node advertises to the rest of the
	// cluster.  These are merged with the automatic "dc-<DataCenter>" role before
	// the Join message is sent.  Parsed from HOCON:
	//
	//	pekko.cluster.roles = ["metrics-exporter"]
	Roles []string

	// Persistence holds persistence-plugin configuration parsed from HOCON.
	//
	//	pekko.persistence.journal.plugin   = "sql"
	//	pekko.persistence.snapshot-store.plugin = "sql"
	//
	// After creating the Cluster, call node.ProvideJournalDB(name, db) and
	// node.ProvideSnapshotStoreDB(name, db) to wire up the provisioned DB.
	Persistence PersistenceConfig

	// Telemetry controls the built-in OTEL instrumentation.
	// Parse from HOCON:
	//
	//	gekka.telemetry {
	//	    tracing.enabled = true
	//	    metrics.enabled = true
	//	}
	//
	// Note: enabling tracing/metrics here only arms the hooks; you must also
	// call telemetry.SetProvider(gekkaotel.NewProvider()) (see telemetry/otel) and configure
	// the OTEL SDK to emit data to an exporter.
	Telemetry TelemetryConfig

	// Management configures the Cluster HTTP Management API (v0.8.0).
	// When Management.Enabled is true, an HTTP server is started on
	// Management.Hostname:Management.Port exposing cluster management endpoints.
	//
	// Parse from HOCON (Pekko Management-compatible):
	//
	//	pekko.management.http {
	//	    hostname = "127.0.0.1"
	//	    port     = 8558
	//	    enabled  = false
	//	}
	//
	// Deprecated fallback: gekka.management.http.*
	//
	// Endpoints:
	//   GET /cluster/members            — list all members and their status
	//   GET /cluster/members/{address}  — detail for a specific member
	Management core.ManagementConfig `hocon:"pekko.management.http"`

	// Metrics holds configuration for the optional metrics exporter that
	// periodically scrapes the Management HTTP API and emits cluster state
	// metrics as structured log entries (placeholder for full OTEL export).
	//
	// Parse from HOCON:
	//
	//	gekka.metrics {
	//	    enabled         = false
	//	    management-url  = "http://127.0.0.1:8558"
	//	    scrape-interval = "15s"
	//	}
	Metrics core.MetricsExporterConfig `hocon:"gekka.metrics"`

	// Discovery configures cluster bootstrap via service discovery.
	// When Enabled is true, the cluster will use the specified Type to
	// discover seed nodes at startup via ClusterBootstrap.
	//
	// Parse from HOCON (Pekko-compatible):
	//
	//	pekko.management.cluster.bootstrap {
	//	    contact-point-discovery {
	//	        discovery-method = "kubernetes-api"
	//	        required-contact-point-nr = 2
	//	        stable-margin = 5s
	//	    }
	//	}
	//	pekko.discovery {
	//	    kubernetes-api {
	//	        namespace      = "default"
	//	        label-selector = "app=gekka"
	//	        port           = 2552
	//	    }
	//	}
	Discovery DiscoveryConfig

	// PubSub holds distributed pub-sub configuration parsed from HOCON.
	// Corresponds to pekko.cluster.pub-sub.*
	PubSub PubSubConfig

	// ClusterClient holds the cluster-client extension settings parsed from
	// HOCON.  Corresponds to pekko.cluster.client.* — used when constructing
	// a client.NewClusterClient for an external (non-cluster-member) process:
	//
	//	cfg, _ := gekka.LoadConfig("client.conf")
	//	cc := client.NewClusterClient(cfg.ClusterClient, router)
	ClusterClient ClusterClientConfig

	// ClusterReceptionist holds settings for the receptionist actor that runs
	// on cluster member nodes and serves external ClusterClient connections.
	// Corresponds to pekko.cluster.client.receptionist.* — passed to
	// client.NewClusterReceptionist when spawning the actor:
	//
	//	rec := client.NewClusterReceptionist(cm, cfg.ClusterReceptionist, router)
	ClusterReceptionist ClusterReceptionistConfig

	// TypedReceptionist holds settings for the typed receptionist actor that
	// runs on cluster member nodes and indexes typed services for discovery
	// via the typed `Receptionist` API. Corresponds to
	// pekko.cluster.typed.receptionist.*.
	TypedReceptionist TypedReceptionistConfig

	// ReliableDelivery holds settings for the typed reliable-delivery
	// ProducerController / ConsumerController / WorkPullingProducerController
	// actors. Corresponds to pekko.reliable-delivery.*.
	// Pre-seeded with delivery.DefaultConfig() during HOCON parsing.
	ReliableDelivery ReliableDeliveryConfig

	// DistributedData configures the Distributed Data Replicator (v0.10.0).
	DistributedData DistributedDataConfig

	// CoordinationLease holds defaults for the pekko.coordination.lease.*
	// namespace, parsed from HOCON.  Round-2 session 18 ships the public
	// surface (cluster/lease) and the in-memory reference provider; sessions
	// 19/20 wire SBR lease-majority and Singleton/Sharding use-lease against
	// these defaults.
	CoordinationLease CoordinationLeaseConfig

	// HOCON holds the raw parsed configuration. When non-nil, NewCluster calls
	// SerializationRegistry.LoadFromConfig on it to register any user-defined
	// serializers declared under pekko.actor.serializers and
	// pekko.actor.serialization-bindings.
	// Populated automatically by ParseHOCONString / LoadConfig / NewClusterFromConfig.
	HOCON *hocon.Config
}

// DiscoveryConfig holds dynamic seed discovery settings.
type DiscoveryConfig struct {
	// Enabled enables dynamic discovery when true.
	Enabled bool

	// Type selects the discovery mechanism: "kubernetes-api" or "kubernetes-dns".
	Type string

	// Config holds provider-specific configuration.
	Config discovery.DiscoveryConfig
}

// PubSubConfig holds distributed pub-sub configuration parsed from HOCON.
type PubSubConfig struct {
	// GossipInterval is the interval between gossip rounds that propagate
	// subscription state across cluster nodes.
	// Corresponds to pekko.cluster.pub-sub.gossip-interval.
	// Default: 1s.
	GossipInterval time.Duration

	// Name is the actor name for the mediator.
	// Corresponds to pekko.cluster.pub-sub.name.
	// Default: "distributedPubSubMediator".
	Name string

	// Role restricts pub-sub participation to nodes with this role.
	// Corresponds to pekko.cluster.pub-sub.role.
	// Default: "" (all nodes).
	Role string

	// RoutingLogic selects the strategy for Send (point-to-point):
	// "random" or "round-robin".
	// Corresponds to pekko.cluster.pub-sub.routing-logic.
	// Default: "random".
	RoutingLogic string

	// RemovedTimeToLive is how long tombstones for removed subscriptions
	// are retained before being reaped.
	// Corresponds to pekko.cluster.pub-sub.removed-time-to-live.
	// Default: 120s.
	RemovedTimeToLive time.Duration

	// MaxDeltaElements caps the number of entries in a single delta message.
	// Corresponds to pekko.cluster.pub-sub.max-delta-elements.
	// Default: 3000.
	MaxDeltaElements int

	// SendToDeadLettersWhenNoSubscribers controls whether a DeadLetter event
	// is published when a Publish or Send finds no matching subscribers.
	// Corresponds to pekko.cluster.pub-sub.send-to-dead-letters-when-no-subscribers.
	// Default: true.
	SendToDeadLettersWhenNoSubscribers bool
}

// DistributedDataConfig holds settings for the CRDT replicator.
type DistributedDataConfig struct {
	// Enabled enables the replicator when true.
	// Corresponds to HOCON: pekko.cluster.distributed-data.enabled
	// Fallback: gekka.cluster.distributed-data.enabled
	Enabled bool

	// GossipInterval is the duration between gossip rounds.
	// Corresponds to HOCON: pekko.cluster.distributed-data.gossip-interval
	// Fallback: gekka.cluster.distributed-data.gossip-interval
	GossipInterval time.Duration

	// Name is the actor name used for the replicator and the receptionist
	// service key. Corresponds to pekko.cluster.distributed-data.name.
	// Default: "ddataReplicator".
	Name string

	// Role restricts replicator participation to cluster members with this role.
	// Empty string means any member. Corresponds to
	// pekko.cluster.distributed-data.role. Default: "".
	Role string

	// NotifySubscribersInterval is how often changed keys are flushed to
	// subscribers. Changes are accumulated and delivered in batches.
	// Corresponds to pekko.cluster.distributed-data.notify-subscribers-interval.
	// Default: 500ms.
	NotifySubscribersInterval time.Duration

	// MaxDeltaElements caps the number of delta entries accumulated per peer
	// before a full-state gossip is sent instead.
	// Corresponds to pekko.cluster.distributed-data.max-delta-elements.
	// Default: 500.
	MaxDeltaElements int

	// DeltaCRDTEnabled enables delta-based gossip for CRDTs that support it.
	// When false, only full-state gossip is used.
	// Corresponds to pekko.cluster.distributed-data.delta-crdt.enabled.
	// Default: true.
	DeltaCRDTEnabled bool

	// DeltaCRDTMaxDeltaSize caps the number of operations per delta message.
	// Corresponds to pekko.cluster.distributed-data.delta-crdt.max-delta-size.
	// Default: 50.
	DeltaCRDTMaxDeltaSize int

	// PreferOldest selects gossip peers in ascending upNumber order
	// (older members first). Corresponds to
	// pekko.cluster.distributed-data.prefer-oldest. Default: false.
	PreferOldest bool

	// PruningInterval is the cadence of the pruning tick that rewrites
	// CRDT state after a node has been removed from the cluster.
	// Corresponds to pekko.cluster.distributed-data.pruning-interval.
	// Default: 120s.
	PruningInterval time.Duration

	// MaxPruningDissemination is the window allowed for pruning state to
	// propagate to all nodes before the tombstone is collected.
	// Corresponds to pekko.cluster.distributed-data.max-pruning-dissemination.
	// Default: 300s.
	MaxPruningDissemination time.Duration

	// PruningMarkerTimeToLive bounds how long a pruning marker is retained
	// after dissemination has finished. Once this much time has elapsed since
	// pruning was initiated, the marker is forgotten.
	// Corresponds to pekko.cluster.distributed-data.pruning-marker-time-to-live.
	// Default: 6h. Zero falls back to the legacy "delete on dissemination
	// complete" behavior used before this knob was wired.
	PruningMarkerTimeToLive time.Duration

	// LogDataSizeExceeding is the per-key serialized-size threshold that,
	// when exceeded, causes the replicator to emit a slog.Warn during gossip.
	// Corresponds to pekko.cluster.distributed-data.log-data-size-exceeding.
	// Default: 10 KiB. Zero disables the warning.
	LogDataSizeExceeding int

	// RecoveryTimeout bounds how long Replicator.Start will wait for the
	// initial reachable peer / durable backend recovery before giving up.
	// Corresponds to pekko.cluster.distributed-data.recovery-timeout.
	// Default: 10s.
	RecoveryTimeout time.Duration

	// SerializerCacheTimeToLive is the TTL applied to the in-memory cache
	// the DData serializer uses to reuse already-serialized CRDT bytes
	// across gossip rounds.
	// Corresponds to pekko.cluster.distributed-data.serializer-cache-time-to-live.
	// Default: 10s. Zero disables caching.
	SerializerCacheTimeToLive time.Duration

	// DurableEnabled toggles the on-disk durable backend. Pekko semantics:
	// any non-empty `durable.keys` list activates the durable path; gekka
	// also accepts an explicit `durable.enabled = on/off` for tests that
	// need to instantiate the store with no key globs configured yet.
	// Corresponds to pekko.cluster.distributed-data.durable.enabled.
	// Default: false (also false when DurableKeys is empty).
	DurableEnabled bool

	// DurableKeys is the prefix-glob list from
	// pekko.cluster.distributed-data.durable.keys.  Only CRDT keys matching
	// one of these patterns are persisted via the DurableStore.  A trailing
	// "*" expands to a prefix match; everything else is exact.
	// Default: [] (no keys are durable).
	DurableKeys []string

	// DurablePruningMarkerTimeToLive bounds how long a pruning marker is
	// retained for *durable* keys.  Distinct from the non-durable marker
	// TTL because durable replicas can rejoin after a long offline window
	// and must not silently merge stale state past this horizon.
	// Corresponds to pekko.cluster.distributed-data.durable.pruning-marker-time-to-live.
	// Default: 10 days (Pekko parity).
	DurablePruningMarkerTimeToLive time.Duration

	// DurableLmdbDir is the directory for the on-disk store. Mapped 1:1 to
	// BoltDurableStoreOptions.Dir.  Corresponds to
	// pekko.cluster.distributed-data.durable.lmdb.dir.  Default: "ddata".
	DurableLmdbDir string

	// DurableLmdbMapSize is the hard cap on the file size in bytes. Mapped
	// 1:1 to BoltDurableStoreOptions.MapSize.  Corresponds to
	// pekko.cluster.distributed-data.durable.lmdb.map-size.  Default: 100 MiB.
	DurableLmdbMapSize int64

	// DurableLmdbWriteBehindInterval, when > 0, batches Store/Delete calls
	// in memory and flushes them on a timer. Mapped 1:1 to
	// BoltDurableStoreOptions.WriteBehindInterval.  Corresponds to
	// pekko.cluster.distributed-data.durable.lmdb.write-behind-interval.
	// Default: 0 (synchronous; matches Pekko's "off").
	DurableLmdbWriteBehindInterval time.Duration

	// TypedReplicatorMessageAdapterUnexpectedAskTimeout bounds the wait that a
	// TypedReplicatorAdapter (used by typed actors that talk to the Replicator)
	// applies to AskGet / AskUpdate dispatches before delivering a synthetic
	// timeout error. Corresponds to
	// pekko.cluster.ddata.typed.replicator-message-adapter-unexpected-ask-timeout.
	// Default: 20s.
	TypedReplicatorMessageAdapterUnexpectedAskTimeout time.Duration
}

// CoordinationLeaseConfig holds defaults for the pekko.coordination.lease.*
// namespace.  Sessions 19/20 read these values when wiring SBR
// lease-majority and Singleton/Sharding use-lease.
type CoordinationLeaseConfig struct {
	// LeaseClass is the default lease implementation name (registered in
	// the LeaseManager).  Empty falls back to "memory" — the in-memory
	// reference provider shipped in cluster/lease.
	// Corresponds to pekko.coordination.lease.lease-class.
	LeaseClass string

	// HeartbeatTimeout is how long a holder may go without renewing before
	// the lease becomes available to other owners.
	// Corresponds to pekko.coordination.lease.heartbeat-timeout.
	// Default: 120s.
	HeartbeatTimeout time.Duration

	// HeartbeatInterval is the cadence at which holders should renew the
	// lease with the underlying coordination service.
	// Corresponds to pekko.coordination.lease.heartbeat-interval.
	// Default: 12s.
	HeartbeatInterval time.Duration

	// LeaseOperationTimeout bounds individual Acquire/Release calls.
	// Corresponds to pekko.coordination.lease.lease-operation-timeout.
	// Default: 5s.
	LeaseOperationTimeout time.Duration
}

// PersistenceConfig holds persistence-plugin settings parsed from HOCON.
// It identifies the registered Journal and SnapshotStore factory names so
// that the Cluster can resolve them from the persistence registry when a
// *sql.DB is provided at runtime.
type PersistenceConfig struct {
	// JournalPlugin is the registry name of the Journal factory.
	// e.g. "sql" or "postgres".
	// Corresponds to pekko.persistence.journal.plugin.
	// Leave empty to use InMemoryJournal (the default).
	JournalPlugin string

	// SnapshotPlugin is the registry name of the SnapshotStore factory.
	// e.g. "sql" or "postgres".
	// Corresponds to pekko.persistence.snapshot-store.plugin.
	// Leave empty to use InMemorySnapshotStore (the default).
	SnapshotPlugin string

	// MaxConcurrentRecoveries limits how many persistent actors can recover
	// (replay events from the journal) concurrently. This prevents a burst
	// of actors from overwhelming the journal backend on startup.
	// Corresponds to pekko.persistence.max-concurrent-recoveries.
	// Default: 50. Zero means use default.
	MaxConcurrentRecoveries int

	// AutoStartJournals lists journal-provider names that should be
	// instantiated eagerly at cluster startup. Names must be registered with
	// persistence.RegisterJournalProvider.
	// Corresponds to pekko.persistence.journal.auto-start-journals.
	// Default: nil (none — journals are created lazily on first use).
	AutoStartJournals []string

	// AutoStartSnapshotStores lists snapshot-store-provider names that should
	// be instantiated eagerly at cluster startup.
	// Corresponds to pekko.persistence.snapshot-store.auto-start-snapshot-stores.
	// Default: nil (none).
	AutoStartSnapshotStores []string

	// AutoMigrateManifest is the manifest written into snapshot envelopes
	// when migrating a legacy class-named snapshot to a manifest-coded one.
	// Corresponds to pekko.persistence.snapshot-store.auto-migrate-manifest.
	// Default: "pekko".
	AutoMigrateManifest string

	// StatePluginFallbackRecoveryTimeout caps how long the durable-state
	// plugin fallback waits for recovery before failing.
	// Corresponds to pekko.persistence.state-plugin-fallback.recovery-timeout.
	// Default: 30s.
	StatePluginFallbackRecoveryTimeout time.Duration

	// TypedStashCapacity caps the number of commands a typed persistent
	// actor will stash while recovering / persisting.
	// Corresponds to pekko.persistence.typed.stash-capacity.
	// Default: 4096.
	TypedStashCapacity int

	// TypedStashOverflowStrategy selects what happens when a typed
	// persistent actor's stash hits its capacity. Currently honored values:
	//   "drop" — drop the incoming command (default; matches Pekko)
	//   "fail" — fail the actor with a stash-overflow error
	// Corresponds to pekko.persistence.typed.stash-overflow-strategy.
	// Default: "drop".
	TypedStashOverflowStrategy string

	// TypedSnapshotOnRecovery, when true, instructs typed persistent actors
	// to save a snapshot once recovery finishes (regardless of the
	// per-behavior SnapshotInterval / SnapshotWhen settings).
	// Corresponds to pekko.persistence.typed.snapshot-on-recovery.
	// Default: false.
	TypedSnapshotOnRecovery bool

	// TypedLogStashing, when true, gates DEBUG log lines around stash and
	// unstash operations inside typed persistent actors during recovery.
	// Corresponds to pekko.persistence.typed.log-stashing.
	// Default: false (off).
	TypedLogStashing bool

	// FSMSnapshotAfter, when > 0, causes PersistentFSM to save a snapshot
	// after every N state transitions.
	// Corresponds to pekko.persistence.fsm.snapshot-after.
	// Default: 0 (off — Pekko's reference default).
	FSMSnapshotAfter int

	// JournalBreakerMaxFailures, JournalBreakerCallTimeout,
	// JournalBreakerResetTimeout govern the circuit breaker that
	// wraps every Journal call when the journal is built through
	// persistence.WrapJournalWithFallbacks. They mirror
	// pekko.persistence.journal-plugin-fallback.circuit-breaker.{
	// max-failures, call-timeout, reset-timeout}.
	// Defaults: 10 / 10s / 30s. Round-2 session 38.
	JournalBreakerMaxFailures  int
	JournalBreakerCallTimeout  time.Duration
	JournalBreakerResetTimeout time.Duration

	// SnapshotBreakerMaxFailures, SnapshotBreakerCallTimeout,
	// SnapshotBreakerResetTimeout govern the breaker for the
	// snapshot store. They mirror
	// pekko.persistence.snapshot-store-plugin-fallback.circuit-breaker.*.
	// Defaults: 5 / 20s / 60s. Round-2 session 38.
	SnapshotBreakerMaxFailures  int
	SnapshotBreakerCallTimeout  time.Duration
	SnapshotBreakerResetTimeout time.Duration

	// ReplayFilterMode, ReplayFilterWindowSize, ReplayFilterMaxOldWriters,
	// ReplayFilterDebug control the replay-filter that guards recovery
	// against duplicated WriterUuid events. Mirrors
	// pekko.persistence.journal-plugin-fallback.replay-filter.*.
	// Defaults: "repair-by-discard-old" / 100 / 10 / off. Round-2 session 38.
	ReplayFilterMode          string
	ReplayFilterWindowSize    int
	ReplayFilterMaxOldWriters int
	ReplayFilterDebug         bool

	// RecoveryEventTimeout caps the inter-event silence the replay
	// path tolerates before failing recovery. Mirrors
	// pekko.persistence.journal-plugin-fallback.recovery-event-timeout.
	// Default: 30s. Round-2 session 38.
	RecoveryEventTimeout time.Duration

	// AtLeastOnceRedeliverInterval is the period between redelivery
	// attempts for unconfirmed messages tracked by AtLeastOnceDelivery.
	// Mirrors pekko.persistence.at-least-once-delivery.redeliver-interval.
	// Default: 5s. Round-2 session 39.
	AtLeastOnceRedeliverInterval time.Duration

	// AtLeastOnceRedeliveryBurstLimit caps the maximum number of
	// redeliveries fired per redeliver-interval tick. Mirrors
	// pekko.persistence.at-least-once-delivery.redelivery-burst-limit.
	// Default: 10000. Round-2 session 39.
	AtLeastOnceRedeliveryBurstLimit int

	// AtLeastOnceWarnAfterNumberOfUnconfirmedAttempts triggers a warn
	// log entry when any pending delivery has been re-attempted at least
	// this many times. Mirrors pekko.persistence.at-least-once-delivery.
	// warn-after-number-of-unconfirmed-attempts.  Default: 5. Round-2 session 39.
	AtLeastOnceWarnAfterNumberOfUnconfirmedAttempts int

	// AtLeastOnceMaxUnconfirmedMessages caps the number of unconfirmed
	// messages an AtLeastOnceDelivery instance accepts. Deliver() returns
	// ErrMaxUnconfirmedMessagesExceeded once this ceiling is reached.
	// Mirrors pekko.persistence.at-least-once-delivery.max-unconfirmed-messages.
	// Default: 100000. Round-2 session 39.
	AtLeastOnceMaxUnconfirmedMessages int
}

// SBRConfig is a re-export of cluster.SBRConfig for use in ClusterConfig.
// Import gekka directly — you do not need to import the cluster sub-package.
type SBRConfig = gcluster.SBRConfig

// ClusterClientConfig is a re-export of cluster/client.Config for use in
// ClusterConfig.  Import gekka directly — you do not need to import the
// cluster/client sub-package to read the values populated by LoadConfig.
type ClusterClientConfig = client.Config

// ClusterReceptionistConfig is a re-export of cluster/client.ReceptionistConfig
// for use in ClusterConfig.  Import gekka directly — you do not need to import
// the cluster/client sub-package to read the values populated by LoadConfig.
type ClusterReceptionistConfig = client.ReceptionistConfig

// ReliableDeliveryConfig is a re-export of delivery.Config for use in
// ClusterConfig. Parsed from HOCON namespace pekko.reliable-delivery.*.
// Defaults match Pekko reference.conf via delivery.DefaultConfig().
type ReliableDeliveryConfig = delivery.Config

// TypedReceptionistConfig holds runtime settings for the typed receptionist
// actor (`actor/typed/receptionist`). Parsed from HOCON namespace
// pekko.cluster.typed.receptionist.*. Defaults match Pekko reference.conf.
type TypedReceptionistConfig struct {
	// WriteConsistency selects the ddata write consistency level used when
	// the receptionist registers or removes a service ref. Accepted values:
	// "local", "majority", "all". Translated to ddata.WriteConsistency at
	// receptionist construction time.
	// Corresponds to pekko.cluster.typed.receptionist.write-consistency.
	// Default: "local".
	WriteConsistency string

	// PruningInterval is the cadence at which the receptionist scans its
	// registered service paths and removes any whose actor refs no longer
	// resolve (i.e., the actor is dead or the path lost its binding).
	// Corresponds to pekko.cluster.typed.receptionist.pruning-interval.
	// Default: 3s. Zero or negative values disable the pruning ticker.
	PruningInterval time.Duration

	// DistributedKeyCount is the number of ORSet shards used to spread the
	// receptionist's keyspace. The bucket name in the ddata replicator is
	// derived as fmt.Sprintf("receptionist-%d-%s", hash(id) % count, id),
	// keeping per-id state localised while avoiding one giant ORSet.
	// Corresponds to pekko.cluster.typed.receptionist.distributed-key-count.
	// Default: 5. Values <= 0 are coerced to 1 at runtime.
	DistributedKeyCount int
}

// FailureDetectorConfig is a re-export of cluster.FailureDetectorConfig.
// It tunes the Phi Accrual Failure Detector parameters.
// Parsed from HOCON: gekka.cluster.failure-detector.*
type FailureDetectorConfig = gcluster.FailureDetectorConfig

// MultiDCFailureDetectorConfig tunes the cross-DC failure detector. Values
// only take effect when communicating with a node in a different data center.
// Zero values fall back to the intra-DC FailureDetector settings.
//
// Parse from HOCON:
//
//	pekko.cluster.multi-data-center.failure-detector {
//	    heartbeat-interval         = 3s
//	    acceptable-heartbeat-pause = 10s
//	    expected-response-after    = 1s
//	}
type MultiDCFailureDetectorConfig struct {
	// HeartbeatInterval is how often heartbeat messages are sent to monitored
	// foreign-DC nodes.
	// Corresponds to pekko.cluster.multi-data-center.failure-detector.heartbeat-interval.
	// Pekko default: 3s.
	HeartbeatInterval time.Duration

	// AcceptableHeartbeatPause is the duration of lost heartbeats acceptable
	// for a foreign-DC node before it is considered an anomaly.
	// Corresponds to pekko.cluster.multi-data-center.failure-detector.acceptable-heartbeat-pause.
	// Pekko default: 10s.
	AcceptableHeartbeatPause time.Duration

	// ExpectedResponseAfter is the expected time between a heartbeat request
	// and its response when communicating with a foreign-DC node.
	// Corresponds to pekko.cluster.multi-data-center.failure-detector.expected-response-after.
	// Pekko default: 1s.
	ExpectedResponseAfter time.Duration
}

// InternalSBRConfig is a re-export of cluster.InternalSBRConfig.
// It configures the lightweight icluster.Strategy (internal SBR primitives).
// Parsed from HOCON: gekka.cluster.split-brain-resolver.*
type InternalSBRConfig = gcluster.InternalSBRConfig

// ClusterSingletonManagerInterface is an alias for gcluster.ClusterSingletonManagerInterface.
type ClusterSingletonManagerInterface = gcluster.ClusterSingletonManagerInterface

// ClusterSingletonProxyInterface is an alias for gcluster.ClusterSingletonProxyInterface.
type ClusterSingletonProxyInterface = gcluster.ClusterSingletonProxyInterface

// ConfigCompatCheckConfig holds configuration compatibility check settings.
type ConfigCompatCheckConfig struct {
	// EnforceOnJoin, when true, validates the incoming InitJoin config against
	// the local node's config. If the downing-provider-class values do not match,
	// the join is rejected. When false, config mismatches are logged but allowed.
	// Corresponds to pekko.cluster.configuration-compatibility-check.enforce-on-join.
	// Default: true (nil means on).
	EnforceOnJoin *bool

	// SensitiveConfigPaths is the user-provided list of config-path prefixes
	// appended to the built-in allowlist (DefaultSensitiveConfigPaths). These
	// prefixes are excluded from the configuration-compatibility-check payload
	// to avoid leaking secrets across nodes.
	// Corresponds to pekko.cluster.configuration-compatibility-check.sensitive-config-paths.<group>.
	SensitiveConfigPaths []string
}

// DefaultSensitiveConfigPaths returns the built-in allowlist of sensitive
// config-path prefixes. Mirrors Pekko's
// pekko.cluster.configuration-compatibility-check.sensitive-config-paths.pekko
// reference list.
func DefaultSensitiveConfigPaths() []string {
	return []string{
		"user.home", "user.name", "user.dir",
		"socksNonProxyHosts", "http.nonProxyHosts", "ftp.nonProxyHosts",
		"pekko.remote.secure-cookie",
		"pekko.remote.classic.netty.ssl.security",
		"pekko.remote.netty.ssl.security",
		"pekko.remote.artery.ssl",
	}
}

// IsSensitiveConfigPath reports whether path matches (by prefix) any entry in
// the effective sensitive-config-paths allowlist (built-in defaults union
// user-provided extras). Mirrors Pekko's
// JoinConfigCompatChecker.removeSensitiveKeys filter.
func (c ConfigCompatCheckConfig) IsSensitiveConfigPath(path string) bool {
	for _, prefix := range DefaultSensitiveConfigPaths() {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	for _, prefix := range c.SensitiveConfigPaths {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

// ActorDebugConfig bundles the pekko.actor.debug.* logging toggles. Each
// field is a Pekko-parity boolean: when true, the corresponding actor-
// framework event is logged at slog.Debug (or slog.Warn for
// RouterMisconfiguration, matching Pekko semantics).
//
// gekka exposes these via helper methods (LogActorReceive, LogActorLifecycle,
// etc.) so framework internals can opt into logging without leaking the
// pointer-to-config dependency to the lower layers.
type ActorDebugConfig struct {
	// Receive enables DEBUG logging of every message delivered to a
	// loggable actor's Receive function.
	// Corresponds to pekko.actor.debug.receive.
	Receive bool

	// Autoreceive enables DEBUG logging of all auto-received system
	// messages (Kill, PoisonPill, Terminate).
	// Corresponds to pekko.actor.debug.autoreceive.
	Autoreceive bool

	// Lifecycle enables DEBUG logging of actor lifecycle transitions
	// (started, stopped, restarted).
	// Corresponds to pekko.actor.debug.lifecycle.
	Lifecycle bool

	// FSM enables DEBUG logging of FSM events, transitions, and timers.
	// Corresponds to pekko.actor.debug.fsm.
	FSM bool

	// EventStream enables DEBUG logging of subscribe/unsubscribe events on
	// the actor system's event stream.
	// Corresponds to pekko.actor.debug.event-stream.
	EventStream bool

	// Unhandled enables DEBUG logging of messages that fall through an
	// actor's Receive without a matching case.
	// Corresponds to pekko.actor.debug.unhandled.
	Unhandled bool

	// RouterMisconfiguration enables WARN logging when a router's HOCON
	// definition is missing or invalid (legacy Pekko name kept for parity).
	// Corresponds to pekko.actor.debug.router-misconfiguration.
	RouterMisconfiguration bool
}

// LogActorReceive emits a DEBUG message for an inbound actor message when
// pekko.actor.debug.receive is enabled. Safe to call when disabled
// (no-op).
func (c ActorDebugConfig) LogActorReceive(actorPath string, msg any) {
	if !c.Receive {
		return
	}
	slog.Debug("actor: received message",
		"actor", actorPath,
		"message_type", fmt.Sprintf("%T", msg))
}

// LogActorAutoreceive emits a DEBUG message for an auto-received system
// message (Kill, PoisonPill, Terminate) when
// pekko.actor.debug.autoreceive is enabled.
func (c ActorDebugConfig) LogActorAutoreceive(actorPath, msgKind string) {
	if !c.Autoreceive {
		return
	}
	slog.Debug("actor: auto-received system message",
		"actor", actorPath,
		"kind", msgKind)
}

// LogActorLifecycle emits a DEBUG message for an actor lifecycle transition
// (started, stopped, restarted) when pekko.actor.debug.lifecycle is
// enabled.
func (c ActorDebugConfig) LogActorLifecycle(actorPath, event string) {
	if !c.Lifecycle {
		return
	}
	slog.Debug("actor: lifecycle event",
		"actor", actorPath,
		"event", event)
}

// LogActorFSM emits a DEBUG message for an FSM transition or timer when
// pekko.actor.debug.fsm is enabled.
func (c ActorDebugConfig) LogActorFSM(actorPath string, attrs ...any) {
	if !c.FSM {
		return
	}
	all := append([]any{"actor", actorPath}, attrs...)
	slog.Debug("actor: FSM event", all...)
}

// LogActorEventStream emits a DEBUG message for event-stream subscribe or
// unsubscribe events when pekko.actor.debug.event-stream is enabled.
func (c ActorDebugConfig) LogActorEventStream(action string, channel any) {
	if !c.EventStream {
		return
	}
	slog.Debug("actor: event-stream event",
		"action", action,
		"channel", fmt.Sprintf("%v", channel))
}

// LogActorUnhandled emits a DEBUG message for an unhandled actor message
// when pekko.actor.debug.unhandled is enabled.
func (c ActorDebugConfig) LogActorUnhandled(actorPath string, msg any) {
	if !c.Unhandled {
		return
	}
	slog.Debug("actor: unhandled message",
		"actor", actorPath,
		"message_type", fmt.Sprintf("%T", msg))
}

// LogRouterMisconfiguration emits a WARN message describing a router
// misconfiguration when pekko.actor.debug.router-misconfiguration is
// enabled. Pekko logs at WARN here; the helper preserves that severity.
func (c ActorDebugConfig) LogRouterMisconfiguration(deploymentPath, reason string) {
	if !c.RouterMisconfiguration {
		return
	}
	slog.Warn("actor: router misconfigured",
		"deployment_path", deploymentPath,
		"reason", reason)
}

// FlightRecorderConfig controls the Artery flight recorder verbosity.
type FlightRecorderConfig struct {
	Enabled bool   // default: true
	Level   string // "lifecycle" (default), "sampled", "full"
}

// ArteryAdvancedConfig maps pekko.remote.artery.advanced.* knobs that control
// lane counts, outbox capacities, and system-message buffering.
//
// Zero-valued fields are treated as "not configured" and are replaced by
// the Pekko defaults below when threaded into NodeManager.
type ArteryAdvancedConfig struct {
	// InboundLanes is the number of parallel inbound dispatch lanes per
	// association. Lane selection is consistent-hash on recipient path to
	// preserve message order per receiver.
	// Corresponds to pekko.remote.artery.advanced.inbound-lanes.
	// Pekko default: 4. Zero means use the default.
	InboundLanes int

	// OutboundLanes is the number of parallel outbound lanes per outbound
	// association. gekka currently serializes outbound writes on a single
	// TCP connection, so this value is recorded for Pekko-config parity and
	// consumed by the per-association lane accounting.
	// Corresponds to pekko.remote.artery.advanced.outbound-lanes.
	// Pekko default: 1. Zero means use the default.
	OutboundLanes int

	// OutboundMessageQueueSize is the capacity of each association's
	// outbound (ordinary) message queue. Frames are dropped when the queue
	// is full.
	// Corresponds to pekko.remote.artery.advanced.outbound-message-queue-size.
	// Pekko default: 3072. Zero means use the default.
	OutboundMessageQueueSize int

	// SystemMessageBufferSize is the capacity of the sender-side buffer of
	// unacknowledged system messages per association. gekka does not yet
	// implement Pekko's system-message resend protocol; the value is
	// recorded on NodeManager for later consumers.
	// Corresponds to pekko.remote.artery.advanced.system-message-buffer-size.
	// Pekko default: 20000. Zero means use the default.
	SystemMessageBufferSize int

	// HandshakeTimeout is the maximum time an outbound association waits for
	// a successful handshake before giving up. On timeout the association
	// is abandoned; the next user message will trigger a fresh dial.
	// Corresponds to pekko.remote.artery.advanced.handshake-timeout.
	// Pekko default: 20s. Zero means use the default.
	HandshakeTimeout time.Duration

	// HandshakeRetryInterval is the cadence at which an outbound association
	// re-sends HandshakeReq while waiting for HandshakeRsp.
	// Corresponds to pekko.remote.artery.advanced.handshake-retry-interval.
	// Pekko default: 1s. Zero means use the default.
	HandshakeRetryInterval time.Duration

	// SystemMessageResendInterval is the cadence at which unacknowledged
	// system messages are retransmitted. Recorded on NodeManager for the
	// sender-side redelivery consumer.
	// Corresponds to pekko.remote.artery.advanced.system-message-resend-interval.
	// Pekko default: 1s. Zero means use the default.
	SystemMessageResendInterval time.Duration

	// GiveUpSystemMessageAfter is the ultimatum after which an unacknowledged
	// system message triggers association quarantine. Recorded on NodeManager
	// for the sender-side redelivery consumer.
	// Corresponds to pekko.remote.artery.advanced.give-up-system-message-after.
	// Pekko default: 6h. Zero means use the default.
	GiveUpSystemMessageAfter time.Duration

	// OutboundControlQueueSize is the capacity of each outbound control-stream
	// association's outbox (streamId=1 — handshake, heartbeat, system
	// messages). Separate from OutboundMessageQueueSize (ordinary stream).
	// Corresponds to pekko.remote.artery.advanced.outbound-control-queue-size.
	// Pekko default: 20000. Zero means use the default.
	OutboundControlQueueSize int

	// StopIdleOutboundAfter is the idle duration after which an outbound
	// association that has not been used is stopped. Recorded on NodeManager
	// for the idle-sweep consumer.
	// Corresponds to pekko.remote.artery.advanced.stop-idle-outbound-after.
	// Pekko default: 5m. Zero means use the default.
	StopIdleOutboundAfter time.Duration

	// QuarantineIdleOutboundAfter is the idle duration after which an unused
	// outbound association is quarantined. Consumed by the NodeManager idle
	// sweeper (SweepIdleOutboundQuarantine).
	// Corresponds to pekko.remote.artery.advanced.quarantine-idle-outbound-after.
	// Pekko default: 6h. Zero means use the default.
	QuarantineIdleOutboundAfter time.Duration

	// StopQuarantinedAfterIdle is the idle duration after which the outbound
	// stream of a quarantined association is stopped. Recorded on NodeManager
	// for the idle-sweep consumer.
	// Corresponds to pekko.remote.artery.advanced.stop-quarantined-after-idle.
	// Pekko default: 3s. Zero means use the default.
	StopQuarantinedAfterIdle time.Duration

	// RemoveQuarantinedAssociationAfter is the duration after which a
	// quarantined association is removed from the registry. Recorded on
	// NodeManager for the idle-sweep consumer.
	// Corresponds to pekko.remote.artery.advanced.remove-quarantined-association-after.
	// Pekko default: 1h. Zero means use the default.
	RemoveQuarantinedAssociationAfter time.Duration

	// ShutdownFlushTimeout is how long ActorSystem termination waits for
	// pending outbound messages to flush. Recorded on NodeManager for the
	// coordinated-shutdown consumer.
	// Corresponds to pekko.remote.artery.advanced.shutdown-flush-timeout.
	// Pekko default: 1s. Zero means use the default.
	ShutdownFlushTimeout time.Duration

	// DeathWatchNotificationFlushTimeout is how long the remote layer waits
	// before sending a DeathWatchNotification, to let prior messages flush.
	// Recorded on NodeManager for the death-watch consumer.
	// Corresponds to pekko.remote.artery.advanced.death-watch-notification-flush-timeout.
	// Pekko default: 3s. Zero means use the default.
	DeathWatchNotificationFlushTimeout time.Duration

	// InboundRestartTimeout is the rolling window in which InboundMaxRestarts
	// caps inbound-stream restarts. Consumed by the NodeManager RestartTracker.
	// Corresponds to pekko.remote.artery.advanced.inbound-restart-timeout.
	// Pekko default: 5s. Zero means use the default.
	InboundRestartTimeout time.Duration

	// InboundMaxRestarts is the maximum number of inbound-stream restarts
	// permitted inside InboundRestartTimeout. Consumed by the NodeManager
	// RestartTracker — exceeding the cap returns false from
	// TryRecordInboundRestart.
	// Corresponds to pekko.remote.artery.advanced.inbound-max-restarts.
	// Pekko default: 5. Zero means use the default.
	InboundMaxRestarts int

	// OutboundRestartBackoff is the backoff applied before retrying an
	// outbound TCP connection. Recorded on NodeManager for the dialer consumer.
	// Corresponds to pekko.remote.artery.advanced.outbound-restart-backoff.
	// Pekko default: 1s. Zero means use the default.
	OutboundRestartBackoff time.Duration

	// OutboundRestartTimeout is the rolling window in which OutboundMaxRestarts
	// caps outbound-stream restarts. Consumed by the NodeManager RestartTracker.
	// Corresponds to pekko.remote.artery.advanced.outbound-restart-timeout.
	// Pekko default: 5s. Zero means use the default.
	OutboundRestartTimeout time.Duration

	// OutboundMaxRestarts is the maximum number of outbound-stream restarts
	// permitted inside OutboundRestartTimeout. Consumed by the NodeManager
	// RestartTracker — exceeding the cap returns false from
	// TryRecordOutboundRestart.
	// Corresponds to pekko.remote.artery.advanced.outbound-max-restarts.
	// Pekko default: 5. Zero means use the default.
	OutboundMaxRestarts int

	// CompressionActorRefsMax is the maximum number of compressed actor-ref
	// entries per received advertisement. Rejected by the
	// CompressionTableManager when the incoming key count exceeds the cap.
	// Corresponds to pekko.remote.artery.advanced.compression.actor-refs.max.
	// Pekko default: 256. Zero means use the default.
	CompressionActorRefsMax int

	// CompressionActorRefsAdvertisementInterval is the cadence at which the
	// local actor-ref compression table is advertised to remote peers.
	// Consumed by CompressionTableManager.StartAdvertisementScheduler.
	// Corresponds to pekko.remote.artery.advanced.compression.actor-refs.advertisement-interval.
	// Pekko default: 1m. Zero means use the default.
	CompressionActorRefsAdvertisementInterval time.Duration

	// CompressionManifestsMax is the maximum number of compressed manifest
	// entries per received advertisement. Rejected by the
	// CompressionTableManager when the incoming key count exceeds the cap.
	// Corresponds to pekko.remote.artery.advanced.compression.manifests.max.
	// Pekko default: 256. Zero means use the default.
	CompressionManifestsMax int

	// CompressionManifestsAdvertisementInterval is the cadence at which the
	// local manifest compression table is advertised to remote peers.
	// Consumed by CompressionTableManager.StartAdvertisementScheduler.
	// Corresponds to pekko.remote.artery.advanced.compression.manifests.advertisement-interval.
	// Pekko default: 1m. Zero means use the default.
	CompressionManifestsAdvertisementInterval time.Duration

	// TcpConnectionTimeout is the TCP dial timeout for outbound Artery
	// connections (threaded into the TcpClient dialer and the DialRemote
	// "wait for association" poll).
	// Corresponds to pekko.remote.artery.advanced.tcp.connection-timeout.
	// Pekko default: 5s. Zero means use the default.
	TcpConnectionTimeout time.Duration

	// TcpOutboundClientHostname, when non-empty, binds outbound TCP
	// connections to this local source hostname (net.Dialer.LocalAddr).
	// Empty means the OS chooses the local address.
	// Corresponds to pekko.remote.artery.advanced.tcp.outbound-client-hostname.
	// Pekko default: "" (unset).
	TcpOutboundClientHostname string

	// BufferPoolSize is the size of the shared receive buffer pool per
	// stream. Recorded on NodeManager for future buffer-pool consumers.
	// Corresponds to pekko.remote.artery.advanced.buffer-pool-size.
	// Pekko default: 128. Zero means use the default.
	BufferPoolSize int

	// MaximumLargeFrameSize is the max frame payload for the large-message
	// stream (streamId=3). Consumed by the large-stream read/write paths.
	// Corresponds to pekko.remote.artery.advanced.maximum-large-frame-size.
	// Pekko default: 2 MiB. Zero means use the default.
	MaximumLargeFrameSize int

	// LargeBufferPoolSize is the size of the shared receive buffer pool for
	// the large-message stream. Recorded on NodeManager for future consumers.
	// Corresponds to pekko.remote.artery.advanced.large-buffer-pool-size.
	// Pekko default: 32. Zero means use the default.
	LargeBufferPoolSize int

	// OutboundLargeMessageQueueSize is the outbox capacity for the
	// large-message stream (streamId=3). Sizes the per-association outbox
	// channel when the large stream is opened.
	// Corresponds to pekko.remote.artery.advanced.outbound-large-message-queue-size.
	// Pekko default: 256. Zero means use the default.
	OutboundLargeMessageQueueSize int
}

// TelemetryConfig controls the built-in OTEL instrumentation hooks.
type TelemetryConfig struct {
	// TracingEnabled enables automatic span creation in actor Receive loops
	// and in ActorRef.Ask.  Effective only when telemetry.SetProvider has
	// been called with a non-noop provider.
	// Corresponds to HOCON: gekka.telemetry.tracing.enabled
	TracingEnabled bool

	// MetricsEnabled enables recording of mailbox-size and processing-duration
	// metrics as well as the cluster member count gauge.
	// Corresponds to HOCON: gekka.telemetry.metrics.enabled
	MetricsEnabled bool

	// OtlpEndpoint is the base URL of an OpenTelemetry Protocol (OTLP) HTTP
	// collector endpoint (e.g. "http://otel-collector:4318").
	// When non-empty, gekka-metrics will push metrics to this endpoint using
	// the OTLP/HTTP exporter.  Leave empty to disable OTLP export.
	// Corresponds to HOCON: gekka.telemetry.exporter.otlp.endpoint
	OtlpEndpoint string
}

// ShardedDaemonProcessConfig holds the portable surface of
// pekko.cluster.sharded-daemon-process.*. Per Pekko's reference.conf comment,
// only `keep-alive-interval` and `sharding.role` are honoured — the other
// `sharding.*` overrides (remember-entities, passivation, number-of-shards)
// are explicitly ignored upstream and not exposed here.
type ShardedDaemonProcessConfig struct {
	// KeepAliveInterval is how often each running ShardedDaemonProcess
	// pings every entity index to ensure the daemon actor is alive.
	// Corresponds to pekko.cluster.sharded-daemon-process.keep-alive-interval.
	// Default: 10s.
	KeepAliveInterval time.Duration

	// ShardingRole, when non-empty, scopes the underlying ShardRegion to
	// nodes advertising this cluster role. Corresponds to
	// pekko.cluster.sharded-daemon-process.sharding.role.
	// Default: "" (any node).
	ShardingRole string
}

// ShardingConfig holds sharding-specific configuration parsed from HOCON.
type ShardingConfig struct {
	// PassivationIdleTimeout is the duration after which an entity that
	// has not received a message is automatically stopped.
	// Corresponds to pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.timeout
	PassivationIdleTimeout time.Duration

	// RememberEntities, when true, persists entity lifecycle events so
	// entities are re-spawned after a Shard restart.
	// Corresponds to pekko.cluster.sharding.remember-entities.
	RememberEntities bool

	// HandoffTimeout is the maximum time a ShardRegion waits for the
	// coordinator to acknowledge shard handoff during coordinated shutdown.
	// Larger clusters or heavily loaded coordinators may need a longer value.
	// Defaults to 10 seconds when zero or unset.
	//
	// HOCON: gekka.cluster.sharding.handoff-timeout
	HandoffTimeout time.Duration

	// NumberOfShards is the total number of shards distributed across the cluster.
	// This value is immutable after sharding starts.
	// Corresponds to pekko.cluster.sharding.number-of-shards.
	// Default: 1000.
	NumberOfShards int

	// Role, when non-empty, restricts shard allocation to cluster members
	// that carry this role.
	// Corresponds to pekko.cluster.sharding.role.
	Role string

	// GuardianName is the actor name for the sharding guardian.
	// Corresponds to pekko.cluster.sharding.guardian-name. Default: "sharding".
	GuardianName string

	// RememberEntitiesStore selects the backend: "eventsourced" or "ddata".
	// Corresponds to pekko.cluster.sharding.remember-entities-store. Default: "ddata".
	RememberEntitiesStore string

	// PassivationStrategy selects the passivation strategy.
	// Recognised values:
	//   - "default-idle-strategy"  — idle-timeout-only (Pekko default)
	//   - "least-recently-used"    — LRU eviction at active-entity-limit (Pekko name)
	//   - "custom-lru-strategy"    — gekka legacy alias for "least-recently-used"
	// Corresponds to pekko.cluster.sharding.passivation.strategy.
	// Round-2 session 24 added the "least-recently-used" name; the legacy
	// "custom-lru-strategy" alias is normalised at parse time so the
	// runtime only has to switch on the Pekko canonical form.
	// Default: "default-idle-strategy".
	PassivationStrategy string

	// PassivationActiveEntityLimit is the cap on active entities before the
	// configured replacement policy passivates the eviction candidate.
	// Corresponds (in priority order) to:
	//   - pekko.cluster.sharding.passivation.least-recently-used-strategy.active-entity-limit
	//   - pekko.cluster.sharding.passivation.custom-lru-strategy.active-entity-limit (legacy)
	// Pekko default: 100000 (round-2 session 24).
	PassivationActiveEntityLimit int

	// PassivationReplacementPolicy names the eviction policy used when the
	// active-entity-limit is reached.  Round-2 session 24 ships only
	// "least-recently-used"; sessions 25/26 add "most-recently-used",
	// "least-frequently-used", and the composite W-TinyLFU strategy.
	// Corresponds to pekko.cluster.sharding.passivation.<strategy>-strategy.replacement.policy.
	// Empty string defers to the strategy's default (LRU when the strategy
	// is least-recently-used).
	PassivationReplacementPolicy string

	// PassivationLFUDynamicAging mirrors Pekko's
	// pekko.cluster.sharding.passivation.least-frequently-used-strategy.dynamic-aging.
	// Round-2 session 25 parses the flag for forward-compat; the actual
	// counter-aging loop is added in a later session once a calibration
	// workload exists.  Default: false.
	PassivationLFUDynamicAging bool

	// ── Round-2 session 26 — composite (W-TinyLFU) passivation knobs ──
	//
	// PassivationWindowProportion is the admission-window size as a
	// fraction of PassivationActiveEntityLimit.  Pekko default: 0.01.
	// HOCON: pekko.cluster.sharding.passivation.strategy-defaults.admission.window.proportion.
	PassivationWindowProportion float64
	// PassivationWindowPolicy is the replacement policy for the window
	// area.  Currently only "least-recently-used" is honoured at
	// runtime.  HOCON: passivation.default-strategy.admission.window.policy.
	PassivationWindowPolicy string
	// PassivationFilter selects the admission filter.  Recognised
	// values: "frequency-sketch" (Pekko default), "off", "none".  HOCON:
	// passivation.default-strategy.admission.filter.
	PassivationFilter string
	// PassivationFrequencySketchDepth is the count-min-sketch depth
	// (number of hash rows).  Pekko default: 4.
	// HOCON: passivation.strategy-defaults.admission.frequency-sketch.depth.
	PassivationFrequencySketchDepth int
	// PassivationFrequencySketchCounterBits names the per-cell counter
	// width.  gekka stores 4-bit values regardless; parsed for
	// forward-compat.  Pekko default: 4.
	PassivationFrequencySketchCounterBits int
	// PassivationFrequencySketchWidthMultiplier sets the sketch width as
	// (active-entity-limit × multiplier).  Pekko default: 4.
	PassivationFrequencySketchWidthMultiplier int
	// PassivationFrequencySketchResetMultiplier triggers the
	// counter-halving reset every (active-entity-limit × multiplier)
	// total accesses.  Pekko default: 10.0.
	PassivationFrequencySketchResetMultiplier float64

	// AdaptiveRebalancing, when enabled, rebalances shards based on real-time
	// node metrics (CPU, Memory, Mailbox size).
	AdaptiveRebalancing AdaptiveRebalancingConfig

	// RebalanceInterval is how often the ShardCoordinator runs its periodic
	// rebalance check. Corresponds to pekko.cluster.sharding.rebalance-interval.
	// Default: 10s (applied by the coordinator when zero).
	RebalanceInterval time.Duration

	// LeastShardAllocation holds knobs for the default
	// LeastShardAllocationStrategy. Used when AdaptiveRebalancing is disabled
	// and no explicit allocation strategy is supplied via ShardingSettings.
	LeastShardAllocation LeastShardAllocationConfig

	// ExternalShardAllocation holds knobs for the external shard allocation
	// strategy. Used when the sharding allocation-strategy type is `external`.
	// Corresponds to pekko.cluster.sharding.external-shard-allocation-strategy.*.
	ExternalShardAllocation ExternalShardAllocationConfig

	// EventSourcedRememberEntitiesStore holds knobs for the eventsourced
	// remember-entities backend.
	// Corresponds to pekko.cluster.sharding.event-sourced-remember-entities-store.*.
	EventSourcedRememberEntitiesStore EventSourcedRememberEntitiesStoreConfig

	// DistributedData holds sharding-specific overrides for the DData
	// replicator that backs coordinator state and remember-entities.
	// Corresponds to pekko.cluster.sharding.distributed-data.*.
	DistributedData ShardingDistributedDataConfig

	// CoordinatorSingleton holds singleton-manager settings for the shard
	// coordinator singleton. Mirrors pekko.cluster.singleton layout — when
	// CoordinatorSingletonRoleOverride is true (the Pekko default), the
	// effective Role is replaced with ShardingConfig.Role at use time.
	// Corresponds to pekko.cluster.sharding.coordinator-singleton.*.
	CoordinatorSingleton SingletonConfig

	// CoordinatorSingletonRoleOverride, when true, replaces
	// CoordinatorSingleton.Role with the sharding role. Corresponds to
	// pekko.cluster.sharding.coordinator-singleton-role-override. Default: true.
	CoordinatorSingletonRoleOverride bool

	// RetryInterval is the period between retries of GetShardHome requests
	// for shards whose home is still unknown.
	// Corresponds to pekko.cluster.sharding.retry-interval. Pekko default: 2s.
	RetryInterval time.Duration

	// BufferSize caps the per-shard pending-message queue used by
	// ShardRegion while waiting for a ShardHome reply.
	// Corresponds to pekko.cluster.sharding.buffer-size. Pekko default: 100000.
	BufferSize int

	// ShardStartTimeout is the maximum time a Shard waits during its own
	// startup before giving up.
	// Corresponds to pekko.cluster.sharding.shard-start-timeout. Pekko default: 10s.
	ShardStartTimeout time.Duration

	// ShardFailureBackoff is the delay before a terminated Shard actor is
	// re-spawned by its ShardRegion.
	// Corresponds to pekko.cluster.sharding.shard-failure-backoff. Pekko default: 10s.
	ShardFailureBackoff time.Duration

	// EntityRestartBackoff is the delay before a terminated entity inside a
	// Shard is re-spawned (used with remember-entities).
	// Corresponds to pekko.cluster.sharding.entity-restart-backoff. Pekko default: 10s.
	EntityRestartBackoff time.Duration

	// CoordinatorFailureBackoff is the delay before the ShardCoordinatorProxy
	// retries reaching the coordinator singleton after a transient failure.
	// Corresponds to pekko.cluster.sharding.coordinator-failure-backoff. Pekko default: 5s.
	CoordinatorFailureBackoff time.Duration

	// WaitingForStateTimeout caps how long the Shard waits for the initial
	// distributed-state read during recovery (DData ReadMajority).
	// Corresponds to pekko.cluster.sharding.waiting-for-state-timeout.
	// Pekko default: 2s. Plumbed to ShardSettings.
	WaitingForStateTimeout time.Duration

	// UpdatingStateTimeout caps how long the Shard waits for a
	// distributed-state update or remember-entities write to complete.
	// Corresponds to pekko.cluster.sharding.updating-state-timeout.
	// Pekko default: 5s. Plumbed to ShardSettings.
	UpdatingStateTimeout time.Duration

	// ShardRegionQueryTimeout caps how long the ShardRegion waits when
	// answering a query that needs to reach every shard.
	// Corresponds to pekko.cluster.sharding.shard-region-query-timeout.
	// Pekko default: 3s. Plumbed to ShardSettings.
	ShardRegionQueryTimeout time.Duration

	// EntityRecoveryStrategy selects how a Shard re-spawns remembered
	// entities. Allowed values: "all" (default) or "constant".
	// Corresponds to pekko.cluster.sharding.entity-recovery-strategy.
	EntityRecoveryStrategy string

	// EntityRecoveryConstantRateFrequency is the delay between successive
	// entity-spawn batches under the "constant" strategy.
	// Corresponds to
	// pekko.cluster.sharding.entity-recovery-constant-rate-strategy.frequency.
	// Pekko default: 100ms.
	EntityRecoveryConstantRateFrequency time.Duration

	// EntityRecoveryConstantRateNumberOfEntities is the batch size for the
	// "constant" entity-recovery strategy.
	// Corresponds to
	// pekko.cluster.sharding.entity-recovery-constant-rate-strategy.number-of-entities.
	// Pekko default: 5.
	EntityRecoveryConstantRateNumberOfEntities int

	// CoordinatorWriteMajorityPlus is the additional number of nodes (above
	// majority) DData writes must reach when persisting coordinator state.
	// Corresponds to
	// pekko.cluster.sharding.coordinator-state.write-majority-plus.
	// Pekko default: 3. Plumbed to ShardSettings.
	CoordinatorWriteMajorityPlus int

	// CoordinatorReadMajorityPlus is the additional number of nodes (above
	// majority) DData reads must reach when retrieving coordinator state.
	// Corresponds to
	// pekko.cluster.sharding.coordinator-state.read-majority-plus.
	// Pekko default: 5. Plumbed to ShardSettings.
	CoordinatorReadMajorityPlus int

	// VerboseDebugLogging gates fine-grained per-message DEBUG logs in
	// the sharding code path.
	// Corresponds to pekko.cluster.sharding.verbose-debug-logging.
	VerboseDebugLogging bool

	// FailOnInvalidEntityStateTransition, when true, makes the Shard
	// panic on an invalid internal state transition.
	// Corresponds to
	// pekko.cluster.sharding.fail-on-invalid-entity-state-transition.
	FailOnInvalidEntityStateTransition bool

	// IdleEntityCheckInterval overrides the cadence of the idle-entity
	// passivation scan. When zero, defaults to PassivationIdleTimeout/2.
	// Corresponds to
	// pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.interval.
	IdleEntityCheckInterval time.Duration

	// HealthCheck holds settings for the sharding readiness probe.
	// Corresponds to pekko.cluster.sharding.healthcheck.*.
	HealthCheck ShardingHealthCheckConfig

	// UseLease names a LeaseProvider registered in the Cluster's LeaseManager.
	// When non-empty, every Shard actor acquires a distributed lease before
	// becoming active and releases it on handoff/stop. Mirrors
	// pekko.cluster.sharding.use-lease. Default: "" (no lease).
	UseLease string

	// LeaseRetryInterval is the backoff between lease-acquisition retries
	// when a previous Acquire returned false or errored.
	// Corresponds to pekko.cluster.sharding.lease-retry-interval. Default: 5s.
	LeaseRetryInterval time.Duration
}

// ShardingHealthCheckConfig captures pekko.cluster.sharding.healthcheck.*.
type ShardingHealthCheckConfig struct {
	// Names is the list of sharding type names that must have a
	// registered coordinator for the health check to pass.
	// Corresponds to pekko.cluster.sharding.healthcheck.names. Default: [].
	Names []string

	// Timeout caps how long the health check is allowed to run.
	// Corresponds to pekko.cluster.sharding.healthcheck.timeout. Pekko default: 5s.
	Timeout time.Duration
}

// EventSourcedRememberEntitiesStoreConfig holds knobs for the eventsourced
// remember-entities backend (the journal-based variant). Corresponds to
// pekko.cluster.sharding.event-sourced-remember-entities-store.*.
type EventSourcedRememberEntitiesStoreConfig struct {
	// MaxUpdatesPerWrite is the upper bound on the number of buffered
	// EntityStarted/EntityStopped events that may be coalesced into a
	// single journal write. Corresponds to max-updates-per-write.
	// Pekko default: 100.
	MaxUpdatesPerWrite int
}

// LeastShardAllocationConfig holds knobs for the default sharding allocation
// strategy. Corresponds to pekko.cluster.sharding.least-shard-allocation-strategy.*.
type LeastShardAllocationConfig struct {
	// RebalanceThreshold is the minimum spread between most- and least-loaded
	// regions required to trigger a rebalance.
	// Corresponds to least-shard-allocation-strategy.rebalance-threshold.
	// Pekko default: 1.
	RebalanceThreshold int

	// MaxSimultaneousRebalance caps the number of in-flight rebalance moves.
	// Corresponds to least-shard-allocation-strategy.max-simultaneous-rebalance.
	// Pekko default: 3.
	MaxSimultaneousRebalance int

	// RebalanceAbsoluteLimit is the maximum number of shards that will be
	// rebalanced in one round under the Pekko 1.0+ algorithm. When > 0, the
	// new two-phase strategy is selected; when 0, the legacy threshold-based
	// strategy is kept. Corresponds to
	// least-shard-allocation-strategy.rebalance-absolute-limit. Pekko default: 0.
	RebalanceAbsoluteLimit int

	// RebalanceRelativeLimit is the fraction (< 1.0) of the total number of
	// known shards that may be rebalanced in one round under the Pekko 1.0+
	// algorithm. Corresponds to
	// least-shard-allocation-strategy.rebalance-relative-limit. Pekko default: 0.1.
	RebalanceRelativeLimit float64
}

// ExternalShardAllocationConfig holds knobs for the external shard allocation
// strategy. Corresponds to pekko.cluster.sharding.external-shard-allocation-strategy.*.
type ExternalShardAllocationConfig struct {
	// ClientTimeout bounds each call to the external allocation HTTP client.
	// Corresponds to external-shard-allocation-strategy.client-timeout.
	// Pekko default: 5s.
	ClientTimeout time.Duration
}

// ShardingDistributedDataConfig holds sharding-specific overrides for the
// DData replicator. Corresponds to pekko.cluster.sharding.distributed-data.*.
type ShardingDistributedDataConfig struct {
	// MajorityMinCap is the minimum quorum size for sharding-state
	// MajorityWrite / MajorityRead operations.
	// Corresponds to majority-min-cap. Pekko default: 5.
	MajorityMinCap int

	// MaxDeltaElements caps delta-message size for the sharding replicator.
	// Corresponds to max-delta-elements. Pekko default: 5.
	MaxDeltaElements int

	// PreferOldest selects oldest-first peer ordering for gossip.
	// Corresponds to prefer-oldest. Pekko default: true (on).
	PreferOldest bool

	// DurableKeys are the key globs persisted to durable DData storage.
	// Corresponds to durable.keys. Pekko default: ["shard-*"].
	DurableKeys []string
}

// AdaptiveRebalancingConfig holds settings for the adaptive rebalancing strategy.
type AdaptiveRebalancingConfig struct {
	// Enabled, when true, activates the adaptive rebalancing strategy.
	// HOCON: gekka.cluster.sharding.adaptive-rebalancing.enabled
	Enabled bool

	// LoadWeight is the blend factor [0.0, 1.0] between load and shard-count.
	// HOCON: gekka.cluster.sharding.adaptive-rebalancing.load-weight
	LoadWeight float64

	// CPUWeight is the relative weight of CPU pressure in load calculation.
	// HOCON: gekka.cluster.sharding.adaptive-rebalancing.cpu-weight
	CPUWeight float64

	// MemoryWeight is the relative weight of memory pressure in load calculation.
	// HOCON: gekka.cluster.sharding.adaptive-rebalancing.memory-weight
	MemoryWeight float64

	// MailboxWeight is the relative weight of mailbox size in load calculation.
	// HOCON: gekka.cluster.sharding.adaptive-rebalancing.mailbox-weight
	MailboxWeight float64

	// RebalanceThreshold is the minimum score spread that triggers a rebalance.
	// HOCON: gekka.cluster.sharding.adaptive-rebalancing.rebalance-threshold
	RebalanceThreshold float64

	// MaxSimultaneousRebalance is the maximum number of shards being moved at once.
	// HOCON: gekka.cluster.sharding.adaptive-rebalancing.max-simultaneous-rebalance
	MaxSimultaneousRebalance int
}

// SingletonConfig holds cluster singleton manager settings parsed from HOCON.
type SingletonConfig struct {
	// Role restricts the singleton to nodes carrying this role.
	// Corresponds to pekko.cluster.singleton.role.
	// Default: "" (any node).
	Role string

	// HandOverRetryInterval is how often the manager retries handover
	// coordination during leadership transfer.
	// Corresponds to pekko.cluster.singleton.hand-over-retry-interval.
	// Default: 1s.
	HandOverRetryInterval time.Duration

	// SingletonName is the name of the child actor spawned by the manager.
	// Corresponds to pekko.cluster.singleton.singleton-name.
	// Default: "singleton".
	SingletonName string

	// MinNumberOfHandOverRetries is the minimum number of hand-over retries
	// before the manager gives up. Corresponds to
	// pekko.cluster.singleton.min-number-of-hand-over-retries.
	// Default: 15.
	MinNumberOfHandOverRetries int

	// UseLease names a LeaseProvider registered in the Cluster's LeaseManager.
	// When non-empty, the singleton manager acquires a distributed lease
	// before spawning the singleton actor and releases it on handoff/stop.
	// Mirrors pekko.cluster.singleton.use-lease. Default: "" (no lease).
	UseLease string

	// LeaseRetryInterval is the backoff between lease-acquisition retries when
	// a previous Acquire returned false or errored.
	// Corresponds to pekko.cluster.singleton.lease-retry-interval. Default: 5s.
	LeaseRetryInterval time.Duration
}

// SingletonProxyConfig holds cluster singleton proxy settings parsed from HOCON.
type SingletonProxyConfig struct {
	// SingletonIdentificationInterval is how often the proxy re-resolves
	// the oldest node to identify the singleton location.
	// Corresponds to pekko.cluster.singleton-proxy.singleton-identification-interval.
	// Default: 1s.
	SingletonIdentificationInterval time.Duration

	// BufferSize is the maximum number of messages buffered when the
	// singleton location is unknown. When the buffer is full, the oldest
	// messages are dropped.
	// Corresponds to pekko.cluster.singleton-proxy.buffer-size.
	// Default: 1000.
	BufferSize int

	// SingletonName is the name of the singleton child actor that the proxy
	// appends to the manager path when routing messages.
	// Corresponds to pekko.cluster.singleton-proxy.singleton-name.
	// Default: "singleton".
	SingletonName string

	// Role restricts which nodes the proxy considers when resolving the
	// oldest node. Corresponds to pekko.cluster.singleton-proxy.role.
	// Default: "" (any node).
	Role string
}

// resolve returns the effective (scheme, system, host, port) for this config.
func (c ClusterConfig) resolve() (scheme, system, host string, port uint32) {
	if c.Address.System != "" {
		return c.Address.Protocol, c.Address.System, c.Address.Host, uint32(c.Address.Port)
	}
	s := c.SystemName
	if s == "" {
		s = "GekkaSystem"
	}
	h := c.Host
	if h == "" {
		h = "127.0.0.1"
	}
	return c.Provider.protoString(), s, h, c.Port
}

// IncomingMessage carries metadata and raw payload for a received Artery message.
type IncomingMessage struct {
	// RecipientPath is the full Pekko actor path this message was addressed to.
	RecipientPath string
	// Payload is the raw serialized bytes.
	Payload []byte
	// SerializerId identifies the serializer used by the sender.
	// Common values: 4 = raw bytes, 2 = Protobuf, 5 = ClusterMessage.
	SerializerId int32
	// Manifest is the type tag embedded in the Artery envelope.
	Manifest string
	// Sender is the actor that sent this message. It is a remote ActorRef
	// constructed from the Artery envelope's sender field. It is the zero-value
	// NoSender when the remote side did not include a sender path.
	//
	// Prefer using BaseActor.Sender() inside Receive; this field is exposed
	// for callbacks that receive IncomingMessage directly (e.g. OnMessage).
	Sender ActorRef

	// DeserializedMessage is the decoded object. If nil, only Payload is available.
	DeserializedMessage any
}

// GetPayload returns the raw message payload.
func (m *IncomingMessage) GetPayload() []byte {
	return m.Payload
}

// Cluster is the single entry point for the gekka library. It wires together
// the TCP server, NodeManager, gcluster.ClusterManager, Router, and Replicator.
//
// Create one with Spawn (or SpawnFromConfig), then call Join (or JoinSeeds)
// to connect to a Pekko gcluster.
type Cluster struct {
	cfg        ClusterConfig
	nm         *core.NodeManager
	cm         *gcluster.ClusterManager
	router     *actor.Router
	repl       *ddata.Replicator
	durable    ddata.DurableStore // optional: bbolt-backed store, closed during shutdown
	mg         *gcluster.MetricsGossip
	server     *core.TcpServer
	udpHandler *core.UdpArteryHandler // non-nil when transport == "aeron-udp"
	ctx        context.Context
	cancel     context.CancelFunc
	localAddr  *gproto_remote.Address
	seedAddr   *gproto_remote.Address // set by the first Join call
	seeds      []actor.Address        // from ClusterConfig.SeedNodes (populated by LoadConfig)
	metrics    *core.NodeMetrics
	monitoring *core.MonitoringServer       // nil when monitoring is disabled
	mgmt       *management.ManagementServer // nil when management API is disabled

	actorsMu sync.RWMutex
	actors   map[string]actor.Actor // actor path suffix → Actor

	remoteWatchersMu sync.Mutex
	// node addr "host:port" → target full path → slice of watcher references
	remoteWatchers map[string]map[string][]ActorRef

	// watchFDStateRef holds the lazy reaper-goroutine bookkeeping for the
	// remote-watch failure detector. Allocated on first watch via watchFD().
	watchFDOnceInit sync.Once
	watchFDStateRef *watchFDState

	// localWatchers tracks remote actors watching local actors.
	// local path → remote node unique address string → slice of remote actor refs
	localWatchersMu sync.Mutex
	localWatchers   map[string]map[string][]*gproto_remote.ProtoActorRef

	// lastSystemSeqNo is the last sequence number used for outbound system messages.
	lastSystemSeqNo atomic.Uint64

	// System is the ActorSystem for this node. Use it to create and register

	// actors by name:
	//
	//	ref, err := node.System.ActorOf(gekka.Props{New: func() actor.Actor {
	//	    return &MyActor{BaseActor: actor.NewBaseActor()}
	//	}}, "my-actor")
	System ActorSystem

	clusterWatcherRef ActorRef
	logHandler        slog.Handler // nil = use slog.Default().Handler()
	onMessage         func(ctx context.Context, msg *IncomingMessage) error
	deployments       map[string]core.DeploymentConfig // keyed by actor path; nil = no deployments

	// Coordinated shutdown — drives the graceful exit sequence.
	cs *actor.CoordinatedShutdown

	// ps holds the optionally-provisioned Journal and SnapshotStore.
	// Managed by cluster_persistence.go.
	ps persistenceState

	// shuttingDown is set to 1 atomically during shutdown; used by DeadLetterLogger.
	shuttingDown int32

	// eventStream is the system-wide publish/subscribe bus.
	eventStream *actor.EventStream

	// shardingRegionsMu guards shardingRegions.
	shardingRegionsMu sync.Mutex
	// shardingRegions holds refs to every ShardRegion actor spawned on this
	// node.  Populated by RegisterShardingRegion; consumed during the
	// cluster-sharding-shutdown-region phase.
	shardingRegions []ActorRef

	// leaseManagerOnce guards lazy initialisation of leaseMgr.
	leaseManagerOnce sync.Once
	// leaseMgr is the per-cluster registry of LeaseProviders consulted by
	// SBR lease-majority and Singleton/Sharding use-lease wiring.  Populated
	// lazily via LeaseManager() with the in-memory reference provider.
	leaseMgr *lease.LeaseManager

	// downingProviders is the per-cluster DowningProvider registry.  Populated
	// during NewCluster with the bundled SBRDowningProvider so an empty
	// `pekko.cluster.downing-provider-class` resolves cleanly.  Operators can
	// register additional providers via Cluster.RegisterDowningProvider before
	// startup completes.  Round-2 session 27.
	downingProviders *gcluster.DowningProviderRegistry
	// downingProvider is the resolved active provider for this cluster — what
	// `Cluster.DowningProvider()` returns and what session 28 will route every
	// downing decision through.
	downingProvider gcluster.DowningProvider

	sched *systemScheduler
}

// applyDiscoveredSeeds resolves the configured discovery provider, calls
// FetchSeedNodes, and appends each discovered "host:port" to cfg.SeedNodes
// after wrapping it as a `<scheme>://<system>@<host:port>` address.  A
// FetchSeedNodes failure is logged but does not abort cluster startup; a
// missing-provider error is fatal (configuration is invalid).
func applyDiscoveredSeeds(cfg *ClusterConfig, scheme, system string) error {
	if !cfg.Discovery.Enabled {
		return nil
	}
	provider, err := discovery.Get(cfg.Discovery.Type, cfg.Discovery.Config)
	if err != nil {
		return fmt.Errorf("gekka: discovery: %w", err)
	}
	discovered, err := provider.FetchSeedNodes()
	if err != nil {
		log.Printf("[Discovery] %s: %v", cfg.Discovery.Type, err)
		return nil
	}
	for _, s := range discovered {
		uri := fmt.Sprintf("%s://%s@%s", scheme, system, s)
		if addr, err := actor.ParseAddress(uri); err == nil {
			cfg.SeedNodes = append(cfg.SeedNodes, addr)
		}
	}
	return nil
}

// NewCluster creates, wires, and starts a Cluster. The TCP listener is bound
// immediately; call node.Addr() to discover the assigned port when ClusterConfig.Port == 0.
func NewCluster(cfg ClusterConfig) (*Cluster, error) {
	scheme, system, host, port := cfg.resolve()

	// pekko.log-config-on-start — dump the resolved cluster configuration
	// at INFO so operators can confirm which layer (reference vs.
	// application) is active.
	if cfg.LogConfigOnStart {
		slog.Info("gekka: resolved cluster configuration", "config", fmt.Sprintf("%+v", cfg))
	}

	// Dynamic seed discovery (v0.9.0)
	if err := applyDiscoveredSeeds(&cfg, scheme, system); err != nil {
		return nil, err
	}

	localAddr := &gproto_remote.Address{
		Protocol: &scheme,
		System:   &system,
		Hostname: &host,
		Port:     &port,
	}
	uid := uint64(time.Now().UnixNano())
	localUA := &gproto_remote.UniqueAddress{
		Address: localAddr,
		Uid:     &uid,
	}

	metrics := &core.NodeMetrics{}
	nm := core.NewNodeManager(localAddr, uid)
	nm.NodeMetrics = metrics
	if cfg.MaxFrameSize > 0 {
		nm.MaxFrameSize = cfg.MaxFrameSize
	}
	// Artery advanced knobs (pekko.remote.artery.advanced.*).
	if cfg.ArteryAdvanced.InboundLanes > 0 {
		nm.InboundLanes = cfg.ArteryAdvanced.InboundLanes
	}
	if cfg.ArteryAdvanced.OutboundLanes > 0 {
		nm.OutboundLanes = cfg.ArteryAdvanced.OutboundLanes
	}
	if cfg.ArteryAdvanced.OutboundMessageQueueSize > 0 {
		nm.OutboundMessageQueueSize = cfg.ArteryAdvanced.OutboundMessageQueueSize
	}
	if cfg.ArteryAdvanced.SystemMessageBufferSize > 0 {
		nm.SystemMessageBufferSize = cfg.ArteryAdvanced.SystemMessageBufferSize
	}
	if cfg.ArteryAdvanced.HandshakeTimeout > 0 {
		nm.HandshakeTimeout = cfg.ArteryAdvanced.HandshakeTimeout
	}
	if cfg.ArteryAdvanced.HandshakeRetryInterval > 0 {
		nm.HandshakeRetryInterval = cfg.ArteryAdvanced.HandshakeRetryInterval
	}
	if cfg.ArteryAdvanced.SystemMessageResendInterval > 0 {
		nm.SystemMessageResendInterval = cfg.ArteryAdvanced.SystemMessageResendInterval
	}
	if cfg.ArteryAdvanced.GiveUpSystemMessageAfter > 0 {
		nm.GiveUpSystemMessageAfter = cfg.ArteryAdvanced.GiveUpSystemMessageAfter
	}
	if cfg.ArteryAdvanced.OutboundControlQueueSize > 0 {
		nm.OutboundControlQueueSize = cfg.ArteryAdvanced.OutboundControlQueueSize
	}
	if cfg.ArteryAdvanced.StopIdleOutboundAfter > 0 {
		nm.StopIdleOutboundAfter = cfg.ArteryAdvanced.StopIdleOutboundAfter
	}
	if cfg.ArteryAdvanced.QuarantineIdleOutboundAfter > 0 {
		nm.QuarantineIdleOutboundAfter = cfg.ArteryAdvanced.QuarantineIdleOutboundAfter
	}
	if cfg.ArteryAdvanced.StopQuarantinedAfterIdle > 0 {
		nm.StopQuarantinedAfterIdle = cfg.ArteryAdvanced.StopQuarantinedAfterIdle
	}
	if cfg.ArteryAdvanced.RemoveQuarantinedAssociationAfter > 0 {
		nm.RemoveQuarantinedAssociationAfter = cfg.ArteryAdvanced.RemoveQuarantinedAssociationAfter
	}
	if cfg.ArteryAdvanced.ShutdownFlushTimeout > 0 {
		nm.ShutdownFlushTimeout = cfg.ArteryAdvanced.ShutdownFlushTimeout
	}
	if cfg.ArteryAdvanced.DeathWatchNotificationFlushTimeout > 0 {
		nm.DeathWatchNotificationFlushTimeout = cfg.ArteryAdvanced.DeathWatchNotificationFlushTimeout
	}
	if cfg.ArteryAdvanced.InboundRestartTimeout > 0 {
		nm.InboundRestartTimeout = cfg.ArteryAdvanced.InboundRestartTimeout
	}
	if cfg.ArteryAdvanced.InboundMaxRestarts > 0 {
		nm.InboundMaxRestarts = cfg.ArteryAdvanced.InboundMaxRestarts
	}
	if cfg.ArteryAdvanced.OutboundRestartBackoff > 0 {
		nm.OutboundRestartBackoff = cfg.ArteryAdvanced.OutboundRestartBackoff
	}
	if cfg.ArteryAdvanced.OutboundRestartTimeout > 0 {
		nm.OutboundRestartTimeout = cfg.ArteryAdvanced.OutboundRestartTimeout
	}
	if cfg.ArteryAdvanced.OutboundMaxRestarts > 0 {
		nm.OutboundMaxRestarts = cfg.ArteryAdvanced.OutboundMaxRestarts
	}
	if cfg.ArteryAdvanced.CompressionActorRefsMax > 0 {
		nm.CompressionActorRefsMax = cfg.ArteryAdvanced.CompressionActorRefsMax
	}
	if cfg.ArteryAdvanced.CompressionActorRefsAdvertisementInterval > 0 {
		nm.CompressionActorRefsAdvertisementInterval = cfg.ArteryAdvanced.CompressionActorRefsAdvertisementInterval
	}
	if cfg.ArteryAdvanced.CompressionManifestsMax > 0 {
		nm.CompressionManifestsMax = cfg.ArteryAdvanced.CompressionManifestsMax
	}
	if cfg.ArteryAdvanced.CompressionManifestsAdvertisementInterval > 0 {
		nm.CompressionManifestsAdvertisementInterval = cfg.ArteryAdvanced.CompressionManifestsAdvertisementInterval
	}
	if cfg.ArteryAdvanced.TcpConnectionTimeout > 0 {
		nm.TcpConnectionTimeout = cfg.ArteryAdvanced.TcpConnectionTimeout
	}
	if cfg.ArteryAdvanced.TcpOutboundClientHostname != "" {
		nm.TcpOutboundClientHostname = cfg.ArteryAdvanced.TcpOutboundClientHostname
	}
	if cfg.ArteryAdvanced.BufferPoolSize > 0 {
		nm.BufferPoolSize = cfg.ArteryAdvanced.BufferPoolSize
	}
	if cfg.ArteryAdvanced.MaximumLargeFrameSize > 0 {
		nm.MaximumLargeFrameSize = cfg.ArteryAdvanced.MaximumLargeFrameSize
	}
	if cfg.ArteryAdvanced.LargeBufferPoolSize > 0 {
		nm.LargeBufferPoolSize = cfg.ArteryAdvanced.LargeBufferPoolSize
	}
	if cfg.ArteryAdvanced.OutboundLargeMessageQueueSize > 0 {
		nm.OutboundLargeMessageQueueSize = cfg.ArteryAdvanced.OutboundLargeMessageQueueSize
	}
	if len(cfg.AcceptProtocolNames) > 0 {
		nm.AcceptProtocolNames = cfg.AcceptProtocolNames
	}
	// Artery debug/observability flags (Round-2 session 09).
	nm.LogReceivedMessages = cfg.LogReceivedMessages
	nm.LogSentMessages = cfg.LogSentMessages
	nm.LogFrameSizeExceeding = cfg.LogFrameSizeExceeding
	nm.PropagateHarmlessQuarantineEvents = cfg.PropagateHarmlessQuarantineEvents
	// Round-2 session 29: large-message-destinations routes matching outbound
	// recipients onto the large-message stream (streamId=3).
	nm.SetLargeMessageDestinations(cfg.LargeMessageDestinations)
	// Round-2 sessions 31+32: F6 message-security inbound enforcement.
	nm.UntrustedMode = cfg.UntrustedMode
	nm.TrustedSelectionPaths = append([]string(nil), cfg.TrustedSelectionPaths...)
	// Apply flight recorder config from HOCON (defaults: enabled=true, level=lifecycle).
	frEnabled := cfg.FlightRecorder.Enabled
	// When neither LoadConfig nor explicit struct init has been called,
	// FlightRecorderConfig is zero-valued (Enabled==false). Treat that as
	// "not configured" and keep the default-on behaviour.
	if cfg.FlightRecorder == (FlightRecorderConfig{}) {
		frEnabled = true
	}
	nm.FlightRec = core.NewFlightRecorder(frEnabled, core.ParseEventLevel(cfg.FlightRecorder.Level))
	typed.SetGlobalMessagingProvider(nm)
	router := actor.NewRouter(nm)
	cm := gcluster.NewClusterManager(core.ToClusterUniqueAddress(localUA), func(ctx context.Context, path string, msg any) error {
		return router.Send(ctx, path, msg)
	})
	cm.Protocol = scheme
	cm.Metrics = metrics
	cm.Router = func(ctx context.Context, path string, msg any) error {
		return router.Send(ctx, path, msg)
	}
	cm.SetLocalDataCenter(cfg.DataCenter)
	if cfg.CrossDataCenterGossipProbability > 0 {
		cm.CrossDataCenterGossipProbability = cfg.CrossDataCenterGossipProbability
	}
	if cfg.CrossDataCenterConnections > 0 {
		cm.CrossDataCenterConnections = cfg.CrossDataCenterConnections
	}
	if len(cfg.Roles) > 0 {
		cm.SetLocalRoles(cfg.Roles)
	}

	// Apply Phi Accrual Failure Detector configuration from HOCON.
	gcluster.ApplyDetectorConfig(cm, cfg.FailureDetector)

	// Apply remote watch failure detector configuration from HOCON
	// (pekko.remote.watch-failure-detector.*).
	gcluster.ApplyWatchDetectorConfig(cm, cfg.WatchFailureDetector)

	// Apply cluster timing configuration from HOCON.
	if cfg.FailureDetector.HeartbeatInterval > 0 {
		cm.HeartbeatInterval = cfg.FailureDetector.HeartbeatInterval
	}
	if cfg.RetryUnsuccessfulJoinAfter > 0 {
		cm.RetryUnsuccessfulJoinAfter = cfg.RetryUnsuccessfulJoinAfter
	}
	if cfg.MinNrOfMembers > 0 {
		cm.MinNrOfMembers = cfg.MinNrOfMembers
	}
	if len(cfg.RoleMinNrOfMembers) > 0 {
		cm.RoleMinNrOfMembers = cfg.RoleMinNrOfMembers
	}
	if cfg.GossipInterval > 0 {
		cm.GossipInterval = cfg.GossipInterval
	}
	if cfg.LeaderActionsInterval > 0 {
		cm.LeaderActionsInterval = cfg.LeaderActionsInterval
	}
	if cfg.PeriodicTasksInitialDelay > 0 {
		cm.PeriodicTasksInitialDelay = cfg.PeriodicTasksInitialDelay
	}
	if cfg.ShutdownAfterUnsuccessfulJoinSeedNodes > 0 {
		cm.ShutdownAfterUnsuccessfulJoinSeedNodes = cfg.ShutdownAfterUnsuccessfulJoinSeedNodes
	}
	if cfg.LogInfo != nil {
		cm.LogInfo = *cfg.LogInfo
	}
	cm.LogInfoVerbose = cfg.LogInfoVerbose
	if cfg.AppVersion != "" {
		cm.SetLocalAppVersion(gcluster.ParseAppVersion(cfg.AppVersion))
	}
	if cfg.AllowWeaklyUpMembers > 0 {
		cm.AllowWeaklyUpMembers = cfg.AllowWeaklyUpMembers
	}
	if cfg.GossipDifferentViewProbability > 0 {
		cm.GossipDifferentViewProbability = cfg.GossipDifferentViewProbability
	}
	if cfg.ReduceGossipDifferentViewProbability > 0 {
		cm.ReduceGossipDifferentViewProbability = cfg.ReduceGossipDifferentViewProbability
	}
	if cfg.GossipTimeToLive > 0 {
		cm.GossipTimeToLive = cfg.GossipTimeToLive
	}
	if cfg.PruneGossipTombstonesAfter > 0 {
		cm.PruneGossipTombstonesAfter = cfg.PruneGossipTombstonesAfter
	}
	if cfg.FailureDetector.MonitoredByNrOfMembers > 0 {
		cm.MonitoredByNrOfMembers = cfg.FailureDetector.MonitoredByNrOfMembers
	}
	if cfg.UnreachableNodesReaperInterval > 0 {
		cm.UnreachableNodesReaperInterval = cfg.UnreachableNodesReaperInterval
	}
	if cfg.PublishStatsInterval > 0 {
		cm.PublishStatsInterval = cfg.PublishStatsInterval
	}
	if cfg.MultiDCFailureDetector.HeartbeatInterval > 0 {
		cm.CrossDCHeartbeatInterval = cfg.MultiDCFailureDetector.HeartbeatInterval
	}
	if cfg.MultiDCFailureDetector.AcceptableHeartbeatPause > 0 {
		cm.CrossDCAcceptableHeartbeatPause = cfg.MultiDCFailureDetector.AcceptableHeartbeatPause
	}
	if cfg.MultiDCFailureDetector.ExpectedResponseAfter > 0 {
		cm.CrossDCExpectedResponseAfter = cfg.MultiDCFailureDetector.ExpectedResponseAfter
	}
	if cfg.DownRemovalMargin > 0 {
		cm.DownRemovalMargin = cfg.DownRemovalMargin
	}
	if cfg.SeedNodeTimeout > 0 {
		cm.SeedNodeTimeout = cfg.SeedNodeTimeout
	}
	// EnforceConfigCompatOnJoin defaults to true (Pekko default).
	cm.EnforceConfigCompatOnJoin = cfg.ConfigCompatCheck.EnforceOnJoin == nil || *cfg.ConfigCompatCheck.EnforceOnJoin

	// Wire the lightweight internal SBR strategy (icluster.Strategy).
	gcluster.ApplyInternalSBRConfig(cm, cfg.InternalSBR)

	// Wire the flight recorder heartbeat-miss emitter.
	cm.MissEmitter = nm.FlightRec

	nm.SetClusterManager(cm)

	ctx, cancel := context.WithCancel(context.Background())

	// Build TLS config when transport is "tls-tcp".
	var tlsCfg *tls.Config
	if strings.EqualFold(cfg.Transport, "tls-tcp") {
		var tlsErr error
		tlsCfg, tlsErr = core.BuildTLSConfig(cfg.TLS)
		if tlsErr != nil {
			cancel()
			return nil, fmt.Errorf("gekka: TLS config: %w", tlsErr)
		}
	}
	nm.TLSConfig = tlsCfg

	// Load user-defined serializers from HOCON config when provided.
	if cfg.HOCON != nil {
		if err := nm.SerializerRegistry.LoadFromConfig(*cfg.HOCON); err != nil {
			cancel()
			return nil, fmt.Errorf("gekka: serialization config: %w", err)
		}
	}

	// Apply persistence recovery concurrency limit from HOCON.
	if cfg.Persistence.MaxConcurrentRecoveries > 0 {
		persistence.SetMaxConcurrentRecoveries(cfg.Persistence.MaxConcurrentRecoveries)
	}

	// Round-2 session 17: persistence small features.
	if cfg.Persistence.AutoMigrateManifest != "" {
		persistence.SetAutoMigrateManifest(cfg.Persistence.AutoMigrateManifest)
	}
	if cfg.Persistence.StatePluginFallbackRecoveryTimeout > 0 {
		persistence.SetStatePluginFallbackRecoveryTimeout(cfg.Persistence.StatePluginFallbackRecoveryTimeout)
	}
	if cfg.ActorTypedRestartStashCapacity > 0 {
		typed.SetDefaultRestartStashCapacity(cfg.ActorTypedRestartStashCapacity)
	}
	if cfg.DistributedData.TypedReplicatorMessageAdapterUnexpectedAskTimeout > 0 {
		ddata.SetDefaultUnexpectedAskTimeout(cfg.DistributedData.TypedReplicatorMessageAdapterUnexpectedAskTimeout)
	}
	if cfg.ShardedDaemonProcess.KeepAliveInterval > 0 {
		sharding.SetDefaultShardedDaemonProcessKeepAliveInterval(cfg.ShardedDaemonProcess.KeepAliveInterval)
	}
	sharding.SetDefaultShardedDaemonProcessShardingRole(cfg.ShardedDaemonProcess.ShardingRole)
	if cfg.Persistence.TypedStashCapacity > 0 {
		persistencetyped.SetDefaultStashCapacity(cfg.Persistence.TypedStashCapacity)
	}
	if cfg.Persistence.TypedStashOverflowStrategy != "" {
		persistencetyped.SetDefaultStashOverflowStrategy(cfg.Persistence.TypedStashOverflowStrategy)
	}
	persistencetyped.SetSnapshotOnRecovery(cfg.Persistence.TypedSnapshotOnRecovery)
	persistencetyped.SetLogStashing(cfg.Persistence.TypedLogStashing)
	persistence.SetDefaultFSMSnapshotAfter(cfg.Persistence.FSMSnapshotAfter)

	// Round-2 session 38 — F10 plugin-fallback (circuit-breaker, replay-filter,
	// recovery-event-timeout). Only non-zero values mutate the persistence-
	// package globals so reference.conf defaults apply when HOCON omits a key.
	persistence.SetJournalBreakerConfig(persistence.CircuitBreakerConfig{
		MaxFailures:  cfg.Persistence.JournalBreakerMaxFailures,
		CallTimeout:  cfg.Persistence.JournalBreakerCallTimeout,
		ResetTimeout: cfg.Persistence.JournalBreakerResetTimeout,
	})
	persistence.SetSnapshotBreakerConfig(persistence.CircuitBreakerConfig{
		MaxFailures:  cfg.Persistence.SnapshotBreakerMaxFailures,
		CallTimeout:  cfg.Persistence.SnapshotBreakerCallTimeout,
		ResetTimeout: cfg.Persistence.SnapshotBreakerResetTimeout,
	})
	persistence.SetReplayFilterConfig(persistence.ReplayFilterConfig{
		Mode:          persistence.ReplayFilterMode(cfg.Persistence.ReplayFilterMode),
		WindowSize:    cfg.Persistence.ReplayFilterWindowSize,
		MaxOldWriters: cfg.Persistence.ReplayFilterMaxOldWriters,
		Debug:         cfg.Persistence.ReplayFilterDebug,
	})
	if cfg.Persistence.RecoveryEventTimeout > 0 {
		persistence.SetRecoveryEventTimeout(cfg.Persistence.RecoveryEventTimeout)
	}

	// Round-2 session 39 — F11 AtLeastOnceDelivery defaults. Zero values
	// keep the persistence-package defaults so reference.conf wins when
	// HOCON omits a key.
	persistence.SetDefaultAtLeastOnceConfig(persistence.AtLeastOnceConfig{
		RedeliverInterval:                       cfg.Persistence.AtLeastOnceRedeliverInterval,
		RedeliveryBurstLimit:                    cfg.Persistence.AtLeastOnceRedeliveryBurstLimit,
		WarnAfterNumberOfUnconfirmedAttempts:    cfg.Persistence.AtLeastOnceWarnAfterNumberOfUnconfirmedAttempts,
		MaxUnconfirmedMessages:                  cfg.Persistence.AtLeastOnceMaxUnconfirmedMessages,
	})

	if len(cfg.Persistence.AutoStartJournals) > 0 {
		var hcfg hocon.Config
		if cfg.HOCON != nil {
			hcfg = *cfg.HOCON
		}
		if _, autoErr := persistence.AutoStartJournals(cfg.Persistence.AutoStartJournals, hcfg); autoErr != nil {
			cancel()
			return nil, fmt.Errorf("gekka: auto-start journals: %w", autoErr)
		}
	}
	if len(cfg.Persistence.AutoStartSnapshotStores) > 0 {
		var hcfg hocon.Config
		if cfg.HOCON != nil {
			hcfg = *cfg.HOCON
		}
		if _, autoErr := persistence.AutoStartSnapshotStores(cfg.Persistence.AutoStartSnapshotStores, hcfg); autoErr != nil {
			cancel()
			return nil, fmt.Errorf("gekka: auto-start snapshot stores: %w", autoErr)
		}
	}

	// Determine the bind address: use BindHostname/BindPort when configured
	// (NAT/Docker), otherwise fall back to the canonical host/port.
	bindHost := host
	if cfg.BindHostname != "" {
		bindHost = cfg.BindHostname
	}
	bindPort := port
	if cfg.BindPort != 0 {
		bindPort = cfg.BindPort
	}

	server, err := core.NewTcpServer(core.TcpServerConfig{
		Addr: fmt.Sprintf("%s:%d", bindHost, bindPort),
		Handler: func(c context.Context, conn net.Conn) error {
			return nm.ProcessConnection(c, conn, core.INBOUND, nil, 0)
		},
		TLSConfig:   tlsCfg,
		BindTimeout: cfg.BindTimeout,
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("gekka: create server: %w", err)
	}
	if err := server.Start(ctx); err != nil {
		cancel()
		return nil, fmt.Errorf("gekka: start server: %w", err)
	}

	// Resolve actual port when the OS assigned one (port == 0).
	actualPort := port
	if port == 0 {
		if tcpAddr, ok := server.Addr().(*net.TCPAddr); ok {
			actualPort = uint32(tcpAddr.Port)
			localAddr.Port = &actualPort
			// Patch the gossip state that was snapshotted in Newgcluster.ClusterManager.
			// This is needed because the port might have been 0 and assigned by the OS.
			nm.LocalAddr = localAddr
			clUA := core.ToClusterUniqueAddress(localUA)
			cm.LocalAddress = clUA
			cm.State.AllAddresses = []*gproto_cluster.UniqueAddress{clUA}
			cm.State.Members = []*gproto_cluster.Member{
				{
					AddressIndex: proto.Int32(0),
					Status:       gproto_cluster.MemberStatus_Joining.Enum(),
					UpNumber:     proto.Int32(0),
				},
			}
		}
	}

	// Aeron-UDP transport: start native UDP handler and override DialRemote.
	var udpH *core.UdpArteryHandler
	if strings.EqualFold(cfg.Transport, "aeron-udp") {
		udpBindHost := host
		if cfg.BindHostname != "" {
			udpBindHost = cfg.BindHostname
		}
		udpBindPort := actualPort
		if cfg.BindPort != 0 {
			udpBindPort = cfg.BindPort
		}
		udpAddr := fmt.Sprintf("%s:%d", udpBindHost, udpBindPort)
		udpH, err = core.NewUdpArteryHandler(ctx, udpAddr, nm, nil)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("gekka: aeron-udp handler: %w", err)
		}
		nm.UDPHandler = udpH
		log.Printf("gekka: aeron-udp transport active on %s", udpAddr)
	}

	nodeID := fmt.Sprintf("%s:%d", host, actualPort)
	repl := ddata.NewReplicator(nodeID, router)
	mg := gcluster.NewMetricsGossip(nodeID, repl, 5*time.Second)

	cluster := &Cluster{
		cfg:            cfg,
		nm:             nm,
		cm:             cm,
		router:         router,
		repl:           repl,
		mg:             mg,
		server:         server,
		udpHandler:     udpH,
		ctx:            ctx,
		cancel:         cancel,
		localAddr:      localAddr,
		seeds:          cfg.SeedNodes,
		metrics:        metrics,
		actors:         make(map[string]actor.Actor),
		remoteWatchers: make(map[string]map[string][]ActorRef),
		localWatchers:  make(map[string]map[string][]*gproto_remote.ProtoActorRef),
		logHandler:     cfg.LogHandler,
		deployments:    cfg.Deployments,
		sched:          newSystemScheduler(),
		eventStream:    actor.NewEventStream(),
	}
	nm.SystemMessageCallback = cluster.HandleSystemMessage

	// Terminate the scheduler when the cluster context is cancelled.
	go func() {
		<-ctx.Done()
		cluster.sched.terminate()
	}()
	cluster.System = cluster
	cluster.cm.Sys = AsActorContext(cluster, "")
	actor.SetMailboxLengthProvider(cluster)
	actor.SetClusterMetricsProvider(cluster)

	// Wire dead letter logger from config.
	actor.NewDeadLetterLogger(cluster.eventStream, cfg.LogDeadLetters,
		cfg.LogDeadLettersDuringShutdown, &cluster.shuttingDown)

	// Set default Artery message dispatcher to handle registered actors.
	nm.UserMessageCallback = cluster.handleArteryMessage

	// Internal watcher to track remote node failures and synthesize Terminated messages
	remoteDeathWatcherProps := Props{
		New: func() actor.Actor {
			return &remoteDeathWatcherActor{
				BaseActor: actor.NewBaseActor(),
				cluster:   cluster,
			}
		},
	}
	rdwRef, err := cluster.System.ActorOf(remoteDeathWatcherProps, "remoteDeathWatcher")
	if err == nil {
		cluster.Subscribe(rdwRef, gcluster.EventUnreachableMember, gcluster.EventMemberRemoved)
	}

	// Start the optional monitoring HTTP server.
	monPort := cfg.MonitoringPort
	if cfg.EnableMonitoring || monPort > 0 {
		cluster.cm.Metrics = cluster.metrics
	}
	if (cfg.EnableMonitoring || monPort > 0) && monPort >= 0 {
		ms, err := core.NewMonitoringServer(cluster, monPort)
		if err != nil {
			cancel()
			_ = server.Shutdown()
			return nil, fmt.Errorf("gekka: monitoring server: %w", err)
		}
		ms.Start(ctx)
		cluster.monitoring = ms
	}

	// Start the optional Cluster HTTP Management API server.
	if cfg.Management.Enabled {
		mgmtSrv, err := management.NewManagementServer(cluster, cfg.Management.Hostname, cfg.Management.Port, cfg.Management.HealthChecksEnabled)
		if err != nil {
			cancel()
			_ = server.Shutdown()
			return nil, fmt.Errorf("gekka: management server: %w", err)
		}
		if cfg.Management.DebugEnabled {
			mgmtSrv.EnableDebug(cluster)
		}
		mgmtSrv.Start(ctx)
		cluster.mgmt = mgmtSrv
	}

	// Internal watcher to clean up dead cluster event subscribers
	watcherProps := Props{
		New: func() actor.Actor {
			return &clusterWatcherActor{
				BaseActor: actor.NewBaseActor(),
				cluster:   cluster,
			}
		},
	}
	watcherRef, err := cluster.System.ActorOf(watcherProps, "internalClusterWatcher")
	if err == nil {
		cluster.clusterWatcherRef = watcherRef
	}

	// Start the gossip loop immediately. It will be a no-op until
	// members join, but it ensures leader actions (like transitioning
	// Joining -> Up) happen as soon as possible.
	go cluster.cm.StartGossipLoop(cluster.ctx)
	cluster.cm.StartReaper(cluster.ctx)
	cluster.cm.StartPublishStatsLoop(cluster.ctx)
	cluster.mg.Start(cluster.ctx)

	// ── Distributed Data ─────────────────────────────────────────────────────
	if cfg.DistributedData.Enabled {
		if cfg.DistributedData.GossipInterval > 0 {
			repl.GossipInterval = cfg.DistributedData.GossipInterval
		}
		if cfg.DistributedData.Name != "" {
			repl.Name = cfg.DistributedData.Name
		}
		repl.Role = cfg.DistributedData.Role
		repl.NotifySubscribersInterval = cfg.DistributedData.NotifySubscribersInterval
		if cfg.DistributedData.MaxDeltaElements > 0 {
			repl.MaxDeltaElements = cfg.DistributedData.MaxDeltaElements
		}
		repl.DeltaCRDTEnabled = cfg.DistributedData.DeltaCRDTEnabled
		if cfg.DistributedData.DeltaCRDTMaxDeltaSize > 0 {
			repl.DeltaCRDTMaxDeltaSize = cfg.DistributedData.DeltaCRDTMaxDeltaSize
		}
		repl.PreferOldest = cfg.DistributedData.PreferOldest
		repl.PruningInterval = cfg.DistributedData.PruningInterval
		if cfg.DistributedData.LogDataSizeExceeding > 0 {
			repl.LogDataSizeExceeding = cfg.DistributedData.LogDataSizeExceeding
		}
		if cfg.DistributedData.RecoveryTimeout > 0 {
			repl.RecoveryTimeout = cfg.DistributedData.RecoveryTimeout
		}
		if cfg.DistributedData.SerializerCacheTimeToLive > 0 {
			repl.SerializerCacheTimeToLive = cfg.DistributedData.SerializerCacheTimeToLive
		}

		// ── Durable backend (round-2 session 23) ──
		// When the operator either sets a non-empty `durable.keys` list or
		// flips `durable.enabled = on` in HOCON, open a bbolt-backed
		// DurableStore at `durable.lmdb.dir` and attach it to the replicator.
		// Failure to open the store is logged but not fatal — gossip can
		// still proceed in volatile mode.
		if cfg.DistributedData.DurableEnabled {
			repl.DurableEnabled = true
			repl.DurableKeys = append(repl.DurableKeys[:0], cfg.DistributedData.DurableKeys...)
			store, derr := ddata.OpenBoltDurableStore(ddata.BoltDurableStoreOptions{
				Dir:                 cfg.DistributedData.DurableLmdbDir,
				MapSize:             cfg.DistributedData.DurableLmdbMapSize,
				WriteBehindInterval: cfg.DistributedData.DurableLmdbWriteBehindInterval,
			})
			if derr != nil {
				slog.Warn("gekka: durable DData backend disabled", "err", derr,
					"dir", cfg.DistributedData.DurableLmdbDir)
				repl.DurableEnabled = false
			} else {
				repl.DurableStore = store
				cluster.durable = store
			}
		}

		// Wire pruning manager. Use the DData-configured role (falls back to
		// cluster role if DData-role is empty) to decide oldness.
		pruningRole := cfg.DistributedData.Role
		selfID := repl.NodeID()
		pruningMgr := ddata.NewPruningManager(
			cfg.DistributedData.PruningInterval,
			cfg.DistributedData.MaxPruningDissemination,
			func() bool {
				ua := cm.OldestNode(pruningRole)
				if ua == nil {
					return false
				}
				a := ua.GetAddress()
				return fmt.Sprintf("%s:%d", a.GetHostname(), a.GetPort()) == selfID
			},
			func() string {
				ua := cm.OldestNode(pruningRole)
				if ua == nil {
					return ""
				}
				a := ua.GetAddress()
				return fmt.Sprintf("%s:%d", a.GetHostname(), a.GetPort())
			},
		)
		// When durable storage is active, marker TTL must outlive the
		// durable horizon — durable replicas may rejoin after the
		// non-durable TTL has elapsed and would otherwise resurrect
		// stale state.  Pick the larger of the two configured TTLs.
		markerTTL := cfg.DistributedData.PruningMarkerTimeToLive
		if cfg.DistributedData.DurableEnabled &&
			cfg.DistributedData.DurablePruningMarkerTimeToLive > markerTTL {
			markerTTL = cfg.DistributedData.DurablePruningMarkerTimeToLive
		}
		pruningMgr.SetPruningMarkerTimeToLive(markerTTL)
		repl.SetPruningManager(pruningMgr)

		repl.Start(cluster.ctx)

		// Spawn typed replicator and register with receptionist
		typedRepl := ddata_typed.Replicator{}
		ref, err := Spawn(cluster, typedRepl.Behavior(repl), repl.Name)
		if err == nil {
			cluster.Receptionist().Tell(Register[any]{Key: ddata_typed.ReplicatorServiceKey, Service: ref})
		} else {
			log.Printf("gekka: failed to spawn typed ddata replicator: %v", err)
		}

		// Automatically manage replicator peers and feed member-removal events
		// into the pruning manager.
		go func() {
			sub := cm.SubscribeChannel()
			defer sub.Cancel()
			for {
				select {
				case <-cluster.ctx.Done():
					return
				case evt, ok := <-sub.C:
					if !ok {
						return
					}
					switch e := evt.(type) {
					case gcluster.MemberUp:
						la := cluster.localAddr
						laStr := fmt.Sprintf("%s://%s@%s:%d", la.GetProtocol(), la.GetSystem(), la.GetHostname(), la.GetPort())
						if e.Member.String() == laStr {
							continue
						}
						addr, perr := actor.ParseAddress(e.Member.String())
						if perr == nil {
							peerPath := addr.WithRoot("user").Child(repl.Name)
							fmt.Printf("Cluster: adding replicator peer: %s\n", peerPath.String())
							repl.AddPeer(peerPath.String())
						}
					case gcluster.MemberRemoved:
						addr, perr := actor.ParseAddress(e.Member.String())
						if perr == nil {
							nodeID := fmt.Sprintf("%s:%d", addr.Host, addr.Port)
							repl.NotifyNodeRemoved(nodeID)
						}
					}
				}
			}
		}()
	}

	// ── Receptionist ────────────────────────────────────────────────────────
	// Spawn the local receptionist actor. It uses the CRDT replicator to
	// propagate service registrations cluster-wide.
	if _, err := spawnReceptionist(cluster, repl, cfg.TypedReceptionist); err != nil {
		log.Printf("gekka: failed to spawn receptionist: %v", err)
	}

	// ── Coordinated Shutdown ────────────────────────────────────────────────
	// Wire the built-in graceful exit sequence so that Shutdown() drives the
	// cluster through Leave → Exiting → Removed before closing TCP connections.
	cluster.cs = actor.NewCoordinatedShutdown()
	cluster.registerBuiltinShutdownTasks()

	// Wire the join-timeout shutdown callback so that
	// shutdown-after-unsuccessful-join-seed-nodes triggers CoordinatedShutdown,
	// matching Pekko's CoordinatedShutdown(system).run(clusterDowningReason).
	cm.ShutdownCallback = func() {
		go cluster.GracefulShutdown(context.Background())
	}

	// ── Split Brain Resolver ─────────────────────────────────────────────────
	// Round-2 session 28 — F4 Downing consolidation: lease-majority defaults
	// + SBRManager construction + DowningProvider wrapping all live behind
	// gcluster.BuildSBRDowningProvider, so this call site no longer holds a
	// direct reference to SBRManager.  ResolveSBRConfigDefaults inside the
	// factory turns into a no-op for non-lease strategies, preserving the
	// pre-S28 behaviour for keep-majority/keep-oldest/static-quorum/etc.
	cluster.downingProviders = gcluster.NewDowningProviderRegistry()
	sbrProvider := gcluster.BuildSBRDowningProvider(cm, cfg.SBR, gcluster.SBRDefaults{
		LeaseManager:      cluster.LeaseManager(),
		LeaseProviderName: lease.MemoryProviderName,
		SystemName:        cfg.SystemName,
		Host:              cfg.Host,
		Port:              cfg.Port,
		LeaseDuration:     cfg.CoordinationLease.HeartbeatTimeout,
		RetryInterval:     cfg.CoordinationLease.HeartbeatInterval,
	})
	cluster.downingProviders.Register(sbrProvider)

	providerName := strings.TrimSpace(cfg.DowningProviderClass)
	if providerName == "" {
		providerName = gcluster.DefaultDowningProviderName
	}
	provider, ok := cluster.downingProviders.Resolve(providerName)
	if !ok {
		log.Printf("downing: %s — falling back to %q",
			gcluster.FormatUnknownProviderError(providerName, cluster.downingProviders.Names()),
			gcluster.DefaultDowningProviderName)
		provider, _ = cluster.downingProviders.Resolve(gcluster.DefaultDowningProviderName)
	}
	cluster.downingProvider = provider
	if provider != nil {
		if err := provider.Start(ctx); err != nil {
			log.Printf("downing: provider %q Start failed: %v", provider.Name(), err)
		}
	}

	// ── Coordinated Shutdown on Self-Down ────────────────────────────────────
	// When run-coordinated-shutdown-when-down is enabled (default), trigger the
	// full coordinated-shutdown sequence when this node is downed by the cluster.
	runCSWhenDown := cfg.RunCoordinatedShutdownWhenDown == nil || *cfg.RunCoordinatedShutdownWhenDown
	if runCSWhenDown {
		go func() {
			sub := cm.SubscribeChannel(gcluster.EventMemberDowned)
			defer sub.Cancel()
			for {
				select {
				case <-ctx.Done():
					return
				case evt, ok := <-sub.C:
					if !ok {
						return
					}
					if downed, ok := evt.(gcluster.MemberDowned); ok {
						// Check if self was downed.
						selfAddr := cm.LocalAddress.GetAddress()
						if downed.Member.Host == selfAddr.GetHostname() &&
							downed.Member.Port == selfAddr.GetPort() {
							log.Printf("[CoordinatedShutdown] self downed — triggering shutdown")
							go cluster.GracefulShutdown(context.Background())
							return
						}
					}
				}
			}
		}()
	}

	// ── Quarantine Removed Node After ────────────────────────────────────────
	// When a member is removed, schedule quarantine of its UID after the
	// configured delay to prevent it from reconnecting.
	quarantineDelay := cfg.QuarantineRemovedNodeAfter
	if quarantineDelay == 0 {
		quarantineDelay = 5 * time.Second // Pekko default
	}
	go func() {
		sub := cm.SubscribeChannel(gcluster.EventMemberRemoved)
		defer sub.Cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case evt, ok := <-sub.C:
				if !ok {
					return
				}
				if removed, ok := evt.(gcluster.MemberRemoved); ok {
					// Don't quarantine self.
					selfAddr := cm.LocalAddress.GetAddress()
					if removed.Member.Host == selfAddr.GetHostname() &&
						removed.Member.Port == selfAddr.GetPort() {
						continue
					}
					// Schedule quarantine after delay.
					go func(addr gcluster.MemberAddress) {
						select {
						case <-ctx.Done():
							return
						case <-time.After(quarantineDelay):
							// Find the association UID for this address and quarantine it.
							if assoc, ok := nm.GetGekkaAssociationByHost(addr.Host, addr.Port); ok && assoc != nil {
								remote := assoc.Remote()
								if remote != nil && remote.GetUid() != 0 {
									nm.RegisterQuarantinedUID(remote)
								}
							}
						}
					}(removed.Member)
				}
			}
		}
	}()

	// ── Telemetry: cluster member count ──────────────────────────────────────
	// Subscribe to member events and update the OTEL UpDownCounter so
	// dashboards can track gekka_cluster_members_count{status,dc}.
	go func() {
		sub := cm.SubscribeChannel()
		defer sub.Cancel()

		meter := telemetry.GetMeter("github.com/sopranoworks/gekka/cluster")
		membersCount := meter.UpDownCounter(
			"gekka_cluster_members_count",
			"Current number of cluster members grouped by status and data center.",
			"{members}",
		)

		for {
			select {
			case <-ctx.Done():
				return
			case evt, ok := <-sub.C:
				if !ok {
					return
				}
				switch e := evt.(type) {
				case gcluster.MemberUp:
					membersCount.Add(ctx, 1,
						telemetry.StringAttr("status", "up"),
						telemetry.StringAttr("dc", e.Member.DataCenter))
				case gcluster.MemberRemoved:
					membersCount.Add(ctx, -1,
						telemetry.StringAttr("status", "up"),
						telemetry.StringAttr("dc", e.Member.DataCenter))
				case gcluster.MemberLeft:
					membersCount.Add(ctx, -1,
						telemetry.StringAttr("status", "up"),
						telemetry.StringAttr("dc", e.Member.DataCenter))
					membersCount.Add(ctx, 1,
						telemetry.StringAttr("status", "leaving"),
						telemetry.StringAttr("dc", e.Member.DataCenter))
				case gcluster.MemberExited:
					membersCount.Add(ctx, -1,
						telemetry.StringAttr("status", "leaving"),
						telemetry.StringAttr("dc", e.Member.DataCenter))
				}
			}
		}
	}()

	// Register built-in serializers
	nm.SerializerRegistry.RegisterSerializer(core.MiscMessageSerializerID, &core.MiscMessageSerializer{})
	nm.SerializerRegistry.RegisterSerializer(ddata.DDataReplicatorMsgSerializerID, &ddata.DDataSerializer{})
	nm.SerializerRegistry.RegisterSerializer(stream.StreamRefSerializerID, &stream.StreamRefSerializer{})
	nm.SerializerRegistry.RegisterManifest("A", reflect.TypeFor[*stream.SequencedOnNext](), stream.StreamRefSerializerID)
	nm.SerializerRegistry.RegisterManifest("B", reflect.TypeFor[*stream.CumulativeDemand](), stream.StreamRefSerializerID)
	nm.SerializerRegistry.RegisterManifest("C", reflect.TypeFor[*stream.RemoteStreamFailure](), stream.StreamRefSerializerID)
	nm.SerializerRegistry.RegisterManifest("D", reflect.TypeFor[*stream.RemoteStreamCompleted](), stream.StreamRefSerializerID)
	nm.SerializerRegistry.RegisterManifest("E", reflect.TypeFor[*stream.SourceRef](), stream.StreamRefSerializerID)
	nm.SerializerRegistry.RegisterManifest("F", reflect.TypeFor[*stream.SinkRef](), stream.StreamRefSerializerID)
	nm.SerializerRegistry.RegisterManifest("G", reflect.TypeFor[*stream.OnSubscribeHandshake](), stream.StreamRefSerializerID)

	return cluster, nil
}

type clusterWatcherActor struct {
	actor.BaseActor
	cluster *Cluster
}

func (a *clusterWatcherActor) Receive(msg any) {
	if term, ok := msg.(Terminated); ok {
		a.cluster.Unsubscribe(term.Actor)
	}
}

// Addr returns the bound TCP address after Spawn. Use this to discover the
// OS-assigned port when ClusterConfig.Port was 0.
func (c *Cluster) Addr() net.Addr {
	return c.server.Addr()
}

// MuteNode silently drops all outbound frames to and all inbound frames from
// the cluster node at host:port. Use this in tests to simulate a network
// partition without stopping the process.
func (c *Cluster) MuteNode(host string, port int) {
	c.nm.MuteNode(host, uint32(port))
}

// UnmuteNode reverses a previous MuteNode call. Safe to call even if the node
// was never muted.
func (c *Cluster) UnmuteNode(host string, port int) {
	c.nm.UnmuteNode(host, uint32(port))
}

// SubscribeChannel creates a buffered channel subscription for cluster domain
// events. The caller must call Cancel on the returned subscription to avoid
// resource leaks. When types is non-empty only those event types are delivered;
// omit types to receive all ClusterDomainEvents.
func (c *Cluster) SubscribeChannel(types ...reflect.Type) *gcluster.ChanSubscription {
	return c.cm.SubscribeChannel(types...)
}

// IsUp returns true if the cluster state contains at least one Up member.
func (c *Cluster) IsUp() bool {
	return c.cm.IsUp()
}

// IsLocalNodeUp returns true if this specific node has reached Up status
// in the gossip state.
func (c *Cluster) IsLocalNodeUp() bool {
	return c.cm.IsLocalNodeUp()
}

// Port returns the TCP port this node is bound to. When ClusterConfig.Port was 0
// this is the OS-assigned port resolved after Spawn.
func (c *Cluster) Port() uint32 {
	return c.localAddr.GetPort()
}

// Context returns the root context of this node. It is cancelled when
// Shutdown is called.
//
// This is the same context used internally by the node for all background
// goroutines. You can use it as a long-lived parent context, or pass it
// (implicitly via nil) to ActorSelection.Resolve and ActorSelection.Ask:
//
//	ref, err := node.ActorSelection("/user/myActor").Resolve(nil)
//	// equivalent to:
//	ref, err := node.ActorSelection("/user/myActor").Resolve(node.Context())
func (c *Cluster) Context() context.Context {
	return c.ctx
}

// MetricsGossip returns the internal metrics gossip service.
func (c *Cluster) MetricsGossip() *gcluster.MetricsGossip {
	return c.mg
}

// OnMessage registers a callback that is invoked for every user-level Artery
// message received by this node that is NOT handled by a registered Actor.
// Cluster-internal messages (heartbeats, gossip, etc.) are handled automatically
// and do not trigger this callback.
//
// Registered actors (see RegisterActor) take priority: messages whose
// recipient path matches a registered actor are dispatched to that actor's
// mailbox and do not reach this callback.
func (c *Cluster) OnMessage(fn func(ctx context.Context, msg *IncomingMessage) error) {
	c.onMessage = fn
}

func (c *Cluster) handleArteryMessage(ctx context.Context, meta *core.ArteryMetadata) error {
	var recipientPath string
	if meta.Recipient != nil {
		recipientPath = meta.Recipient.GetPath()
	}

	// If the recipient is a full URI, extract the path segment for local actor lookup.
	if strings.HasPrefix(recipientPath, "pekko://") || strings.HasPrefix(recipientPath, "akka://") {
		if ap, err := actor.ParseActorPath(recipientPath); err == nil {
			recipientPath = ap.Path()
		}
	}

	// Build a sender ActorRef from the Artery envelope's sender field.
	// This allows the receiving actor to reply via a.Sender().Tell(…, a.Self()).
	var senderRef ActorRef
	if meta.Sender != nil && meta.Sender.GetPath() != "" {
		senderRef = ActorRef{fullPath: meta.Sender.GetPath(), sys: c}
	}

	incoming := &IncomingMessage{
		RecipientPath:       recipientPath,
		Payload:             meta.Payload,
		SerializerId:        meta.SerializerId,
		Manifest:            string(meta.MessageManifest),
		Sender:              senderRef,
		DeserializedMessage: meta.DeserializedMessage,
	}

	// Try registered actors first; deliver via actor.Envelope so that
	// BaseActor.currentSender is set before Receive is called.
	c.actorsMu.RLock()
	a, found := c.actors[recipientPath]
	c.actorsMu.RUnlock()
	if found {
		payload := any(incoming)
		if incoming.DeserializedMessage != nil {
			payload = incoming.DeserializedMessage
		}
		env := actor.Envelope{Payload: payload, Sender: senderRef}
		var sendCause string
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					sendCause = "mailbox-closed"
				}
			}()
			select {
			case a.Mailbox() <- env:
			default:
				sendCause = "mailbox-full"
			}
		}()
		if sendCause != "" {
			recipientRef := ActorRef{fullPath: c.SelfPathURI(recipientPath), sys: c, local: a}
			c.eventStream.Publish(actor.DeadLetter{
				Message:   payload,
				Sender:    senderRef,
				Recipient: recipientRef,
				Cause:     sendCause,
			})
		}
		return nil
	}

	// Try pending replies (Ask pattern)
	if strings.HasPrefix(recipientPath, "/temp/") {
		if c.nm.RoutePendingReply(recipientPath, meta) {
			return nil
		}
	}

	if c.onMessage == nil {
		return nil
	}
	return c.onMessage(ctx, incoming)
}

// lookupDeployment returns the core.DeploymentConfig for the given actor path, if any.
// It tries both the exact path and the alternate short/full form so that
// "/user/myRouter" and "/myRouter" both resolve to the same deployment entry.
func (c *Cluster) lookupDeployment(path string) (core.DeploymentConfig, bool) {
	if len(c.deployments) == 0 {
		return core.DeploymentConfig{}, false
	}
	for _, candidate := range core.DeploymentKeyCandidates(path) {
		if d, ok := c.deployments[candidate]; ok {
			return d, true
		}
	}
	return core.DeploymentConfig{}, false
}

// RegisterActor wires an Actor to a local actor path so that incoming Artery
// messages addressed to that path are pushed into the actor's mailbox.
//
// path must be the full actor path suffix as it appears in Artery envelopes,
// for example "/user/myActor". The node's own address is prepended automatically
// by Pekko on the sender side, so you only need to supply the path segment:
//
//	a := &MyActor{BaseActor: actor.NewBaseActor()}
//	actor.Start(a)
//	node.RegisterActor("/user/myActor", a)
//
// Calling RegisterActor replaces any previously registered actor for the same path.
func (c *Cluster) RegisterActor(path string, a actor.Actor) {
	c.actorsMu.Lock()
	if c.actors == nil {
		c.actors = make(map[string]actor.Actor)
	}
	c.actors[path] = a
	c.actorsMu.Unlock()
}

// UnregisterActor removes the actor registered at path, if any.
// After this call, messages addressed to path fall through to the OnMessage
// callback (if set).
func (c *Cluster) UnregisterActor(path string) {
	c.actorsMu.Lock()
	delete(c.actors, path)
	c.actorsMu.Unlock()
}

// SelfAddress returns the node's own address as a typed actor.Address.
// Use it to build local or remote actor paths:
//
//	self := node.SelfAddress()
//	path := self.WithRoot("user").Child("myActor")
//	remoteAddr := actor.Address{Protocol: self.Protocol, System: self.System, Host: "10.0.0.2", Port: 2552}
func (c *Cluster) SelfAddress() actor.Address {
	return actor.Address{
		Protocol: c.localAddr.GetProtocol(),
		System:   c.localAddr.GetSystem(),
		Host:     c.localAddr.GetHostname(),
		Port:     int(c.localAddr.GetPort()),
	}
}

// Send resolves the destination and delivers msg to the remote actor.
//
// The dst argument can be:
//   - actor.ActorPath  — typed path: addr.WithRoot("user").Child("myActor")
//   - actor.Address    — address root guardian
//   - string           — raw URI: "pekko://System@host:port/user/actor"
//   - fmt.Stringer     — any type whose String() returns a valid actor path URI
//
// Supported message types:
//   - []byte           → raw bytes (Pekko ByteArraycore.Serializer, ID 4)
//   - proto.Message    → Protobuf (ID 2)
//   - cluster messages → handled automatically by Router
func (c *Cluster) Send(ctx context.Context, dst any, msg any) error {
	var pathStr string
	switch d := dst.(type) {
	case string:
		pathStr = d
	case fmt.Stringer:
		pathStr = d.String()
	default:
		return fmt.Errorf("gekka: Send: unsupported destination type %T (want string, actor.ActorPath, or fmt.Stringer)", dst)
	}
	return c.router.Send(ctx, pathStr, msg)
}

// Ask sends msg to dst and blocks until the remote actor replies or the context
// is cancelled. It sets a unique temporary actor path as the Artery sender so
// the remote can route the reply back to this node.
//
// The dst and msg arguments follow the same rules as Send.
// The returned *IncomingMessage contains the raw reply payload, serializer ID,
// and manifest.
//
// A context deadline or timeout is strongly recommended — Ask will block
// forever if the remote actor never replies.
//
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	defer cancel()
//	reply, err := node.Ask(ctx, echoPath, []byte("ping"))
func (c *Cluster) Ask(ctx context.Context, dst any, msg any) (*IncomingMessage, error) {
	// Resolve destination path string.
	var pathStr string
	switch d := dst.(type) {
	case string:
		pathStr = d
	case actor.ActorPath:
		pathStr = d.String()
	case fmt.Stringer:
		pathStr = d.String()
	default:
		return nil, fmt.Errorf("gekka: Ask: unsupported destination type %T (want string, actor.ActorPath, or fmt.Stringer)", dst)
	}

	// Resolve target address to determine if local or remote
	ap, err := actor.ParseActorPath(pathStr)
	if err != nil {
		return nil, fmt.Errorf("gekka: Ask: parse: %w", err)
	}

	localAddress := c.localAddr
	isLocal := ap.Address.System == localAddress.GetSystem() && ap.Address.Host == localAddress.GetHostname() && uint32(ap.Address.Port) == localAddress.GetPort()

	if isLocal {
		localPath := ap.Path()
		c.actorsMu.RLock()
		actor, found := c.actors[localPath]
		c.actorsMu.RUnlock()
		if found {
			// Ask local actor
			return askLocal(ctx, actor, msg)
		}
	}

	// Build a unique temporary sender path.
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return nil, fmt.Errorf("gekka: Ask: generate id: %w", err)
	}
	id := hex.EncodeToString(buf[:])
	tempPath := c.SelfPathURI("/temp/ask-" + id)

	// Register a reply channel keyed by the path portion only (e.g. "/temp/ask-<id>").
	// handleUserMessage extracts just the path from the incoming Artery recipient URI.
	pathKey := "/temp/ask-" + id
	replyCh := make(chan *core.ArteryMetadata, 1)
	c.nm.RegisterPendingReply(pathKey, replyCh)
	defer c.nm.UnregisterPendingReply(pathKey)

	if err := c.router.SendWithSender(ctx, pathStr, tempPath, msg); err != nil {
		return nil, fmt.Errorf("gekka: Ask: send: %w", err)
	}

	select {
	case meta := <-replyCh:
		return &IncomingMessage{
			RecipientPath:       tempPath,
			Payload:             meta.Payload,
			SerializerId:        meta.SerializerId,
			Manifest:            string(meta.MessageManifest),
			DeserializedMessage: meta.DeserializedMessage,
		}, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("gekka: Ask: %w", ctx.Err())
	}
}

func askLocal(ctx context.Context, a actor.Actor, msg any) (*IncomingMessage, error) {
	replyCh := make(chan actor.Envelope, 1)
	sender := &localReplyActor{replyCh: replyCh}

	env := actor.Envelope{Payload: msg, Sender: sender}
	select {
	case a.Mailbox() <- env:
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, fmt.Errorf("local actor mailbox full")
	}

	select {
	case replyEnv := <-replyCh:
		return &IncomingMessage{
			DeserializedMessage: replyEnv.Payload,
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type localReplyActor struct {
	replyCh chan actor.Envelope
}

func (l *localReplyActor) Path() string { return "/temp/local-ask" }
func (l *localReplyActor) Tell(msg any, sender ...actor.Ref) {
	l.replyCh <- actor.Envelope{Payload: msg}
}

// Join sends an InitJoin to the seed node, starts heartbeats, and starts the
// background gossip loop. It is non-blocking: cluster convergence happens
// asynchronously. Use WaitForHandshake to confirm the TCP association is up.
func (c *Cluster) ClusterManager() *gcluster.ClusterManager {
	return c.cm
}

func (c *Cluster) NodeManager() *core.NodeManager {
	return c.nm
}

func (c *Cluster) Metrics() *core.NodeMetrics {
	return c.metrics
}

// ActorPaths returns a snapshot of every actor path currently registered on
// this node.  Used by the debug introspection endpoints.  Order is not
// defined — callers should sort if they need it.
func (c *Cluster) ActorPaths() []string {
	c.actorsMu.RLock()
	defer c.actorsMu.RUnlock()
	out := make([]string, 0, len(c.actors))
	for path := range c.actors {
		out = append(out, path)
	}
	return out
}

// CRDTList implements management.DebugProvider.  Returns every CRDT known to
// this node's Replicator as a slice of management.CRDTEntry.
func (c *Cluster) CRDTList() []management.CRDTEntry {
	if c.repl == nil {
		return nil
	}
	entries := c.repl.Entries()
	out := make([]management.CRDTEntry, len(entries))
	for i, e := range entries {
		out[i] = management.CRDTEntry{Key: e.Key, Type: e.Type}
	}
	return out
}

// CRDT implements management.DebugProvider.  Looks up key across all CRDT
// types in the Replicator and returns the current snapshot shaped for JSON.
// Returns (nil, nil) when the key is not registered on this node.
func (c *Cluster) CRDT(key string) (*management.CRDTValue, error) {
	if c.repl == nil {
		return nil, nil
	}

	if g, ok := c.repl.LookupGCounter(key); ok {
		snap := g.Snapshot()
		var total uint64
		for _, v := range snap {
			total += v
		}
		return &management.CRDTValue{
			Key:  key,
			Type: "gcounter",
			Value: map[string]any{
				"total":    total,
				"per_node": snap,
			},
		}, nil
	}

	if s, ok := c.repl.LookupORSet(key); ok {
		elems := s.Elements()
		sort.Strings(elems)
		return &management.CRDTValue{
			Key:   key,
			Type:  "orset",
			Value: elems,
		}, nil
	}

	if m, ok := c.repl.LookupLWWMap(key); ok {
		return &management.CRDTValue{
			Key:   key,
			Type:  "lwwmap",
			Value: m.Entries(),
		}, nil
	}

	if p, ok := c.repl.LookupPNCounter(key); ok {
		snap := p.Snapshot()
		return &management.CRDTValue{
			Key:  key,
			Type: "pncounter",
			Value: map[string]any{
				"total":      p.Value(),
				"increments": snap.Pos,
				"decrements": snap.Neg,
			},
		}, nil
	}

	if f, ok := c.repl.LookupORFlag(key); ok {
		return &management.CRDTValue{
			Key:   key,
			Type:  "orflag",
			Value: f.Value(),
		}, nil
	}

	if reg, ok := c.repl.LookupLWWRegister(key); ok {
		snap := reg.Snapshot()
		return &management.CRDTValue{
			Key:  key,
			Type: "lwwregister",
			Value: map[string]any{
				"value":     snap.Value,
				"timestamp": snap.Timestamp,
				"node_id":   snap.NodeID,
			},
		}, nil
	}

	return nil, nil
}

// Actors implements management.DebugProvider.  Returns every actor registered
// on this node, sorted alphabetically.  When includeSystem is false, /system/*
// paths are filtered out.
func (c *Cluster) Actors(includeSystem bool) []management.ActorEntry {
	paths := c.ActorPaths()
	sort.Strings(paths)
	out := make([]management.ActorEntry, 0, len(paths))
	for _, p := range paths {
		kind := "user"
		if strings.HasPrefix(p, "/system/") {
			kind = "system"
		}
		if !includeSystem && kind == "system" {
			continue
		}
		out = append(out, management.ActorEntry{Path: p, Kind: kind})
	}
	return out
}

func (c *Cluster) Join(seedHost string, seedPort uint32) error {
	scheme := c.localAddr.GetProtocol() // "pekko" or "akka"
	seedSystem := c.localAddr.GetSystem()
	c.seedAddr = &gproto_remote.Address{
		Protocol: &scheme,
		System:   &seedSystem,
		Hostname: &seedHost,
		Port:     &seedPort,
	}
	if err := c.cm.JoinCluster(c.ctx, seedHost, seedPort); err != nil {
		return err
	}
	c.cm.StartHeartbeat(core.ToClusterAddress(c.seedAddr))
	return nil
}

// JoinSeeds connects to the seed nodes supplied via LoadConfig
// (ClusterConfig.SeedNodes). It skips self and joins the first remote seed;
// if all configured seeds are self (i.e. this node is the bootstrap seed),
// it joins its own address so Pekko's leader-election Handshake completes.
//
// Returns an error when no seeds are configured or Join fails.
func (c *Cluster) JoinSeeds() error {
	if len(c.seeds) == 0 {
		return fmt.Errorf("gekka: JoinSeeds: no seed nodes configured (use LoadConfig or set ClusterConfig.SeedNodes)")
	}
	self := c.SelfAddress()
	for _, s := range c.seeds {
		if s.Host != self.Host || s.Port != self.Port {
			return c.Join(s.Host, uint32(s.Port))
		}
	}
	// All seeds are self — this node is the sole seed; join self.
	s := c.seeds[0]
	return c.Join(s.Host, uint32(s.Port))
}

// Seeds returns the seed-node addresses parsed from HOCON configuration.
// Returns nil when the node was created without LoadConfig.
func (c *Cluster) Seeds() []actor.Address {
	return c.seeds
}

// Leave gracefully departs the cluster by broadcasting a Leave message to all
// known Up/WeaklyUp members. The Pekko SBR will remove this node shortly after.
func (c *Cluster) Leave() error {
	return c.cm.LeaveCluster()
}

// LeaveMember initiates a graceful leave for the cluster member identified by
// the Artery address string (e.g. "pekko://GekkaSystem@127.0.0.1:2552").
//
// If address refers to the local node, LeaveCluster is called directly.
// For remote nodes a Leave protocol message is sent to the member's cluster
// core daemon path so that the remote node initiates its own departure.
// Returns an error when address cannot be parsed, the member is not found, or
// message delivery fails.
func (c *Cluster) LeaveMember(address string) error {
	addr, err := actor.ParseAddress(address)
	if err != nil {
		return fmt.Errorf("management: parse address %q: %w", address, err)
	}

	localHost := c.cm.LocalAddress.GetAddress().GetHostname()
	localPort := c.cm.LocalAddress.GetAddress().GetPort()
	if addr.Host == localHost && uint32(addr.Port) == localPort {
		return c.cm.LeaveCluster()
	}

	// Remote member — verify it exists in gossip, then send Leave via Artery.
	c.cm.Mu.RLock()
	found := false
	for _, m := range c.cm.State.GetMembers() {
		a := c.cm.State.GetAllAddresses()[m.GetAddressIndex()].GetAddress()
		if a.GetHostname() == addr.Host && uint32(addr.Port) == a.GetPort() {
			found = true
			break
		}
	}
	c.cm.Mu.RUnlock()

	if !found {
		return fmt.Errorf("management: member %q not found in cluster", address)
	}

	// Build the Leave proto message (an Address) and send to the remote core daemon.
	// The router maps *gproto_cluster.Address → cluster serializer ID with manifest "L".
	remotePath := fmt.Sprintf("%s://%s@%s:%d/system/cluster/core/daemon",
		addr.Protocol, addr.System, addr.Host, addr.Port)
	leaveProto := addr.Protocol
	leaveSys := addr.System
	leaveHost := addr.Host
	leavePort := uint32(addr.Port)
	leaveMsg := &gproto_cluster.Address{
		Protocol: &leaveProto,
		System:   &leaveSys,
		Hostname: &leaveHost,
		Port:     &leavePort,
	}
	if err := c.cm.Router(context.Background(), remotePath, leaveMsg); err != nil {
		return fmt.Errorf("management: send Leave to %q: %w", remotePath, err)
	}
	return nil
}

// DownMember marks the cluster member identified by the Artery address string
// (e.g. "pekko://GekkaSystem@127.0.0.1:2552") as Down in the local gossip state.
//
// The cluster leader will subsequently transition the Down member to Removed.
// Returns an error when address cannot be parsed or the member is not found.
func (c *Cluster) DownMember(address string) error {
	addr, err := actor.ParseAddress(address)
	if err != nil {
		return fmt.Errorf("management: parse address %q: %w", address, err)
	}

	// Verify the member exists before delegating.
	c.cm.Mu.RLock()
	found := false
	for _, m := range c.cm.State.GetMembers() {
		a := c.cm.State.GetAllAddresses()[m.GetAddressIndex()].GetAddress()
		if a.GetHostname() == addr.Host && uint32(addr.Port) == a.GetPort() {
			found = true
			break
		}
	}
	c.cm.Mu.RUnlock()

	if !found {
		return fmt.Errorf("management: member %q not found in cluster", address)
	}

	c.cm.DownMember(gcluster.MemberAddress{
		Protocol: addr.Protocol,
		System:   addr.System,
		Host:     addr.Host,
		Port:     uint32(addr.Port),
	})
	return nil
}

// HasQuarantinedAssociation reports whether any Artery association is in
// QUARANTINED state.  Satisfies management.ClusterStateProvider; used by the
// /health/ready probe.
func (c *Cluster) HasQuarantinedAssociation() bool {
	return c.nm.HasQuarantinedAssociation()
}

// StopHeartbeat suspends heartbeats to the seed node, simulating a node
// failure from Pekko's perspective. Used in tests and for graceful pre-leave.
func (c *Cluster) StopHeartbeat() {
	if c.seedAddr != nil {
		c.cm.StopHeartbeat(core.ToClusterAddress(c.seedAddr))
	}
}

// StartHeartbeat resumes heartbeats to the seed node after StopHeartbeat.
// It explicitly clears the heartbeatMuted flag first so that
// ClusterManager.StartHeartbeat creates the task (it skips creation while
// muted to prevent connectToNewMembers from undoing a StopHeartbeat).
func (c *Cluster) StartHeartbeat() {
	if c.seedAddr != nil {
		c.cm.ClearHeartbeatMute()
		c.cm.StartHeartbeat(core.ToClusterAddress(c.seedAddr))
	}
}

// WaitForHandshake blocks until the Artery Handshake with the given host:port
// completes (i.e. the association reaches ASSOCIATED state). Returns an error
// if the context is cancelled or the 30-second built-in timeout expires.
func (c *Cluster) WaitForHandshake(ctx context.Context, host string, port uint32) error {
	for {
		if assoc, ok := c.nm.GetAssociationByHost(host, port); ok {
			gassoc := assoc.(*core.GekkaAssociation)
			select {
			case <-gassoc.Handshake:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(30 * time.Second):
				return fmt.Errorf("gekka: Handshake timeout for %s:%d", host, port)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// Replicator returns the node's CRDT Replicator. Register peers with
// AddPeer before calling Start, then use IncrementCounter / AddToSet / RemoveFromSet
// to mutate state that propagates to all peers via periodic gossip.
func (c *Cluster) Replicator() *ddata.Replicator {
	return c.repl
}

// SingletonProxy returns a ClusterSingletonProxy that routes messages to
// the singleton actor hosted on the oldest Up cluster node.
//
//	proxy := node.SingletonProxy("/user/singletonManager", "")
//	proxy.Send(ctx, []byte("ping"))
func (c *Cluster) SingletonProxy(managerPath, role string) gcluster.ClusterSingletonProxyInterface {
	if role == "" && c.cfg.SingletonProxy.Role != "" {
		role = c.cfg.SingletonProxy.Role
	}
	proxy := singleton.NewClusterSingletonProxy(c.cm, c.router, managerPath, role)
	if c.cfg.SingletonProxy.SingletonIdentificationInterval > 0 {
		proxy.WithIdentificationInterval(c.cfg.SingletonProxy.SingletonIdentificationInterval)
	}
	if c.cfg.SingletonProxy.BufferSize > 0 {
		proxy.WithBufferSize(c.cfg.SingletonProxy.BufferSize)
	}
	if c.cfg.SingletonProxy.SingletonName != "" {
		proxy.WithSingletonName(c.cfg.SingletonProxy.SingletonName)
	}
	proxy.Start()
	return proxy
}

// LeaseManager returns the cluster-wide LeaseManager, lazily initialised with
// the in-memory reference provider registered under "memory".  Callers may
// register additional providers (e.g. a Kubernetes-leases extension) to make
// them available to SBR lease-majority and Singleton/Sharding use-lease.
// DowningProviders returns the cluster's DowningProvider registry.
// Operators can register custom providers on it before the cluster
// boots; the bundled SBR provider is pre-registered under
// gcluster.DefaultDowningProviderName ("split-brain-resolver").
// Round-2 session 27.
func (c *Cluster) DowningProviders() *gcluster.DowningProviderRegistry {
	return c.downingProviders
}

// DowningProvider returns the resolved active provider for this
// cluster — the one whose Start was invoked during NewCluster.  Nil
// when no downing provider could be resolved (no SBR strategy
// configured and no operator override).  Round-2 session 27.
func (c *Cluster) DowningProvider() gcluster.DowningProvider {
	return c.downingProvider
}

func (c *Cluster) LeaseManager() *lease.LeaseManager {
	c.leaseManagerOnce.Do(func() {
		c.leaseMgr = lease.NewDefaultManager()
	})
	return c.leaseMgr
}

// resolveSingletonLease returns a Lease bound to settings derived from the
// cluster's CoordinationLease defaults, or nil when UseLease is empty.
func (c *Cluster) resolveSingletonLease(typeName string) lease.Lease {
	name := strings.TrimSpace(c.cfg.Singleton.UseLease)
	if name == "" {
		return nil
	}
	settings := lease.LeaseSettings{
		LeaseName:     fmt.Sprintf("%s-singleton-%s", c.cfg.SystemName, typeName),
		OwnerName:     fmt.Sprintf("%s:%d", c.cfg.Host, c.cfg.Port),
		LeaseDuration: c.cfg.CoordinationLease.HeartbeatTimeout,
		RetryInterval: c.cfg.CoordinationLease.HeartbeatInterval,
	}
	if settings.LeaseDuration == 0 {
		settings.LeaseDuration = 120 * time.Second
	}
	if settings.RetryInterval == 0 {
		settings.RetryInterval = 12 * time.Second
	}
	l, err := c.LeaseManager().GetLease(name, settings)
	if err != nil {
		log.Printf("gekka: singleton use-lease %q resolve failed: %v", name, err)
		return nil
	}
	return l
}

// resolveShardingLease returns a Lease bound to settings derived from the
// cluster's CoordinationLease defaults, scoped to the entity type, or nil
// when sharding.use-lease is empty.  Each Shard acquires this lease before
// becoming active so two replicas of the same shard never run concurrently
// across an SBR-induced split.
func (c *Cluster) resolveShardingLease(typeName string) lease.Lease {
	name := strings.TrimSpace(c.cfg.Sharding.UseLease)
	if name == "" {
		return nil
	}
	settings := lease.LeaseSettings{
		LeaseName:     fmt.Sprintf("%s-sharding-%s", c.cfg.SystemName, typeName),
		OwnerName:     fmt.Sprintf("%s:%d", c.cfg.Host, c.cfg.Port),
		LeaseDuration: c.cfg.CoordinationLease.HeartbeatTimeout,
		RetryInterval: c.cfg.CoordinationLease.HeartbeatInterval,
	}
	if settings.LeaseDuration == 0 {
		settings.LeaseDuration = 120 * time.Second
	}
	if settings.RetryInterval == 0 {
		settings.RetryInterval = 12 * time.Second
	}
	l, err := c.LeaseManager().GetLease(name, settings)
	if err != nil {
		log.Printf("gekka: sharding use-lease %q resolve failed: %v", name, err)
		return nil
	}
	return l
}

// SingletonManager creates a ClusterSingletonManager that hosts a singleton
// actor on the oldest Up cluster node. HOCON settings for singleton.role and
// singleton.hand-over-retry-interval are applied automatically.
//
//	mgr := node.SingletonManager(actor.Props{
//	    New: func() actor.Actor { return &MyActor{} },
//	}, "")
//	ref, _ := node.System.ActorOf(actor.Props{New: func() actor.Actor { return mgr }}, "singletonManager")
func (c *Cluster) SingletonManager(singletonProps actor.Props, role string) gcluster.ClusterSingletonManagerInterface {
	if role == "" && c.cfg.Singleton.Role != "" {
		role = c.cfg.Singleton.Role
	}
	mgr := singleton.NewClusterSingletonManager(c.cm, singletonProps, role)
	if c.cfg.Singleton.HandOverRetryInterval > 0 {
		mgr.WithHandOverRetryInterval(c.cfg.Singleton.HandOverRetryInterval)
	}
	if c.cfg.Singleton.SingletonName != "" {
		mgr.WithSingletonName(c.cfg.Singleton.SingletonName)
	}
	if c.cfg.Singleton.MinNumberOfHandOverRetries > 0 {
		mgr.WithMinHandOverRetries(c.cfg.Singleton.MinNumberOfHandOverRetries)
	}
	if l := c.resolveSingletonLease("singleton"); l != nil {
		mgr.WithLease(l)
		if c.cfg.Singleton.LeaseRetryInterval > 0 {
			mgr.WithLeaseRetryDelay(c.cfg.Singleton.LeaseRetryInterval)
		}
	}
	return mgr
}

// Subscribe registers an ActorRef to receive cluster domain events.
//
// Pass Event* variables to filter specific types; omit to receive all events:
//
//	node.Subscribe(myActorRef, gekka.gcluster.EventMemberUp, gekka.gcluster.EventMemberRemoved)
//
// When the subscriber actor is stopped, it is automatically unsubscribed to
// prevent memory leaks.
func (c *Cluster) Subscribe(ref ActorRef, types ...reflect.Type) {
	c.cm.Subscribe(ref, types...)
	if c.clusterWatcherRef.fullPath != "" {
		c.System.Watch(c.clusterWatcherRef, ref)
	}
}

// Unsubscribe removes the actor from the cluster event subscriber list.
// Safe to call even if the actor was never subscribed or has already been removed.
func (c *Cluster) Unsubscribe(ref ActorRef) {
	c.cm.Unsubscribe(ref)
	if c.clusterWatcherRef.fullPath != "" {
		c.System.Unwatch(c.clusterWatcherRef, ref)
	}
}

// Metrics returns a point-in-time snapshot of all internal counters.
// It is safe to call concurrently with any other Cluster method.
//
//	snap := cluster.Metrics()
//	log.Printf("sent=%d received=%d gossips=%d", snap.MessagesSent, snap.MessagesReceived, snap.GossipsReceived)
func (c *Cluster) MetricsSnapshot() core.MetricsSnapshot {
	return c.metrics.Snapshot(c.nm.CountAssociations())
}

// MonitoringAddr returns the TCP address of the built-in HTTP monitoring server,
// or nil if monitoring was not enabled in ClusterConfig.
func (c *Cluster) MonitoringAddr() net.Addr {
	if c.monitoring == nil {
		return nil
	}
	return c.monitoring.Addr()
}

// Serialization returns the core.SerializationRegistry for registering custom
// Protobuf or JSON message types and their manifests.
func (c *Cluster) Serialization() *core.SerializationRegistry {
	return c.nm.SerializerRegistry
}

// RegisterSerializer implements ActorSystem. It registers s under its
// Identifier() in this node's SerializationRegistry, replacing any existing
// entry for the same ID.
func (c *Cluster) RegisterSerializer(id int32, s core.Serializer) {
	c.nm.SerializerRegistry.RegisterSerializer(id, s)
}

// RegisterSerializer registers a custom core.Serializer with this node's
// core.SerializationRegistry, keyed by s.Identifier(). Overwrites any existing
// entry for the same ID.
//
//	cluster.RegisterSerializerByValue(&MyJSONSerializer{})
func (c *Cluster) RegisterSerializerByValue(s core.Serializer) {
	c.nm.SerializerRegistry.RegisterSerializer(s.Identifier(), s)
}

// RegisterSerializationBinding implements ActorSystem. It binds manifest to
// serializerID in this node's SerializationRegistry so that outbound messages
// matching manifest are encoded with the given serializer.
func (c *Cluster) RegisterSerializationBinding(manifest string, serializerID int32) {
	c.nm.SerializerRegistry.RegisterBinding(manifest, serializerID)
}

// RegisterType binds a manifest string to a Go reflect.Type so that
// Protobufcore.Serializer and JSONcore.Serializer can instantiate the correct struct
// when deserializing an incoming message.
//
// manifest is typically the fully-qualified Scala/Java class name used by the
// remote Pekko side, or any agreed-upon string for Go-to-Go communication.
//
//	cluster.RegisterType("com.example.OrderPlaced", reflect.TypeOf(OrderPlaced{}))
//	cluster.RegisterType("com.example.UserCreated", reflect.TypeOf((*UserCreated)(nil)))
func (c *Cluster) RegisterType(manifest string, typ reflect.Type) {
	c.nm.SerializerRegistry.RegisterManifest(manifest, typ)
}

// GetTypeByManifest returns the reflect.Type registered for manifest.
func (c *Cluster) GetTypeByManifest(manifest string) (reflect.Type, bool) {
	return c.nm.SerializerRegistry.GetTypeByManifest(manifest)
}

// CoordinatedShutdown returns the node's CoordinatedShutdown manager.
// Call AddTask on it to register custom shutdown logic before calling
// GracefulShutdown.
func (c *Cluster) CoordinatedShutdown() *actor.CoordinatedShutdown {
	return c.cs
}

// RegisterShardingRegion records a ShardRegion actor ref so that the
// coordinated-shutdown sequence can stop it gracefully before the cluster
// departs.  Call this once per ShardRegion immediately after spawning it.
func (c *Cluster) RegisterShardingRegion(ref ActorRef) {
	c.shardingRegionsMu.Lock()
	c.shardingRegions = append(c.shardingRegions, ref)
	c.shardingRegionsMu.Unlock()
}

// registerBuiltinShutdownTasks wires the standard cluster lifecycle tasks into
// the CoordinatedShutdown instance.  Must be called once during NewCluster.
func (c *Cluster) registerBuiltinShutdownTasks() {
	// ── service-unbind ───────────────────────────────────────────────────────
	// Mark the management server as shutting down so /health/ready immediately
	// returns 503 with reason "shutting_down".  This causes Kubernetes to stop
	// routing new requests to this node before any cluster state changes occur,
	// which is the first gate of rolling-update stability.
	c.cs.AddTask("service-unbind", "mark-management-shutting-down", func(_ context.Context) error {
		if c.mgmt != nil {
			c.mgmt.SetShuttingDown()
		}
		return nil
	})

	// ── cluster-sharding-shutdown-region ────────────────────────────────────
	// Stop every locally registered ShardRegion before departing the cluster.
	// ShardRegion.PostStop sends RegionHandoffRequest to the coordinator and
	// waits for HandoffComplete, ensuring shards are reallocated to surviving
	// members before the Leave message is sent in the next phase.
	c.cs.AddTask("cluster-sharding-shutdown-region", "stop-local-regions", func(ctx context.Context) error {
		c.shardingRegionsMu.Lock()
		regions := make([]ActorRef, len(c.shardingRegions))
		copy(regions, c.shardingRegions)
		c.shardingRegionsMu.Unlock()

		for _, r := range regions {
			c.System.Stop(r)
		}
		return nil
	})

	// ── cluster-leave ───────────────────────────────────────────────────────
	// Send a Leave message to all Up/WeaklyUp members, then wait for this
	// node's own status to reach Removed (driven by the cluster leader).
	c.cs.AddTask("cluster-leave", "send-leave-and-wait", func(ctx context.Context) error {
		if err := c.cm.LeaveCluster(); err != nil {
			// Non-fatal: we might not be fully joined yet.
			log.Printf("[CoordinatedShutdown] LeaveCluster: %v", err)
		}
		return c.cm.WaitForSelfRemoved(ctx)
	})

	// ── cluster-shutdown ────────────────────────────────────────────────────
	// Stop the CRDT replicator after the cluster has been left so that no
	// gossip is sent to nodes that have already seen us depart.
	c.cs.AddTask("cluster-shutdown", "stop-replicator", func(_ context.Context) error {
		c.repl.Stop()
		return nil
	})

	// ── actor-system-terminate ──────────────────────────────────────────────
	// Cancel the root context (stops gossip / heartbeat loops) and close all
	// TCP connections.  This is the last phase.
	c.cs.AddTask("actor-system-terminate", "close-transport", func(_ context.Context) error {
		if c.cancel != nil {
			c.cancel()
		}
		if c.udpHandler != nil {
			c.udpHandler.Close()
		}
		if c.server != nil {
			return c.server.Shutdown()
		}
		return nil
	})
}

// GracefulShutdown executes the full coordinated-shutdown sequence and waits
// for it to complete before returning.  Use this instead of Shutdown() when
// you need the cluster to drive through Leave → Exiting → Removed before the
// node closes its TCP connections.
//
// The provided ctx acts as a hard deadline.  If it expires mid-sequence the
// remaining phases are still attempted so the transport is always closed.
//
//	shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
//	defer cancel()
//	node.GracefulShutdown(shutCtx)
func (c *Cluster) GracefulShutdown(ctx context.Context) error {
	return c.cs.Run(ctx)
}

// Shutdown stops the TCP server and cancels all background goroutines.
// It is safe to call multiple times.
//
// For a graceful exit that drives the node through the cluster leave/exiting/
// removed lifecycle, call GracefulShutdown instead.
func (c *Cluster) Shutdown() error {
	atomic.StoreInt32(&c.shuttingDown, 1)
	c.stopWatchFDReaper()
	if c.cancel != nil {
		c.cancel()
	}
	if c.durable != nil {
		_ = c.durable.Close()
	}
	if c.server != nil {
		return c.server.Shutdown()
	}
	return nil
}

// Terminate implements ActorSystem.
func (c *Cluster) Terminate() {
	_ = c.Shutdown()
}

// Resolve looks up and returns the ActorRef for an already-registered actor
// at path.
func (c *Cluster) Resolve(path string) (ActorRef, error) {
	// For Resolve we can use ActorSelection and return an ActorRef
	ref, err := c.ActorSelection(path).Resolve(context.Background())
	if err != nil {
		return ActorRef{}, err
	}
	if ar, ok := ref.(ActorRef); ok {
		return ar, nil
	}
	return ActorRef{}, fmt.Errorf("resolved ref is not ActorRef: %T", ref)
}

// ActorSelection returns a handle to one or more actors identified by path.
func (c *Cluster) ActorSelection(path string) actor.ActorSelection {
	if strings.Contains(path, "://") {
		ap, err := actor.ParseActorPath(path)
		if err == nil {
			// Anchor at the system root of the URI
			anchorPath := ap.Address.String() + "/"
			return actor.ActorSelection{
				Anchor: ActorRef{fullPath: anchorPath, sys: c},
				Path:   actor.ParseSelectionElements(ap.Path()),
				System: c,
			}
		}
	}

	return actor.ActorSelection{
		Anchor: ActorRef{fullPath: c.SelfPathURI("/"), sys: c},
		Path:   actor.ParseSelectionElements(path),
		System: c,
	}
}

// DeliverSelection delivers msg to the actors identified by selection.
func (c *Cluster) DeliverSelection(s actor.ActorSelection, msg any, sender ...actor.Ref) {
	if s.Anchor == nil {
		return
	}

	var senderPath string
	if len(sender) > 0 && sender[0] != nil {
		senderPath = sender[0].Path()
	}

	ap, err := actor.ParseActorPath(s.Anchor.Path())
	if err != nil {
		return
	}

	self := c.SelfAddress()
	isLocal := ap.Address.System == self.System && ap.Address.Host == self.Host && ap.Address.Port == self.Port

	if isLocal {
		// Resolve and deliver locally
		ref, err := c.ResolveSelection(s, context.Background())
		if err == nil {
			ref.Tell(msg, sender...)
		}
		return
	}

	// Remote delivery via SelectionEnvelope (ID 6)
	targetAddr := ap.Address.ToProto()
	assoc, ok := c.nm.GetGekkaAssociationByHost(targetAddr.GetHostname(), targetAddr.GetPort())
	if !ok {
		var err error
		rawAssoc, err := c.nm.DialRemote(context.Background(), targetAddr)
		if err != nil {
			return
		}
		assoc = rawAssoc.(*core.GekkaAssociation)
	}

	elements := make([]*gproto_remote.Selection, len(s.Path))
	for i, p := range s.Path {
		matcher := p.Matcher
		elements[i] = &gproto_remote.Selection{
			Type:    gproto_remote.PatternType(p.Type).Enum(),
			Matcher: &matcher,
		}
	}

	selMsg := &core.ActorSelectionMessage{
		Message:  msg,
		Elements: elements,
	}

	ser, err := c.nm.SerializerRegistry.GetSerializer(core.MessageContainerSerializerID)
	if err != nil {
		return
	}
	payload, err := ser.ToBinary(selMsg)
	if err != nil {
		return
	}

	recipient := ap.Path()
	if senderPath != "" {
		_ = assoc.SendWithSender(recipient, senderPath, payload, core.MessageContainerSerializerID, "sel")
	} else {
		_ = assoc.Send(recipient, payload, core.MessageContainerSerializerID, "sel")
	}
}

// ResolveSelection resolves a selection to a concrete Ref.
func (c *Cluster) ResolveSelection(s actor.ActorSelection, ctx context.Context) (actor.Ref, error) {
	// Build full path string from anchor and elements
	path := s.Anchor.Path()
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	for i, e := range s.Path {
		if e.Type == 0 { // Parent
			lastSlash := strings.LastIndex(strings.TrimSuffix(path, "/"), "/")
			if lastSlash != -1 {
				path = path[:lastSlash+1]
			}
		} else {
			path += e.Matcher
			if i < len(s.Path)-1 {
				path += "/"
			}
		}
	}

	// Check if path is absolute URI or local path
	if strings.Contains(path, "://") {
		ap, err := actor.ParseActorPath(path)
		if err != nil {
			return nil, err
		}
		self := c.SelfAddress()
		if ap.Address.System == self.System && ap.Address.Host == self.Host && ap.Address.Port == self.Port {
			localPath := ap.Path()
			if a, found := c.GetLocalActor(localPath); found {
				return ActorRef{fullPath: path, sys: c, local: a}, nil
			}
			return nil, fmt.Errorf("actor not found: %s", localPath)
		}
		return ActorRef{fullPath: path, sys: c}, nil
	}

	// Local path
	if a, found := c.GetLocalActor(path); found {
		return ActorRef{fullPath: c.SelfPathURI(path), sys: c, local: a}, nil
	}
	return nil, fmt.Errorf("actor not found: %s", path)
}

func (c *Cluster) AskSelection(s actor.ActorSelection, ctx context.Context, msg any) (any, error) {
	ref, err := c.ResolveSelection(s, ctx)
	if err != nil {
		return nil, err
	}
	if ar, ok := ref.(ActorRef); ok {
		return c.Ask(ctx, ar, msg)
	}
	return nil, fmt.Errorf("AskSelection: resolved ref is not ActorRef: %T", ref)
}

// WhenTerminated implements ActorSystem.
func (c *Cluster) WhenTerminated() <-chan struct{} {
	return c.ctx.Done()
}

// ActorOf implements ActorSystem.
func (c *Cluster) ActorOf(props Props, name string) (ActorRef, error) {
	return c.ActorOfHierarchical(props, name, "/user")
}

// Spawn implements ActorSystem.
func (c *Cluster) Spawn(behavior any, name string) (ActorRef, error) {
	props := Props{
		New: func() actor.Actor { return typed.NewTypedActorGeneric(behavior) },
	}
	return c.ActorOf(props, name)
}

// SpawnAnonymous implements ActorSystem.
func (c *Cluster) SpawnAnonymous(behavior any) (ActorRef, error) {
	return c.Spawn(behavior, "")
}

// SystemActorOf implements ActorSystem.
func (c *Cluster) SystemActorOf(behavior any, name string) (ActorRef, error) {
	props := Props{
		New: func() actor.Actor { return typed.NewTypedActorGeneric(behavior) },
	}
	return c.ActorOfHierarchical(props, name, "/system")
}

// ActorOfHierarchical creates a new actor as a child of parentPath.
func (c *Cluster) ActorOfHierarchical(props Props, name string, parentPath string) (ActorRef, error) {
	// Generate a unique name when none is supplied.
	if name == "" {
		n := autoNameCounter.Add(1)
		name = fmt.Sprintf("$%d", n)
	}

	// Reject names that contain '/' — they would silently create nested paths.
	if strings.ContainsRune(name, '/') {
		return ActorRef{}, fmt.Errorf("actorOf: name %q must not contain '/'", name)
	}

	if parentPath == "" {
		parentPath = "/user"
	}
	path := parentPath + "/" + name

	// Check for duplicates before constructing the actor.
	c.actorsMu.RLock()
	_, exists := c.actors[path]
	c.actorsMu.RUnlock()
	if exists {
		return ActorRef{}, fmt.Errorf("actorOf: actor already registered at %q", path)
	}

	// Deployment interception: auto-provision a router when the path has a
	// matching deployment entry. GroupRouters do not need props.New (they route
	// to pre-existing actors); PoolRouters do need it (to create workers).
	if d, ok := c.lookupDeployment(path); ok && d.Router != "" {
		if core.IsGroupRouter(d.Router) {
			group, err := core.DeploymentToGroupRouter(c.cm, d)
			if err != nil {
				return ActorRef{}, fmt.Errorf("actorOf: deployment config for %q: %w", path, err)
			}
			return c.SpawnActor(path, group, Props{}).(ActorRef), nil
		}
		// Pool router — worker factory is required.
		if props.New == nil {
			return ActorRef{}, fmt.Errorf("actorOf: Props.New must not be nil for pool router deployment at %q", path)
		}
		pool, err := core.DeploymentToPoolRouter(c.cm, d, props)
		if err != nil {
			return ActorRef{}, fmt.Errorf("actorOf: deployment config for %q: %w", path, err)
		}
		return c.SpawnActor(path, pool, Props{}).(ActorRef), nil
	}

	// Plain actor — Props.New is required.
	if props.New == nil {
		return ActorRef{}, fmt.Errorf("actorOf: Props.New must not be nil")
	}
	a := props.New()
	return c.SpawnActor(path, a, props).(ActorRef), nil
}

// Watch implements ActorSystem.
func (c *Cluster) Watch(watcher ActorRef, target ActorRef) {
	if target.local != nil {
		target.local.AddWatcher(watcher)
	} else {
		// Target is remote
		c.watchRemote(watcher, target)
	}
}

// Unwatch implements ActorSystem.
func (c *Cluster) Unwatch(watcher ActorRef, target ActorRef) {
	if target.local != nil {
		target.local.RemoveWatcher(watcher)
	} else {
		// Target is remote
		c.unwatchRemote(watcher, target)
	}
}

// Stop implements ActorSystem.
func (c *Cluster) Stop(target ActorRef) {
	if target.local != nil {
		if cl, ok := target.local.(actor.MailboxCloser); ok {
			cl.CloseMailbox()
		} else {
			close(target.local.Mailbox())
		}
	}
}

// EventStream implements ActorSystem.
func (c *Cluster) EventStream() *actor.EventStream {
	return c.eventStream
}

// Scheduler implements ActorSystem.
func (c *Cluster) Scheduler() Scheduler {
	return c.sched
}

// Materializer implements ActorSystem.
func (c *Cluster) Materializer() stream.Materializer {
	return stream.ActorMaterializer{}
}

// Receptionist implements ActorSystem.
func (c *Cluster) Receptionist() typed.TypedActorRef[any] {
	return Receptionist()
}

// RemoteActorOf implements ActorSystem.
func (c *Cluster) RemoteActorOf(address actor.Address, path string) ActorRef {
	fullPath := address.String()
	if !strings.HasPrefix(path, "/") {
		fullPath += "/"
	}
	fullPath += path
	return ActorRef{fullPath: fullPath, sys: c}
}

// SendWithSender implements internalSystem.
func (c *Cluster) SendWithSender(ctx context.Context, path string, senderPath string, msg any) error {
	return c.router.SendWithSender(ctx, path, senderPath, msg)
}

// SerializationRegistry implements internalSystem.
func (c *Cluster) SerializationRegistry() *core.SerializationRegistry {
	return c.nm.SerializerRegistry
}

// GetLocalActor implements internalSystem.
func (c *Cluster) GetLocalActor(path string) (actor.Actor, bool) {
	c.actorsMu.RLock()
	defer c.actorsMu.RUnlock()
	a, ok := c.actors[path]
	return a, ok
}

// SelfPathURI implements internalSystem.
func (c *Cluster) SelfPathURI(path string) string {
	if len(path) > 0 && path[0] == '/' {
		self := c.SelfAddress()
		return fmt.Sprintf("%s://%s@%s:%d%s",
			self.Protocol, self.System, self.Host, self.Port, path)
	}
	return path
}

// LookupDeployment implements internalSystem.
func (c *Cluster) LookupDeployment(path string) (core.DeploymentConfig, bool) {
	return c.lookupDeployment(path)
}

// SpawnActor starts a and registers it at path, then returns an ActorRef for
// that actor.
func (n *Cluster) SpawnActor(path string, a actor.Actor, props actor.Props) actor.Ref {
	ref := ActorRef{fullPath: n.SelfPathURI(path), sys: n, local: a}

	// Apply custom mailbox before the actor goroutine starts.
	// Priority: Props.Mailbox > dispatcher's mailbox-type from HOCON config.
	if props.Mailbox != nil {
		actor.InjectMailbox(a, props.Mailbox)
	} else if props.DispatcherKey != "" {
		if dcfg, ok := actor.GetDispatcherConfig(props.DispatcherKey); ok {
			if mf := dcfg.ResolveMailbox(); mf != nil {
				actor.InjectMailbox(a, mf)
			}
		}
	}

	// Inject the actor's own reference so it can use Self() inside Receive.
	type selfSetter interface{ SetSelf(actor.Ref) }
	if ss, ok := a.(selfSetter); ok {
		ss.SetSelf(ref)
	}

	// Resolve the parent path (e.g., "/user/parent/child" -> "/user/parent")
	parentPath := "/user"
	lastSlash := strings.LastIndex(path, "/")
	if lastSlash > 0 {
		parentPath = path[:lastSlash]
	}

	// Inject the ActorContext so actors can spawn peers and access the
	// node lifecycle context via a.System(). Uses actor.InjectSystem so that
	// the package-local type assertion reaches the unexported setSystem method.
	actor.InjectSystem(a, AsActorContext(n, path))

	// Inject SupervisorStrategy from Props
	actor.InjectSupervisorStrategy(a, props.SupervisorStrategy)

	// Inject parent reference if this is a child actor.
	if parentPath != "/user" {
		if parentActor, found := n.GetLocalActor(parentPath); found {
			if parentRef, err := n.ActorSelection(n.SelfPathURI(parentPath)).Resolve(context.TODO()); err == nil {
				actor.InjectParent(a, parentRef)
				// Also register this child with the parent
				type childAdder interface {
					AddChild(string, actor.Ref, actor.Props)
				}
				if ca, ok := parentActor.(childAdder); ok {
					ca.AddChild(path[lastSlash+1:], ref, props)
				}
			}
		}
	}

	// Initialise the actor-aware logger. Uses actor.InjectLog for the same
	// package-locality reason.
	actor.InjectLog(a, n.logHandler, ref)

	a.SetOnStop(func() {
		// Stop all children recursively
		type childrenGetter interface{ Children() map[string]actor.Ref }
		if cg, ok := any(a).(childrenGetter); ok {
			for _, child := range cg.Children() {
				if childRef, ok := child.(ActorRef); ok {
					n.System.Stop(childRef)
				}
			}
		}

		n.UnregisterActor(path)
		n.triggerLocalActorDeath(path, ref)
		terminatedMsg := Terminated{Actor: ref}
		for _, w := range a.Watchers() {
			if watcherRef, ok := w.(ActorRef); ok {
				watcherRef.Tell(terminatedMsg)
			}
		}

		// Remove from parent's children list
		if parentPath != "/user" {
			if parentActor, found := n.GetLocalActor(parentPath); found {
				type childRemover interface{ RemoveChild(string) }
				if cr, ok := parentActor.(childRemover); ok {
					cr.RemoveChild(path[lastSlash+1:])
				}
			}
		}
	})
	dispType := props.Dispatcher
	if props.DispatcherKey != "" {
		dispType = actor.ResolveDispatcherKey(props.DispatcherKey)
	}
	actor.StartWithDispatcher(a, dispType)
	n.RegisterActor(path, a)
	return ref
}

// SubscribeToReceptionist allows internal components (like GroupRouter) to
// subscribe to service updates without direct dependency on the receptionist protocol.
func (c *Cluster) SubscribeToReceptionist(keyID string, subscriber typed.TypedActorRef[any], callback func([]string)) {
	recept := Receptionist()
	if recept.Untyped() == nil {
		log.Printf("Cluster: Receptionist not initialized")
		return
	}

	recept.Tell(receptionist.BridgeInternal(keyID, subscriber, callback))
}

// GetMailboxLengths implements actor.MailboxLengthProvider.
func (c *Cluster) GetMailboxLengths() map[string]int {
	c.actorsMu.RLock()
	defer c.actorsMu.RUnlock()
	res := make(map[string]int, len(c.actors))
	for path, a := range c.actors {
		res[path] = len(a.Mailbox())
	}
	return res
}

// GetClusterPressure implements actor.ClusterMetricsProvider.
func (c *Cluster) GetClusterPressure() map[string]actor.NodePressure {
	return c.mg.ClusterPressure()
}
