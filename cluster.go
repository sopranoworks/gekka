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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/actor/typed/receptionist"
	gcluster "github.com/sopranoworks/gekka/cluster"
	"github.com/sopranoworks/gekka/cluster/ddata"
	ddata_typed "github.com/sopranoworks/gekka/cluster/ddata/typed"
	"github.com/sopranoworks/gekka/cluster/singleton"
	"github.com/sopranoworks/gekka/discovery"
	"github.com/sopranoworks/gekka/internal/core"
	"github.com/sopranoworks/gekka/internal/management"
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
	LogLevel string `hocon:"gekka.logging.level"`

	// Transport selects the Artery transport: "tcp" (default) or "tls-tcp".
	// When "tls-tcp", the TLS field must be populated with valid PEM paths.
	Transport string `hocon:"pekko.remote.artery.transport"`

	// TLS holds TLS parameters; only used when Transport == "tls-tcp".
	TLS core.TLSConfig

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

	// FailureDetector tunes the Phi Accrual Failure Detector.
	// Zero values are replaced by safe defaults (threshold=10.0, maxSamples=1000,
	// minStdDeviation=500ms).
	//
	// Parse from HOCON:
	//
	//	gekka.cluster.failure-detector {
	//	    threshold         = 10.0
	//	    max-sample-size   = 1000
	//	    min-std-deviation = 100ms
	//	}
	FailureDetector FailureDetectorConfig

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
	Sharding ShardingConfig

	// DataCenter identifies which data center this node belongs to.
	// Corresponds to pekko.cluster.multi-data-center.self-data-center.
	// Defaults to "default" when unset or when parsed from HOCON without the key.
	//
	//	pekko.cluster.multi-data-center.self-data-center = "us-east"
	DataCenter string

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
	// Parse from HOCON:
	//
	//	gekka.management.http {
	//	    hostname = "127.0.0.1"
	//	    port     = 8558
	//	    enabled  = false
	//	}
	//
	// Endpoints:
	//   GET /cluster/members            — list all members and their status
	//   GET /cluster/members/{address}  — detail for a specific member
	Management core.ManagementConfig `hocon:"gekka.management.http"`

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

	// Discovery configures the dynamic seed node discovery (v0.9.0).
	// When Enabled is true, the cluster will use the specified Type to
	// discover seed nodes at startup.
	//
	// Parse from HOCON:
	//
	//	gekka.cluster.discovery {
	//	    enabled = true
	//	    type    = "kubernetes-api"
	//	    config {
	//	        namespace      = "default"
	//	        label-selector = "app=gekka"
	//	        port           = 2552
	//	    }
	//	}
	Discovery DiscoveryConfig

	// DistributedData configures the Distributed Data Replicator (v0.10.0).
	DistributedData DistributedDataConfig
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

// DistributedDataConfig holds settings for the CRDT replicator.
type DistributedDataConfig struct {
	// Enabled enables the replicator when true.
	// Corresponds to HOCON: gekka.cluster.distributed-data.enabled
	Enabled bool

	// GossipInterval is the duration between gossip rounds.
	// Corresponds to HOCON: gekka.cluster.distributed-data.gossip-interval
	GossipInterval time.Duration
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
}

// SBRConfig is a re-export of cluster.SBRConfig for use in ClusterConfig.
// Import gekka directly — you do not need to import the cluster sub-package.
type SBRConfig = gcluster.SBRConfig

// FailureDetectorConfig is a re-export of cluster.FailureDetectorConfig.
// It tunes the Phi Accrual Failure Detector parameters.
// Parsed from HOCON: gekka.cluster.failure-detector.*
type FailureDetectorConfig = gcluster.FailureDetectorConfig

// InternalSBRConfig is a re-export of cluster.InternalSBRConfig.
// It configures the lightweight icluster.Strategy (internal SBR primitives).
// Parsed from HOCON: gekka.cluster.split-brain-resolver.*
type InternalSBRConfig = gcluster.InternalSBRConfig

// ClusterSingletonManagerInterface is an alias for gcluster.ClusterSingletonManagerInterface.
type ClusterSingletonManagerInterface = gcluster.ClusterSingletonManagerInterface

// ClusterSingletonProxyInterface is an alias for gcluster.ClusterSingletonProxyInterface.
type ClusterSingletonProxyInterface = gcluster.ClusterSingletonProxyInterface

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

// ShardingConfig holds sharding-specific configuration parsed from HOCON.
type ShardingConfig struct {
	// PassivationIdleTimeout is the duration after which an entity that
	// has not received a message is automatically stopped.
	// Corresponds to pekko.cluster.sharding.passivation.idle-timeout.
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
	mg         *gcluster.MetricsGossip
	server     *core.TcpServer
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

	// shardingRegionsMu guards shardingRegions.
	shardingRegionsMu sync.Mutex
	// shardingRegions holds refs to every ShardRegion actor spawned on this
	// node.  Populated by RegisterShardingRegion; consumed during the
	// cluster-sharding-shutdown-region phase.
	shardingRegions []ActorRef

	sched *systemScheduler
}

// NewCluster creates, wires, and starts a Cluster. The TCP listener is bound
// immediately; call node.Addr() to discover the assigned port when ClusterConfig.Port == 0.
func NewCluster(cfg ClusterConfig) (*Cluster, error) {
	scheme, system, host, port := cfg.resolve()

	// Dynamic seed discovery (v0.9.0)
	if cfg.Discovery.Enabled {
		provider, err := discovery.Get(cfg.Discovery.Type, cfg.Discovery.Config)
		if err != nil {
			return nil, fmt.Errorf("gekka: discovery: %w", err)
		}

		discovered, err := provider.FetchSeedNodes()
		if err != nil {
			log.Printf("[Discovery] %s: %v", cfg.Discovery.Type, err)
		} else {
			for _, s := range discovered {
				// Discovered seeds must match the same system and protocol.
				uri := fmt.Sprintf("%s://%s@%s", scheme, system, s)
				if addr, err := actor.ParseAddress(uri); err == nil {
					cfg.SeedNodes = append(cfg.SeedNodes, addr)
				}
			}
		}
	}

	localAddr := &gproto_remote.Address{
		Protocol: &scheme,
		System:   proto.String(system),
		Hostname: proto.String(host),
		Port:     proto.Uint32(port),
	}
	uid := uint64(time.Now().UnixNano())
	localUA := &gproto_remote.UniqueAddress{
		Address: localAddr,
		Uid:     proto.Uint64(uid),
	}

	metrics := &core.NodeMetrics{}
	nm := core.NewNodeManager(localAddr, uid)
	nm.NodeMetrics = metrics
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
	if len(cfg.Roles) > 0 {
		cm.SetLocalRoles(cfg.Roles)
	}

	// Apply Phi Accrual Failure Detector configuration from HOCON.
	gcluster.ApplyDetectorConfig(cm, cfg.FailureDetector)

	// Wire the lightweight internal SBR strategy (icluster.Strategy).
	gcluster.ApplyInternalSBRConfig(cm, cfg.InternalSBR)

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

	server, err := core.NewTcpServer(core.TcpServerConfig{
		Addr: fmt.Sprintf("%s:%d", host, port),
		Handler: func(c context.Context, conn net.Conn) error {
			return nm.ProcessConnection(c, conn, core.INBOUND, nil, 0)
		},
		TLSConfig: tlsCfg,
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
			localAddr.Port = proto.Uint32(actualPort)
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
	}
	nm.SystemMessageCallback = cluster.HandleSystemMessage

	// Terminate the scheduler when the cluster context is cancelled.
	go func() {
		<-ctx.Done()
		cluster.sched.terminate()
	}()
	cluster.System = cluster
	cluster.cm.Sys = asActorContext(cluster, "")
	actor.SetMailboxLengthProvider(cluster)
	actor.SetClusterMetricsProvider(cluster)

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
	cluster.mg.Start(cluster.ctx)

	// ── Distributed Data ─────────────────────────────────────────────────────
	if cfg.DistributedData.Enabled {
		if cfg.DistributedData.GossipInterval > 0 {
			repl.GossipInterval = cfg.DistributedData.GossipInterval
		}
		repl.Start(cluster.ctx)

		// Spawn typed replicator and register with receptionist
		typedRepl := ddata_typed.Replicator{}
		ref, err := Spawn(cluster, typedRepl.Behavior(repl), "ddataReplicator")
		if err == nil {
			cluster.Receptionist().Tell(Register[any]{Key: ddata_typed.ReplicatorServiceKey, Service: ref})
		} else {
			log.Printf("gekka: failed to spawn typed ddata replicator: %v", err)
		}
	}

	// ── Receptionist ────────────────────────────────────────────────────────
	// Spawn the local receptionist actor. It uses the CRDT replicator to
	// propagate service registrations cluster-wide.
	if _, err := spawnReceptionist(cluster, repl); err != nil {
		log.Printf("gekka: failed to spawn receptionist: %v", err)
	}

	// ── Coordinated Shutdown ────────────────────────────────────────────────
	// Wire the built-in graceful exit sequence so that Shutdown() drives the
	// cluster through Leave → Exiting → Removed before closing TCP connections.
	cluster.cs = actor.NewCoordinatedShutdown()
	cluster.registerBuiltinShutdownTasks()

	// ── Split Brain Resolver ─────────────────────────────────────────────────
	// Start the SBR manager goroutine when a strategy is configured.
	if sbr := gcluster.NewSBRManager(cm, cfg.SBR); sbr != nil {
		go sbr.Start(ctx)
	}

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
		select {
		case a.Mailbox() <- env:
		default:
			// Mailbox full — drop, just like a dead-letter in Pekko.
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
func (c *Cluster) Send(ctx context.Context, dst interface{}, msg interface{}) error {
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
func (c *Cluster) Ask(ctx context.Context, dst interface{}, msg interface{}) (*IncomingMessage, error) {
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

func (c *Cluster) Join(seedHost string, seedPort uint32) error {
	scheme := c.localAddr.GetProtocol() // "pekko" or "akka"
	c.seedAddr = &gproto_remote.Address{
		Protocol: &scheme,
		System:   proto.String(c.localAddr.GetSystem()),
		Hostname: proto.String(seedHost),
		Port:     proto.Uint32(seedPort),
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
	leaveMsg := &gproto_cluster.Address{
		Protocol: proto.String(addr.Protocol),
		System:   proto.String(addr.System),
		Hostname: proto.String(addr.Host),
		Port:     proto.Uint32(uint32(addr.Port)),
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
func (c *Cluster) StartHeartbeat() {
	if c.seedAddr != nil {
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
	return singleton.NewClusterSingletonProxy(c.cm, c.router, managerPath, role)
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

// RegisterSerializer registers a custom core.Serializer with this node's
// core.SerializationRegistry, keyed by s.Identifier(). Overwrites any existing
// entry for the same ID.
//
//	cluster.RegisterSerializer(&MyJSONSerializer{})
func (c *Cluster) RegisterSerializer(s core.Serializer) {
	c.nm.SerializerRegistry.RegisterSerializer(s.Identifier(), s)
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
	if c.cancel != nil {
		c.cancel()
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
		elements[i] = &gproto_remote.Selection{
			Type:    gproto_remote.PatternType(p.Type).Enum(),
			Matcher: proto.String(p.Matcher),
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
		close(target.local.Mailbox())
	}
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
	actor.InjectSystem(a, asActorContext(n, path))

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
	actor.Start(a)
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
func (c *Cluster) GetClusterPressure() map[string]float64 {
	return c.mg.ClusterPressure()
}
