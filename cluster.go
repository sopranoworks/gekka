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
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/sopranoworks/gekka/actor"
	gcluster "github.com/sopranoworks/gekka/cluster"
	"github.com/sopranoworks/gekka/crdt"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"github.com/sopranoworks/gekka/internal/core"

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
	nm         *core.NodeManager
	cm         *gcluster.ClusterManager
	router     *actor.Router
	repl       *crdt.Replicator
	server     *core.TcpServer
	ctx        context.Context
	cancel     context.CancelFunc
	localAddr  *gproto_remote.Address
	seedAddr   *gproto_remote.Address // set by the first Join call
	seeds      []actor.Address // from ClusterConfig.SeedNodes (populated by LoadConfig)
	metrics    *core.NodeMetrics
	monitoring *core.MonitoringServer // nil when monitoring is disabled

	actorsMu sync.RWMutex
	actors   map[string]actor.Actor // actor path suffix → Actor

	remoteWatchersMu sync.Mutex
	// node addr "host:port" → target full path → slice of watcher references
	remoteWatchers map[string]map[string][]ActorRef

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
}

// NewCluster creates, wires, and starts a Cluster. The TCP listener is bound
// immediately; call node.Addr() to discover the assigned port when ClusterConfig.Port == 0.
func NewCluster(cfg ClusterConfig) (*Cluster, error) {
	scheme, system, host, port := cfg.resolve()

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
	router := actor.NewRouter(nm)
	cm := gcluster.NewClusterManager(core.ToClusterUniqueAddress(localUA), func(ctx context.Context, path string, msg any) error {
		return router.Send(ctx, path, msg)
	})
	cm.Protocol = scheme
	cm.Metrics = metrics
	cm.Router = func(ctx context.Context, path string, msg any) error {
		return router.Send(ctx, path, msg)
	}
	nm.SetClusterManager(cm)

	ctx, cancel := context.WithCancel(context.Background())

	server, err := core.NewTcpServer(core.TcpServerConfig{
		Addr: fmt.Sprintf("%s:%d", host, port),
		Handler: func(c context.Context, conn net.Conn) error {
			return nm.ProcessConnection(c, conn, core.INBOUND, nil, 0)
		},
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
	repl := crdt.NewReplicator(nodeID, router)

	cluster := &Cluster{
		nm:             nm,
		cm:             cm,
		router:         router,
		repl:           repl,
		server:         server,
		ctx:            ctx,
		cancel:         cancel,
		localAddr:      localAddr,
		seeds:          cfg.SeedNodes,
		metrics:        metrics,
		actors:         make(map[string]actor.Actor),
		remoteWatchers: make(map[string]map[string][]ActorRef),
		logHandler:     cfg.LogHandler,
		deployments:    cfg.Deployments,
	}
	cluster.System = cluster
	cluster.cm.Sys = asActorContext(cluster, "")

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
		RecipientPath: recipientPath,
		Payload:       meta.Payload,
		SerializerId:  meta.SerializerId,
		Manifest:      string(meta.MessageManifest),
		Sender:        senderRef,
	}

	// Try registered actors first; deliver via actor.Envelope so that
	// BaseActor.currentSender is set before Receive is called.
	c.actorsMu.RLock()
	a, found := c.actors[recipientPath]
	c.actorsMu.RUnlock()
	if found {
		env := actor.Envelope{Payload: incoming, Sender: senderRef}
		select {
		case a.Mailbox() <- env:
		default:
			// Mailbox full — drop, just like a dead-letter in Pekko.
		}
		return nil
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
			return AskLocal(ctx, actor, msg)
		}
	}

	// Build a unique temporary sender path.
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return nil, fmt.Errorf("gekka: Ask: generate id: %w", err)
	}
	id := hex.EncodeToString(buf[:])
	self := c.SelfAddress()
	tempPath := fmt.Sprintf("%s://%s@%s:%d/temp/ask-%s",
		self.Protocol, self.System, self.Host, self.Port, id)

	// Register a reply channel keyed by the temp path.
	replyCh := make(chan *core.ArteryMetadata, 1)
	c.nm.RegisterPendingReply(tempPath, replyCh)
	defer c.nm.UnregisterPendingReply(tempPath)

	if err := c.router.SendWithSender(ctx, pathStr, tempPath, msg); err != nil {
		return nil, fmt.Errorf("gekka: Ask: send: %w", err)
	}

	select {
	case meta := <-replyCh:
		return &IncomingMessage{
			RecipientPath: tempPath,
			Payload:       meta.Payload,
			SerializerId:  meta.SerializerId,
			Manifest:      string(meta.MessageManifest),
			DeserializedMessage: meta.DeserializedMessage,
		}, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("gekka: Ask: %w", ctx.Err())
	}
}

func AskLocal(ctx context.Context, a actor.Actor, msg any) (*IncomingMessage, error) {
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
	go c.cm.StartGossipLoop(c.ctx)
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

// StopHeartbeat suspends heartbeats to the seed node, simulating a node
// failure from Pekko's perspective. Used in tests and for graceful pre-leave.
func (c *Cluster) StopHeartbeat() {
	if c.seedAddr != nil {
		c.cm.StopHeartbeat(core.ToClusterAddress(c.seedAddr))
		// Immediately restart — this counts as a reset.
		c.cm.StartHeartbeat(core.ToClusterAddress(c.seedAddr))
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
func (c *Cluster) Replicator() *crdt.Replicator {
	return c.repl
}

// SingletonProxy returns a ClusterSingletonProxy that routes messages to
// the singleton actor hosted on the oldest Up cluster node.
//
//	proxy := node.SingletonProxy("/user/singletonManager", "")
//	proxy.Send(ctx, []byte("ping"))
func (c *Cluster) SingletonProxy(managerPath, role string) *gcluster.ClusterSingletonProxy {
	return gcluster.NewClusterSingletonProxy(c.cm, c.router, managerPath, role)
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

// Shutdown stops the TCP server and cancels all background goroutines.
// It is safe to call multiple times.
func (c *Cluster) Shutdown() error {
	if c.cancel != nil {
		c.cancel()
	}
	if c.server != nil {
		return c.server.Shutdown()
	}
	return nil
}

// ActorOf implements ActorSystem.
func (c *Cluster) ActorOf(props Props, name string) (ActorRef, error) {
	return c.ActorOfHierarchical(props, name, "/user")
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
			return c.SpawnActor(path, group, Props{}), nil
		}
		// Pool router — worker factory is required.
		if props.New == nil {
			return ActorRef{}, fmt.Errorf("actorOf: Props.New must not be nil for pool router deployment at %q", path)
		}
		pool, err := core.DeploymentToPoolRouter(c.cm, d, props)
		if err != nil {
			return ActorRef{}, fmt.Errorf("actorOf: deployment config for %q: %w", path, err)
		}
		return c.SpawnActor(path, pool, Props{}), nil
	}

	// Plain actor — Props.New is required.
	if props.New == nil {
		return ActorRef{}, fmt.Errorf("actorOf: Props.New must not be nil")
	}
	a := props.New()
	return c.SpawnActor(path, a, props), nil
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
	return c.selfPathURI(path)
}

// LookupDeployment implements internalSystem.
func (c *Cluster) LookupDeployment(path string) (core.DeploymentConfig, bool) {
	return c.lookupDeployment(path)
}

// SpawnActor starts a and registers it at path, then returns an ActorRef for
// that actor. It is a convenient alternative to the manual three-step sequence
// of actor.Start / node.RegisterActor / building an ActorRef:
//
//	ref := node.SpawnActor("/user/myActor", &MyActor{BaseActor: actor.NewBaseActor()})
//	ref.Tell("Hello, local actor!")
//
// path must be the full path suffix as used in Artery envelopes, e.g.
// "/user/myActor". Do NOT call actor.Start yourself before SpawnActor — that
// would launch two receive goroutines.
func (n *Cluster) SpawnActor(path string, a actor.Actor, props actor.Props) ActorRef {
	ref := ActorRef{fullPath: n.selfPathURI(path), sys: n, local: a}

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
		if parentActor, found := n.actors[parentPath]; found {
			if parentRef, err := n.ActorSelection(n.selfPathURI(parentPath)).Resolve(context.TODO()); err == nil {
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
		terminatedMsg := Terminated{Actor: ref}
		for _, w := range a.Watchers() {
			if watcherRef, ok := w.(ActorRef); ok {
				watcherRef.Tell(terminatedMsg)
			}
		}

		// Remove from parent's children list
		if parentPath != "/user" {
			if parentActor, found := n.actors[parentPath]; found {
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

// selfPathURI converts a local path suffix such as "/user/myActor" into the
// full actor-path URI for this node. If path is already absolute it is
// returned unchanged.
func (n *Cluster) selfPathURI(path string) string {
	if len(path) > 0 && path[0] == '/' {
		self := n.SelfAddress()
		return fmt.Sprintf("%s://%s@%s:%d%s",
			self.Protocol, self.System, self.Host, self.Port, path)
	}
	return path
}


