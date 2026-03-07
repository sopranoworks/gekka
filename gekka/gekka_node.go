/*
 * gekka_node.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"fmt"
	"net"
	"time"

	"gekka/gekka/actor"
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

// NodeConfig specifies how to initialize a GekkaNode.
//
// The simplest form uses flat fields:
//
//	gekka.Spawn(gekka.NodeConfig{SystemName: "ClusterSystem", Host: "127.0.0.1", Port: 2553})
//
// You can also supply a typed Address directly:
//
//	addr := actor.Address{Protocol: "pekko", System: "ClusterSystem", Host: "127.0.0.1", Port: 2553}
//	gekka.Spawn(gekka.NodeConfig{Address: addr})
//
// When Address is set it takes precedence over SystemName/Host/Port/Provider.
//
// LoadConfig populates all fields from a HOCON application.conf automatically.
type NodeConfig struct {
	// Address sets the node's own address using the typed actor.Address.
	// When non-zero it overrides SystemName, Host, Port, and Provider.
	Address actor.Address

	// SystemName is the actor system name shared by all cluster members.
	// Ignored when Address is set. Defaults to "GekkaSystem".
	SystemName string

	// Host is the TCP bind/advertised address (e.g. "127.0.0.1").
	// Ignored when Address is set. Defaults to "127.0.0.1".
	Host string

	// Port is the TCP listen port. 0 lets the OS assign a free port.
	// Ignored when Address is set.
	Port uint32

	// Provider selects the actor-path protocol prefix.
	// Ignored when Address is set. Defaults to ProviderPekko.
	Provider Provider

	// SeedNodes is the list of cluster seed nodes parsed from HOCON
	// (pekko.cluster.seed-nodes). Populated by LoadConfig; ignored by Spawn.
	// Use JoinSeeds() to connect to the first reachable seed after Spawn.
	SeedNodes []actor.Address
}

// resolve returns the effective (scheme, system, host, port) for this config.
func (c NodeConfig) resolve() (scheme, system, host string, port uint32) {
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
	// Common values: 4 = raw bytes, 2 = Protobuf, 5 = ClusterMessageSerializer.
	SerializerId int32
	// Manifest is the type tag embedded in the Artery envelope.
	Manifest string
}

// GekkaNode is the single entry point for the gekka library. It wires together
// the TCP server, NodeManager, ClusterManager, Router, and Replicator.
//
// Create one with Spawn (or SpawnFromConfig), then call Join (or JoinSeeds)
// to connect to a Pekko cluster.
type GekkaNode struct {
	nm        *NodeManager
	cm        *ClusterManager
	router    *Router
	repl      *Replicator
	server    *TcpServer
	ctx       context.Context
	cancel    context.CancelFunc
	localAddr *Address
	seedAddr  *Address        // set by the first Join call
	seeds     []actor.Address // from NodeConfig.SeedNodes (populated by LoadConfig)
}

// Spawn creates, wires, and starts a GekkaNode. The TCP listener is bound
// immediately; call node.Addr() to discover the assigned port when NodeConfig.Port == 0.
func Spawn(cfg NodeConfig) (*GekkaNode, error) {
	scheme, system, host, port := cfg.resolve()

	localAddr := &Address{
		Protocol: &scheme,
		System:   proto.String(system),
		Hostname: proto.String(host),
		Port:     proto.Uint32(port),
	}
	uid := uint64(time.Now().UnixNano())
	localUA := &UniqueAddress{
		Address: localAddr,
		Uid:     proto.Uint64(uid),
	}

	nm := NewNodeManager(localAddr)
	cm := NewClusterManager(localUA, nil)
	cm.protocol = scheme
	router := NewRouter(nm)
	cm.router = router
	nm.SetClusterManager(cm)

	ctx, cancel := context.WithCancel(context.Background())

	server, err := NewTcpServer(TcpServerConfig{
		Addr: fmt.Sprintf("%s:%d", host, port),
		Handler: func(c context.Context, conn net.Conn) error {
			return nm.ProcessConnection(c, conn, INBOUND, nil)
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
			// Patch the gossip state that was snapshotted in NewClusterManager.
			if len(cm.state.AllAddresses) > 0 && cm.state.AllAddresses[0].Address != nil {
				cm.state.AllAddresses[0].Address.Port = proto.Uint32(actualPort)
			}
		}
	}

	nodeID := fmt.Sprintf("%s:%d", host, actualPort)
	repl := NewReplicator(nodeID, router)

	return &GekkaNode{
		nm:        nm,
		cm:        cm,
		router:    router,
		repl:      repl,
		server:    server,
		ctx:       ctx,
		cancel:    cancel,
		localAddr: localAddr,
		seeds:     cfg.SeedNodes,
	}, nil
}

// Addr returns the bound TCP address after Spawn. Use this to discover the
// OS-assigned port when NodeConfig.Port was 0.
func (n *GekkaNode) Addr() net.Addr {
	return n.server.Addr()
}

// OnMessage registers a callback that is invoked for every user-level Artery
// message received by this node. Cluster-internal messages (heartbeats, gossip,
// etc.) are handled automatically and do not trigger this callback.
func (n *GekkaNode) OnMessage(fn func(ctx context.Context, msg *IncomingMessage) error) {
	n.nm.UserMessageCallback = func(ctx context.Context, meta *ArteryMetadata) error {
		return fn(ctx, &IncomingMessage{
			Payload:      meta.Payload,
			SerializerId: meta.SerializerId,
			Manifest:     string(meta.MessageManifest),
		})
	}
}

// SelfAddress returns the node's own address as a typed actor.Address.
// Use it to build local or remote actor paths:
//
//	self := node.SelfAddress()
//	path := self.WithRoot("user").Child("myActor")
//	remoteAddr := actor.Address{Protocol: self.Protocol, System: self.System, Host: "10.0.0.2", Port: 2552}
func (n *GekkaNode) SelfAddress() actor.Address {
	return actor.Address{
		Protocol: n.localAddr.GetProtocol(),
		System:   n.localAddr.GetSystem(),
		Host:     n.localAddr.GetHostname(),
		Port:     int(n.localAddr.GetPort()),
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
//   - []byte           → raw bytes (Pekko ByteArraySerializer, ID 4)
//   - proto.Message    → Protobuf (ID 2)
//   - cluster messages → handled automatically by Router
func (n *GekkaNode) Send(ctx context.Context, dst interface{}, msg interface{}) error {
	var pathStr string
	switch d := dst.(type) {
	case string:
		pathStr = d
	case fmt.Stringer:
		pathStr = d.String()
	default:
		return fmt.Errorf("gekka: Send: unsupported destination type %T (want string, actor.ActorPath, or fmt.Stringer)", dst)
	}
	return n.router.Send(ctx, pathStr, msg)
}

// Join sends an InitJoin to the seed node, starts heartbeats, and starts the
// background gossip loop. It is non-blocking: cluster convergence happens
// asynchronously. Use WaitForHandshake to confirm the TCP association is up.
func (n *GekkaNode) Join(seedHost string, seedPort uint32) error {
	scheme := n.localAddr.GetProtocol() // "pekko" or "akka"
	n.seedAddr = &Address{
		Protocol: &scheme,
		System:   proto.String(n.localAddr.GetSystem()),
		Hostname: proto.String(seedHost),
		Port:     proto.Uint32(seedPort),
	}
	if err := n.cm.JoinCluster(n.ctx, seedHost, seedPort); err != nil {
		return err
	}
	n.cm.StartHeartbeat(n.seedAddr)
	go n.cm.StartGossipLoop(n.ctx)
	return nil
}

// JoinSeeds connects to the seed nodes supplied via LoadConfig
// (NodeConfig.SeedNodes). It skips self and joins the first remote seed;
// if all configured seeds are self (i.e. this node is the bootstrap seed),
// it joins its own address so Pekko's leader-election handshake completes.
//
// Returns an error when no seeds are configured or Join fails.
func (n *GekkaNode) JoinSeeds() error {
	if len(n.seeds) == 0 {
		return fmt.Errorf("gekka: JoinSeeds: no seed nodes configured (use LoadConfig or set NodeConfig.SeedNodes)")
	}
	self := n.SelfAddress()
	for _, s := range n.seeds {
		if s.Host != self.Host || s.Port != self.Port {
			return n.Join(s.Host, uint32(s.Port))
		}
	}
	// All seeds are self — this node is the sole seed; join self.
	s := n.seeds[0]
	return n.Join(s.Host, uint32(s.Port))
}

// Seeds returns the seed-node addresses parsed from HOCON configuration.
// Returns nil when the node was created without LoadConfig.
func (n *GekkaNode) Seeds() []actor.Address {
	return n.seeds
}

// Leave gracefully departs the cluster by broadcasting a Leave message to all
// known Up/WeaklyUp members. The Pekko SBR will remove this node shortly after.
func (n *GekkaNode) Leave() error {
	return n.cm.LeaveCluster()
}

// StopHeartbeat suspends heartbeats to the seed node, simulating a node
// failure from Pekko's perspective. Used in tests and for graceful pre-leave.
func (n *GekkaNode) StopHeartbeat() {
	if n.seedAddr != nil {
		n.cm.StopHeartbeat(n.seedAddr)
	}
}

// StartHeartbeat resumes heartbeats to the seed node after StopHeartbeat.
func (n *GekkaNode) StartHeartbeat() {
	if n.seedAddr != nil {
		n.cm.StartHeartbeat(n.seedAddr)
	}
}

// WaitForHandshake blocks until the Artery handshake with the given host:port
// completes (i.e. the association reaches ASSOCIATED state). Returns an error
// if the context is cancelled or the 30-second built-in timeout expires.
func (n *GekkaNode) WaitForHandshake(ctx context.Context, host string, port uint32) error {
	for {
		if assoc, ok := n.router.getAssociationByHost(host, port); ok {
			select {
			case <-assoc.handshake:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(30 * time.Second):
				return fmt.Errorf("gekka: handshake timeout for %s:%d", host, port)
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
func (n *GekkaNode) Replicator() *Replicator {
	return n.repl
}

// SingletonProxy returns a ClusterSingletonProxy that routes messages to
// the singleton actor hosted on the oldest Up cluster node.
//
//	proxy := node.SingletonProxy("/user/singletonManager", "")
//	proxy.Send(ctx, []byte("ping"))
func (n *GekkaNode) SingletonProxy(managerPath, role string) *ClusterSingletonProxy {
	return NewClusterSingletonProxy(n.cm, n.router, managerPath, role)
}

// Serialization returns the SerializationRegistry for registering custom
// Protobuf or JSON message types and their manifests.
func (n *GekkaNode) Serialization() *SerializationRegistry {
	return n.nm.SerializerRegistry
}

// Shutdown stops the TCP server and cancels all background goroutines.
// It is safe to call multiple times.
func (n *GekkaNode) Shutdown() error {
	n.cancel()
	return n.server.Shutdown()
}
