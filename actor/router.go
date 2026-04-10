/*
 * router.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"hash/crc32"
	"log"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"

	"google.golang.org/protobuf/proto"
)

// RemoteMessagingProvider abstracts the remoting infrastructure (NodeManager)
// so the actor package can perform remote delivery without importing gekka.
type RemoteMessagingProvider interface {
	// LocalAddress returns the address of the node hosting this provider.
	LocalAddress() *gproto_remote.Address

	// GetAssociationByHost retrieves an existing connection to the given host:port.
	GetAssociationByHost(host string, port uint32) (RemoteAssociation, bool)

	// DialRemote initiates a new connection to the target address.
	DialRemote(ctx context.Context, target *gproto_remote.Address) (RemoteAssociation, error)

	// Serializer retrieves the serializer registered for the given ID.
	Serializer(id int32) (RemoteSerializer, error)

	// Metrics returns the record-keeping interface for the node.
	Metrics() RemoteMetrics
}

// RemoteAssociation represents a connection to a remote node.
type RemoteAssociation interface {
	// Send delivers a message to the recipient on the remote node.
	Send(recipient string, payload []byte, serializerId int32, manifest string) error

	// SendWithSender delivers a message to the recipient with an explicit sender path.
	SendWithSender(recipient, senderPath string, payload []byte, serializerId int32, manifest string) error
}

// RemoteSerializer defines the contract for serialization used by the Router.
type RemoteSerializer interface {
	ToBinary(obj any) ([]byte, error)
}

// RemoteSerializable may be implemented by message types that need a specific
// Artery serializer ID and manifest string for network transport.  Router.Send
// checks this interface before falling back to the default (JSON) serializer.
//
// Custom serializers must be registered with the node before messages of the
// corresponding types are sent or received.
type RemoteSerializable interface {
	ArterySerializerID() int32
	ArteryManifest() string
}

// RemoteMetrics records message-delivery statistics.
type RemoteMetrics interface {
	IncrementMessagesSent()
	IncrementBytesSent(n int64)
}

// Router handles actor path resolution and message delivery for remoting.
// It is the Go equivalent of Artery's outbound router.
type Router struct {
	provider RemoteMessagingProvider
}

// NewRouter creates a remoting Router backed by the given provider.
func NewRouter(p RemoteMessagingProvider) *Router {
	return &Router{provider: p}
}

// Well-known serializer IDs used in Artery frames.
const (
	// JSONSerializerID is the Artery serializer ID for the built-in
	// JSONSerializer. Pekko occupies IDs 1–31; IDs ≥ 100 are safe for application use.
	JSONSerializerID int32 = 9

	// ArteryInternalSerializerID is the standard serializer ID for Artery control messages.
	ArteryInternalSerializerID int32 = 17

	// ClusterSerializerID is the Pekko ClusterMessageSerializer ID.
	ClusterSerializerID int32 = 5

	// StringSerializerID matches Akka 2.6's built-in StringSerializer (ID=20)
	// which encodes java.lang.String as raw UTF-8 bytes (no Java-serialisation
	// overhead).  Go uses this ID when sending strings to Akka nodes.
	StringSerializerID int32 = 20
	)

// AssociationState represents the state of a connection to a remote node.
type AssociationState int

const (
	ASSOCIATED            AssociationState = 1
	QUARANTINED           AssociationState = 2
	INITIATED             AssociationState = 3
	WAITING_FOR_HANDSHAKE AssociationState = 4
)

// AssociationRole indicates whether the connection was initiated locally or remotely.
type AssociationRole int

const (
	INBOUND AssociationRole = iota
	OUTBOUND
)

// RoutingLogic defines the strategy used to select a target routee for each
// message. Implement this interface to add custom distribution algorithms
// (consistent-hash, smallest-mailbox, etc.).
//
// Select must be safe for concurrent use from multiple goroutines.
type RoutingLogic interface {
	// Select chooses one Ref from routees based on message and the
	// implementation's internal state. Returns nil only when routees is empty.
	Select(message any, routees []Ref) Ref
}

// AdvancedRoutingLogic is an optional interface that RoutingLogic can implement
// to take full control of the routing process, including spawning temporary
// actors or performing multiple deliveries.
type AdvancedRoutingLogic interface {
	RoutingLogic
	// Route handles the message delivery. Returns true if the message was handled.
	Route(router *RouterActor, msg any) bool
}

// Broadcast is a wrapper message that instructs the RouterActor to deliver
// the inner Message to every routee, bypassing the RoutingLogic selector.
//
//	router.Tell(actor.Broadcast{Message: []byte("ping")})
type Broadcast struct {
	// Message is the payload forwarded to every routee.
	Message any
}

// RouterActor forwards incoming messages to a set of routees according to a
// pluggable RoutingLogic. It implements the Actor interface by embedding
// BaseActor, so it can be registered and started like any other actor.
//
// Create one with NewRouterActor and register it via node.SpawnActor or
// node.System.ActorOf. Routees are plain actor.Ref values, so they can be
// local or remote actors (including gekka.ActorRef).
//
// Design notes for future extension:
//
//   - Pool routers will override Receive to intercept management messages
//     (e.g. AdjustPoolSize) and spawn/stop child routees via System().ActorOf.
//   - Group routers will resolve routee refs by path at message-delivery time
//     via System().ActorOf / ActorSelection rather than storing a fixed slice.
type RouterActor struct {
	BaseActor
	Logic   RoutingLogic
	Routees []Ref
}

// NewRouterActor creates a RouterActor with the given routing logic and an
// initial set of routees. The routees slice is copied so later mutations to
// the caller's slice do not affect the router.
func NewRouterActor(logic RoutingLogic, routees []Ref) *RouterActor {
	cp := make([]Ref, len(routees))
	copy(cp, routees)
	return &RouterActor{
		BaseActor: NewBaseActor(),
		Logic:     logic,
		Routees:   cp,
	}
}

// Routees returns a snapshot of the current routee list.
func (r *RouterActor) RouteesSnapshot() []Ref {
	cp := make([]Ref, len(r.Routees))
	copy(cp, r.Routees)
	return cp
}

// AddRoutee appends ref to the routee list. Safe to call from Receive; not
// safe to call concurrently from outside the actor's goroutine.
func (r *RouterActor) AddRoutee(ref Ref) {
	r.Routees = append(r.Routees, ref)
}

// RemoveRoutee removes the first routee whose path matches ref.Path().
// No-op when ref is not in the list.
func (r *RouterActor) RemoveRoutee(ref Ref) {
	for i, rt := range r.Routees {
		if rt.Path() == ref.Path() {
			r.Routees = append(r.Routees[:i], r.Routees[i+1:]...)
			return
		}
	}
}

// Receive handles incoming messages.
//
// A Broadcast wrapper delivers Message to every routee (ignoring the logic).
// Any other value is forwarded to the single routee selected by logic.Select.
// The original sender (r.Sender()) is preserved in both cases.
func (r *RouterActor) Receive(msg any) {
	switch m := msg.(type) {
	case Broadcast:
		// Deliver inner message to all routees, preserving sender.
		for _, rt := range r.Routees {
			rt.Tell(m.Message, r.Sender())
		}
	default:
		if len(r.Routees) == 0 {
			return
		}

		// Support AdvancedRoutingLogic
		if adv, ok := r.Logic.(AdvancedRoutingLogic); ok {
			if adv.Route(r, msg) {
				return
			}
		}

		// Support BroadcastRoutingLogic explicitly
		if _, ok := r.Logic.(*BroadcastRoutingLogic); ok {
			for _, rt := range r.Routees {
				rt.Tell(msg, r.Sender())
			}
			return
		}

		target := r.Logic.Select(msg, r.Routees)
		if target != nil {
			target.Tell(msg, r.Sender())
		}
	}
}

// ── Pool router ───────────────────────────────────────────────────────────────

// AdjustPoolSize is a management message sent to a PoolRouter to request a
// change in the number of active routees.
//
// Positive Delta adds new children; negative Delta is reserved for future
// graceful-shrink support.
//
//	router.Tell(actor.AdjustPoolSize{Delta: 2}) // grow pool by 2
type AdjustPoolSize struct {
	// Delta is the signed change in pool size. Only positive values are acted
	// upon in the current implementation.
	Delta int
}

// PoolRouter is a RouterActor that owns and manages its routees as child actors.
//
// Children are spawned during PreStart using the configured Props. When a child
// stops, the router receives a TerminatedMessage and removes that routee from
// the active list. The pool can be grown at runtime by sending AdjustPoolSize.
//
// The PoolRouter uses the parent path of its own registered path as the
// namespace for children, e.g. for a pool at /user/pool the children will be
// at /user/pool/$pool-0, /user/pool/$pool-1, etc.
//
// Usage:
//
//	pool := actor.NewPoolRouter(
//	    &actor.RoundRobinRoutingLogic{},
//	    3,
//	    actor.Props{New: func() actor.Actor {
//	        return &WorkerActor{BaseActor: actor.NewBaseActor()}
//	    }},
//	)
//	node.System.ActorOf(actor.Props{New: func() actor.Actor { return pool }}, "pool")
type PoolRouter struct {
	RouterActor
	nrOfInstances int
	props         Props
}

// NrOfInstances returns the current configured pool size (the count that was
// set at creation or updated by AdjustPoolSize). It reflects the target size,
// not the number of currently live routees.
func (r *PoolRouter) NrOfInstances() int { return r.nrOfInstances }

// NewPoolRouter creates a PoolRouter that will spawn nrOfInstances children
// in PreStart, routing normal messages using logic.
func NewPoolRouter(logic RoutingLogic, nrOfInstances int, props Props) *PoolRouter {
	return &PoolRouter{
		RouterActor:   RouterActor{BaseActor: NewBaseActor(), Logic: logic},
		nrOfInstances: nrOfInstances,
		props:         props,
	}
}

// PreStart is called by Start before the first message is delivered.
// It spawns nrOfInstances child actors and registers this pool as a watcher
// of each, so the pool receives a TerminatedMessage when any child stops.
func (r *PoolRouter) PreStart() {
	sys := r.System()
	if sys == nil || r.nrOfInstances <= 0 || r.props.New == nil {
		return
	}
	r.Routees = make([]Ref, 0, r.nrOfInstances)
	for i := 0; i < r.nrOfInstances; i++ {
		name := fmt.Sprintf("$pool-%d", i)
		ref, err := sys.ActorOf(r.props, name)
		if err != nil {
			continue
		}
		r.Routees = append(r.Routees, ref)
		sys.Watch(r.Self(), ref)
	}
}

// Receive handles lifecycle and management messages before delegating to the
// embedded RouterActor.
//
//   - TerminatedMessage: removes the stopped routee from the active list.
//   - AdjustPoolSize: spawns additional children (positive Delta only).
//   - Everything else: forwarded via RouterActor.Receive.
func (r *PoolRouter) Receive(msg any) {
	switch m := msg.(type) {
	case TerminatedMessage:
		// A watched child has stopped — evict it from the routee list.
		r.RemoveRoutee(m.TerminatedActor())

	case AdjustPoolSize:
		// Grow-only resize for now; shrink support is future work.
		if m.Delta <= 0 || r.System() == nil || r.props.New == nil {
			return
		}
		for i := 0; i < m.Delta; i++ {
			name := fmt.Sprintf("$pool-%d", r.nrOfInstances+i)
			ref, err := r.System().ActorOf(r.props, name)
			if err != nil {
				continue
			}
			r.Routees = append(r.Routees, ref)
			r.System().Watch(r.Self(), ref)
		}
		r.nrOfInstances += m.Delta

	default:
		r.RouterActor.Receive(msg)
	}
}

// ── Routing strategies ────────────────────────────────────────────────────────

// RoundRobinRoutingLogic distributes messages to routees in a rotating,
// sequential order. The counter is managed with sync/atomic so multiple
// goroutines can call Select concurrently without a mutex.
type RoundRobinRoutingLogic struct {
	counter atomic.Uint64
}

// Select returns the next routee in the round-robin sequence.
func (l *RoundRobinRoutingLogic) Select(_ any, routees []Ref) Ref {
	if len(routees) == 0 {
		return nil
	}
	idx := l.counter.Add(1) - 1
	return routees[idx%uint64(len(routees))]
}

// RandomRoutingLogic selects a uniformly random routee for each message.
// It is safe for concurrent use from multiple goroutines (Go 1.20+
// global rand source is automatically seeded and goroutine-safe).
type RandomRoutingLogic struct{}

// Select returns a randomly chosen routee, or nil when routees is empty.
func (l *RandomRoutingLogic) Select(_ any, routees []Ref) Ref {
	if len(routees) == 0 {
		return nil
	}
	return routees[rand.Intn(len(routees))]
}

// BroadcastRoutingLogic is a special routing logic that indicates the message
// should be delivered to all routees. While RouterActor handles Broadcast
// messages explicitly, this logic allows configuring a router to always
// broadcast every message it receives.
type BroadcastRoutingLogic struct{}

// Select returns nil for BroadcastRoutingLogic because it doesn't select a
// single routee; the RouterActor must handle this logic specifically.
func (l *BroadcastRoutingLogic) Select(_ any, _ []Ref) Ref {
	return nil
}

// ── Consistent Hashing ───────────────────────────────────────────────────────

// ConsistentHashable is implemented by messages that want to provide an
// explicit key for consistent-hash routing. If a message does not implement
// this interface, the message itself is used as the key.
type ConsistentHashable interface {
	// ConsistentHashKey returns the value used to hash this message.
	ConsistentHashKey() any
}

type nodeHash struct {
	hash uint32
	ref  Ref
}

// ConsistentHashRoutingLogic maps messages to routees based on a hash key.
// Messages with the same key are consistently delivered to the same routee,
// provided the set of routees remains stable.
//
// It follows the standard consistent hashing algorithm with virtual nodes
// for improved distribution balance.
type ConsistentHashRoutingLogic struct {
	// VirtualNodesFactor determines how many tokens each routee gets on the
	// hash ring (default 10). A higher factor improves balance but increases
	// memory and CPU for ring rebuilding.
	VirtualNodesFactor int

	mu              sync.Mutex
	ring            []nodeHash
	lastRouteesHash uint64
}

// Select returns a routee mapped from the message's hash key.
func (l *ConsistentHashRoutingLogic) Select(message any, routees []Ref) Ref {
	if len(routees) == 0 {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// 1. Rebuild ring if routees changed
	h := l.calculateRouteesHash(routees)
	if h != l.lastRouteesHash || len(l.ring) == 0 {
		l.rebuildRing(routees)
		l.lastRouteesHash = h
	}

	// 2. Extract key
	var key any = message
	if ch, ok := message.(ConsistentHashable); ok {
		key = ch.ConsistentHashKey()
	}

	// 3. Hash the key
	keyHash := l.hashKey(key)

	// 4. Binary search on the ring
	idx := sort.Search(len(l.ring), func(i int) bool {
		return l.ring[i].hash >= keyHash
	})

	if idx == len(l.ring) {
		idx = 0
	}
	return l.ring[idx].ref
}

func (l *ConsistentHashRoutingLogic) calculateRouteesHash(routees []Ref) uint64 {
	// Simple hash of paths and length for change detection.
	var h uint64 = uint64(len(routees))
	for _, r := range routees {
		p := r.Path()
		for i := 0; i < len(p); i++ {
			h = h*31 + uint64(p[i])
		}
	}
	return h
}

func (l *ConsistentHashRoutingLogic) rebuildRing(routees []Ref) {
	vnodes := l.VirtualNodesFactor
	if vnodes <= 0 {
		vnodes = 100
	}

	l.ring = make([]nodeHash, 0, len(routees)*vnodes)
	for _, r := range routees {
		path := r.Path()
		for i := 0; i < vnodes; i++ {
			// Virtual node key is "path#index"
			vnodeKey := fmt.Sprintf("%s#%d", path, i)
			h := crc32.ChecksumIEEE([]byte(vnodeKey))
			l.ring = append(l.ring, nodeHash{hash: h, ref: r})
		}
	}

	// Sort by hash to enable binary search.
	sort.Slice(l.ring, func(i, j int) bool {
		return l.ring[i].hash < l.ring[j].hash
	})
}

func (l *ConsistentHashRoutingLogic) hashKey(key any) uint32 {
	switch v := key.(type) {
	case string:
		return crc32.ChecksumIEEE([]byte(v))
	case []byte:
		return crc32.ChecksumIEEE(v)
	case int, int32, int64, uint, uint32, uint64:
		return crc32.ChecksumIEEE([]byte(fmt.Sprint(v)))
	default:
		return crc32.ChecksumIEEE([]byte(fmt.Sprintf("%v", v)))
	}
}

// ── Group router ──────────────────────────────────────────────────────────────

// GroupRouter routes messages to a fixed set of pre-existing actor references.
// Unlike PoolRouter it does not spawn or supervise routees — it treats them as
// external actors whose lifecycle it does not control.
//
// Routees can be supplied either as live Refs (NewGroupRouter) or as path
// strings resolved at startup time (NewGroupRouterWithPaths). Path resolution
// is performed during PreStart using ActorContext.Resolve; any path that
// cannot be resolved at that moment is silently skipped.
//
// Usage — explicit refs:
//
//	group := actor.NewGroupRouter(
//	    &actor.RoundRobinRoutingLogic{},
//	    []actor.Ref{ref1, ref2, ref3},
//	)
//	node.System.ActorOf(actor.Props{New: func() actor.Actor { return group }}, "group")
//
// Usage — HOCON deployment (paths resolved automatically by the system):
//
//	pekko.actor.deployment {
//	  "/user/group" {
//	    router        = round-robin-group
//	    routees.paths = ["/user/worker1", "/user/worker2"]
//	  }
//	}
type GroupRouter struct {
	RouterActor
	Paths []string // paths resolved in PreStart; empty when refs were supplied directly
}

// NewGroupRouter creates a GroupRouter pre-loaded with the supplied routee Refs.
// The slice is copied so later mutations by the caller do not affect the router.
func NewGroupRouter(logic RoutingLogic, routees []Ref) *GroupRouter {
	cp := make([]Ref, len(routees))
	copy(cp, routees)
	return &GroupRouter{
		RouterActor: RouterActor{BaseActor: NewBaseActor(), Logic: logic, Routees: cp},
	}
}

// NewGroupRouterWithPaths creates a GroupRouter that resolves its routees from
// path strings during PreStart. Paths are looked up via ActorContext.Resolve;
// unreachable paths are skipped without error.
func NewGroupRouterWithPaths(logic RoutingLogic, paths []string) *GroupRouter {
	cp := make([]string, len(paths))
	copy(cp, paths)
	return &GroupRouter{
		RouterActor: RouterActor{BaseActor: NewBaseActor(), Logic: logic},
		Paths:       cp,
	}
}

// RouteePathsForTest returns a snapshot of the configured routee paths.
// Intended for test assertions; prefer Routees() for the resolved live refs.
func (r *GroupRouter) RouteePathsForTest() []string {
	cp := make([]string, len(r.Paths))
	copy(cp, r.Paths)
	return cp
}

// PreStart resolves routeePaths into live Refs via ActorContext.Resolve.
// Paths that cannot be resolved (e.g. actor not yet started) are skipped.
// If routeePaths is empty (refs were supplied at construction time) this is
// a no-op.
func (r *GroupRouter) PreStart() {
	sys := r.System()
	if sys == nil || len(r.Paths) == 0 {
		return
	}
	for _, path := range r.Paths {
		ref, err := sys.Resolve(path)
		if err != nil {
			continue
		}
		r.Routees = append(r.Routees, ref)
	}
}

// NewScatterGatherPool returns a PoolRouter using ScatterGatherRoutingLogic.
func NewScatterGatherPool(nrOfInstances int, props Props, within time.Duration) *PoolRouter {
	return NewPoolRouter(&ScatterGatherRoutingLogic{Within: within}, nrOfInstances, props)
}

// NewTailChoppingGroup returns a GroupRouter using TailChoppingRoutingLogic.
func NewTailChoppingGroup(routees []Ref, within time.Duration) *GroupRouter {
	return NewGroupRouter(&TailChoppingRoutingLogic{Within: within}, routees)
}

// ── Router Factories ─────────────────────────────────────────────────────

// Send resolves the path and delivers the message.
// It automatically detects the appropriate serializer and manifest based on the message type.
func (r *Router) Send(ctx context.Context, path string, msg any) error {
	ap, err := ParseActorPath(path)
	if err != nil {
		return err
	}

	// 1. Check if local
	local := r.provider.LocalAddress()
	if ap.Address.System == local.GetSystem() && ap.Address.Host == local.GetHostname() && uint32(ap.Address.Port) == local.GetPort() {
		log.Printf("Router: local delivery to %s", ap.Path())
		// In a real system, this would go to the local actor's mailbox.
		return nil
	}

	// 2. Remote delivery
	targetAddr := ap.Address.ToProto()

	assoc, ok := r.provider.GetAssociationByHost(targetAddr.GetHostname(), targetAddr.GetPort())
	if !ok {
		log.Printf("Router: initiating new connection to %s:%d", targetAddr.GetHostname(), targetAddr.GetPort())
		var err error
		assoc, err = r.provider.DialRemote(ctx, targetAddr)
		if err != nil {
			return fmt.Errorf("failed to dial remote: %w", err)
		}
	}

	// 3. Serialize and send
	payload, serializerId, manifest, err := r.prepareMessage(msg)
	if err != nil {
		return err
	}

	if assoc == nil {
		return fmt.Errorf("failed to dial remote: association is nil")
	}

	// Track user message metrics (cluster-internal messages are excluded).
	if metrics := r.provider.Metrics(); metrics != nil &&
		serializerId != ClusterSerializerID &&
		serializerId != ArteryInternalSerializerID {
		metrics.IncrementMessagesSent()
		metrics.IncrementBytesSent(int64(len(payload)))
	}

	return assoc.Send(path, payload, serializerId, manifest)
}

// SendWithSender resolves the path, serializes msg, and delivers it to the remote
// actor with senderPath set as the Artery sender (used by the Ask pattern so the
// remote actor can reply to the temporary sender path).
func (r *Router) SendWithSender(ctx context.Context, path string, senderPath string, msg any) error {
	ap, err := ParseActorPath(path)
	if err != nil {
		return err
	}

	targetAddr := ap.Address.ToProto()
	assoc, ok := r.provider.GetAssociationByHost(targetAddr.GetHostname(), targetAddr.GetPort())
	if !ok {
		var err error
		assoc, err = r.provider.DialRemote(ctx, targetAddr)
		if err != nil {
			return fmt.Errorf("failed to dial remote: %w", err)
		}
	}

	payload, serializerId, manifest, err := r.prepareMessage(msg)
	if err != nil {
		return err
	}

	if assoc == nil {
		return fmt.Errorf("failed to dial remote: association is nil")
	}

	// SendWithSender is only used by the Ask pattern — always a user message.
	if metrics := r.provider.Metrics(); metrics != nil {
		metrics.IncrementMessagesSent()
		metrics.IncrementBytesSent(int64(len(payload)))
	}

	return assoc.SendWithSender(path, senderPath, payload, serializerId, manifest)
}

func (r *Router) prepareMessage(msg any) ([]byte, int32, string, error) {
	var payload []byte
	var errSerialize error
	var sid int32
	var manifest string

	msgType := reflect.TypeOf(msg)
	switch msgType {
	case reflect.TypeOf((*gproto_cluster.InitJoin)(nil)):
		sid, manifest = ClusterSerializerID, "IJ"
	case reflect.TypeOf((*gproto_cluster.InitJoinAck)(nil)):
		sid, manifest = ClusterSerializerID, "IJA"
	case reflect.TypeOf((*gproto_cluster.Join)(nil)):
		sid, manifest = ClusterSerializerID, "J"
	case reflect.TypeOf((*gproto_cluster.Welcome)(nil)):
		sid, manifest = ClusterSerializerID, "W"
	case reflect.TypeOf((*gproto_cluster.Heartbeat)(nil)):
		sid, manifest = ClusterSerializerID, "HB"
	case reflect.TypeOf((*gproto_cluster.HeartBeatResponse)(nil)):
		sid, manifest = ClusterSerializerID, "HBR"
	case reflect.TypeOf((*gproto_cluster.GossipStatus)(nil)):
		sid, manifest = ClusterSerializerID, "GS"
	case reflect.TypeOf((*gproto_cluster.GossipEnvelope)(nil)):
		sid, manifest = ClusterSerializerID, "GE"
	case reflect.TypeOf((*gproto_cluster.Address)(nil)):
		sid, manifest = ClusterSerializerID, "L"
	case reflect.TypeOf((*gproto_cluster.UniqueAddress)(nil)):
		// "EC" = ExitingConfirmed. Pekko's ClusterCoreDaemon adds the
		// node UID to its `exitingConfirmed` set, which is a precondition
		// for the leader's Exiting → Removed transition. The wire format
		// is the UniqueAddress proto bytes.
		sid, manifest = ClusterSerializerID, "EC"
	default:
		if rs, ok := msg.(RemoteSerializable); ok {
			// Custom message type with its own serializer ID and manifest.
			sid = rs.ArterySerializerID()
			manifest = rs.ArteryManifest()
		} else if _, isProto := msg.(proto.Message); isProto {
			sid = 2
			manifest = msgType.String()
		} else if _, isBytes := msg.([]byte); isBytes {
		        sid = 4
		        manifest = ""
		} else if msgType.Kind() == reflect.String {
		        sid = StringSerializerID
		        manifest = ""
		} else {
		        sid = JSONSerializerID
		        manifest = msgType.String()
		}
	}

	if manifest == "W" {
		if pmsg, ok := msg.(proto.Message); ok {
			raw, err := proto.Marshal(pmsg)
			if err != nil {
				return nil, 0, "", fmt.Errorf("marshal Welcome: %w", err)
			}
			payload, errSerialize = gzipCompress(raw)
		}
	} else if pmsg, ok := msg.(proto.Message); ok {
		payload, errSerialize = proto.Marshal(pmsg)
	} else if b, ok := msg.([]byte); ok {
		payload = b
	} else {
		s, err := r.provider.Serializer(sid)
		if err == nil {
			payload, errSerialize = s.ToBinary(msg)
		} else {
			errSerialize = err
		}
	}

	if errSerialize != nil {
		return nil, 0, "", fmt.Errorf("serialize error: %w", errSerialize)
	}
	return payload, sid, manifest, nil
}

func gzipCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
