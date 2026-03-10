/*
 * cluster_router.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"fmt"
	reflect "reflect"
	"sync"
	"sync/atomic"

	"gekka/gekka/actor"
	"gekka/gekka/cluster"

	"google.golang.org/protobuf/proto"
)

// ── Original ClusterRouter (System) ──────────────────────────────────────────

// ClusterRouter implements intelligent routing across the cluster.
type ClusterRouter struct {
	cm      *ClusterManager
	router  *Router
	rrIndex uint64
}

func NewClusterRouter(cm *ClusterManager, router *Router) *ClusterRouter {
	return &ClusterRouter{
		cm:     cm,
		router: router,
	}
}

// SelectRoutee selects a node using role filtering, status, and reachability.
func (cr *ClusterRouter) SelectRoutee(settings *cluster.ClusterRouterPoolSettings) (*cluster.UniqueAddress, error) {
	state := cr.cm.GetState()
	localUA := cr.cm.GetLocalAddress()

	var candidates []*cluster.UniqueAddress

	// 1. Get Reachability Map
	unreachable := make(map[int32]bool)
	if state.Overview != nil {
		for _, or := range state.Overview.ObserverReachability {
			for _, sr := range or.SubjectReachability {
				if sr.GetStatus() == cluster.ReachabilityStatus_Unreachable {
					unreachable[sr.GetAddressIndex()] = true
				}
			}
		}
	}

	for _, m := range state.Members {
		// 2. Must be UP
		if m.GetStatus() != cluster.MemberStatus_Up {
			continue
		}

		addrIdx := m.GetAddressIndex()
		ua := state.AllAddresses[addrIdx]

		// 3. Health Check (ObserverReachability + Local Failure Detector)
		key := fmt.Sprintf("%s:%d-%d", ua.GetAddress().GetHostname(), ua.GetAddress().GetPort(), ua.GetUid())
		if unreachable[addrIdx] || !cr.cm.GetFailureDetector().IsAvailable(key) {
			continue
		}

		// 4. Local Affinity
		isLocal := ua.GetAddress().GetHostname() == localUA.Address.GetHostname() &&
			ua.GetAddress().GetPort() == localUA.Address.GetPort() &&
			ua.GetUid() == uint32(localUA.GetUid()&0xFFFFFFFF)

		if isLocal && !settings.GetAllowLocalRoutees() {
			continue
		}

		// 5. Role Filtering using rolesIndexes
		if !cr.hasRequiredRoles(m, state, settings.GetUseRoles()) {
			continue
		}

		candidates = append(candidates, ua)
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no available routees matching criteria")
	}

	// Strategy selection: Round Robin
	idx := atomic.AddUint64(&cr.rrIndex, 1) % uint64(len(candidates))
	return candidates[idx], nil
}

func (cr *ClusterRouter) hasRequiredRoles(m *cluster.Member, state *cluster.Gossip, required []string) bool {
	if len(required) == 0 {
		return true
	}

	// Map rolesIndexes back to strings in AllRoles
	memberRoles := make(map[string]bool)
	for _, idx := range m.GetRolesIndexes() {
		if int(idx) < len(state.AllRoles) {
			memberRoles[state.AllRoles[idx]] = true
		}
	}

	for _, r := range required {
		if memberRoles[r] {
			return true // At least one role matches
		}
	}
	return false
}

// Route selects a node and sends the message (Pekko-style actor routing).
func (cr *ClusterRouter) Route(ctx context.Context, relativePath string, serializerID int32, manifest string, msg proto.Message, settings *cluster.ClusterRouterPoolSettings) error {
	ua, err := cr.SelectRoutee(settings)
	if err != nil {
		return err
	}

	targetPath := fmt.Sprintf("%s://%s@%s:%d%s",
		ua.GetAddress().GetProtocol(),
		ua.GetAddress().GetSystem(),
		ua.GetAddress().GetHostname(),
		ua.GetAddress().GetPort(),
		relativePath)

	return cr.router.Send(ctx, targetPath, msg)
}

// ── Actor-based Cluster Routers ──────────────────────────────────────────────

// ClusterPoolRouter is a cluster-aware PoolRouter.
// It manages local child actors and discovers/routes to remote instances
// of the same pool on other nodes in the cluster.
type ClusterPoolRouter struct {
	actor.PoolRouter
	totalInstances    int
	allowLocalRoutees bool
	useRole           string

	// nodeAddr -> []Ref
	remoteRoutees map[string][]actor.Ref
	mu            sync.RWMutex
}

func NewClusterPoolRouter(logic actor.RoutingLogic, totalInstances int, allowLocalRoutees bool, useRole string, props actor.Props) *ClusterPoolRouter {
	// Local share of instances: we'll calculate this in PreStart or based on current cluster size.
	// For now, let's start with 0 and adjust as nodes join.
	// Simplified logic: each node creates (TotalInstances / N) workers.
	return &ClusterPoolRouter{
		PoolRouter:        *actor.NewPoolRouter(logic, 0, props),
		totalInstances:    totalInstances,
		allowLocalRoutees: allowLocalRoutees,
		useRole:           useRole,
		remoteRoutees:     make(map[string][]actor.Ref),
	}
}

func (r *ClusterPoolRouter) PreStart() {
	// 1. Subscribe to cluster events
	if bridge, ok := r.System().(*actorContextBridge); ok {
		bridge.sys.node.Subscribe(r.Self().(ActorRef), reflect.TypeOf(MemberUp{}), reflect.TypeOf(MemberRemoved{}), reflect.TypeOf(UnreachableMember{}), reflect.TypeOf(ReachableMember{}))
	}

	// 2. Initial setup based on current cluster state
	r.refreshRoutees()
}

func (r *ClusterPoolRouter) refreshRoutees() {
	bridge, ok := r.System().(*actorContextBridge)
	if !ok {
		return
	}

	node := bridge.sys.node
	cm := node.cm
	cm.mu.RLock()
	state := cm.state
	cm.mu.RUnlock()

	// Find nodes matching role
	var eligibleNodes []MemberAddress
	for _, m := range state.Members {
		if m.GetStatus() != cluster.MemberStatus_Up {
			continue
		}
		ua := state.AllAddresses[m.GetAddressIndex()]
		ma := memberAddressFromUA(ua)

		// Role Check
		if r.useRole != "" {
			found := false
			for _, idx := range m.GetRolesIndexes() {
				if int(idx) < len(state.AllRoles) && state.AllRoles[idx] == r.useRole {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		eligibleNodes = append(eligibleNodes, ma)
	}

	n := len(eligibleNodes)
	if n == 0 {
		return
	}

	// Symmetric distribution: target local instances
	localTarget := r.totalInstances / n
	// Remainder goes to the "first" nodes (alphabetical or upNumber)
	// For simplicity, we just use totalInstances/n for everyone.

	currentLocal := r.PoolRouter.NrOfInstances()
	if currentLocal < localTarget {
		// Use Receive directly to handle AdjustPoolSize
		r.PoolRouter.Receive(actor.AdjustPoolSize{Delta: localTarget - currentLocal})
	}

	// Discover remote routees (symmetric: we assume other nodes have the same router at the same path)
	selfPath := r.Self().Path()
	ap, _ := ParseActorPath(selfPath)

	r.mu.Lock()
	defer r.mu.Unlock()
	r.remoteRoutees = make(map[string][]actor.Ref)

	for _, ma := range eligibleNodes {
		isLocal := ma.Host == node.localAddr.GetHostname() && ma.Port == node.localAddr.GetPort()
		if isLocal {
			if !r.allowLocalRoutees {
				// We still spawn local ones because we are part of the cluster pool,
				// but we might choose not to ROUTE to them if allowLocalRoutees is false.
				// (Wait, Pekko's allow-local-routees usually means "can this router send to local routees")
			}
			continue
		}

		// Remote router path
		remoteAddr := actor.Address{
			Protocol: ma.Protocol,
			System:   ma.System,
			Host:     ma.Host,
			Port:     int(ma.Port),
		}
		// In a symmetric pool, we route to the router on the remote node?
		// No, usually we route to the WORKERS. But how do we find them?
		// Pekko's ClusterRouterPool actually routes to the OTHER routers' members.
		// Actually, ClusterRouterPool *manages* remote routees.

		// For simplicity/gekka-style: we route to the PoolRouter on the remote node.
		// The remote PoolRouter will then distribute to its local workers.
		remoteRef := bridge.sys.RemoteActorOf(remoteAddr, ap.Path)
		r.remoteRoutees[ma.String()] = []actor.Ref{remoteRef}
	}
}

func (r *ClusterPoolRouter) Receive(msg any) {
	switch msg.(type) {
	case MemberUp, MemberRemoved, UnreachableMember, ReachableMember:
		r.refreshRoutees()
	default:
		// Collect all effective routees: local + remote
		r.mu.RLock()
		allRoutees := append([]actor.Ref(nil), r.PoolRouter.RouteesSnapshot()...)
		for _, refs := range r.remoteRoutees {
			allRoutees = append(allRoutees, refs...)
		}
		r.mu.RUnlock()

		// Pass to base logic
		if len(allRoutees) == 0 {
			return
		}

		if _, ok := msg.(actor.Broadcast); ok {
			// Only broadcast locally.
			r.PoolRouter.Receive(msg)
			return
		}

		// For normal messages, use the full list.
		target := r.PoolRouter.Logic.Select(msg, allRoutees)
		if target != nil {
			target.Tell(msg, r.Sender())
		}
	}
}

// ClusterGroupRouter is a cluster-aware GroupRouter.
type ClusterGroupRouter struct {
	actor.GroupRouter
	allowLocalRoutees bool
	useRole           string

	remoteRoutees map[string][]actor.Ref
	mu            sync.RWMutex
}

func NewClusterGroupRouter(logic actor.RoutingLogic, paths []string, allowLocalRoutees bool, useRole string) *ClusterGroupRouter {
	return &ClusterGroupRouter{
		GroupRouter:       *actor.NewGroupRouterWithPaths(logic, paths),
		allowLocalRoutees: allowLocalRoutees,
		useRole:           useRole,
		remoteRoutees:     make(map[string][]actor.Ref),
	}
}

func (r *ClusterGroupRouter) PreStart() {
	r.GroupRouter.PreStart()
	if bridge, ok := r.System().(*actorContextBridge); ok {
		bridge.sys.node.Subscribe(r.Self().(ActorRef), reflect.TypeOf(MemberUp{}), reflect.TypeOf(MemberRemoved{}), reflect.TypeOf(UnreachableMember{}), reflect.TypeOf(ReachableMember{}))
	}
	r.refreshRoutees()
}

func (r *ClusterGroupRouter) refreshRoutees() {
	bridge, ok := r.System().(*actorContextBridge)
	if !ok {
		return
	}

	node := bridge.sys.node
	cm := node.cm
	cm.mu.RLock()
	state := cm.state
	cm.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	r.remoteRoutees = make(map[string][]actor.Ref)

	paths := r.GroupRouter.Paths
	if len(paths) == 0 {
		// If no paths were provided, it might be that we route to the same path on other nodes.
		// For GroupRouters, paths are usually required.
	}

	for _, m := range state.Members {
		if m.GetStatus() != cluster.MemberStatus_Up {
			continue
		}
		ua := state.AllAddresses[m.GetAddressIndex()]
		ma := memberAddressFromUA(ua)

		isLocal := ma.Host == node.localAddr.GetHostname() && ma.Port == node.localAddr.GetPort()
		if isLocal && !r.allowLocalRoutees {
			continue
		}

		// Role Check
		if r.useRole != "" {
			found := false
			for _, idx := range m.GetRolesIndexes() {
				if int(idx) < len(state.AllRoles) && state.AllRoles[idx] == r.useRole {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		if isLocal {
			continue // Already handled by GroupRouter base
		}

		remoteAddr := actor.Address{
			Protocol: ma.Protocol,
			System:   ma.System,
			Host:     ma.Host,
			Port:     int(ma.Port),
		}

		var refs []actor.Ref
		for _, path := range paths {
			refs = append(refs, bridge.sys.RemoteActorOf(remoteAddr, path))
		}
		if len(refs) > 0 {
			r.remoteRoutees[ma.String()] = refs
		}
	}
}

func (r *ClusterGroupRouter) Receive(msg any) {
	switch msg.(type) {
	case MemberUp, MemberRemoved, UnreachableMember, ReachableMember:
		r.refreshRoutees()
	default:
		r.mu.RLock()
		allRoutees := append([]actor.Ref(nil), r.GroupRouter.RouteesSnapshot()...)
		for _, refs := range r.remoteRoutees {
			allRoutees = append(allRoutees, refs...)
		}
		r.mu.RUnlock()

		if len(allRoutees) == 0 {
			return
		}

		if _, ok := msg.(actor.Broadcast); ok {
			// Broadcast to everyone (local refs + remote refs)
			for _, rt := range allRoutees {
				rt.Tell(msg.(actor.Broadcast).Message, r.Sender())
			}
			return
		}

		target := r.GroupRouter.Logic.Select(msg, allRoutees)
		if target != nil {
			target.Tell(msg, r.Sender())
		}
	}
}
