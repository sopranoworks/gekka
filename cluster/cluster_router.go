/*
 * cluster_router.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"context"
	"fmt"
	reflect "reflect"
	"sync"
	"sync/atomic"

	"github.com/sopranoworks/gekka/actor"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"

	"google.golang.org/protobuf/proto"
)

// Router defines the interface for message delivery, decoupled from the root package.
type Router interface {
	Send(ctx context.Context, path string, msg any) error
	SendWithSender(ctx context.Context, path string, senderPath string, msg any) error
}

// ── Original ClusterRouter (System) ──────────────────────────────────────────

// ClusterRouter implements intelligent routing across the cluster.
type ClusterRouter struct {
	cm      *ClusterManager
	router  Router
	rrIndex uint64
}

func NewClusterRouter(cm *ClusterManager, router Router) *ClusterRouter {
	return &ClusterRouter{
		cm:     cm,
		router: router,
	}
}

// SelectRoutee selects a node using role filtering, status, and reachability.
func (cr *ClusterRouter) SelectRoutee(settings *gproto_cluster.ClusterRouterPoolSettings) (*gproto_cluster.UniqueAddress, error) {
	state := cr.cm.GetState()
	localUA := cr.cm.GetLocalAddress()

	var candidates []*gproto_cluster.UniqueAddress

	// 1. Get Reachability Map
	unreachable := make(map[int32]bool)
	if state.Overview != nil {
		for _, or := range state.Overview.ObserverReachability {
			for _, sr := range or.SubjectReachability {
				if sr.GetStatus() == gproto_cluster.ReachabilityStatus_Unreachable {
					unreachable[sr.GetAddressIndex()] = true
				}
			}
		}
	}

	for _, m := range state.Members {
		// 2. Must be UP
		if m.GetStatus() != gproto_cluster.MemberStatus_Up {
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

func (cr *ClusterRouter) hasRequiredRoles(m *gproto_cluster.Member, state *gproto_cluster.Gossip, required []string) bool {
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
func (cr *ClusterRouter) Route(ctx context.Context, relativePath string, serializerID int32, manifest string, msg proto.Message, settings *gproto_cluster.ClusterRouterPoolSettings) error {
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
	cm                *ClusterManager

	// nodeAddr -> []Ref
	remoteRoutees map[string][]actor.Ref
	Mu            sync.RWMutex
}

func NewClusterPoolRouter(cm *ClusterManager, logic actor.RoutingLogic, totalInstances int, allowLocalRoutees bool, useRole string, props actor.Props) *ClusterPoolRouter {
	return &ClusterPoolRouter{
		PoolRouter:        *actor.NewPoolRouter(logic, 0, props),
		totalInstances:    totalInstances,
		allowLocalRoutees: allowLocalRoutees,
		useRole:           useRole,
		cm:                cm,
		remoteRoutees:     make(map[string][]actor.Ref),
	}
}

// NewClusterPoolRouterWithResizer creates a ClusterPoolRouter with an automatic
// Resizer attached to the embedded PoolRouter.
func NewClusterPoolRouterWithResizer(cm *ClusterManager, logic actor.RoutingLogic, totalInstances int, allowLocalRoutees bool, useRole string, props actor.Props, resizer actor.Resizer) *ClusterPoolRouter {
	r := NewClusterPoolRouter(cm, logic, totalInstances, allowLocalRoutees, useRole, props)
	r.PoolRouter.Resizer = resizer
	return r
}

// PoolSize returns the current number of local routees in the embedded PoolRouter.
func (r *ClusterPoolRouter) PoolSize() int {
	return r.PoolRouter.NrOfInstances()
}

func (r *ClusterPoolRouter) PreStart() {
	// 1. Subscribe to cluster events
	r.cm.Subscribe(r.Self(), reflect.TypeOf(MemberUp{}), reflect.TypeOf(MemberRemoved{}), reflect.TypeOf(UnreachableMember{}), reflect.TypeOf(ReachableMember{}))

	// 2. Initial setup based on current cluster state
	r.refreshRoutees()
}

func (r *ClusterPoolRouter) refreshRoutees() {
	cm := r.cm
	cm.Mu.RLock()
	state := cm.State
	cm.Mu.RUnlock()

	// Find nodes matching role
	var eligibleNodes []MemberAddress
	for _, m := range state.Members {
		if m.GetStatus() != gproto_cluster.MemberStatus_Up {
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
	ap, _ := actor.ParseActorPath(selfPath)

	r.Mu.Lock()
	defer r.Mu.Unlock()
	r.remoteRoutees = make(map[string][]actor.Ref)

	for _, ma := range eligibleNodes {
		isLocal := ma.Host == cm.LocalAddress.Address.GetHostname() && ma.Port == cm.LocalAddress.Address.GetPort()
		if isLocal {
			continue
		}

		// Remote router path
		remoteAddr := actor.Address{
			Protocol: ma.Protocol,
			System:   ma.System,
			Host:     ma.Host,
			Port:     int(ma.Port),
		}
		// In a symmetric pool, we route to the PoolRouter on the remote node.
		// The remote PoolRouter will then distribute to its local workers.
		remoteRef, _ := r.cm.Sys.Resolve(remoteAddr.String() + ap.Path())
		r.remoteRoutees[ma.String()] = []actor.Ref{remoteRef}
	}
}

func (r *ClusterPoolRouter) Receive(msg any) {
	switch msg.(type) {
	case MemberUp, MemberRemoved, UnreachableMember, ReachableMember:
		r.refreshRoutees()
	default:
		// Collect all effective routees: local + remote
		r.Mu.RLock()
		allRoutees := append([]actor.Ref(nil), r.PoolRouter.RouteesSnapshot()...)
		for _, refs := range r.remoteRoutees {
			allRoutees = append(allRoutees, refs...)
		}
		r.Mu.RUnlock()

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
		r.PoolRouter.EvaluateResizeIfNeeded()
	}
}

// ClusterGroupRouter is a cluster-aware GroupRouter.
type ClusterGroupRouter struct {
	actor.GroupRouter
	allowLocalRoutees bool
	useRole           string
	cm                *ClusterManager

	remoteRoutees map[string][]actor.Ref
	Mu            sync.RWMutex
}

func NewClusterGroupRouter(cm *ClusterManager, logic actor.RoutingLogic, paths []string, allowLocalRoutees bool, useRole string) *ClusterGroupRouter {
	return &ClusterGroupRouter{
		GroupRouter:       *actor.NewGroupRouterWithPaths(logic, paths),
		allowLocalRoutees: allowLocalRoutees,
		useRole:           useRole,
		cm:                cm,
		remoteRoutees:     make(map[string][]actor.Ref),
	}
}

func (r *ClusterGroupRouter) PreStart() {
	r.GroupRouter.PreStart()
	r.cm.Subscribe(r.Self(), reflect.TypeOf(MemberUp{}), reflect.TypeOf(MemberRemoved{}), reflect.TypeOf(UnreachableMember{}), reflect.TypeOf(ReachableMember{}))
	r.refreshRoutees()
}

func (r *ClusterGroupRouter) refreshRoutees() {
	cm := r.cm
	cm.Mu.RLock()
	state := cm.State
	cm.Mu.RUnlock()

	r.Mu.Lock()
	defer r.Mu.Unlock()
	r.remoteRoutees = make(map[string][]actor.Ref)

	paths := r.GroupRouter.Paths

	for _, m := range state.Members {
		if m.GetStatus() != gproto_cluster.MemberStatus_Up {
			continue
		}
		ua := state.AllAddresses[m.GetAddressIndex()]
		ma := memberAddressFromUA(ua)

		isLocal := ma.Host == r.cm.LocalAddress.Address.GetHostname() && ma.Port == r.cm.LocalAddress.Address.GetPort()
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
			ref, _ := r.cm.Sys.Resolve(remoteAddr.String() + path)
			refs = append(refs, ref)
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
		r.Mu.RLock()
		allRoutees := append([]actor.Ref(nil), r.GroupRouter.RouteesSnapshot()...)
		for _, refs := range r.remoteRoutees {
			allRoutees = append(allRoutees, refs...)
		}
		r.Mu.RUnlock()

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
