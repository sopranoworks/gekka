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
	"sync/atomic"

	"gekka/gekka/cluster"

	"google.golang.org/protobuf/proto"
)

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
