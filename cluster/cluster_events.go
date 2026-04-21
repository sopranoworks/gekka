/*
 * cluster_events.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/sopranoworks/gekka/actor"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
)

// ── Event types ───────────────────────────────────────────────────────────────

// ClusterDomainEvent is the marker interface for all cluster lifecycle events.
// Subscribe to events with Cluster.Subscribe; receive them on a buffered
// channel and type-switch to handle specific transitions.
//
//	ch := make(chan gekka.ClusterDomainEvent, 16)
//	node.Subscribe(ch, gekka.EventMemberUp, gekka.EventMemberRemoved)
//	go func() {
//	    for evt := range ch {
//	        switch e := evt.(type) {
//	        case gekka.MemberUp:
//	            log.Printf("member up: %s", e.Member)
//	        case gekka.MemberRemoved:
//	            log.Printf("member removed: %s", e.Member)
//	        }
//	    }
//	}()
type ClusterDomainEvent interface {
	clusterDomainEvent() // unexported — prevents external types from implementing
}

// MemberAddress identifies the cluster member that an event concerns.
type MemberAddress struct {
	Protocol   string     // "pekko" or "akka"
	System     string     // actor system name, e.g. "ClusterSystem"
	Host       string     // hostname or IP
	Port       uint32     // TCP port
	DataCenter string     // data-center name, e.g. "us-east" (from "dc-us-east" role); "default" when unset
	AppVersion AppVersion // application version for rolling updates; zero when unset
}

// String returns the member's address in Artery URI form ("pekko://System@host:port").
func (m MemberAddress) String() string {
	return fmt.Sprintf("%s://%s@%s:%d", m.Protocol, m.System, m.Host, m.Port)
}

// DataCenterForMember extracts the data-center name from a member's roles.
// Pekko encodes the DC as a role with the "dc-" prefix (e.g. "dc-us-east" → "us-east").
// Returns "default" when no DC role is present.
func DataCenterForMember(gossip *gproto_cluster.Gossip, member *gproto_cluster.Member) string {
	if gossip == nil || member == nil {
		return "default"
	}
	for _, idx := range member.GetRolesIndexes() {
		if int(idx) < len(gossip.AllRoles) {
			role := gossip.AllRoles[idx]
			if strings.HasPrefix(role, "dc-") {
				return strings.TrimPrefix(role, "dc-")
			}
		}
	}
	return "default"
}

// MemberUp is published when a cluster member transitions to Up status.
// The member is now available for work and cluster singleton hosting.
type MemberUp struct{ Member MemberAddress }

// MemberWeaklyUp is published when a Joining member is promoted to WeaklyUp
// because convergence was not achieved within the allow-weakly-up-members timeout.
type MemberWeaklyUp struct{ Member MemberAddress }

// MemberLeft is published when a member requests graceful departure
// (transitions to Leaving status).
type MemberLeft struct{ Member MemberAddress }

// MemberExited is published when a departing member completes its handshake
// and transitions to Exiting status.
type MemberExited struct{ Member MemberAddress }

// MemberDowned is published when a member is marked as Down.
type MemberDowned struct{ Member MemberAddress }

// MemberRemoved is published when a member is fully evicted from the
// After this event the member's address may be reused by a new node.
type MemberRemoved struct{ Member MemberAddress }

// UnreachableMember is published when the phi-accrual failure detector marks
// a member as unreachable.
type UnreachableMember struct{ Member MemberAddress }

// ReachableMember is published when a previously-unreachable member is detected
// as reachable again (e.g. after a transient network partition heals).
type ReachableMember struct{ Member MemberAddress }

// AppVersionChanged is published when a member's advertised application version
// changes (typically during a rolling update).
type AppVersionChanged struct {
	Member     MemberAddress
	OldVersion AppVersion
	NewVersion AppVersion
}

// Marker method implementations — satisfy ClusterDomainEvent.
func (MemberUp) clusterDomainEvent()          {}
func (MemberWeaklyUp) clusterDomainEvent()    {}
func (MemberLeft) clusterDomainEvent()        {}
func (MemberExited) clusterDomainEvent()      {}
func (MemberDowned) clusterDomainEvent()      {}
func (MemberRemoved) clusterDomainEvent()     {}
func (UnreachableMember) clusterDomainEvent() {}
func (ReachableMember) clusterDomainEvent()   {}
func (AppVersionChanged) clusterDomainEvent() {}

// Convenience reflect.Type values for use with Cluster.Subscribe.
// Pass one or more of these to filter specific event types.
//
//	node.Subscribe(ch, gekka.EventMemberUp, gekka.EventMemberRemoved)
var (
	EventMemberUp          = reflect.TypeOf(MemberUp{})
	EventMemberLeft        = reflect.TypeOf(MemberLeft{})
	EventMemberExited      = reflect.TypeOf(MemberExited{})
	EventMemberDowned      = reflect.TypeOf(MemberDowned{})
	EventMemberRemoved     = reflect.TypeOf(MemberRemoved{})
	EventUnreachableMember = reflect.TypeOf(UnreachableMember{})
	EventReachableMember   = reflect.TypeOf(ReachableMember{})
	EventAppVersionChanged = reflect.TypeOf(AppVersionChanged{})
)

// ── Subscriber management (methods on ClusterManager) ────────────────────────

// eventSubscriber is an internal record for one registered subscriber.
type eventSubscriber struct {
	ref   actor.Ref
	types map[reflect.Type]struct{} // nil = subscribe to all event types
}

// Subscribe registers an actor.Ref to receive cluster domain events.
//
// Pass Event* values to receive only specific types; omit types to subscribe
// to every ClusterDomainEvent.
func (cm *ClusterManager) Subscribe(ref actor.Ref, types ...reflect.Type) {
	var typeSet map[reflect.Type]struct{}
	if len(types) > 0 {
		typeSet = make(map[reflect.Type]struct{}, len(types))
		for _, t := range types {
			typeSet[t] = struct{}{}
		}
	}
	cm.SubMu.Lock()
	cm.Subs = append(cm.Subs, eventSubscriber{ref: ref, types: typeSet})
	cm.SubMu.Unlock()
}

// Unsubscribe removes the actor from the subscriber list. Safe to call even when
// the actor was never subscribed or has already been unsubscribed.
func (cm *ClusterManager) Unsubscribe(ref actor.Ref) {
	cm.SubMu.Lock()
	defer cm.SubMu.Unlock()
	kept := cm.Subs[:0]
	for _, s := range cm.Subs {
		if s.ref.Path() != ref.Path() {
			kept = append(kept, s)
		}
	}
	cm.Subs = kept
}

// ForcePublishEvent injects a synthetic ClusterDomainEvent directly into the
// subscriber bus without going through the gossip state machine.
//
// This is intended for use in tests that need to trigger SBR or other
// subscriber logic without running a full cluster.
func (cm *ClusterManager) ForcePublishEvent(evt ClusterDomainEvent) {
	cm.publishEvent(evt)
}

// publishEvent delivers evt to all matching subscribers.
//
// Safe to call while holding cm.Mu because it only acquires cm.SubMu (a separate lock).
func (cm *ClusterManager) publishEvent(evt ClusterDomainEvent) {
	cm.SubMu.RLock()
	subs := append([]eventSubscriber(nil), cm.Subs...)
	cm.SubMu.RUnlock()

	evtType := reflect.TypeOf(evt)
	for _, s := range subs {
		if s.types != nil {
			if _, ok := s.types[evtType]; !ok {
				continue
			}
		}
		s.ref.Tell(evt)
	}
}

// ── Gossip state diffing ──────────────────────────────────────────────────────

// diffGossipMembers computes the ClusterDomainEvents implied by member status
// changes between oldState and newState.  Called by processIncomingGossip to
// emit events for transitions that Pekko's remote leader already performed.
func diffGossipMembers(oldState, newState *gproto_cluster.Gossip) []ClusterDomainEvent {
	type key struct {
		host string
		port uint32
	}

	type memberSnapshot struct {
		status     gproto_cluster.MemberStatus
		appVersion string
	}

	// Build a snapshot of the old member statuses and versions.
	old := make(map[key]memberSnapshot)
	if oldState != nil {
		for _, m := range oldState.Members {
			a := oldState.AllAddresses[m.GetAddressIndex()].GetAddress()
			ver := ""
			if idx := m.GetAppVersionIndex(); idx >= 0 && int(idx) < len(oldState.AllAppVersions) {
				ver = oldState.AllAppVersions[idx]
			}
			old[key{a.GetHostname(), a.GetPort()}] = memberSnapshot{
				status:     m.GetStatus(),
				appVersion: ver,
			}
		}
	}

	var events []ClusterDomainEvent
	for _, m := range newState.Members {
		ua := newState.AllAddresses[m.GetAddressIndex()]
		a := ua.GetAddress()
		k := key{a.GetHostname(), a.GetPort()}
		newSt := m.GetStatus()
		newVer := ""
		if idx := m.GetAppVersionIndex(); idx >= 0 && int(idx) < len(newState.AllAppVersions) {
			newVer = newState.AllAppVersions[idx]
		}
		oldSnap, existed := old[k]

		ma := memberAddressFromGossip(ua, newVer)

		if !existed || oldSnap.status != newSt {
			switch newSt {
			case gproto_cluster.MemberStatus_Up:
				events = append(events, MemberUp{Member: ma})
			case gproto_cluster.MemberStatus_Leaving:
				events = append(events, MemberLeft{Member: ma})
			case gproto_cluster.MemberStatus_Exiting:
				events = append(events, MemberExited{Member: ma})
			case gproto_cluster.MemberStatus_Down:
				events = append(events, MemberDowned{Member: ma})
			case gproto_cluster.MemberStatus_Removed:
				events = append(events, MemberRemoved{Member: ma})
			}
		}

		// Emit AppVersionChanged if the version changed (and member existed before).
		if existed && oldSnap.appVersion != newVer && newVer != "" {
			events = append(events, AppVersionChanged{
				Member:     ma,
				OldVersion: ParseAppVersion(oldSnap.appVersion),
				NewVersion: ParseAppVersion(newVer),
			})
		}
	}
	return events
}

// memberAddressFromUA converts a UniqueAddress to a MemberAddress (without version).
func memberAddressFromUA(ua *gproto_cluster.UniqueAddress) MemberAddress {
	a := ua.GetAddress()
	return MemberAddress{
		Protocol: a.GetProtocol(),
		System:   a.GetSystem(),
		Host:     a.GetHostname(),
		Port:     a.GetPort(),
	}
}

// memberAddressFromGossip converts a UniqueAddress + version string to a MemberAddress.
func memberAddressFromGossip(ua *gproto_cluster.UniqueAddress, appVersion string) MemberAddress {
	a := ua.GetAddress()
	return MemberAddress{
		Protocol:   a.GetProtocol(),
		System:     a.GetSystem(),
		Host:       a.GetHostname(),
		Port:       a.GetPort(),
		AppVersion: ParseAppVersion(appVersion),
	}
}
