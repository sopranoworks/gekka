/*
 * cluster_events.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

// Package-level cluster event types and the subscriber management methods
// for ClusterManager.  The struct fields (subMu, subs) are declared in
// cluster_manager.go alongside the rest of the ClusterManager definition.

import (
	"fmt"
	"log"
	"reflect"

	"gekka/gekka/cluster"
)

// ── Event types ───────────────────────────────────────────────────────────────

// ClusterDomainEvent is the marker interface for all cluster lifecycle events.
// Subscribe to events with GekkaNode.Subscribe; receive them on a buffered
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
	Protocol string // "pekko" or "akka"
	System   string // actor system name, e.g. "ClusterSystem"
	Host     string // hostname or IP
	Port     uint32 // TCP port
}

// String returns the member's address in Artery URI form ("pekko://System@host:port").
func (m MemberAddress) String() string {
	return fmt.Sprintf("%s://%s@%s:%d", m.Protocol, m.System, m.Host, m.Port)
}

// MemberUp is published when a cluster member transitions to Up status.
// The member is now available for work and cluster singleton hosting.
type MemberUp struct{ Member MemberAddress }

// MemberLeft is published when a member requests graceful departure
// (transitions to Leaving status).
type MemberLeft struct{ Member MemberAddress }

// MemberExited is published when a departing member completes its handshake
// and transitions to Exiting status.
type MemberExited struct{ Member MemberAddress }

// MemberRemoved is published when a member is fully evicted from the cluster.
// After this event the member's address may be reused by a new node.
type MemberRemoved struct{ Member MemberAddress }

// UnreachableMember is published when the phi-accrual failure detector marks
// a member as unreachable.
type UnreachableMember struct{ Member MemberAddress }

// ReachableMember is published when a previously-unreachable member is detected
// as reachable again (e.g. after a transient network partition heals).
type ReachableMember struct{ Member MemberAddress }

// Marker method implementations — satisfy ClusterDomainEvent.
func (MemberUp) clusterDomainEvent()          {}
func (MemberLeft) clusterDomainEvent()        {}
func (MemberExited) clusterDomainEvent()      {}
func (MemberRemoved) clusterDomainEvent()     {}
func (UnreachableMember) clusterDomainEvent() {}
func (ReachableMember) clusterDomainEvent()   {}

// Convenience reflect.Type values for use with GekkaNode.Subscribe.
// Pass one or more of these to filter specific event types.
//
//	node.Subscribe(ch, gekka.EventMemberUp, gekka.EventMemberRemoved)
var (
	EventMemberUp          = reflect.TypeOf(MemberUp{})
	EventMemberLeft        = reflect.TypeOf(MemberLeft{})
	EventMemberExited      = reflect.TypeOf(MemberExited{})
	EventMemberRemoved     = reflect.TypeOf(MemberRemoved{})
	EventUnreachableMember = reflect.TypeOf(UnreachableMember{})
	EventReachableMember   = reflect.TypeOf(ReachableMember{})
)

// ── Subscriber management (methods on ClusterManager) ────────────────────────

// eventSubscriber is an internal record for one registered subscriber channel.
type eventSubscriber struct {
	ch    chan<- ClusterDomainEvent
	types map[reflect.Type]struct{} // nil = subscribe to all event types
}

// Subscribe registers ch to receive cluster domain events.
//
// Pass Event* values to receive only specific types; omit types to subscribe
// to every ClusterDomainEvent.  ch must be a buffered channel — publishEvent
// drops events rather than blocking when the channel is full, so size the
// buffer to accommodate your slowest consumer (16–64 is typical).
func (cm *ClusterManager) Subscribe(ch chan<- ClusterDomainEvent, types ...reflect.Type) {
	var typeSet map[reflect.Type]struct{}
	if len(types) > 0 {
		typeSet = make(map[reflect.Type]struct{}, len(types))
		for _, t := range types {
			typeSet[t] = struct{}{}
		}
	}
	cm.subMu.Lock()
	cm.subs = append(cm.subs, eventSubscriber{ch: ch, types: typeSet})
	cm.subMu.Unlock()
}

// Unsubscribe removes ch from the subscriber list.  Safe to call even when ch
// was never subscribed or has already been unsubscribed.
func (cm *ClusterManager) Unsubscribe(ch chan<- ClusterDomainEvent) {
	cm.subMu.Lock()
	defer cm.subMu.Unlock()
	kept := cm.subs[:0]
	for _, s := range cm.subs {
		if s.ch != ch {
			kept = append(kept, s)
		}
	}
	cm.subs = kept
}

// publishEvent delivers evt to all matching subscribers.
//
// Non-blocking: if a subscriber's channel is full the event is silently
// dropped and a warning is logged.  Safe to call while holding cm.mu because
// it only acquires cm.subMu (a separate lock).
func (cm *ClusterManager) publishEvent(evt ClusterDomainEvent) {
	cm.subMu.RLock()
	subs := append([]eventSubscriber(nil), cm.subs...)
	cm.subMu.RUnlock()

	evtType := reflect.TypeOf(evt)
	for _, s := range subs {
		if s.types != nil {
			if _, ok := s.types[evtType]; !ok {
				continue
			}
		}
		select {
		case s.ch <- evt:
		default:
			log.Printf("ClusterEvents: subscriber channel full, dropping %T", evt)
		}
	}
}

// ── Gossip state diffing ──────────────────────────────────────────────────────

// diffGossipMembers computes the ClusterDomainEvents implied by member status
// changes between oldState and newState.  Called by processIncomingGossip to
// emit events for transitions that Pekko's remote leader already performed.
func diffGossipMembers(oldState, newState *cluster.Gossip) []ClusterDomainEvent {
	type key struct {
		host string
		port uint32
	}

	// Build a snapshot of the old member statuses.
	old := make(map[key]cluster.MemberStatus)
	if oldState != nil {
		for _, m := range oldState.Members {
			a := oldState.AllAddresses[m.GetAddressIndex()].GetAddress()
			old[key{a.GetHostname(), a.GetPort()}] = m.GetStatus()
		}
	}

	var events []ClusterDomainEvent
	for _, m := range newState.Members {
		ua := newState.AllAddresses[m.GetAddressIndex()]
		a := ua.GetAddress()
		k := key{a.GetHostname(), a.GetPort()}
		newSt := m.GetStatus()
		oldSt, existed := old[k]
		if existed && oldSt == newSt {
			continue // no transition
		}
		ma := memberAddressFromUA(ua)
		switch newSt {
		case cluster.MemberStatus_Up:
			events = append(events, MemberUp{Member: ma})
		case cluster.MemberStatus_Leaving:
			events = append(events, MemberLeft{Member: ma})
		case cluster.MemberStatus_Exiting:
			events = append(events, MemberExited{Member: ma})
		case cluster.MemberStatus_Removed:
			events = append(events, MemberRemoved{Member: ma})
		}
	}
	return events
}

// memberAddressFromUA converts a cluster.UniqueAddress to a MemberAddress.
func memberAddressFromUA(ua *cluster.UniqueAddress) MemberAddress {
	a := ua.GetAddress()
	return MemberAddress{
		Protocol: a.GetProtocol(),
		System:   a.GetSystem(),
		Host:     a.GetHostname(),
		Port:     a.GetPort(),
	}
}
