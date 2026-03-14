/*
 * sbr_manager.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/sopranoworks/gekka/actor"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
)

// ── eventChanRef — bridges actor.Ref to a Go channel ─────────────────────────

// eventChanRef implements actor.Ref by routing Tell calls into a buffered channel.
// Used by SubscribeChannel to expose the cluster event bus as a Go channel.
type eventChanRef struct {
	path  string
	ch    chan ClusterDomainEvent
	types map[reflect.Type]struct{} // nil = accept all
}

func (r *eventChanRef) Tell(msg any, _ ...actor.Ref) {
	evt, ok := msg.(ClusterDomainEvent)
	if !ok {
		return
	}
	if r.types != nil {
		if _, accept := r.types[reflect.TypeOf(evt)]; !accept {
			return
		}
	}
	select {
	case r.ch <- evt:
	default:
		// Drop if the consumer is slow to avoid blocking the gossip loop.
	}
}

func (r *eventChanRef) Path() string { return r.path }

var chanRefSeq atomic.Uint64

// ChanSubscription is returned by SubscribeChannel and holds both the
// receive-only channel and a handle to cancel the subscription.
type ChanSubscription struct {
	// C is the receive-only channel of ClusterDomainEvents.
	C  <-chan ClusterDomainEvent
	cm *ClusterManager
	r  *eventChanRef
}

// Cancel removes the subscription. Safe to call multiple times.
func (cs *ChanSubscription) Cancel() {
	cs.cm.Unsubscribe(cs.r)
}

// SubscribeChannel creates a buffered channel subscription and returns a
// ChanSubscription. Call Cancel on the returned subscription when done to
// avoid resource leaks. When types is non-empty only those event types are
// delivered; omit types to receive all ClusterDomainEvents.
//
//	sub := cm.SubscribeChannel(EventUnreachableMember, EventReachableMember)
//	defer sub.Cancel()
//	for evt := range sub.C { ... }
func (cm *ClusterManager) SubscribeChannel(types ...reflect.Type) *ChanSubscription {
	var typeSet map[reflect.Type]struct{}
	if len(types) > 0 {
		typeSet = make(map[reflect.Type]struct{}, len(types))
		for _, t := range types {
			typeSet[t] = struct{}{}
		}
	}
	seq := chanRefSeq.Add(1)
	ref := &eventChanRef{
		path:  fmt.Sprintf("/system/sbr/channel-%d", seq),
		ch:    make(chan ClusterDomainEvent, 64),
		types: typeSet,
	}
	// Register using the existing Subscribe mechanism (typeSet already
	// applied inside Tell, so pass no types here to avoid double filtering).
	cm.SubMu.Lock()
	cm.Subs = append(cm.Subs, eventSubscriber{ref: ref, types: nil})
	cm.SubMu.Unlock()

	return &ChanSubscription{C: ref.ch, cm: cm, r: ref}
}

// ── SBRManager ────────────────────────────────────────────────────────────────

// SBRManager watches cluster domain events and triggers the configured
// Split Brain Resolver strategy after a stable-after delay.
//
// When the strategy returns DownSelf the local node calls LeaveCluster.
// When it returns DownMembers the local leader downs those members directly
// in the gossip state via ClusterManager.DownMember.
type SBRManager struct {
	cm       *ClusterManager
	strategy Strategy
	cfg      SBRConfig
}

// NewSBRManager constructs a SBRManager. Returns nil when cfg.ActiveStrategy
// is empty (SBR disabled) or when strategy construction fails.
func NewSBRManager(cm *ClusterManager, cfg SBRConfig) *SBRManager {
	strat, err := NewStrategy(cfg)
	if err != nil {
		log.Printf("SBR: strategy init error: %v — SBR disabled", err)
		return nil
	}
	if strat == nil {
		return nil // SBR disabled
	}
	if cfg.StableAfter <= 0 {
		cfg.StableAfter = 20 * time.Second
	}
	return &SBRManager{cm: cm, strategy: strat, cfg: cfg}
}

// Start runs the SBR event loop. It subscribes to cluster domain events,
// starts a stable-after timer whenever an UnreachableMember event arrives,
// and invokes the strategy when the timer fires without a ReachableMember
// event cancelling it.
//
// Call in a goroutine; returns when ctx is cancelled.
func (m *SBRManager) Start(ctx context.Context) {
	sub := m.cm.SubscribeChannel(EventUnreachableMember, EventReachableMember)
	defer sub.Cancel()

	var timer *time.Timer
	resetTimer := func() {
		if timer != nil {
			timer.Stop()
		}
		timer = time.NewTimer(m.cfg.StableAfter)
	}
	stopTimer := func() {
		if timer != nil {
			timer.Stop()
			timer = nil
		}
	}

	for {
		var timerC <-chan time.Time
		if timer != nil {
			timerC = timer.C
		}

		select {
		case <-ctx.Done():
			stopTimer()
			return

		case evt, ok := <-sub.C:
			if !ok {
				return
			}
			switch evt.(type) {
			case UnreachableMember:
				log.Printf("SBR: unreachable event — starting stable-after timer (%s)", m.cfg.StableAfter)
				resetTimer()
			case ReachableMember:
				if !m.hasUnreachable() {
					log.Printf("SBR: all members reachable — cancelling stable-after timer")
					stopTimer()
				}
			}

		case <-timerC:
			timer = nil
			m.decide()
		}
	}
}

// hasUnreachable returns true if any Up/WeaklyUp member is still unreachable.
func (m *SBRManager) hasUnreachable() bool {
	_, unreachable := m.classifyMembers()
	return len(unreachable) > 0
}

// decide runs the strategy and applies its decision.
func (m *SBRManager) decide() {
	reachable, unreachable := m.classifyMembers()
	if len(unreachable) == 0 {
		log.Printf("SBR: stable-after elapsed but no unreachable members — no action")
		return
	}

	self := memberAddressFromUA(m.cm.LocalAddress)
	decision := m.strategy.Decide(self, reachable, unreachable)

	log.Printf("SBR: decision — downSelf=%v downMembers=%d", decision.DownSelf, len(decision.DownMembers))

	if decision.DownSelf {
		log.Printf("SBR: downing self (%s)", self)
		if err := m.cm.LeaveCluster(); err != nil {
			log.Printf("SBR: LeaveCluster error: %v", err)
		}
		return
	}

	for _, dm := range decision.DownMembers {
		log.Printf("SBR: downing member %s", dm.Address)
		m.cm.DownMember(dm.Address)
	}
}

// classifyMembers reads the current gossip state and separates Up/WeaklyUp
// members into reachable and unreachable sets from the local observer's
// perspective (address index 0 in AllAddresses).
func (m *SBRManager) classifyMembers() (reachable, unreachable []Member) {
	m.cm.Mu.RLock()
	defer m.cm.Mu.RUnlock()

	state := m.cm.State

	// Build a set of unreachable address indices as seen by the local observer.
	unreachableIdx := make(map[int32]struct{})
	if state.Overview != nil {
		for _, obs := range state.Overview.ObserverReachability {
			if obs.GetAddressIndex() != 0 {
				continue // not the local node's observation record
			}
			for _, sub := range obs.SubjectReachability {
				if sub.GetStatus() == gproto_cluster.ReachabilityStatus_Unreachable {
					unreachableIdx[sub.GetAddressIndex()] = struct{}{}
				}
			}
		}
	}

	for _, mem := range state.Members {
		st := mem.GetStatus()
		if st != gproto_cluster.MemberStatus_Up && st != gproto_cluster.MemberStatus_WeaklyUp {
			continue
		}
		ua := state.AllAddresses[mem.GetAddressIndex()]
		ma := memberAddressFromUA(ua)
		_, isUnreachable := unreachableIdx[mem.GetAddressIndex()]

		// Resolve role names from index references in the gossip state.
		roles := make([]string, 0, len(mem.GetRolesIndexes()))
		for _, idx := range mem.GetRolesIndexes() {
			if int(idx) < len(state.AllRoles) {
				roles = append(roles, state.AllRoles[idx])
			}
		}
		mbr := Member{
			Address:   ma,
			Reachable: !isUnreachable,
			UpNumber:  mem.GetUpNumber(),
			Roles:     roles,
		}
		if isUnreachable {
			unreachable = append(unreachable, mbr)
		} else {
			reachable = append(reachable, mbr)
		}
	}
	return
}
