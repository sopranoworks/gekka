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
// When an InfrastructureProvider is configured (SetInfraProvider), the manager
// first queries the provider when a member becomes unreachable.  If the
// provider returns InfraDead the member is downed immediately — bypassing the
// stable-after wait.  If the provider returns InfraUnknown or InfraAlive the
// normal stable-after timer is started instead.
//
// When the strategy returns DownSelf the local node calls LeaveCluster.
// When it returns DownMembers the local leader downs those members directly
// in the gossip state via ClusterManager.DownMember.
type SBRManager struct {
	cm       *ClusterManager
	strategy Strategy
	cfg      SBRConfig

	// infra is the optional infrastructure provider; nil means disabled.
	infra InfrastructureProvider

	// unreachableSince records when each member first became unreachable.
	// Keyed by MemberAddress.String().  Used by the auto-down-unreachable-after
	// feature to enforce a hard maximum on how long a member may stay in the
	// cluster.
	unreachableSince map[string]time.Time

	// downFn and leaveFn are the actions the manager takes when the strategy
	// decides to down a remote member or down the local node, respectively.
	// Defaults to cm.DownMember and cm.LeaveCluster; overridable in tests.
	downFn  func(MemberAddress)
	leaveFn func() error
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
	return &SBRManager{
		cm:               cm,
		strategy:         strat,
		cfg:              cfg,
		unreachableSince: make(map[string]time.Time),
		downFn:           cm.DownMember,
		leaveFn:          cm.LeaveCluster,
	}
}

// SetInfraProvider attaches an InfrastructureProvider to the manager.
// When non-nil, PodStatus is queried on every UnreachableMember event and
// also during the decide phase so that infra-confirmed-dead members are
// downed individually before running the strategy on the remainder.
//
// Must be called before Start.
func (m *SBRManager) SetInfraProvider(p InfrastructureProvider) {
	m.infra = p
}

// SetDownFnForTest replaces the action taken when the strategy decides to down
// a remote member.  For testing only; allows test code to capture down calls
// without wiring a full ClusterManager gossip state.
func (m *SBRManager) SetDownFnForTest(fn func(MemberAddress)) { m.downFn = fn }

// SetLeaveFnForTest replaces the action taken when the strategy decides to
// down the local node.  For testing only.
func (m *SBRManager) SetLeaveFnForTest(fn func() error) { m.leaveFn = fn }

// Start runs the SBR event loop. It subscribes to cluster domain events,
// queries the InfrastructureProvider (if configured) on UnreachableMember
// events for a fast-down path, and falls back to the stable-after timer when
// infrastructure status is unknown.
//
// Call in a goroutine; returns when ctx is cancelled.
func (m *SBRManager) Start(ctx context.Context) {
	sub := m.cm.SubscribeChannel(EventUnreachableMember, EventReachableMember)
	defer sub.Cancel()

	var stableTimer *time.Timer
	resetStableTimer := func() {
		if stableTimer != nil {
			stableTimer.Stop()
		}
		stableTimer = time.NewTimer(m.cfg.StableAfter)
	}
	stopStableTimer := func() {
		if stableTimer != nil {
			stableTimer.Stop()
			stableTimer = nil
		}
	}

	// autoDownTicker fires at AutoDownUnreachableAfter/4 intervals so that
	// the per-member age check has sub-second granularity even for short timeouts.
	var autoDownTickC <-chan time.Time
	if m.cfg.AutoDownUnreachableAfter > 0 {
		interval := m.cfg.AutoDownUnreachableAfter / 4
		if interval < 50*time.Millisecond {
			interval = 50 * time.Millisecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		autoDownTickC = ticker.C
	}

	for {
		var stableC <-chan time.Time
		if stableTimer != nil {
			stableC = stableTimer.C
		}

		select {
		case <-ctx.Done():
			stopStableTimer()
			return

		case evt, ok := <-sub.C:
			if !ok {
				return
			}
			switch e := evt.(type) {
			case UnreachableMember:
				addr := e.Member // MemberAddress
				key := addr.String()

				// Record when this member first became unreachable.
				if _, seen := m.unreachableSince[key]; !seen {
					m.unreachableSince[key] = time.Now()
				}

				// Fast-path: if the infra provider confirms the pod is dead,
				// down it immediately without waiting for stable-after.
				if m.infra != nil {
					if m.infra.PodStatus(ctx, addr) == InfraDead {
						log.Printf("SBR: infra confirms %s is dead — downing immediately", addr)
						m.downFn(addr)
						delete(m.unreachableSince, key)
						if !m.hasUnreachable() {
							stopStableTimer()
						}
						continue
					}
				}

				log.Printf("SBR: unreachable %s — starting stable-after timer (%s)", addr, m.cfg.StableAfter)
				resetStableTimer()

			case ReachableMember:
				delete(m.unreachableSince, e.Member.String())
				if !m.hasUnreachable() {
					log.Printf("SBR: all members reachable — cancelling stable-after timer")
					stopStableTimer()
				}
			}

		case <-stableC:
			stableTimer = nil
			m.decide(ctx)

		case <-autoDownTickC:
			m.checkAutoDown(ctx)
		}
	}
}

// checkAutoDown enforces the AutoDownUnreachableAfter hard limit: if any member
// has been unreachable longer than the configured threshold, the strategy is
// invoked immediately (regardless of the stable-after timer).
func (m *SBRManager) checkAutoDown(ctx context.Context) {
	if m.cfg.AutoDownUnreachableAfter <= 0 {
		return
	}
	now := time.Now()
	_, unreachable := m.classifyMembers()
	for _, u := range unreachable {
		since, ok := m.unreachableSince[u.Address.String()]
		if !ok {
			continue
		}
		if now.Sub(since) >= m.cfg.AutoDownUnreachableAfter {
			log.Printf("SBR: auto-down-unreachable-after (%s) elapsed for %s — executing strategy",
				m.cfg.AutoDownUnreachableAfter, u.Address)
			m.decide(ctx)
			return // decide handles all unreachable members at once
		}
	}
}

// hasUnreachable returns true if any Up/WeaklyUp member is still unreachable.
func (m *SBRManager) hasUnreachable() bool {
	_, unreachable := m.classifyMembers()
	return len(unreachable) > 0
}

// decide runs the strategy and applies its decision.
// If an InfrastructureProvider is configured, infra-confirmed-dead members are
// downed individually before the strategy evaluates the remainder.
func (m *SBRManager) decide(ctx context.Context) {
	reachable, unreachable := m.classifyMembers()
	if len(unreachable) == 0 {
		log.Printf("SBR: stable-after elapsed but no unreachable members — no action")
		return
	}

	// Fast-down infra-confirmed-dead members before running the strategy.
	if m.infra != nil {
		var remaining []Member
		for _, u := range unreachable {
			if m.infra.PodStatus(ctx, u.Address) == InfraDead {
				log.Printf("SBR: decide: infra confirms %s dead — downing", u.Address)
				m.downFn(u.Address)
				delete(m.unreachableSince, u.Address.String())
			} else {
				remaining = append(remaining, u)
			}
		}
		unreachable = remaining
	}

	if len(unreachable) == 0 {
		return // all accounted for via infra
	}

	self := memberAddressFromUA(m.cm.LocalAddress)
	decision := m.strategy.Decide(self, reachable, unreachable)

	log.Printf("SBR: decision — downSelf=%v downMembers=%d", decision.DownSelf, len(decision.DownMembers))

	// Always down remote members first so the rest of the cluster converges
	// before this node leaves (important for DownAll where both sides act).
	for _, dm := range decision.DownMembers {
		log.Printf("SBR: downing member %s", dm.Address)
		m.downFn(dm.Address)
	}

	if decision.DownSelf {
		log.Printf("SBR: downing self (%s)", self)
		if err := m.leaveFn(); err != nil {
			log.Printf("SBR: LeaveCluster error: %v", err)
		}
	}
}

// classifyMembers reads the current gossip state and separates Up/WeaklyUp
// members into reachable and unreachable sets from the local observer's
// perspective. The local observer index is looked up dynamically — it is
// only 0 when the local node was the first joiner; later joiners have a
// non-zero index and a hardcoded 0 silently drops every observation,
// leaving the SBR strategy with no unreachable members and no decision.
func (m *SBRManager) classifyMembers() (reachable, unreachable []Member) {
	m.cm.Mu.RLock()
	defer m.cm.Mu.RUnlock()

	state := m.cm.State

	// Find the local observer's address index in AllAddresses.
	localHost := m.cm.LocalAddress.Address.GetHostname()
	localPort := m.cm.LocalAddress.Address.GetPort()
	localIdx := int32(-1)
	for i, ua := range state.AllAddresses {
		if ua.GetAddress().GetHostname() == localHost && ua.GetAddress().GetPort() == localPort {
			localIdx = int32(i)
			break
		}
	}

	// Build a set of unreachable address indices as seen by the local observer.
	unreachableIdx := make(map[int32]struct{})
	if state.Overview != nil && localIdx >= 0 {
		for _, obs := range state.Overview.ObserverReachability {
			if obs.GetAddressIndex() != localIdx {
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
