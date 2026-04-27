/*
 * sbr_strategy.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package cluster provides cluster membership, gossip, and failure resolution.
package cluster

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	icluster "github.com/sopranoworks/gekka/internal/cluster"
)

// LeaseChecker is the minimal interface that LeaseMajority needs from a
// distributed lease implementation. It is satisfied by internal/cluster.Lease
// as well as by TestLease.
type LeaseChecker interface {
	// CheckLease returns true if this node currently holds the lease.
	CheckLease() bool
}

// SBRConfig configures the Split Brain Resolver strategy.
//
// ActiveStrategy selects which algorithm is used to decide which partition
// survives after a network partition:
//
//   - "keep-majority"    — the side with more nodes survives.
//   - "keep-oldest"      — the side containing the oldest node survives.
//   - "keep-referee"     — the side that can reach a specific "referee" node survives.
//   - "static-quorum"    — a side needs at least QuorumSize reachable nodes to survive.
//   - "lease-majority"   — the side holding a distributed lease survives.
//   - "down-all"         — all members on both sides are downed (forces cluster restart).
type SBRConfig struct {
	// ActiveStrategy is one of: "keep-majority", "keep-oldest",
	// "keep-referee", "static-quorum", "lease-majority", "down-all".
	// Defaults to "" (disabled).
	ActiveStrategy string

	// StableAfter is the duration the reachability must remain stable before
	// SBR takes action. Mirrors pekko.cluster.split-brain-resolver.stable-after.
	// Defaults to 20s.
	StableAfter time.Duration

	// AutoDownUnreachableAfter is the maximum time an unreachable member may
	// remain in the cluster before the SBR forcibly executes the configured
	// strategy to restore cluster health.
	//
	// This acts as a last-resort backstop when the InfrastructureProvider
	// cannot determine liveness (e.g. during a genuine network partition where
	// both sides are up).  After this duration the strategy (e.g. keep-majority)
	// is invoked regardless of the stable-after timer state.
	//
	// Maps to: gekka.cluster.sbr.auto-down-unreachable-after
	// Defaults to 0 (disabled).  Recommended value: 60s.
	AutoDownUnreachableAfter time.Duration

	// DownAllWhenUnstable triggers a down-all decision when the cluster remains
	// unstable (has unreachable members that neither recover nor get resolved by
	// the configured strategy) for longer than stable-after + this duration.
	//
	// This mirrors Pekko's pekko.cluster.split-brain-resolver.down-all-when-unstable.
	//
	// Values:
	//   - positive duration: explicit timeout after stable-after fires
	//   - zero with DownAllWhenUnstableEnabled=true ("on"): derived as 3/4 of
	//     StableAfter, minimum 4s
	//   - DownAllWhenUnstableEnabled=false ("off"): feature disabled
	//
	// Default: enabled ("on"), derived duration.
	DownAllWhenUnstable time.Duration

	// DownAllWhenUnstableEnabled controls whether the down-all-when-unstable
	// safety net is active. When true and DownAllWhenUnstable is zero, the
	// timeout is derived as max(3/4 * StableAfter, 4s).
	DownAllWhenUnstableEnabled *bool

	// Role restricts SBR to only count members with this role.
	// Empty means count all members.
	Role string

	// DownIfAlone, when true for keep-oldest, downs the oldest node if it is
	// the only member on its side of the partition (i.e. isolated).
	DownIfAlone bool

	// RefereeAddress is the host:port of the referee node for keep-referee.
	// Required when ActiveStrategy == "keep-referee".
	RefereeAddress string

	// QuorumSize is the minimum number of reachable members required by
	// static-quorum. Required when ActiveStrategy == "static-quorum".
	QuorumSize int

	// StaticQuorumRole restricts quorum counting to members with this role.
	// Corresponds to pekko.cluster.split-brain-resolver.static-quorum.role.
	// When non-empty, overrides Role for the static-quorum strategy.
	StaticQuorumRole string

	// Lease is the distributed lease used by lease-majority when no
	// LeaseImplementation/LeaseManager pair is configured.  This legacy path
	// reads CheckLease() only; the strategy never invokes Acquire/Release.
	Lease LeaseChecker

	// LeaseImplementation names a LeaseProvider registered in LeaseManager.
	// When non-empty (and LeaseManager is set), NewStrategy resolves the
	// provider, obtains a Lease bound to LeaseSettings, and constructs a
	// lease-majority strategy with full Acquire/Release semantics including
	// the AcquireLeaseDelayForMinority delay.
	//
	// Mirrors pekko.cluster.split-brain-resolver.lease-majority.lease-implementation.
	LeaseImplementation string

	// LeaseManager is the registry consulted to resolve LeaseImplementation.
	// Typically populated by gekka.NewCluster from cfg.LeaseManager and
	// defaulted to lease.NewDefaultManager() when unset and lease-majority is
	// the active strategy.
	LeaseManager *icluster.LeaseManager

	// LeaseSettings configures the resolved Lease instance.  When fields are
	// zero, the gekka top-level wiring fills sensible defaults: LeaseName =
	// "<system>-pekko-sbr", OwnerName = "host:port", LeaseDuration sourced
	// from pekko.coordination.lease.heartbeat-timeout.
	LeaseSettings icluster.LeaseSettings

	// AcquireLeaseDelayForMinority is the wait applied on the minority side
	// (by member count, after applying LeaseMajorityRole/Role filtering)
	// before attempting to acquire the lease, giving the majority side first
	// chance to win.  Only honored when LeaseImplementation is configured.
	//
	// Mirrors pekko.cluster.split-brain-resolver.lease-majority.acquire-lease-delay-for-minority.
	// Defaults to 2s when zero.
	AcquireLeaseDelayForMinority time.Duration

	// LeaseMajorityRole, when non-empty, restricts majority/minority counting
	// for lease-majority to members carrying this role.  Overrides Role for
	// lease-majority only.
	//
	// Mirrors pekko.cluster.split-brain-resolver.lease-majority.role.
	LeaseMajorityRole string
}

// Member represents a cluster member as seen by the SBR strategy.
// It is a flattened, SBR-friendly view of the gossip proto types.
type Member struct {
	Address   MemberAddress
	Reachable bool  // false = marked unreachable by local FD
	UpNumber  int32 // monotonic join order; lower = older
	Roles     []string
}

// String returns a short representation for logging.
func (m Member) String() string {
	r := "reachable"
	if !m.Reachable {
		r = "unreachable"
	}
	return fmt.Sprintf("%s(%s,up=%d)", m.Address, r, m.UpNumber)
}

// Decision is the result of a strategy's Decide call.
// DownSelf, when true, means the local node should down itself.
// DownMembers lists remote members the local leader should mark Down.
type Decision struct {
	DownSelf    bool
	DownMembers []Member
}

// Strategy is the interface implemented by each SBR algorithm.
type Strategy interface {
	// Decide returns a Decision given the reachable and unreachable members as seen by the local node.
	// self is the local node's address.
	Decide(self MemberAddress, reachable, unreachable []Member) Decision
}

// NewStrategy constructs the Strategy described by cfg.
// Returns nil, nil when cfg.ActiveStrategy is empty (SBR disabled).
func NewStrategy(cfg SBRConfig) (Strategy, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.ActiveStrategy)) {
	case "":
		return nil, nil
	case "keep-majority":
		return &KeepMajority{Role: cfg.Role}, nil
	case "keep-oldest":
		return &KeepOldest{Role: cfg.Role, DownIfAlone: cfg.DownIfAlone}, nil
	case "keep-referee":
		if cfg.RefereeAddress == "" {
			return nil, fmt.Errorf("sbr: keep-referee requires referee-node address")
		}
		return &KeepReferee{RefereeAddress: cfg.RefereeAddress, Role: cfg.Role}, nil
	case "static-quorum":
		if cfg.QuorumSize <= 0 {
			return nil, fmt.Errorf("sbr: static-quorum requires quorum-size > 0")
		}
		role := cfg.StaticQuorumRole
		if role == "" {
			role = cfg.Role
		}
		return &StaticQuorum{QuorumSize: cfg.QuorumSize, Role: role}, nil
	case "lease-majority":
		role := cfg.LeaseMajorityRole
		if role == "" {
			role = cfg.Role
		}
		delay := cfg.AcquireLeaseDelayForMinority
		if delay <= 0 {
			delay = 2 * time.Second
		}
		if cfg.LeaseImplementation != "" {
			if cfg.LeaseManager == nil {
				return nil, fmt.Errorf(
					"sbr: lease-majority lease-implementation %q requires a LeaseManager",
					cfg.LeaseImplementation)
			}
			l, err := cfg.LeaseManager.GetLease(cfg.LeaseImplementation, cfg.LeaseSettings)
			if err != nil {
				return nil, fmt.Errorf("sbr: lease-majority resolve provider: %w", err)
			}
			return &LeaseMajority{Role: role, LeaseHolder: l, AcquireDelay: delay}, nil
		}
		if cfg.Lease != nil {
			return &LeaseMajority{Role: role, Lease: cfg.Lease, AcquireDelay: delay}, nil
		}
		return nil, fmt.Errorf(
			"sbr: lease-majority requires either Lease or LeaseImplementation+LeaseManager")
	case "down-all":
		return &DownAll{}, nil
	case "down-all-nodes-in-data-center":
		return &DownAllNodesInDataCenter{DataCenter: cfg.Role}, nil
	default:
		return nil, fmt.Errorf("sbr: unknown strategy %q", cfg.ActiveStrategy)
	}
}

// ── KeepMajority ─────────────────────────────────────────────────────────────

// KeepMajority keeps the largest partition. On a tie (equal-size partitions)
// the side containing the member with the lexicographically lowest address
// survives. When Role is set only members with that role are counted.
type KeepMajority struct {
	Role string
}

func (s *KeepMajority) Decide(self MemberAddress, reachable, unreachable []Member) Decision {
	r := filterByRole(reachable, s.Role)
	u := filterByRole(unreachable, s.Role)
	rn, un := len(r), len(u)

	if rn > un {
		return Decision{DownMembers: unreachable}
	}
	if rn < un {
		return Decision{DownSelf: true}
	}

	// Tie: the side containing the member with the lowest address survives.
	lowest := lowestMember(append(r, u...))
	if containsAddr(r, lowest.Address) {
		// Lowest member is on our (reachable) side — down the other side.
		return Decision{DownMembers: unreachable}
	}
	return Decision{DownSelf: true}
}

// ── KeepOldest ───────────────────────────────────────────────────────────────

// KeepOldest keeps the partition that contains the oldest (lowest UpNumber)
// member. If DownIfAlone is true and the oldest member is isolated (the only
// member on its side), it downs itself instead.
type KeepOldest struct {
	Role        string
	DownIfAlone bool
}

func (s *KeepOldest) Decide(self MemberAddress, reachable, unreachable []Member) Decision {
	all := filterByRole(append(reachable, unreachable...), s.Role)
	if len(all) == 0 {
		return Decision{}
	}

	oldest := oldestMember(all)

	// Is the oldest reachable from our perspective?
	oldestReachable := containsAddr(reachable, oldest.Address)

	if oldestReachable {
		if s.DownIfAlone && len(reachable) == 1 && reachable[0].Address == oldest.Address {
			// Oldest is alone — down self (only the oldest can execute this but
			// the SBRManager checks DownSelf and calls LeaveCluster/DownSelf).
			return Decision{DownSelf: true}
		}
		return Decision{DownMembers: unreachable}
	}
	return Decision{DownSelf: true}
}

// ── KeepReferee ──────────────────────────────────────────────────────────────

// KeepReferee keeps the partition that can reach the referee node.
// The referee is identified by its host:port (RefereeAddress).
type KeepReferee struct {
	RefereeAddress string // "host:port"
	Role           string
}

func (s *KeepReferee) Decide(self MemberAddress, reachable, unreachable []Member) Decision {
	// Check whether the referee is in the reachable set.
	for _, m := range reachable {
		addr := fmt.Sprintf("%s:%d", m.Address.Host, m.Address.Port)
		if addr == s.RefereeAddress {
			return Decision{DownMembers: unreachable}
		}
	}
	// Referee is unreachable from us — we must down ourselves.
	return Decision{DownSelf: true}
}

// ── StaticQuorum ─────────────────────────────────────────────────────────────

// StaticQuorum requires at least QuorumSize reachable members to survive.
// When the count is below the quorum, the local node (and partition) is downed.
type StaticQuorum struct {
	QuorumSize int
	Role       string
}

func (s *StaticQuorum) Decide(self MemberAddress, reachable, unreachable []Member) Decision {
	r := filterByRole(reachable, s.Role)
	if len(r) >= s.QuorumSize {
		return Decision{DownMembers: unreachable}
	}
	return Decision{DownSelf: true}
}

// ── LeaseMajority ─────────────────────────────────────────────────────────────

// LeaseMajority keeps the partition that holds a distributed lease. The side
// that holds the lease survives; the side that does not hold the lease downs
// itself.
//
// Two construction paths are supported:
//
//   - LeaseHolder set (full lease, preferred for production): Decide filters
//     members by Role, computes whether this node is on the majority or
//     minority side, applies AcquireDelay on the minority side to give the
//     majority first chance, then calls Acquire.  The side that wins the
//     Acquire race survives.
//   - Lease set (legacy LeaseChecker): Decide reads CheckLease() only and
//     does not invoke Acquire/Release.  Used for unit tests and back-compat.
//
// Mirrors Pekko's pekko.cluster.split-brain-resolver.active-strategy =
// lease-majority.
type LeaseMajority struct {
	Role         string
	Lease        LeaseChecker
	LeaseHolder  icluster.Lease
	AcquireDelay time.Duration
}

// Decide implements lease-majority.
//
// When LeaseHolder is non-nil the strategy executes the full Pekko algorithm:
// majority/minority is computed from role-filtered member counts, the
// minority side waits AcquireDelay before calling Acquire, and the side that
// successfully acquires the lease keeps the cluster.
//
// When LeaseHolder is nil the strategy falls back to the LeaseChecker-only
// path: it surveys CheckLease() and downs self when the lease is not held.
func (s *LeaseMajority) Decide(_ MemberAddress, reachable, unreachable []Member) Decision {
	if s.LeaseHolder != nil {
		r := filterByRole(reachable, s.Role)
		u := filterByRole(unreachable, s.Role)
		if len(r) < len(u) && s.AcquireDelay > 0 {
			time.Sleep(s.AcquireDelay)
		}
		held, _ := s.LeaseHolder.Acquire(context.Background(), nil)
		if held {
			return Decision{DownMembers: unreachable}
		}
		return Decision{DownSelf: true}
	}

	if s.Lease != nil && s.Lease.CheckLease() {
		return Decision{DownMembers: unreachable}
	}
	return Decision{DownSelf: true}
}

// ── DownAll ───────────────────────────────────────────────────────────────────

// DownAll is a Split Brain Resolver strategy that downs every member of the
// cluster — both sides of a partition — when any unreachable member is
// detected. Every surviving partition executes this decision, causing all
// nodes to leave the cluster and forcing a clean restart.
//
// Use this strategy only in environments where a complete restart is
// acceptable: CI clusters, short-lived test environments, or deployments
// with fast automated re-provisioning.
type DownAll struct{}

// Decide returns a decision to down every unreachable member AND down self,
// so that both partitions terminate. When there are no unreachable members
// no action is taken.
func (s *DownAll) Decide(_ MemberAddress, _ []Member, unreachable []Member) Decision {
	if len(unreachable) == 0 {
		return Decision{}
	}
	// DownSelf=true causes the local node to leave.
	// DownMembers=unreachable lets the leader explicitly down the other side
	// before leaving, accelerating gossip convergence.
	return Decision{DownSelf: true, DownMembers: unreachable}
}

// ── DownAllNodesInDataCenter ──────────────────────────────────────────────────

// DownAllNodesInDataCenter downs every member that belongs to the named data
// center when at least one member from that DC is unreachable.  This strategy
// is suited for drastic DC-level failure scenarios where the entire DC should
// be ejected from the cluster rather than allowing it to block convergence.
//
// The DataCenter field must match the name encoded in the member's "dc-<name>"
// role (e.g. DataCenter="us-east" matches role "dc-us-east").
// When DataCenter is empty the strategy is a no-op.
type DownAllNodesInDataCenter struct {
	// DataCenter is the name of the data center to target.
	DataCenter string
}

func (s *DownAllNodesInDataCenter) Decide(self MemberAddress, reachable, unreachable []Member) Decision {
	if s.DataCenter == "" {
		return Decision{}
	}

	// Check whether any unreachable member belongs to the target DC.
	anyUnreachableInDC := false
	for _, m := range unreachable {
		if m.Address.DataCenter == s.DataCenter {
			anyUnreachableInDC = true
			break
		}
	}
	if !anyUnreachableInDC {
		return Decision{} // no partition in target DC — nothing to do
	}

	// Down ALL members in the target DC (both reachable and unreachable sides).
	var toDown []Member
	for _, m := range append(reachable, unreachable...) {
		if m.Address.DataCenter == s.DataCenter {
			toDown = append(toDown, m)
		}
	}
	return Decision{DownMembers: toDown}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func filterByRole(members []Member, role string) []Member {
	if role == "" {
		return members
	}
	out := make([]Member, 0, len(members))
	for _, m := range members {
		for _, r := range m.Roles {
			if r == role {
				out = append(out, m)
				break
			}
		}
	}
	return out
}

func lowestMember(members []Member) Member {
	sorted := make([]Member, len(members))
	copy(sorted, members)
	sort.Slice(sorted, func(i, j int) bool {
		a, b := sorted[i].Address, sorted[j].Address
		if a.Host != b.Host {
			return a.Host < b.Host
		}
		return a.Port < b.Port
	})
	return sorted[0]
}

func oldestMember(members []Member) Member {
	best := members[0]
	for _, m := range members[1:] {
		if m.UpNumber < best.UpNumber ||
			(m.UpNumber == best.UpNumber && m.Address.Host < best.Address.Host) {
			best = m
		}
	}
	return best
}

func containsAddr(members []Member, addr MemberAddress) bool {
	for _, m := range members {
		if m.Address == addr {
			return true
		}
	}
	return false
}
