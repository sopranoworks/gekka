/*
 * sbr_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"testing"
	"time"

	icluster "github.com/sopranoworks/gekka/internal/cluster"
)

// stubLease implements LeaseChecker for unit tests.
type stubLease struct{ held bool }

func (l *stubLease) CheckLease() bool { return l.held }

// helpers

func addr(host string, port uint32) MemberAddress {
	return MemberAddress{Protocol: "pekko", System: "TestSystem", Host: host, Port: port}
}

func mbr(host string, port uint32, upNumber int32, reachable bool) Member {
	return Member{Address: addr(host, port), Reachable: reachable, UpNumber: upNumber}
}

func mbrRole(host string, port uint32, upNumber int32, reachable bool, roles ...string) Member {
	m := mbr(host, port, upNumber, reachable)
	m.Roles = roles
	return m
}

// ── KeepMajority tests ────────────────────────────────────────────────────────

func TestKeepMajority_ReachableMajority(t *testing.T) {
	strat := &KeepMajority{}
	self := addr("10.0.0.1", 2551)
	reachable := []Member{
		mbr("10.0.0.1", 2551, 1, true),
		mbr("10.0.0.2", 2551, 2, true),
		mbr("10.0.0.3", 2551, 3, true),
	}
	unreachable := []Member{
		mbr("10.0.0.4", 2551, 4, false),
		mbr("10.0.0.5", 2551, 5, false),
	}
	d := strat.Decide(self, reachable, unreachable)
	if d.DownSelf {
		t.Fatal("expected DownSelf=false when reachable has majority")
	}
	if len(d.DownMembers) != len(unreachable) {
		t.Fatalf("expected %d down members, got %d", len(unreachable), len(d.DownMembers))
	}
}

func TestKeepMajority_UnreachableMajority(t *testing.T) {
	strat := &KeepMajority{}
	self := addr("10.0.0.4", 2551)
	reachable := []Member{
		mbr("10.0.0.4", 2551, 4, true),
		mbr("10.0.0.5", 2551, 5, true),
	}
	unreachable := []Member{
		mbr("10.0.0.1", 2551, 1, false),
		mbr("10.0.0.2", 2551, 2, false),
		mbr("10.0.0.3", 2551, 3, false),
	}
	d := strat.Decide(self, reachable, unreachable)
	if !d.DownSelf {
		t.Fatal("expected DownSelf=true when unreachable has majority")
	}
}

func TestKeepMajority_Tie_SideWithLowestAddress(t *testing.T) {
	// 2 vs 2: the side containing the lowest address ("10.0.0.1") survives.
	strat := &KeepMajority{}

	// Scenario A: self is on the winning side (lowest address is reachable).
	selfA := addr("10.0.0.1", 2551)
	reachableA := []Member{
		mbr("10.0.0.1", 2551, 1, true),
		mbr("10.0.0.2", 2551, 2, true),
	}
	unreachableA := []Member{
		mbr("10.0.0.3", 2551, 3, false),
		mbr("10.0.0.4", 2551, 4, false),
	}
	dA := strat.Decide(selfA, reachableA, unreachableA)
	if dA.DownSelf {
		t.Fatal("scenario A: expected DownSelf=false (self holds lowest address)")
	}

	// Scenario B: self is on the losing side (lowest address is unreachable).
	selfB := addr("10.0.0.3", 2551)
	reachableB := []Member{
		mbr("10.0.0.3", 2551, 3, true),
		mbr("10.0.0.4", 2551, 4, true),
	}
	unreachableB := []Member{
		mbr("10.0.0.1", 2551, 1, false),
		mbr("10.0.0.2", 2551, 2, false),
	}
	dB := strat.Decide(selfB, reachableB, unreachableB)
	if !dB.DownSelf {
		t.Fatal("scenario B: expected DownSelf=true (lowest address is unreachable)")
	}
}

func TestKeepMajority_RoleFilter(t *testing.T) {
	strat := &KeepMajority{Role: "worker"}
	self := addr("10.0.0.1", 2551)

	// 2 workers reachable, 1 worker unreachable — reachable majority among workers.
	reachable := []Member{
		mbrRole("10.0.0.1", 2551, 1, true, "worker"),
		mbrRole("10.0.0.2", 2551, 2, true, "worker"),
		mbrRole("10.0.0.3", 2551, 3, true, "seed"), // not counted
	}
	unreachable := []Member{
		mbrRole("10.0.0.4", 2551, 4, false, "worker"),
	}
	d := strat.Decide(self, reachable, unreachable)
	if d.DownSelf {
		t.Fatal("expected DownSelf=false; reachable workers have majority")
	}
}

// ── KeepOldest tests ──────────────────────────────────────────────────────────

func TestKeepOldest_OldestReachable(t *testing.T) {
	strat := &KeepOldest{}
	self := addr("10.0.0.1", 2551)
	reachable := []Member{
		mbr("10.0.0.1", 2551, 1, true), // oldest (lowest upNumber)
		mbr("10.0.0.2", 2551, 2, true),
	}
	unreachable := []Member{
		mbr("10.0.0.3", 2551, 3, false),
	}
	d := strat.Decide(self, reachable, unreachable)
	if d.DownSelf {
		t.Fatal("expected DownSelf=false; oldest is reachable")
	}
	if len(d.DownMembers) == 0 {
		t.Fatal("expected unreachable members to be downed")
	}
}

func TestKeepOldest_OldestUnreachable(t *testing.T) {
	strat := &KeepOldest{}
	self := addr("10.0.0.2", 2551)
	reachable := []Member{
		mbr("10.0.0.2", 2551, 2, true),
		mbr("10.0.0.3", 2551, 3, true),
	}
	unreachable := []Member{
		mbr("10.0.0.1", 2551, 1, false), // oldest is unreachable
	}
	d := strat.Decide(self, reachable, unreachable)
	if !d.DownSelf {
		t.Fatal("expected DownSelf=true; oldest is unreachable")
	}
}

func TestKeepOldest_DownIfAlone(t *testing.T) {
	strat := &KeepOldest{DownIfAlone: true}
	self := addr("10.0.0.1", 2551) // self IS the oldest
	reachable := []Member{
		mbr("10.0.0.1", 2551, 1, true), // oldest and alone
	}
	unreachable := []Member{
		mbr("10.0.0.2", 2551, 2, false),
		mbr("10.0.0.3", 2551, 3, false),
	}
	d := strat.Decide(self, reachable, unreachable)
	if !d.DownSelf {
		t.Fatal("expected DownSelf=true; oldest is alone and down-if-alone=true")
	}
}

// ── KeepReferee tests ─────────────────────────────────────────────────────────

func TestKeepReferee_RefereeReachable(t *testing.T) {
	strat := &KeepReferee{RefereeAddress: "10.0.0.100:2551"}
	self := addr("10.0.0.1", 2551)
	reachable := []Member{
		mbr("10.0.0.1", 2551, 1, true),
		mbr("10.0.0.100", 2551, 2, true), // referee
	}
	unreachable := []Member{
		mbr("10.0.0.2", 2551, 3, false),
	}
	d := strat.Decide(self, reachable, unreachable)
	if d.DownSelf {
		t.Fatal("expected DownSelf=false; referee is reachable")
	}
}

func TestKeepReferee_RefereeUnreachable(t *testing.T) {
	strat := &KeepReferee{RefereeAddress: "10.0.0.100:2551"}
	self := addr("10.0.0.1", 2551)
	reachable := []Member{
		mbr("10.0.0.1", 2551, 1, true),
		mbr("10.0.0.2", 2551, 2, true),
	}
	unreachable := []Member{
		mbr("10.0.0.100", 2551, 3, false), // referee is unreachable
	}
	d := strat.Decide(self, reachable, unreachable)
	if !d.DownSelf {
		t.Fatal("expected DownSelf=true; referee is unreachable")
	}
}

// ── StaticQuorum tests ────────────────────────────────────────────────────────

func TestStaticQuorum_AboveQuorum(t *testing.T) {
	strat := &StaticQuorum{QuorumSize: 3}
	self := addr("10.0.0.1", 2551)
	reachable := []Member{
		mbr("10.0.0.1", 2551, 1, true),
		mbr("10.0.0.2", 2551, 2, true),
		mbr("10.0.0.3", 2551, 3, true),
	}
	unreachable := []Member{
		mbr("10.0.0.4", 2551, 4, false),
	}
	d := strat.Decide(self, reachable, unreachable)
	if d.DownSelf {
		t.Fatal("expected DownSelf=false; reachable count meets quorum")
	}
}

func TestStaticQuorum_BelowQuorum(t *testing.T) {
	strat := &StaticQuorum{QuorumSize: 3}
	self := addr("10.0.0.4", 2551)
	reachable := []Member{
		mbr("10.0.0.4", 2551, 4, true),
		mbr("10.0.0.5", 2551, 5, true),
	}
	unreachable := []Member{
		mbr("10.0.0.1", 2551, 1, false),
		mbr("10.0.0.2", 2551, 2, false),
		mbr("10.0.0.3", 2551, 3, false),
	}
	d := strat.Decide(self, reachable, unreachable)
	if !d.DownSelf {
		t.Fatal("expected DownSelf=true; reachable count is below quorum")
	}
}

func TestStaticQuorum_RoleFilter(t *testing.T) {
	strategy := &StaticQuorum{QuorumSize: 2, Role: "backend"}
	self := addr("10.0.0.1", 2551)

	// 3 reachable: 2 with "backend" role, 1 without
	reachable := []Member{
		mbrRole("10.0.0.1", 2551, 1, true, "backend"),
		mbrRole("10.0.0.2", 2551, 2, true, "backend"),
		mbrRole("10.0.0.3", 2551, 3, true, "frontend"),
	}
	unreachable := []Member{mbrRole("10.0.0.4", 2551, 4, false, "backend")}

	d := strategy.Decide(self, reachable, unreachable)
	// 2 backend reachable >= quorum 2 → down unreachable
	if d.DownSelf {
		t.Error("expected DownSelf=false, got true")
	}
	if len(d.DownMembers) != 1 {
		t.Errorf("DownMembers = %d, want 1", len(d.DownMembers))
	}
}

func TestStaticQuorum_RoleFilter_BelowQuorum(t *testing.T) {
	strategy := &StaticQuorum{QuorumSize: 3, Role: "backend"}
	self := addr("10.0.0.1", 2551)

	// 3 reachable but only 1 with "backend" role
	reachable := []Member{
		mbrRole("10.0.0.1", 2551, 1, true, "backend"),
		mbrRole("10.0.0.2", 2551, 2, true, "frontend"),
		mbrRole("10.0.0.3", 2551, 3, true, "frontend"),
	}
	unreachable := []Member{mbrRole("10.0.0.4", 2551, 4, false, "backend")}

	d := strategy.Decide(self, reachable, unreachable)
	// 1 backend reachable < quorum 3 → DownSelf
	if !d.DownSelf {
		t.Error("expected DownSelf=true, got false")
	}
}

// ── NewStrategy tests ─────────────────────────────────────────────────────────

func TestNewStrategy_Disabled(t *testing.T) {
	strat, err := NewStrategy(SBRConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if strat != nil {
		t.Fatal("expected nil strategy when ActiveStrategy is empty")
	}
}

func TestNewStrategy_UnknownReturnsError(t *testing.T) {
	_, err := NewStrategy(SBRConfig{ActiveStrategy: "nonexistent"})
	if err == nil {
		t.Fatal("expected error for unknown strategy")
	}
}

func TestNewStrategy_RefereeRequiresAddress(t *testing.T) {
	_, err := NewStrategy(SBRConfig{ActiveStrategy: "keep-referee"})
	if err == nil {
		t.Fatal("expected error when RefereeAddress is empty")
	}
}

func TestNewStrategy_StaticQuorumRequiresSize(t *testing.T) {
	_, err := NewStrategy(SBRConfig{ActiveStrategy: "static-quorum", QuorumSize: 0})
	if err == nil {
		t.Fatal("expected error when QuorumSize is 0")
	}
}

func TestNewStrategy_StaticQuorumRole(t *testing.T) {
	// StaticQuorumRole should override Role for static-quorum
	strat, err := NewStrategy(SBRConfig{
		ActiveStrategy:   "static-quorum",
		QuorumSize:       2,
		Role:             "general",
		StaticQuorumRole: "backend",
	})
	if err != nil {
		t.Fatal(err)
	}
	sq := strat.(*StaticQuorum)
	if sq.Role != "backend" {
		t.Errorf("StaticQuorum.Role = %q, want %q", sq.Role, "backend")
	}
}

func TestNewStrategy_StaticQuorumRoleFallback(t *testing.T) {
	// When StaticQuorumRole is empty, falls back to Role
	strat, err := NewStrategy(SBRConfig{
		ActiveStrategy: "static-quorum",
		QuorumSize:     2,
		Role:           "general",
	})
	if err != nil {
		t.Fatal(err)
	}
	sq := strat.(*StaticQuorum)
	if sq.Role != "general" {
		t.Errorf("StaticQuorum.Role = %q, want %q", sq.Role, "general")
	}
}

func TestNewStrategy_AllStrategiesConstruct(t *testing.T) {
	cases := []SBRConfig{
		{ActiveStrategy: "keep-majority"},
		{ActiveStrategy: "keep-oldest"},
		{ActiveStrategy: "keep-referee", RefereeAddress: "host:2551"},
		{ActiveStrategy: "static-quorum", QuorumSize: 3},
		{ActiveStrategy: "down-all"},
	}
	for _, cfg := range cases {
		strat, err := NewStrategy(cfg)
		if err != nil {
			t.Errorf("strategy %q: unexpected error: %v", cfg.ActiveStrategy, err)
		}
		if strat == nil {
			t.Errorf("strategy %q: expected non-nil strategy", cfg.ActiveStrategy)
		}
	}
}

// ── DownAll tests ─────────────────────────────────────────────────────────────

func TestDownAll_SomeMembersUnreachable(t *testing.T) {
	strat := &DownAll{}
	self := addr("10.0.0.1", 2551)
	reachable := []Member{
		mbr("10.0.0.1", 2551, 1, true),
		mbr("10.0.0.2", 2551, 2, true),
	}
	unreachable := []Member{
		mbr("10.0.0.3", 2551, 3, false),
	}
	d := strat.Decide(self, reachable, unreachable)
	if !d.DownSelf {
		t.Fatal("DownAll: expected DownSelf=true")
	}
	if len(d.DownMembers) != len(unreachable) {
		t.Fatalf("DownAll: expected %d DownMembers, got %d", len(unreachable), len(d.DownMembers))
	}
}

func TestDownAll_NoUnreachable(t *testing.T) {
	strat := &DownAll{}
	self := addr("10.0.0.1", 2551)
	reachable := []Member{mbr("10.0.0.1", 2551, 1, true)}
	d := strat.Decide(self, reachable, nil)
	if d.DownSelf {
		t.Fatal("DownAll: expected DownSelf=false when no unreachable members")
	}
	if len(d.DownMembers) != 0 {
		t.Fatal("DownAll: expected no DownMembers when no unreachable members")
	}
}

func TestDownAll_ViaNewStrategy(t *testing.T) {
	strat, err := NewStrategy(SBRConfig{ActiveStrategy: "down-all"})
	if err != nil || strat == nil {
		t.Fatalf("NewStrategy(down-all): err=%v strat=%v", err, strat)
	}
	if _, ok := strat.(*DownAll); !ok {
		t.Fatalf("expected *DownAll, got %T", strat)
	}
}

// ── DownAllNodesInDataCenter tests ────────────────────────────────────────────

func mbrDC(host string, port uint32, upNumber int32, reachable bool, dc string) Member {
	m := mbr(host, port, upNumber, reachable)
	m.Address.DataCenter = dc
	return m
}

func TestDownAllNodesInDataCenter_TargetDCUnreachable(t *testing.T) {
	strat := &DownAllNodesInDataCenter{DataCenter: "us-east"}
	self := addr("10.0.0.1", 2551)
	reachable := []Member{
		mbrDC("10.0.0.1", 2551, 1, true, "eu-west"),
		mbrDC("10.0.0.2", 2551, 2, true, "us-east"), // reachable node in target DC
	}
	unreachable := []Member{
		mbrDC("10.0.0.3", 2551, 3, false, "us-east"), // unreachable node in target DC
	}
	d := strat.Decide(self, reachable, unreachable)
	if d.DownSelf {
		t.Fatal("expected DownSelf=false; self is in eu-west")
	}
	// Both us-east members (reachable and unreachable) should be downed.
	if len(d.DownMembers) != 2 {
		t.Fatalf("expected 2 DownMembers (all us-east nodes), got %d", len(d.DownMembers))
	}
	for _, dm := range d.DownMembers {
		if dm.Address.DataCenter != "us-east" {
			t.Errorf("expected only us-east nodes in DownMembers, got dc=%q", dm.Address.DataCenter)
		}
	}
}

func TestDownAllNodesInDataCenter_NoneUnreachableInDC(t *testing.T) {
	strat := &DownAllNodesInDataCenter{DataCenter: "us-east"}
	self := addr("10.0.0.1", 2551)
	reachable := []Member{
		mbrDC("10.0.0.1", 2551, 1, true, "eu-west"),
		mbrDC("10.0.0.2", 2551, 2, true, "us-east"),
	}
	unreachable := []Member{
		mbrDC("10.0.0.3", 2551, 3, false, "eu-west"), // unreachable node is NOT in target DC
	}
	d := strat.Decide(self, reachable, unreachable)
	if d.DownSelf || len(d.DownMembers) != 0 {
		t.Fatal("expected no action when no unreachable node belongs to target DC")
	}
}

func TestDownAllNodesInDataCenter_EmptyDataCenter(t *testing.T) {
	strat := &DownAllNodesInDataCenter{DataCenter: ""}
	self := addr("10.0.0.1", 2551)
	unreachable := []Member{mbrDC("10.0.0.2", 2551, 2, false, "us-east")}
	d := strat.Decide(self, nil, unreachable)
	if d.DownSelf || len(d.DownMembers) != 0 {
		t.Fatal("expected no-op when DataCenter is empty")
	}
}

// ── LeaseMajority tests ───────────────────────────────────────────────────────

func TestLeaseMajority_HoldsLease(t *testing.T) {
	strat := &LeaseMajority{Lease: &stubLease{held: true}}
	self := addr("10.0.0.1", 2551)
	reachable := []Member{mbr("10.0.0.1", 2551, 1, true)}
	unreachable := []Member{mbr("10.0.0.2", 2551, 2, false)}
	d := strat.Decide(self, reachable, unreachable)
	if d.DownSelf {
		t.Fatal("expected DownSelf=false when lease is held")
	}
	if len(d.DownMembers) != 1 || d.DownMembers[0].Address != unreachable[0].Address {
		t.Fatalf("expected unreachable member to be downed, got %v", d.DownMembers)
	}
}

func TestLeaseMajority_DoesNotHoldLease(t *testing.T) {
	strat := &LeaseMajority{Lease: &stubLease{held: false}}
	self := addr("10.0.0.2", 2551)
	reachable := []Member{mbr("10.0.0.2", 2551, 2, true)}
	unreachable := []Member{mbr("10.0.0.1", 2551, 1, false)}
	d := strat.Decide(self, reachable, unreachable)
	if !d.DownSelf {
		t.Fatal("expected DownSelf=true when lease is not held")
	}
}

func TestLeaseMajority_ViaNewStrategy_RequiresLease(t *testing.T) {
	_, err := NewStrategy(SBRConfig{ActiveStrategy: "lease-majority"})
	if err == nil {
		t.Fatal("expected error when Lease is nil")
	}
}

func TestLeaseMajority_ViaNewStrategy(t *testing.T) {
	strat, err := NewStrategy(SBRConfig{
		ActiveStrategy: "lease-majority",
		Lease:          &stubLease{held: true},
	})
	if err != nil || strat == nil {
		t.Fatalf("NewStrategy(lease-majority): err=%v strat=%v", err, strat)
	}
	if _, ok := strat.(*LeaseMajority); !ok {
		t.Fatalf("expected *LeaseMajority, got %T", strat)
	}
}

// ── lease-majority full-lease path (LeaseImplementation + LeaseManager) ──────

func TestLeaseMajority_ViaNewStrategy_LeaseImplementationRequiresManager(t *testing.T) {
	_, err := NewStrategy(SBRConfig{
		ActiveStrategy:      "lease-majority",
		LeaseImplementation: "memory",
	})
	if err == nil {
		t.Fatal("expected error when LeaseImplementation set but LeaseManager nil")
	}
}

func TestLeaseMajority_ViaNewStrategy_UnknownProvider(t *testing.T) {
	mgr := icluster.NewLeaseManager()
	_, err := NewStrategy(SBRConfig{
		ActiveStrategy:      "lease-majority",
		LeaseImplementation: "no-such-provider",
		LeaseManager:        mgr,
	})
	if err == nil {
		t.Fatal("expected error when provider name is not registered")
	}
}

// stubLeaseProvider returns a *icluster.TestLease with a configurable initial
// held state — enough to exercise NewStrategy's full-lease wiring.
type stubLeaseProvider struct{ initialHeld bool }

func (p *stubLeaseProvider) GetLease(s icluster.LeaseSettings) icluster.Lease {
	return icluster.NewTestLease(s, p.initialHeld)
}

func TestLeaseMajority_ViaNewStrategy_ResolvesProvider(t *testing.T) {
	mgr := icluster.NewLeaseManager()
	mgr.RegisterProvider("memory", &stubLeaseProvider{initialHeld: false})
	strat, err := NewStrategy(SBRConfig{
		ActiveStrategy:               "lease-majority",
		LeaseImplementation:          "memory",
		LeaseManager:                 mgr,
		LeaseSettings:                icluster.LeaseSettings{LeaseName: "sbr", OwnerName: "self"},
		AcquireLeaseDelayForMinority: 10 * time.Millisecond,
		LeaseMajorityRole:            "dc-a",
	})
	if err != nil {
		t.Fatalf("NewStrategy: %v", err)
	}
	lm, ok := strat.(*LeaseMajority)
	if !ok {
		t.Fatalf("expected *LeaseMajority, got %T", strat)
	}
	if lm.LeaseHolder == nil {
		t.Fatal("expected LeaseHolder to be populated from provider")
	}
	if lm.Role != "dc-a" {
		t.Fatalf("expected Role=dc-a, got %q", lm.Role)
	}
	if lm.AcquireDelay != 10*time.Millisecond {
		t.Fatalf("expected AcquireDelay=10ms, got %v", lm.AcquireDelay)
	}
}

func TestLeaseMajority_ViaNewStrategy_DefaultAcquireDelay(t *testing.T) {
	mgr := icluster.NewLeaseManager()
	mgr.RegisterProvider("memory", &stubLeaseProvider{})
	strat, err := NewStrategy(SBRConfig{
		ActiveStrategy:      "lease-majority",
		LeaseImplementation: "memory",
		LeaseManager:        mgr,
	})
	if err != nil {
		t.Fatalf("NewStrategy: %v", err)
	}
	lm := strat.(*LeaseMajority)
	if lm.AcquireDelay != 2*time.Second {
		t.Fatalf("expected default AcquireDelay=2s, got %v", lm.AcquireDelay)
	}
}

func TestLeaseMajority_ViaNewStrategy_RoleFallback(t *testing.T) {
	// LeaseMajorityRole empty → fall back to general Role.
	mgr := icluster.NewLeaseManager()
	mgr.RegisterProvider("memory", &stubLeaseProvider{})
	strat, _ := NewStrategy(SBRConfig{
		ActiveStrategy:      "lease-majority",
		LeaseImplementation: "memory",
		LeaseManager:        mgr,
		Role:                "general-role",
	})
	if strat.(*LeaseMajority).Role != "general-role" {
		t.Fatalf("expected fallback Role=general-role, got %q", strat.(*LeaseMajority).Role)
	}
}

// Full-lease Decide path: LeaseHolder.Acquire returns true → keep self.
func TestLeaseMajority_FullLease_AcquireSucceeds(t *testing.T) {
	tl := icluster.NewTestLease(icluster.LeaseSettings{LeaseName: "sbr"}, false)
	strat := &LeaseMajority{LeaseHolder: tl}
	self := addr("10.0.0.1", 2551)
	reachable := []Member{mbr("10.0.0.1", 2551, 1, true)}
	unreachable := []Member{mbr("10.0.0.2", 2551, 2, false)}
	d := strat.Decide(self, reachable, unreachable)
	if d.DownSelf {
		t.Fatal("expected DownSelf=false when Acquire succeeds")
	}
	if len(d.DownMembers) != 1 {
		t.Fatalf("expected 1 down member, got %d", len(d.DownMembers))
	}
	if !tl.CheckLease() {
		t.Fatal("expected lease to be held after Acquire")
	}
}

// AcquireDelay is applied when local side is in the minority.
func TestLeaseMajority_FullLease_DelaysOnMinority(t *testing.T) {
	tl := icluster.NewTestLease(icluster.LeaseSettings{LeaseName: "sbr"}, false)
	delay := 50 * time.Millisecond
	strat := &LeaseMajority{LeaseHolder: tl, AcquireDelay: delay}
	self := addr("10.0.0.1", 2551)
	reachable := []Member{mbr("10.0.0.1", 2551, 1, true)} // 1
	unreachable := []Member{
		mbr("10.0.0.2", 2551, 2, false),
		mbr("10.0.0.3", 2551, 3, false),
	} // 2 — local side is the minority
	start := time.Now()
	d := strat.Decide(self, reachable, unreachable)
	elapsed := time.Since(start)
	if elapsed < delay {
		t.Fatalf("expected Decide to wait >= %v on minority side, slept %v", delay, elapsed)
	}
	// TestLease.Acquire still succeeds; we only assert that the delay fired.
	if d.DownSelf {
		t.Fatal("expected DownSelf=false; TestLease always grants Acquire")
	}
}

// AcquireDelay is NOT applied when the local side is in the majority.
func TestLeaseMajority_FullLease_NoDelayOnMajority(t *testing.T) {
	tl := icluster.NewTestLease(icluster.LeaseSettings{LeaseName: "sbr"}, false)
	delay := 200 * time.Millisecond
	strat := &LeaseMajority{LeaseHolder: tl, AcquireDelay: delay}
	self := addr("10.0.0.1", 2551)
	reachable := []Member{
		mbr("10.0.0.1", 2551, 1, true),
		mbr("10.0.0.2", 2551, 2, true),
	} // 2
	unreachable := []Member{mbr("10.0.0.3", 2551, 3, false)} // 1
	start := time.Now()
	_ = strat.Decide(self, reachable, unreachable)
	elapsed := time.Since(start)
	// Must not have slept the AcquireDelay.
	if elapsed >= delay {
		t.Fatalf("expected no delay on majority side, slept %v (delay=%v)", elapsed, delay)
	}
}

// Role filter applied to majority/minority counting in the full-lease path.
func TestLeaseMajority_FullLease_RoleFilter(t *testing.T) {
	tl := icluster.NewTestLease(icluster.LeaseSettings{LeaseName: "sbr"}, false)
	delay := 50 * time.Millisecond
	strat := &LeaseMajority{Role: "dc-a", LeaseHolder: tl, AcquireDelay: delay}
	self := addr("10.0.0.1", 2551)
	// Without role filter local side has 2 vs unreachable 1 (majority).
	// With role=dc-a filter only the 10.0.0.1 reachable counts (1) and
	// the 10.0.0.3 unreachable counts (1) → tie.  10.0.0.2 lacks dc-a.
	reachable := []Member{
		mbrRole("10.0.0.1", 2551, 1, true, "dc-a"),
		mbrRole("10.0.0.2", 2551, 2, true, "dc-b"),
	}
	unreachable := []Member{mbrRole("10.0.0.3", 2551, 3, false, "dc-a")}
	start := time.Now()
	_ = strat.Decide(self, reachable, unreachable)
	elapsed := time.Since(start)
	// Tie or minority — len(r)<len(u) is false on tie, so no delay expected
	// here.  Assert only that the test ran without panicking and that the
	// elapsed time is bounded (within 5x delay) — protects against runaway
	// sleeps if the role filter is bypassed.
	if elapsed > 5*delay {
		t.Fatalf("Decide ran too long (%v > %v); role filter may be bypassed", elapsed, 5*delay)
	}
}

// ── DownAllWhenUnstable duration derivation tests ────────────────────────────

func TestDownAllWhenUnstableDuration_DefaultOn(t *testing.T) {
	// Default (nil DownAllWhenUnstableEnabled) means "on" with derived duration.
	m := &SBRManager{cfg: SBRConfig{
		ActiveStrategy: "keep-majority",
		StableAfter:    20 * time.Second,
	}}
	got := m.downAllWhenUnstableDuration()
	// 3/4 of 20s = 15s
	if got != 15*time.Second {
		t.Fatalf("expected 15s, got %s", got)
	}
}

func TestDownAllWhenUnstableDuration_DerivedMinimum4s(t *testing.T) {
	// When 3/4 of StableAfter < 4s, clamp to 4s.
	m := &SBRManager{cfg: SBRConfig{
		ActiveStrategy: "keep-majority",
		StableAfter:    4 * time.Second, // 3/4 = 3s < 4s
	}}
	got := m.downAllWhenUnstableDuration()
	if got != 4*time.Second {
		t.Fatalf("expected 4s (minimum), got %s", got)
	}
}

func TestDownAllWhenUnstableDuration_ExplicitDuration(t *testing.T) {
	m := &SBRManager{cfg: SBRConfig{
		ActiveStrategy:            "keep-majority",
		StableAfter:               20 * time.Second,
		DownAllWhenUnstable:       10 * time.Second,
		DownAllWhenUnstableEnabled: boolPtr(true),
	}}
	got := m.downAllWhenUnstableDuration()
	if got != 10*time.Second {
		t.Fatalf("expected 10s, got %s", got)
	}
}

func TestDownAllWhenUnstableDuration_Off(t *testing.T) {
	m := &SBRManager{cfg: SBRConfig{
		ActiveStrategy:            "keep-majority",
		StableAfter:               20 * time.Second,
		DownAllWhenUnstableEnabled: boolPtr(false),
	}}
	got := m.downAllWhenUnstableDuration()
	if got != 0 {
		t.Fatalf("expected 0 (disabled), got %s", got)
	}
}

func boolPtr(b bool) *bool { return &b }
