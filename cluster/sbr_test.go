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
)

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

func TestNewStrategy_AllStrategiesConstruct(t *testing.T) {
	cases := []SBRConfig{
		{ActiveStrategy: "keep-majority"},
		{ActiveStrategy: "keep-oldest"},
		{ActiveStrategy: "keep-referee", RefereeAddress: "host:2551"},
		{ActiveStrategy: "static-quorum", QuorumSize: 3},
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
