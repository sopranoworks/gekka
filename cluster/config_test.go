/*
 * config_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"context"
	"testing"
	"time"

	icluster "github.com/sopranoworks/gekka/internal/cluster"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"google.golang.org/protobuf/proto"
)

// newTestCM creates a minimal ClusterManager suitable for config tests.
func newTestCM() *ClusterManager {
	local := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			System:   proto.String("TestSystem"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(2551),
		},
		Uid:  proto.Uint32(1),
		Uid2: proto.Uint32(0),
	}
	router := func(_ context.Context, _ string, _ any) error { return nil }
	return NewClusterManager(local, router)
}

// ── NewInternalSBRStrategy ────────────────────────────────────────────────────

func TestNewInternalSBRStrategy_Off(t *testing.T) {
	for _, active := range []string{"", "off", "OFF", "  off  "} {
		strat := NewInternalSBRStrategy(InternalSBRConfig{ActiveStrategy: active})
		if strat != nil {
			t.Errorf("ActiveStrategy=%q: expected nil, got %T", active, strat)
		}
	}
}

func TestNewInternalSBRStrategy_Unknown(t *testing.T) {
	strat := NewInternalSBRStrategy(InternalSBRConfig{ActiveStrategy: "nonexistent"})
	if strat != nil {
		t.Errorf("unknown strategy: expected nil, got %T", strat)
	}
}

func TestNewInternalSBRStrategy_StaticQuorum(t *testing.T) {
	strat := NewInternalSBRStrategy(InternalSBRConfig{
		ActiveStrategy: "static-quorum",
		QuorumSize:     5,
	})
	sq, ok := strat.(*icluster.StaticQuorumStrategy)
	if !ok {
		t.Fatalf("expected *icluster.StaticQuorumStrategy, got %T", strat)
	}
	if sq.QuorumSize != 5 {
		t.Errorf("QuorumSize = %d, want 5", sq.QuorumSize)
	}
}

func TestNewInternalSBRStrategy_StaticQuorum_DefaultsToOne(t *testing.T) {
	strat := NewInternalSBRStrategy(InternalSBRConfig{
		ActiveStrategy: "static-quorum",
		QuorumSize:     0, // zero → default to 1
	})
	sq, ok := strat.(*icluster.StaticQuorumStrategy)
	if !ok {
		t.Fatalf("expected *icluster.StaticQuorumStrategy, got %T", strat)
	}
	if sq.QuorumSize != 1 {
		t.Errorf("QuorumSize = %d, want 1 (default)", sq.QuorumSize)
	}
}

func TestNewInternalSBRStrategy_KeepOldest(t *testing.T) {
	strat := NewInternalSBRStrategy(InternalSBRConfig{
		ActiveStrategy: "keep-oldest",
		Role:           "worker",
		DownIfAlone:    true,
	})
	ko, ok := strat.(*icluster.KeepOldestStrategy)
	if !ok {
		t.Fatalf("expected *icluster.KeepOldestStrategy, got %T", strat)
	}
	if ko.Role != "worker" {
		t.Errorf("Role = %q, want %q", ko.Role, "worker")
	}
	if !ko.DownIfAlone {
		t.Error("DownIfAlone = false, want true")
	}
}

// ── ApplyDetectorConfig ───────────────────────────────────────────────────────

func TestApplyDetectorConfig_NoOp_WhenZero(t *testing.T) {
	cm := newTestCM()
	// Capture baseline threshold (set by NewClusterManager → NewPhiAccrualFailureDetector).
	before := cm.Fd.threshold
	ApplyDetectorConfig(cm, FailureDetectorConfig{})
	if cm.Fd.threshold != before {
		t.Errorf("threshold changed after no-op apply: %v → %v", before, cm.Fd.threshold)
	}
}

func TestApplyDetectorConfig_SetsFields(t *testing.T) {
	cm := newTestCM()
	ApplyDetectorConfig(cm, FailureDetectorConfig{
		Threshold:       15.0,
		MaxSampleSize:   200,
		MinStdDeviation: 300 * time.Millisecond,
	})
	if cm.Fd.threshold != 15.0 {
		t.Errorf("threshold = %v, want 15.0", cm.Fd.threshold)
	}
	if cm.Fd.maxSampleSize != 200 {
		t.Errorf("maxSampleSize = %d, want 200", cm.Fd.maxSampleSize)
	}
	if cm.Fd.minStdDeviation != 300*time.Millisecond {
		t.Errorf("minStdDeviation = %v, want 300ms", cm.Fd.minStdDeviation)
	}
}

func TestApplyDetectorConfig_FallsBackToDefaults_WhenFieldsNegative(t *testing.T) {
	cm := newTestCM()
	// Negative values should be replaced by safe defaults.
	ApplyDetectorConfig(cm, FailureDetectorConfig{
		Threshold:       -1,
		MaxSampleSize:   -1,
		MinStdDeviation: -1,
	})
	if cm.Fd.threshold != 10.0 {
		t.Errorf("threshold = %v, want default 10.0", cm.Fd.threshold)
	}
	if cm.Fd.maxSampleSize != 1000 {
		t.Errorf("maxSampleSize = %d, want default 1000", cm.Fd.maxSampleSize)
	}
	if cm.Fd.minStdDeviation != 500*time.Millisecond {
		t.Errorf("minStdDeviation = %v, want default 500ms", cm.Fd.minStdDeviation)
	}
}

// ── ApplyInternalSBRConfig ────────────────────────────────────────────────────

func TestApplyInternalSBRConfig_NilStrategy_WhenOff(t *testing.T) {
	cm := newTestCM()
	ApplyInternalSBRConfig(cm, InternalSBRConfig{ActiveStrategy: "off"})
	if cm.SBRStrategy != nil {
		t.Errorf("expected nil SBRStrategy, got %T", cm.SBRStrategy)
	}
}

func TestApplyInternalSBRConfig_SetsStrategy(t *testing.T) {
	cm := newTestCM()
	ApplyInternalSBRConfig(cm, InternalSBRConfig{
		ActiveStrategy: "static-quorum",
		QuorumSize:     3,
		StableAfter:    15 * time.Second,
	})
	if _, ok := cm.SBRStrategy.(*icluster.StaticQuorumStrategy); !ok {
		t.Errorf("expected *icluster.StaticQuorumStrategy, got %T", cm.SBRStrategy)
	}
	if cm.SBRStableAfter != 15*time.Second {
		t.Errorf("SBRStableAfter = %v, want 15s", cm.SBRStableAfter)
	}
}

func TestApplyInternalSBRConfig_DoesNotOverrideStableAfter_WhenZero(t *testing.T) {
	cm := newTestCM()
	cm.SBRStableAfter = 30 * time.Second
	ApplyInternalSBRConfig(cm, InternalSBRConfig{
		ActiveStrategy: "keep-oldest",
		StableAfter:    0, // zero → leave existing value
	})
	if cm.SBRStableAfter != 30*time.Second {
		t.Errorf("SBRStableAfter overwritten: got %v, want 30s", cm.SBRStableAfter)
	}
}
