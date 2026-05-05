/*
 * compression_production_wiring_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Sub-plan 8g — production-wiring tests for CompressionTableManager.
// These tests observe runtime behaviour produced by
// StartCompressionTableManager: that the CTM is attached to the
// NodeManager (so the inbound advert handler at association.go line
// ~2802 reaches it), that the configured caps are enforced through the
// production-attached CTM, and that the configured advertisement
// intervals drive a real ticker spawned from production code.

package core

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

func newWiringTestNodeManager() (*NodeManager, *actor.Router) {
	nm := NewNodeManager(&gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("local"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(0),
	}, 1)
	return nm, actor.NewRouter(nm)
}

func TestStartCompressionTableManager_AttachesCtmToNodeManager(t *testing.T) {
	nm, router := newWiringTestNodeManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctm := StartCompressionTableManager(ctx, nm, router)
	if ctm == nil {
		t.Fatal("StartCompressionTableManager returned nil CTM")
	}
	if nm.compressionMgr == nil {
		t.Fatal("nm.compressionMgr is nil after StartCompressionTableManager — production wiring did not attach")
	}
	if nm.compressionMgr != ctm {
		t.Fatal("nm.compressionMgr does not match returned CTM")
	}
}

func TestStartCompressionTableManager_ActorRefsMaxRejectsOversize(t *testing.T) {
	nm, router := newWiringTestNodeManager()
	nm.CompressionActorRefsMax = 2
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	StartCompressionTableManager(ctx, nm, router)
	if nm.compressionMgr == nil {
		t.Fatal("CTM not attached to NodeManager in production wiring path")
	}

	originUid := uint64(4242)
	nm.compressionMgr.UpdateActorRefTable(originUid, 1,
		[]string{"a", "b", "c"},
		[]uint32{1, 2, 3},
	)
	if _, err := nm.compressionMgr.LookupActorRef(originUid, 1); err == nil {
		t.Fatal("oversize ActorRef table update was accepted; production-wired actor-refs.max cap not enforced")
	}
}

func TestStartCompressionTableManager_ManifestsMaxRejectsOversize(t *testing.T) {
	nm, router := newWiringTestNodeManager()
	nm.CompressionManifestsMax = 2
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	StartCompressionTableManager(ctx, nm, router)
	if nm.compressionMgr == nil {
		t.Fatal("CTM not attached to NodeManager in production wiring path")
	}

	originUid := uint64(5151)
	nm.compressionMgr.UpdateManifestTable(originUid, 1,
		[]string{"M1", "M2", "M3"},
		[]uint32{1, 2, 3},
	)
	if _, err := nm.compressionMgr.LookupManifest(originUid, 1); err == nil {
		t.Fatal("oversize Manifest table update was accepted; production-wired manifests.max cap not enforced")
	}
}

func TestStartCompressionTableManager_ActorRefsSchedulerFires(t *testing.T) {
	nm, router := newWiringTestNodeManager()
	nm.CompressionActorRefsAdvertisementInterval = 20 * time.Millisecond
	// Manifests interval left at default (1m) so that ticker doesn't
	// fire and pollute the actor-ref counter.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	StartCompressionTableManager(ctx, nm, router)

	var actorRefTicks atomic.Int64
	nm.compressionMgr.SetAdvertiseCallback(func(isActorRef bool) {
		if isActorRef {
			actorRefTicks.Add(1)
		}
	})

	time.Sleep(120 * time.Millisecond)
	cancel()

	if got := actorRefTicks.Load(); got < 3 {
		t.Fatalf("actor-refs scheduler fired %d times in 120ms with 20ms interval; production interval not driving the ticker", got)
	}
}

func TestStartCompressionTableManager_ManifestsSchedulerFires(t *testing.T) {
	nm, router := newWiringTestNodeManager()
	nm.CompressionManifestsAdvertisementInterval = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	StartCompressionTableManager(ctx, nm, router)

	var manifestTicks atomic.Int64
	nm.compressionMgr.SetAdvertiseCallback(func(isActorRef bool) {
		if !isActorRef {
			manifestTicks.Add(1)
		}
	})

	time.Sleep(120 * time.Millisecond)
	cancel()

	if got := manifestTicks.Load(); got < 3 {
		t.Fatalf("manifests scheduler fired %d times in 120ms with 20ms interval; production interval not driving the ticker", got)
	}
}
