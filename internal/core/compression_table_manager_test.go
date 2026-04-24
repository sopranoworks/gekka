/*
 * compression_table_manager_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

func TestCompressionTableManager_UpdateAndLookup(t *testing.T) {
	nm := NewNodeManager(&gproto_remote.Address{Hostname: proto.String("local")}, 0)
	router := actor.NewRouter(nm)
	ctm := NewCompressionTableManager(router)

	originUid := uint64(12345)

	// Update ActorRef table
	keys1 := []string{"pekko://sys@host:2552/user/a", "pekko://sys@host:2552/user/b"}
	values1 := []uint32{1, 2}
	ctm.UpdateActorRefTable(originUid, 1, keys1, values1)

	// Lookup Valid ActorRef
	res, err := ctm.LookupActorRef(originUid, 1)
	if err != nil || res != keys1[0] {
		t.Errorf("expected %s, got %s (err: %v)", keys1[0], res, err)
	}

	res, err = ctm.LookupActorRef(originUid, 2)
	if err != nil || res != keys1[1] {
		t.Errorf("expected %s, got %s (err: %v)", keys1[1], res, err)
	}

	// Lookup Invalid ID
	_, err = ctm.LookupActorRef(originUid, 99)
	if err == nil {
		t.Error("expected error for missing id 99, got nil")
	}

	// Lookup Invalid UID
	_, err = ctm.LookupActorRef(99999, 1)
	if err == nil {
		t.Error("expected error for missing originUid, got nil")
	}

	// Update with higher version
	keys2 := []string{"pekko://sys@host:2552/user/c"}
	values2 := []uint32{3}
	ctm.UpdateActorRefTable(originUid, 2, keys2, values2)

	// Old keys should be gone (table replaced)
	_, err = ctm.LookupActorRef(originUid, 1)
	if err == nil {
		t.Error("expected error for old id 1 after version update, got nil")
	}

	// New key should exist
	res, err = ctm.LookupActorRef(originUid, 3)
	if err != nil || res != keys2[0] {
		t.Errorf("expected %s, got %s (err: %v)", keys2[0], res, err)
	}

	// Ignore lower version update
	ctm.UpdateActorRefTable(originUid, 1, keys1, values1)
	res, _ = ctm.LookupActorRef(originUid, 3)
	if res != keys2[0] {
		t.Errorf("expected table to remain at version 2, got %s", res)
	}
}

func TestCompressionTableManager_ConcurrentAccess(t *testing.T) {
	nm := NewNodeManager(&gproto_remote.Address{Hostname: proto.String("local")}, 0)
	router := actor.NewRouter(nm)
	ctm := NewCompressionTableManager(router)

	originUid := uint64(777)

	// Start by adding an initial state
	ctm.UpdateManifestTable(originUid, 1, []string{"ManifestA"}, []uint32{1})

	var wg sync.WaitGroup
	wg.Add(2)

	// Writer goroutine
	go func() {
		defer wg.Done()
		for i := uint32(2); i <= 100; i++ {
			ctm.UpdateManifestTable(originUid, i, []string{"ManifestX"}, []uint32{i})
		}
	}()

	// Reader goroutine
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			// Ignore errors, just checking for race panics
			_, _ = ctm.LookupManifest(originUid, 1)
			_, _ = ctm.LookupManifest(originUid, 100)
		}
	}()

	wg.Wait()
}

func TestHandleAdvertisement(t *testing.T) {
	nm := NewNodeManager(&gproto_remote.Address{Hostname: proto.String("local")}, 0)
	router := actor.NewRouter(nm)
	ctm := NewCompressionTableManager(router)

	originUid := uint64(555)
	version := uint32(1)
	keys := []string{"ActorX"}
	values := []uint32{42}

	adv := &gproto_remote.CompressionTableAdvertisement{
		From:         &gproto_remote.UniqueAddress{Address: &gproto_remote.Address{System: proto.String("sys"), Hostname: proto.String("remote"), Port: proto.Uint32(2552)}},
		OriginUid:    &originUid,
		TableVersion: &version,
		Keys:         keys,
		Values:       values,
	}

	local := &gproto_remote.UniqueAddress{Address: &gproto_remote.Address{System: proto.String("sys"), Hostname: proto.String("local"), Port: proto.Uint32(2552)}}

	// Since router has no real connection, it might error on send, but we mainly care if table updates
	_ = ctm.HandleAdvertisement(context.Background(), adv, true, local)

	res, err := ctm.LookupActorRef(originUid, 42)
	if err != nil || res != "ActorX" {
		t.Errorf("expected HandleAdvertisement to update table, got %v err %v", res, err)
	}
}

// TestCompressionTableManager_ActorRefsMax_RejectsOversize verifies that an
// advertisement whose key count exceeds compression.actor-refs.max is
// rejected (the table is not updated).
func TestCompressionTableManager_ActorRefsMax_RejectsOversize(t *testing.T) {
	nm := NewNodeManager(&gproto_remote.Address{Hostname: proto.String("local")}, 0)
	router := actor.NewRouter(nm)
	ctm := NewCompressionTableManager(router)
	ctm.SetActorRefsMax(2)

	originUid := uint64(1)
	// Three keys > cap of 2 → must be rejected, table stays empty.
	ctm.UpdateActorRefTable(originUid, 1,
		[]string{"a", "b", "c"},
		[]uint32{1, 2, 3},
	)
	if _, err := ctm.LookupActorRef(originUid, 1); err == nil {
		t.Error("over-cap advertisement must be rejected; table should remain empty")
	}

	// At-cap update must succeed.
	ctm.UpdateActorRefTable(originUid, 2,
		[]string{"x", "y"},
		[]uint32{10, 20},
	)
	got, err := ctm.LookupActorRef(originUid, 10)
	if err != nil || got != "x" {
		t.Errorf("at-cap update should succeed; got %q err %v", got, err)
	}
}

// TestCompressionTableManager_ManifestsMax_RejectsOversize verifies the same
// cap enforcement on the manifest dictionary.
func TestCompressionTableManager_ManifestsMax_RejectsOversize(t *testing.T) {
	nm := NewNodeManager(&gproto_remote.Address{Hostname: proto.String("local")}, 0)
	router := actor.NewRouter(nm)
	ctm := NewCompressionTableManager(router)
	ctm.SetManifestsMax(1)

	originUid := uint64(2)
	ctm.UpdateManifestTable(originUid, 1,
		[]string{"m1", "m2"},
		[]uint32{1, 2},
	)
	if _, err := ctm.LookupManifest(originUid, 1); err == nil {
		t.Error("over-cap manifest advertisement must be rejected")
	}

	ctm.UpdateManifestTable(originUid, 2,
		[]string{"m1"},
		[]uint32{7},
	)
	got, err := ctm.LookupManifest(originUid, 7)
	if err != nil || got != "m1" {
		t.Errorf("at-cap manifest update should succeed; got %q err %v", got, err)
	}
}

// TestCompressionTableManager_AdvertisementScheduler_Fires verifies that the
// advertisement scheduler invokes the registered callback at the configured
// cadence for both actor-refs and manifests.
func TestCompressionTableManager_AdvertisementScheduler_Fires(t *testing.T) {
	nm := NewNodeManager(&gproto_remote.Address{Hostname: proto.String("local")}, 0)
	router := actor.NewRouter(nm)
	ctm := NewCompressionTableManager(router)

	ctm.SetAdvertisementIntervals(15*time.Millisecond, 25*time.Millisecond)

	var refTicks, manifestTicks atomic.Int32
	ctm.SetAdvertiseCallback(func(isActorRef bool) {
		if isActorRef {
			refTicks.Add(1)
		} else {
			manifestTicks.Add(1)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctm.StartAdvertisementScheduler(ctx)

	// Run long enough to see multiple ticks of each.
	time.Sleep(120 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond) // let goroutines exit

	if r := refTicks.Load(); r < 3 {
		t.Errorf("actor-ref scheduler fired %d times, want at least 3", r)
	}
	if m := manifestTicks.Load(); m < 2 {
		t.Errorf("manifest scheduler fired %d times, want at least 2", m)
	}
}

// TestCompressionTableManager_AdvertisementScheduler_ZeroIntervalSkipped
// verifies a zero interval keeps that ticker dormant (no callback invocations).
func TestCompressionTableManager_AdvertisementScheduler_ZeroIntervalSkipped(t *testing.T) {
	nm := NewNodeManager(&gproto_remote.Address{Hostname: proto.String("local")}, 0)
	router := actor.NewRouter(nm)
	ctm := NewCompressionTableManager(router)

	ctm.SetAdvertisementIntervals(0, 0)

	var ticks atomic.Int32
	ctm.SetAdvertiseCallback(func(bool) { ticks.Add(1) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctm.StartAdvertisementScheduler(ctx)

	time.Sleep(40 * time.Millisecond)
	if got := ticks.Load(); got != 0 {
		t.Errorf("scheduler with zero intervals fired %d times, want 0", got)
	}
}
