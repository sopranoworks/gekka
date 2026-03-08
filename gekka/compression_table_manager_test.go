/*
 * compression_table_manager_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestCompressionTableManager_UpdateAndLookup(t *testing.T) {
	nm := NewNodeManager(&Address{Hostname: proto.String("local")}, 0)
	router := NewRouter(nm)
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
	nm := NewNodeManager(&Address{Hostname: proto.String("local")}, 0)
	router := NewRouter(nm)
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
			ctm.LookupManifest(originUid, 1)
			ctm.LookupManifest(originUid, 100)
		}
	}()

	wg.Wait()
}

func TestHandleAdvertisement(t *testing.T) {
	nm := NewNodeManager(&Address{Hostname: proto.String("local")}, 0)
	router := NewRouter(nm)
	ctm := NewCompressionTableManager(router)

	originUid := uint64(555)
	version := uint32(1)
	keys := []string{"ActorX"}
	values := []uint32{42}

	adv := &CompressionTableAdvertisement{
		From:         &UniqueAddress{Address: &Address{System: proto.String("sys"), Hostname: proto.String("remote"), Port: proto.Uint32(2552)}},
		OriginUid:    &originUid,
		TableVersion: &version,
		Keys:         keys,
		Values:       values,
	}

	local := &UniqueAddress{Address: &Address{System: proto.String("sys"), Hostname: proto.String("local"), Port: proto.Uint32(2552)}}

	// Since router has no real connection, it might error on send, but we mainly care if table updates
	_ = ctm.HandleAdvertisement(context.Background(), adv, true, local)

	res, err := ctm.LookupActorRef(originUid, 42)
	if err != nil || res != "ActorX" {
		t.Errorf("expected HandleAdvertisement to update table, got %v err %v", res, err)
	}
}
