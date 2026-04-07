/*
 * persistence_test_kit_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package testkit

import (
	"context"
	"errors"
	"testing"

	"github.com/sopranoworks/gekka/persistence"
)

func TestPersistenceTestKit_PersistAndReplay(t *testing.T) {
	kit := NewPersistenceTestKit()
	ctx := context.Background()
	pid := "test-actor-1"

	// Persist two events.
	err := kit.Journal().AsyncWriteMessages(ctx, []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: "event-1"},
		{PersistenceID: pid, SequenceNr: 2, Payload: "event-2"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// PersistedInStorage should return both.
	events := kit.PersistedInStorage(pid)
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	// ExpectNextPersisted walks the events in order.
	if err := kit.ExpectNextPersisted(pid, "event-1"); err != nil {
		t.Fatal(err)
	}
	if err := kit.ExpectNextPersisted(pid, "event-2"); err != nil {
		t.Fatal(err)
	}
	// No more events.
	if err := kit.ExpectNextPersisted(pid, "event-3"); err == nil {
		t.Fatal("expected error for exhausted events")
	}

	// Replay should deliver both events.
	var replayed []string
	err = kit.Journal().ReplayMessages(ctx, pid, 1, 100, 0, func(repr persistence.PersistentRepr) {
		replayed = append(replayed, repr.Payload.(string))
	})
	if err != nil {
		t.Fatalf("replay error: %v", err)
	}
	if len(replayed) != 2 || replayed[0] != "event-1" || replayed[1] != "event-2" {
		t.Fatalf("unexpected replayed: %v", replayed)
	}
}

func TestPersistenceTestKit_FailNextPersist(t *testing.T) {
	kit := NewPersistenceTestKit()
	ctx := context.Background()
	pid := "fail-persist"

	injected := errors.New("disk full")
	kit.FailNextPersist(pid, injected)

	// First write should fail.
	err := kit.Journal().AsyncWriteMessages(ctx, []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: "x"},
	})
	if !errors.Is(err, injected) {
		t.Fatalf("expected injected error, got: %v", err)
	}

	// Nothing should have been stored.
	if events := kit.PersistedInStorage(pid); len(events) != 0 {
		t.Fatalf("expected 0 events after failure, got %d", len(events))
	}

	// Retry should succeed (failure consumed).
	err = kit.Journal().AsyncWriteMessages(ctx, []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: "x"},
	})
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if events := kit.PersistedInStorage(pid); len(events) != 1 {
		t.Fatalf("expected 1 event after retry, got %d", len(events))
	}
}

func TestPersistenceTestKit_FailNextRead(t *testing.T) {
	kit := NewPersistenceTestKit()
	ctx := context.Background()
	pid := "fail-read"

	// Store an event.
	_ = kit.Journal().AsyncWriteMessages(ctx, []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: "hello"},
	})

	injected := errors.New("corruption")
	kit.FailNextRead(pid, injected)

	// Replay should fail.
	err := kit.Journal().ReplayMessages(ctx, pid, 1, 100, 0, func(repr persistence.PersistentRepr) {
		t.Fatal("should not have called callback")
	})
	if !errors.Is(err, injected) {
		t.Fatalf("expected injected error, got: %v", err)
	}

	// Retry should succeed.
	var got []string
	err = kit.Journal().ReplayMessages(ctx, pid, 1, 100, 0, func(repr persistence.PersistentRepr) {
		got = append(got, repr.Payload.(string))
	})
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if len(got) != 1 || got[0] != "hello" {
		t.Fatalf("unexpected replayed: %v", got)
	}
}

func TestPersistenceTestKit_SnapshotSaveAndLoad(t *testing.T) {
	kit := NewPersistenceTestKit()
	ctx := context.Background()
	pid := "snap-actor"

	meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: 10, Timestamp: 1000}
	err := kit.SnapshotStore().SaveSnapshot(ctx, meta, "state-at-10")
	if err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	criteria := persistence.SnapshotSelectionCriteria{MaxSequenceNr: 100, MaxTimestamp: 9999}
	snap, err := kit.SnapshotStore().LoadSnapshot(ctx, pid, criteria)
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if snap == nil || snap.Snapshot != "state-at-10" {
		t.Fatalf("unexpected snapshot: %v", snap)
	}
}

func TestPersistenceTestKit_FailNextSnapshot(t *testing.T) {
	kit := NewPersistenceTestKit()
	ctx := context.Background()
	pid := "snap-fail"

	injected := errors.New("snapshot write error")
	kit.FailNextSnapshot(pid, injected)

	meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: 5}
	err := kit.SnapshotStore().SaveSnapshot(ctx, meta, "state")
	if !errors.Is(err, injected) {
		t.Fatalf("expected injected error, got: %v", err)
	}

	// Retry succeeds.
	err = kit.SnapshotStore().SaveSnapshot(ctx, meta, "state")
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
}

func TestPersistenceTestKit_FailNextLoadSnapshot(t *testing.T) {
	kit := NewPersistenceTestKit()
	ctx := context.Background()
	pid := "snap-load-fail"

	meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: 3, Timestamp: 100}
	_ = kit.SnapshotStore().SaveSnapshot(ctx, meta, "saved")

	injected := errors.New("snapshot read error")
	kit.FailNextLoadSnapshot(pid, injected)

	criteria := persistence.SnapshotSelectionCriteria{MaxSequenceNr: 100, MaxTimestamp: 9999}
	_, err := kit.SnapshotStore().LoadSnapshot(ctx, pid, criteria)
	if !errors.Is(err, injected) {
		t.Fatalf("expected injected error, got: %v", err)
	}

	// Retry succeeds.
	snap, err := kit.SnapshotStore().LoadSnapshot(ctx, pid, criteria)
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if snap == nil || snap.Snapshot != "saved" {
		t.Fatalf("unexpected: %v", snap)
	}
}

func TestPersistenceTestKit_ClearAll(t *testing.T) {
	kit := NewPersistenceTestKit()
	ctx := context.Background()
	pid := "clear-test"

	_ = kit.Journal().AsyncWriteMessages(ctx, []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: "e1"},
	})
	meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: 1, Timestamp: 100}
	_ = kit.SnapshotStore().SaveSnapshot(ctx, meta, "snap")

	kit.ClearAll()

	if events := kit.PersistedInStorage(pid); len(events) != 0 {
		t.Fatalf("expected 0 events after clear, got %d", len(events))
	}

	criteria := persistence.SnapshotSelectionCriteria{MaxSequenceNr: 100, MaxTimestamp: 9999}
	snap, _ := kit.SnapshotStore().LoadSnapshot(ctx, pid, criteria)
	if snap != nil {
		t.Fatalf("expected nil snapshot after clear, got %v", snap)
	}
}

func TestPersistenceTestKit_ReadHighestSequenceNr(t *testing.T) {
	kit := NewPersistenceTestKit()
	ctx := context.Background()
	pid := "seq-test"

	high, err := kit.Journal().ReadHighestSequenceNr(ctx, pid, 0)
	if err != nil || high != 0 {
		t.Fatalf("expected 0 for empty, got %d, err=%v", high, err)
	}

	_ = kit.Journal().AsyncWriteMessages(ctx, []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: "a"},
		{PersistenceID: pid, SequenceNr: 5, Payload: "b"},
	})

	high, err = kit.Journal().ReadHighestSequenceNr(ctx, pid, 0)
	if err != nil || high != 5 {
		t.Fatalf("expected 5, got %d, err=%v", high, err)
	}
}

func TestPersistenceTestKit_DeleteMessages(t *testing.T) {
	kit := NewPersistenceTestKit()
	ctx := context.Background()
	pid := "del-test"

	_ = kit.Journal().AsyncWriteMessages(ctx, []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: "a"},
		{PersistenceID: pid, SequenceNr: 2, Payload: "b"},
		{PersistenceID: pid, SequenceNr: 3, Payload: "c"},
	})

	_ = kit.Journal().AsyncDeleteMessagesTo(ctx, pid, 2)

	events := kit.PersistedInStorage(pid)
	if len(events) != 1 || events[0].Payload != "c" {
		t.Fatalf("expected only event-3, got %v", events)
	}
}

func TestPersistenceTestKit_ExpectNextPersisted_Mismatch(t *testing.T) {
	kit := NewPersistenceTestKit()
	ctx := context.Background()
	pid := "mismatch"

	_ = kit.Journal().AsyncWriteMessages(ctx, []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: "actual"},
	})

	err := kit.ExpectNextPersisted(pid, "wrong")
	if err == nil {
		t.Fatal("expected mismatch error")
	}
}
