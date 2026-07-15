/*
 * store_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package bboltstore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/sopranoworks/gekka/persistence"
)

// testAccount is a representative durable-state payload: a small struct, the
// same shape a DurableStateBehavior (or GitCote's rohrpostlink ExchangeStore)
// would persist.
type testAccount struct {
	Owner   string `json:"owner"`
	Balance int64  `json:"balance"`
}

// childEnvVar carries the bbolt file path to the re-exec'd child process in the
// process-restart durability test.
const childEnvVar = "GEKKA_BBOLT_RESTART_CHILD_PATH"

// TestBoltDurableStateStore_SurvivesProcessRestart is the headline durability
// proof. A child process opens the store, writes a value, and exits *hard*
// (os.Exit, no clean Close) — simulating a crash immediately after the write
// commits. The parent process then opens the same file the dead child left
// behind and confirms the state, revision, and concrete Go type all survived.
//
// This exercises real on-disk durability across a genuine process boundary, not
// merely in-memory retention.
func TestBoltDurableStateStore_SurvivesProcessRestart(t *testing.T) {
	if path := os.Getenv(childEnvVar); path != "" {
		runRestartChild(path)
		return // unreachable; runRestartChild calls os.Exit
	}

	path := filepath.Join(t.TempDir(), "state.db")

	cmd := exec.Command(os.Args[0], "-test.run", "^TestBoltDurableStateStore_SurvivesProcessRestart$")
	cmd.Env = append(os.Environ(), childEnvVar+"="+path)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child writer process failed: %v\n%s", err, out)
	}

	// Parent reopens the file — the child is gone, so the exclusive lock is
	// free and only the committed on-disk state remains.
	store, err := Open(path, nil)
	if err != nil {
		t.Fatalf("reopen after child exit: %v", err)
	}
	defer store.Close()
	store.Codec().(*JSONCodec).Register(testAccount{})

	got, rev, err := store.Get(context.Background(), "acct-1")
	if err != nil {
		t.Fatalf("Get after restart: %v", err)
	}
	if rev != 7 {
		t.Fatalf("revision after restart = %d, want 7", rev)
	}
	acct, ok := got.(testAccount)
	if !ok {
		t.Fatalf("Get returned %T, want testAccount", got)
	}
	if acct.Owner != "alice" || acct.Balance != 4200 {
		t.Fatalf("state after restart = %+v, want {alice 4200}", acct)
	}
}

// runRestartChild is the child half of the process-restart test: write one
// value and exit hard without closing, simulating a crash after commit.
func runRestartChild(path string) {
	store, err := Open(path, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child open: %v\n", err)
		os.Exit(3)
	}
	store.Codec().(*JSONCodec).Register(testAccount{})
	if err := store.Upsert(context.Background(), "acct-1", 7, testAccount{Owner: "alice", Balance: 4200}, "accounts"); err != nil {
		fmt.Fprintf(os.Stderr, "child upsert: %v\n", err)
		os.Exit(4)
	}
	// Intentionally no Close: the Upsert transaction is already committed and
	// fsynced. Exit hard to model an abrupt process death.
	os.Exit(0)
}

// TestBoltDurableStateStore_SurvivesCloseReopen proves durability within a
// single process by fully closing the store (releasing the OS file handle) and
// reopening a brand-new store on the same path.
func TestBoltDurableStateStore_SurvivesCloseReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	ctx := context.Background()

	store, err := Open(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	store.Codec().(*JSONCodec).Register(testAccount{})
	if err := store.Upsert(ctx, "acct-1", 3, testAccount{Owner: "bob", Balance: 99}, "t"); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(path, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	reopened.Codec().(*JSONCodec).Register(testAccount{})

	got, rev, err := reopened.Get(ctx, "acct-1")
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if rev != 3 {
		t.Fatalf("revision = %d, want 3", rev)
	}
	if acct, ok := got.(testAccount); !ok || acct.Owner != "bob" || acct.Balance != 99 {
		t.Fatalf("state = %#v (%T), want testAccount{bob 99}", got, got)
	}
}

// TestBoltDurableStateStore_GetMissingReturnsNil verifies the "no state yet"
// contract: (nil, 0, nil).
func TestBoltDurableStateStore_GetMissingReturnsNil(t *testing.T) {
	store := openTemp(t)
	state, rev, err := store.Get(context.Background(), "never-written")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if state != nil || rev != 0 {
		t.Fatalf("Get missing = (%v, %d), want (nil, 0)", state, rev)
	}
}

// TestBoltDurableStateStore_UpsertGetRoundTrip covers state + revision + tag
// round-tripping through the codec.
func TestBoltDurableStateStore_UpsertGetRoundTrip(t *testing.T) {
	store := openTemp(t)
	store.Codec().(*JSONCodec).Register(testAccount{})
	ctx := context.Background()

	want := testAccount{Owner: "carol", Balance: 512}
	if err := store.Upsert(ctx, "acct-9", 42, want, "vip"); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	got, rev, err := store.Get(ctx, "acct-9")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rev != 42 {
		t.Fatalf("revision = %d, want 42", rev)
	}
	if acct, ok := got.(testAccount); !ok || acct != want {
		t.Fatalf("state = %#v, want %#v", got, want)
	}
}

// TestBoltDurableStateStore_OverwriteLatestWins confirms a second Upsert
// replaces the first and updates the revision.
func TestBoltDurableStateStore_OverwriteLatestWins(t *testing.T) {
	store := openTemp(t)
	store.Codec().(*JSONCodec).Register(testAccount{})
	ctx := context.Background()

	if err := store.Upsert(ctx, "acct-1", 1, testAccount{Owner: "x", Balance: 1}, ""); err != nil {
		t.Fatalf("upsert 1: %v", err)
	}
	if err := store.Upsert(ctx, "acct-1", 2, testAccount{Owner: "x", Balance: 2}, ""); err != nil {
		t.Fatalf("upsert 2: %v", err)
	}

	got, rev, err := store.Get(ctx, "acct-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rev != 2 {
		t.Fatalf("revision = %d, want 2 (latest)", rev)
	}
	if acct := got.(testAccount); acct.Balance != 2 {
		t.Fatalf("balance = %d, want 2 (latest)", acct.Balance)
	}
}

// TestBoltDurableStateStore_Delete confirms a deleted key reads back as absent.
func TestBoltDurableStateStore_Delete(t *testing.T) {
	store := openTemp(t)
	store.Codec().(*JSONCodec).Register(testAccount{})
	ctx := context.Background()

	if err := store.Upsert(ctx, "acct-1", 1, testAccount{Owner: "x", Balance: 1}, ""); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := store.Delete(ctx, "acct-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	state, rev, err := store.Get(ctx, "acct-1")
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if state != nil || rev != 0 {
		t.Fatalf("get after delete = (%v, %d), want (nil, 0)", state, rev)
	}

	// Deleting an absent key is a no-op, not an error.
	if err := store.Delete(ctx, "never-existed"); err != nil {
		t.Fatalf("delete absent key: %v", err)
	}
}

// TestBoltDurableStateStore_ConcurrentAccess stresses the store from multiple
// goroutines. Combined with `go test -race` it guards the concurrency contract.
func TestBoltDurableStateStore_ConcurrentAccess(t *testing.T) {
	store := openTemp(t)
	store.Codec().(*JSONCodec).Register(testAccount{})
	ctx := context.Background()

	const workers = 8
	const perWorker = 50

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			id := fmt.Sprintf("acct-%d", w)
			for i := 0; i < perWorker; i++ {
				if err := store.Upsert(ctx, id, uint64(i+1), testAccount{Owner: id, Balance: int64(i)}, ""); err != nil {
					t.Errorf("worker %d upsert: %v", w, err)
					return
				}
				if _, _, err := store.Get(ctx, id); err != nil {
					t.Errorf("worker %d get: %v", w, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	// Each worker's final revision must be the last write it made.
	for w := 0; w < workers; w++ {
		id := fmt.Sprintf("acct-%d", w)
		got, rev, err := store.Get(ctx, id)
		if err != nil {
			t.Fatalf("final get %s: %v", id, err)
		}
		if rev != perWorker {
			t.Fatalf("%s final revision = %d, want %d", id, rev, perWorker)
		}
		if acct := got.(testAccount); acct.Balance != perWorker-1 {
			t.Fatalf("%s final balance = %d, want %d", id, acct.Balance, perWorker-1)
		}
	}
}

// TestBboltProvider_ConfigDriven proves the provider is registered and
// selectable by name exactly like the in-memory/redis providers — this is the
// path a config-driven consumer (e.g. GitCote's ExchangeStore) uses to adopt
// the backend without touching Gekka internals.
func TestBboltProvider_ConfigDriven(t *testing.T) {
	path := filepath.Join(t.TempDir(), "provider.db")
	cfg, err := ParseConfigString(fmt.Sprintf("path = %q", path))
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}

	ds, err := persistence.NewDurableStateStore("bbolt", *cfg)
	if err != nil {
		t.Fatalf("NewDurableStateStore(bbolt): %v", err)
	}
	defer ds.(*BoltDurableStateStore).Close()

	DefaultCodec.Register(testAccount{})
	ctx := context.Background()
	if err := ds.Upsert(ctx, "acct-1", 5, testAccount{Owner: "dan", Balance: 7}, ""); err != nil {
		t.Fatalf("upsert via provider: %v", err)
	}
	got, rev, err := ds.Get(ctx, "acct-1")
	if err != nil {
		t.Fatalf("get via provider: %v", err)
	}
	if rev != 5 {
		t.Fatalf("revision = %d, want 5", rev)
	}
	if acct, ok := got.(testAccount); !ok || acct.Owner != "dan" {
		t.Fatalf("state = %#v, want testAccount{dan ...}", got)
	}
}

// TestBboltProvider_MissingConfigErrors confirms the provider fails fast with a
// clear message when neither 'path' nor 'dir' is set.
func TestBboltProvider_MissingConfigErrors(t *testing.T) {
	cfg, err := ParseConfigString("")
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if _, err := persistence.NewDurableStateStore("bbolt", *cfg); err == nil {
		t.Fatal("expected an error when neither path nor dir is configured, got nil")
	}
}

// TestJSONCodec_UnregisteredFallsBackToRawMessage documents the codec's
// fallback: an unregistered manifest yields json.RawMessage rather than an
// error, matching the Redis backend.
func TestJSONCodec_UnregisteredFallsBackToRawMessage(t *testing.T) {
	store := openTemp(t) // no type registered
	ctx := context.Background()
	if err := store.Upsert(ctx, "acct-1", 1, testAccount{Owner: "x", Balance: 1}, ""); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got, _, err := store.Get(ctx, "acct-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, ok := got.(json.RawMessage); !ok {
		t.Fatalf("Get with unregistered type = %T, want json.RawMessage", got)
	}
}

// openTemp opens a bbolt store in a per-test temp dir and registers cleanup.
func openTemp(t *testing.T) *BoltDurableStateStore {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "state.db"), nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}
