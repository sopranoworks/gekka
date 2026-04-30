/*
 * typed_replicator_unexpected_ask_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata_test

import (
	"errors"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/cluster/ddata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTypedReplicatorAdapter_UnexpectedAskTimeout_DefaultsTo20s locks in the
// reference.conf default for
// pekko.cluster.ddata.typed.replicator-message-adapter-unexpected-ask-timeout.
func TestTypedReplicatorAdapter_UnexpectedAskTimeout_DefaultsTo20s(t *testing.T) {
	defer ddata.SetDefaultUnexpectedAskTimeout(0)

	ddata.SetDefaultUnexpectedAskTimeout(0) // reset
	if got, want := ddata.GetDefaultUnexpectedAskTimeout(), 20*time.Second; got != want {
		t.Errorf("default GetDefaultUnexpectedAskTimeout = %v, want %v", got, want)
	}

	ddata.SetDefaultUnexpectedAskTimeout(-3 * time.Second)
	if got, want := ddata.GetDefaultUnexpectedAskTimeout(), 20*time.Second; got != want {
		t.Errorf("negative override = %v, want %v", got, want)
	}

	ddata.SetDefaultUnexpectedAskTimeout(5 * time.Second)
	if got, want := ddata.GetDefaultUnexpectedAskTimeout(), 5*time.Second; got != want {
		t.Errorf("override = %v, want %v", got, want)
	}
}

// TestTypedReplicatorAdapter_AskGet_TimeoutOnSlowGetter exercises the runtime
// effect of UnexpectedAskTimeout: when the underlying getter does not return a
// value within the configured window, AskGet must deliver a
// ReplicatorResponse with Err = ErrUnexpectedAskTimeout.
func TestTypedReplicatorAdapter_AskGet_TimeoutOnSlowGetter(t *testing.T) {
	repl := ddata.NewReplicator("node1", nil)

	type Cmd struct {
		Resp ddata.ReplicatorResponse[*ddata.GCounter]
	}

	ctx, ref := newFakeCtx[Cmd](1)
	adapter := ddata.NewTypedReplicatorAdapter(ctx, repl,
		func(r ddata.ReplicatorResponse[*ddata.GCounter]) Cmd { return Cmd{Resp: r} })
	adapter.UnexpectedAskTimeout = 100 * time.Millisecond

	slowGetter := func(*ddata.Replicator, string) (*ddata.GCounter, bool) {
		time.Sleep(2 * time.Second) // longer than the test budget
		return nil, false
	}

	start := time.Now()
	adapter.AskGet("hits", slowGetter)

	select {
	case cmd := <-ref.ch:
		elapsed := time.Since(start)
		require.Error(t, cmd.Resp.Err, "expected an error response on timeout")
		assert.True(t, errors.Is(cmd.Resp.Err, ddata.ErrUnexpectedAskTimeout),
			"expected ErrUnexpectedAskTimeout, got %v", cmd.Resp.Err)
		assert.False(t, cmd.Resp.Found, "Found must be false on timeout")
		assert.Equal(t, "hits", cmd.Resp.Key)
		assert.Less(t, elapsed, 500*time.Millisecond,
			"timeout should fire within ~UnexpectedAskTimeout, got %v", elapsed)
	case <-time.After(time.Second):
		t.Fatal("timeout response never delivered")
	}
}

// TestTypedReplicatorAdapter_AskUpdate_TimeoutOnSlowGetter mirrors the AskGet
// case for AskUpdate.
func TestTypedReplicatorAdapter_AskUpdate_TimeoutOnSlowGetter(t *testing.T) {
	repl := ddata.NewReplicator("node1", nil)

	type Cmd struct {
		Resp ddata.ReplicatorResponse[*ddata.GCounter]
	}

	ctx, ref := newFakeCtx[Cmd](1)
	adapter := ddata.NewTypedReplicatorAdapter(ctx, repl,
		func(r ddata.ReplicatorResponse[*ddata.GCounter]) Cmd { return Cmd{Resp: r} })
	adapter.UnexpectedAskTimeout = 100 * time.Millisecond

	noopModify := func(*ddata.Replicator, string) {}
	slowGetter := func(*ddata.Replicator, string) (*ddata.GCounter, bool) {
		time.Sleep(2 * time.Second)
		return nil, false
	}

	start := time.Now()
	adapter.AskUpdate("requests", noopModify, ddata.WriteLocal, slowGetter)

	select {
	case cmd := <-ref.ch:
		elapsed := time.Since(start)
		require.Error(t, cmd.Resp.Err, "expected an error response on timeout")
		assert.True(t, errors.Is(cmd.Resp.Err, ddata.ErrUnexpectedAskTimeout),
			"expected ErrUnexpectedAskTimeout, got %v", cmd.Resp.Err)
		assert.Less(t, elapsed, 500*time.Millisecond,
			"timeout should fire within ~UnexpectedAskTimeout, got %v", elapsed)
	case <-time.After(time.Second):
		t.Fatal("timeout response never delivered")
	}
}

// TestTypedReplicatorAdapter_AskGet_FastGetterDeliversValue confirms the
// fast-path: when the getter returns before UnexpectedAskTimeout, the actor
// receives the value with no timeout error.
func TestTypedReplicatorAdapter_AskGet_FastGetterDeliversValue(t *testing.T) {
	repl := ddata.NewReplicator("node1", nil)
	c := repl.GCounter("hits")
	c.Increment("node1", 7)

	type Cmd struct {
		Resp ddata.ReplicatorResponse[*ddata.GCounter]
	}

	ctx, ref := newFakeCtx[Cmd](1)
	adapter := ddata.NewTypedReplicatorAdapter(ctx, repl,
		func(r ddata.ReplicatorResponse[*ddata.GCounter]) Cmd { return Cmd{Resp: r} })
	adapter.UnexpectedAskTimeout = 200 * time.Millisecond

	adapter.AskGet("hits", ddata.GCounterGetter)

	select {
	case cmd := <-ref.ch:
		assert.NoError(t, cmd.Resp.Err)
		assert.True(t, cmd.Resp.Found)
		assert.Equal(t, uint64(7), cmd.Resp.Data.Value())
	case <-time.After(time.Second):
		t.Fatal("AskGet response never delivered")
	}
}
