/*
 * replicator_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/cluster/ddata"
	"github.com/stretchr/testify/assert"
)

func TestTypedReplicator_GCounter(t *testing.T) {
	// Mock environment
	repl := ddata.NewReplicator("node1", nil)

	counter := repl.GCounter("count")
	assert.Equal(t, uint64(0), counter.Value())

	update := Update[*ddata.GCounter]{
		Key: "count",
		Modify: func(c *ddata.GCounter) *ddata.GCounter {
			c.Increment("node1", 5)
			return c
		},
	}

	update.handle(repl, nil)
	assert.Equal(t, uint64(5), counter.Value())

	// Test Get
	replyTo := &mockGetResponseRef[*ddata.GCounter]{
		replyCh: make(chan GetResponse[*ddata.GCounter], 1),
	}

	get := Get[*ddata.GCounter]{
		Key:     "count",
		ReplyTo: typed.NewTypedActorRef[GetResponse[*ddata.GCounter]](replyTo),
	}

	get.handle(repl, nil)

	select {
	case res := <-replyTo.replyCh:
		assert.True(t, res.Found)
		assert.Equal(t, uint64(5), res.Data.Value())
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for GetResponse")
	}
}

type mockGetResponseRef[L any] struct {
	actor.Ref
	replyCh chan GetResponse[L]
}

func (m *mockGetResponseRef[L]) Tell(msg any, sender ...actor.Ref) {
	if res, ok := msg.(GetResponse[L]); ok {
		m.replyCh <- res
	}
}

func (m *mockGetResponseRef[L]) Path() string { return "/user/mock" }
