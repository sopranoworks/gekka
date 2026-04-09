/*
 * typed_stash_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"testing"

	"github.com/sopranoworks/gekka/actor"
	"github.com/stretchr/testify/assert"
)

// Compile-time assertion: *actor.StashBufferImpl[string] must satisfy
// typed.StashBuffer[string]. If this ever fails to compile, the typed
// interface and the concrete impl have drifted.
var _ StashBuffer[string] = (*actor.StashBufferImpl[string])(nil)

func TestTypedStashBuffer_ImplementsInterface(t *testing.T) {
	var delivered []string
	buf := actor.NewStashBuffer[string](10, func(msg string) {
		delivered = append(delivered, msg)
	})

	var iface StashBuffer[string] = buf

	assert.NoError(t, iface.Stash("first"))
	assert.NoError(t, iface.Stash("second"))
	assert.Equal(t, 2, iface.Size())
	assert.False(t, iface.IsFull())

	assert.NoError(t, iface.UnstashAll())
	assert.Equal(t, []string{"first", "second"}, delivered)
	assert.Equal(t, 0, iface.Size())
}

func TestTypedStashBuffer_CapacityRejection(t *testing.T) {
	buf := actor.NewStashBuffer[string](1, func(string) {})
	var iface StashBuffer[string] = buf

	assert.NoError(t, iface.Stash("a"))
	assert.True(t, iface.IsFull())
	assert.Error(t, iface.Stash("b"))
	assert.Equal(t, 1, iface.Size())
}

// TestTypedStashBuffer_ReStashDuringRedeliver locks in the snapshot-and-clear
// semantic documented in actor/stash.go UnstashAll: a redeliver callback that
// re-stashes a message must land in the FRESH buffer (not the pass being
// drained), so the re-stashed message is held for the NEXT UnstashAll. The
// typed actor drain loop depends on this — without it, an actor that calls
// ctx.Stash().Stash(cmd) from within its command handler during an unstash
// pass could see an infinite loop or FIFO re-ordering.
func TestTypedStashBuffer_ReStashDuringRedeliver(t *testing.T) {
	var delivered []int

	// Declare buf via var so the closure can reference it by name; the
	// closure will re-stash "5" exactly once on its first invocation.
	var buf *actor.StashBufferImpl[int]
	reStashed := false
	buf = actor.NewStashBuffer[int](10, func(msg int) {
		delivered = append(delivered, msg)
		if msg == 1 && !reStashed {
			reStashed = true
			// Re-stash a new value during the drain pass. This must NOT
			// be delivered in the current UnstashAll() call — it should
			// land in the fresh buffer and be held for the next one.
			_ = buf.Stash(99)
		}
	})

	assert.NoError(t, buf.Stash(1))
	assert.NoError(t, buf.Stash(2))
	assert.NoError(t, buf.Stash(3))

	// First drain: delivers 1, 2, 3 (in order). The re-stash of 99
	// during delivery of 1 lands in the fresh buffer.
	assert.NoError(t, buf.UnstashAll())
	assert.Equal(t, []int{1, 2, 3}, delivered, "first pass must deliver the original three in order")
	assert.Equal(t, 1, buf.Size(), "re-stashed value must be held for next UnstashAll")

	// Second drain: picks up the re-stashed 99.
	assert.NoError(t, buf.UnstashAll())
	assert.Equal(t, []int{1, 2, 3, 99}, delivered, "second pass must deliver the re-stashed value")
	assert.Equal(t, 0, buf.Size())
}
