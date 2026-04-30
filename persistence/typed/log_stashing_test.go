/*
 * log_stashing_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/persistence"
	"github.com/stretchr/testify/assert"
)

// captureSlog redirects slog.Default to a buffer-backed handler at DEBUG level
// for the duration of the test, returning a fetch closure for the captured
// output and a restore closure to undo the redirection.
func captureSlog(t *testing.T) (func() string, func()) {
	t.Helper()
	prev := slog.Default()
	var (
		mu  sync.Mutex
		buf bytes.Buffer
	)
	h := slog.NewTextHandler(&safeWriter{w: &buf, mu: &mu}, &slog.HandlerOptions{Level: slog.LevelDebug})
	slog.SetDefault(slog.New(h))
	return func() string {
			mu.Lock()
			defer mu.Unlock()
			return buf.String()
		}, func() {
			slog.SetDefault(prev)
		}
}

type safeWriter struct {
	w  *bytes.Buffer
	mu *sync.Mutex
}

func (s *safeWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

func newLogStashingBehavior(id string) *EventSourcedBehavior[int, int, counterState] {
	return &EventSourcedBehavior[int, int, counterState]{
		PersistenceID: id,
		Journal:       persistence.NewInMemoryJournal(),
		InitialState:  counterState{Value: 0},
		CommandHandler: func(ctx typed.TypedContext[int], s counterState, cmd int) Effect[int, counterState] {
			return Persist[int, counterState](cmd)
		},
		EventHandler: func(s counterState, e int) counterState {
			return counterState{Value: s.Value + e}
		},
	}
}

// TestLogStashing_OffSuppressesDebugLines locks in the default behavior:
// pekko.persistence.typed.log-stashing = off ⇒ no DEBUG lines for the
// stash-during-recovery code path.
func TestLogStashing_OffSuppressesDebugLines(t *testing.T) {
	prevCap := GetDefaultStashCapacity()
	prev := GetLogStashing()
	defer SetDefaultStashCapacity(prevCap)
	defer SetLogStashing(prev)

	SetDefaultStashCapacity(8)
	SetLogStashing(false)

	read, restore := captureSlog(t)
	defer restore()

	act := NewPersistentActor(newLogStashingBehavior("log-off")).(*persistentActor[int, int, counterState])
	act.SetSystem(&typedMockContext{})
	act.SetSelf(&typedMockRef{path: "/user/log-off"})

	// recovering remains true (PreStart not invoked) so Receive routes to the
	// recovery stash path under test.
	act.Receive(7)
	act.Receive(11)
	act.processStash()

	out := read()
	assert.NotContains(t, out, "Stashing command during recovery",
		"toggle off must not emit DEBUG stash lines; got: %s", out)
	assert.NotContains(t, out, "Unstashing recovery commands",
		"toggle off must not emit DEBUG unstash lines; got: %s", out)
}

// TestLogStashing_OnEmitsDebugLines verifies the runtime effect of
// pekko.persistence.typed.log-stashing = on: each stash + unstash emits a
// DEBUG line tagged with the persistenceID.
func TestLogStashing_OnEmitsDebugLines(t *testing.T) {
	prevCap := GetDefaultStashCapacity()
	prev := GetLogStashing()
	defer SetDefaultStashCapacity(prevCap)
	defer SetLogStashing(prev)

	SetDefaultStashCapacity(8)
	SetLogStashing(true)

	read, restore := captureSlog(t)
	defer restore()

	act := NewPersistentActor(newLogStashingBehavior("log-on")).(*persistentActor[int, int, counterState])
	act.SetSystem(&typedMockContext{})
	act.SetSelf(&typedMockRef{path: "/user/log-on"})

	act.Receive(7)
	act.Receive(11)

	stashOut := read()
	assert.Equal(t, 2, strings.Count(stashOut, "Stashing command during recovery"),
		"toggle on must emit one DEBUG stash line per stashed command; got: %s", stashOut)
	assert.Contains(t, stashOut, `persistenceID=log-on`)

	act.processStash()
	out := read()
	assert.Contains(t, out, "Unstashing recovery commands",
		"toggle on must emit a DEBUG unstash line; got: %s", out)
}
