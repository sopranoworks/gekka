/*
 * receive_timeout_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type receiveTimeoutTestActor struct {
	BaseActor
	mu   sync.Mutex
	msgs []string
}

func (a *receiveTimeoutTestActor) Receive(msg any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch m := msg.(type) {
	case ReceiveTimeout:
		a.msgs = append(a.msgs, "timeout")
	case string:
		a.msgs = append(a.msgs, m)
	}
}

func (a *receiveTimeoutTestActor) getMsgs() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	res := make([]string, len(a.msgs))
	copy(res, a.msgs)
	return res
}

func TestReceiveTimeout_Classic_IdleActorReceives(t *testing.T) {
	a := &receiveTimeoutTestActor{BaseActor: NewBaseActor()}
	selfRef := &localTestRef{path: "/user/timeout-test", mb: a.Mailbox()}
	a.SetSelf(selfRef)
	Start(a)
	defer a.CloseMailbox()

	a.SetReceiveTimeout(100 * time.Millisecond)

	time.Sleep(300 * time.Millisecond)

	msgs := a.getMsgs()
	found := false
	for _, m := range msgs {
		if m == "timeout" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected ReceiveTimeout message, got %v", msgs)
	}
}

func TestReceiveTimeout_Classic_ActiveActorResets(t *testing.T) {
	var timeoutCount atomic.Int32

	a := &receiveTimeoutTestActor{BaseActor: NewBaseActor()}
	selfRef := &localTestRef{path: "/user/active-test", mb: a.Mailbox()}
	a.SetSelf(selfRef)
	Start(a)
	defer a.CloseMailbox()

	a.SetReceiveTimeout(200 * time.Millisecond)

	// Keep sending messages faster than the timeout
	for i := 0; i < 5; i++ {
		time.Sleep(100 * time.Millisecond)
		a.Mailbox() <- "keep-alive"
	}

	msgs := a.getMsgs()
	for _, m := range msgs {
		if m == "timeout" {
			timeoutCount.Add(1)
		}
	}

	if c := timeoutCount.Load(); c > 0 {
		t.Errorf("expected 0 timeout messages during active period, got %d", c)
	}
}

func TestReceiveTimeout_Classic_CancelStopsTimeout(t *testing.T) {
	var timeoutCount atomic.Int32
	a := &receiveTimeoutTestActor{BaseActor: NewBaseActor()}
	selfRef := &localTestRef{path: "/user/cancel-test", mb: a.Mailbox()}
	a.SetSelf(selfRef)
	Start(a)
	defer a.CloseMailbox()

	a.SetReceiveTimeout(100 * time.Millisecond)
	a.CancelReceiveTimeout()

	time.Sleep(300 * time.Millisecond)

	msgs := a.getMsgs()
	for _, m := range msgs {
		if m == "timeout" {
			timeoutCount.Add(1)
		}
	}
	if c := timeoutCount.Load(); c != 0 {
		t.Errorf("expected 0 timeout messages after cancel, got %d", c)
	}
}
