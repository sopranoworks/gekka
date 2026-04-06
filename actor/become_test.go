/*
 * become_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"sync"
	"testing"
	"time"
)

type becomeTestActor struct {
	BaseActor
	mu      sync.Mutex
	log     []string
}

func (a *becomeTestActor) Receive(msg any) {
	switch m := msg.(type) {
	case string:
		a.mu.Lock()
		a.log = append(a.log, "original:"+m)
		a.mu.Unlock()

		if m == "become-happy" {
			a.Become(func(msg any) {
				if s, ok := msg.(string); ok {
					a.mu.Lock()
					a.log = append(a.log, "happy:"+s)
					a.mu.Unlock()
					if s == "unbecome" {
						a.Unbecome()
					}
					if s == "become-angry" {
						a.Become(func(msg any) {
							if s2, ok := msg.(string); ok {
								a.mu.Lock()
								a.log = append(a.log, "angry:"+s2)
								a.mu.Unlock()
								if s2 == "unbecome" {
									a.Unbecome()
								}
							}
						})
					}
				}
			})
		}
	}
}

func (a *becomeTestActor) getLog() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	res := make([]string, len(a.log))
	copy(res, a.log)
	return res
}

func TestBecome_ChangesHandler(t *testing.T) {
	a := &becomeTestActor{BaseActor: NewBaseActor()}
	Start(a)
	defer a.CloseMailbox()

	a.Mailbox() <- "hello"
	a.Mailbox() <- "become-happy"
	a.Mailbox() <- "world"
	time.Sleep(100 * time.Millisecond)

	log := a.getLog()
	expected := []string{"original:hello", "original:become-happy", "happy:world"}
	if len(log) != len(expected) {
		t.Fatalf("expected %d entries, got %d: %v", len(expected), len(log), log)
	}
	for i, e := range expected {
		if log[i] != e {
			t.Errorf("log[%d]: expected %q, got %q", i, e, log[i])
		}
	}
}

func TestUnbecome_Reverts(t *testing.T) {
	a := &becomeTestActor{BaseActor: NewBaseActor()}
	Start(a)
	defer a.CloseMailbox()

	a.Mailbox() <- "become-happy"
	a.Mailbox() <- "in-happy"
	a.Mailbox() <- "unbecome"
	a.Mailbox() <- "back-to-original"
	time.Sleep(100 * time.Millisecond)

	log := a.getLog()
	expected := []string{"original:become-happy", "happy:in-happy", "happy:unbecome", "original:back-to-original"}
	if len(log) != len(expected) {
		t.Fatalf("expected %d entries, got %d: %v", len(expected), len(log), log)
	}
	for i, e := range expected {
		if log[i] != e {
			t.Errorf("log[%d]: expected %q, got %q", i, e, log[i])
		}
	}
}

func TestBecome_StackDepthGreaterThan2(t *testing.T) {
	a := &becomeTestActor{BaseActor: NewBaseActor()}
	Start(a)
	defer a.CloseMailbox()

	a.Mailbox() <- "become-happy"
	a.Mailbox() <- "become-angry"
	a.Mailbox() <- "msg1"
	a.Mailbox() <- "unbecome" // back to happy
	a.Mailbox() <- "msg2"
	a.Mailbox() <- "unbecome" // back to original
	a.Mailbox() <- "msg3"
	time.Sleep(100 * time.Millisecond)

	log := a.getLog()
	expected := []string{
		"original:become-happy",
		"happy:become-angry",
		"angry:msg1",
		"angry:unbecome",
		"happy:msg2",
		"happy:unbecome",
		"original:msg3",
	}
	if len(log) != len(expected) {
		t.Fatalf("expected %d entries, got %d: %v", len(expected), len(log), log)
	}
	for i, e := range expected {
		if log[i] != e {
			t.Errorf("log[%d]: expected %q, got %q", i, e, log[i])
		}
	}
}

func TestUnbecome_EmptyStack(t *testing.T) {
	a := &becomeTestActor{BaseActor: NewBaseActor()}
	Start(a)
	defer a.CloseMailbox()

	// Unbecome on empty stack should be no-op, still uses original Receive
	a.Unbecome()
	a.Mailbox() <- "hello"
	time.Sleep(50 * time.Millisecond)

	log := a.getLog()
	if len(log) != 1 || log[0] != "original:hello" {
		t.Errorf("expected [original:hello], got %v", log)
	}
}
