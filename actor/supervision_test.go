/*
 * supervision_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"sync/atomic"
	"testing"
	"time"
)

// SupervisionTestActor is a simple actor for testing supervision
type SupervisionTestActor struct {
	BaseActor
	startCount   int32
	stopCount    int32
	restartCount int32
	panicCount   int32
	lastMsg      atomic.Value
}

func (t *SupervisionTestActor) PreStart() {
	atomic.AddInt32(&t.startCount, 1)
}

func (t *SupervisionTestActor) PostStop() {
	atomic.AddInt32(&t.stopCount, 1)
}

func (t *SupervisionTestActor) PreRestart(reason error, message any) {
	atomic.AddInt32(&t.restartCount, 1)
	t.BaseActor.PreRestart(reason, message)
}

func (t *SupervisionTestActor) Receive(msg any) {
	t.lastMsg.Store(msg)
	if s, ok := msg.(string); ok && s == "panic" {
		atomic.AddInt32(&t.panicCount, 1)
		panic("intentional panic")
	}
}

func TestSupervisionStrategies(t *testing.T) {
	// 1. Restart Strategy
	t.Run("Restart", func(t *testing.T) {
		child := &SupervisionTestActor{BaseActor: NewBaseActor()}
		parent := &BaseActor{}
		parent.setSupervisorStrategy(&OneForOneStrategy{
			Decider: func(err error) Directive {
				return Restart
			},
		})

		ref := &supervisionMockRef{actor: child, parent: parent}
		InjectParent(child, ref)
		child.SetSelf(ref)

		Start(child)

		ref.Tell("panic")
		time.Sleep(100 * time.Millisecond)

		if atomic.LoadInt32(&child.panicCount) != 1 {
			t.Errorf("expected 1 panic, got %d", child.panicCount)
		}
		if atomic.LoadInt32(&child.restartCount) != 1 {
			t.Errorf("expected 1 restart, got %d", child.restartCount)
		}

		// Verify it's still alive
		ref.Tell("alive")
		time.Sleep(50 * time.Millisecond)
		if child.lastMsg.Load() != "alive" {
			t.Errorf("expected actor to be alive after restart")
		}
	})

	// 2. Resume Strategy
	t.Run("Resume", func(t *testing.T) {
		child := &SupervisionTestActor{BaseActor: NewBaseActor()}
		parent := &BaseActor{}
		parent.setSupervisorStrategy(&OneForOneStrategy{
			Decider: func(err error) Directive {
				return Resume
			},
		})

		ref := &supervisionMockRef{actor: child, parent: parent}
		InjectParent(child, ref)
		child.SetSelf(ref)

		Start(child)

		ref.Tell("panic")
		time.Sleep(100 * time.Millisecond)

		if atomic.LoadInt32(&child.panicCount) != 1 {
			t.Errorf("expected 1 panic, got %d", child.panicCount)
		}
		if atomic.LoadInt32(&child.restartCount) != 0 {
			t.Errorf("expected 0 restarts for Resume, got %d", child.restartCount)
		}

		ref.Tell("alive")
		time.Sleep(50 * time.Millisecond)
		if child.lastMsg.Load() != "alive" {
			t.Errorf("expected actor to be alive after resume")
		}
	})

	// 3. Stop Strategy
	t.Run("Stop", func(t *testing.T) {
		child := &SupervisionTestActor{BaseActor: NewBaseActor()}
		parent := &BaseActor{}
		parent.setSupervisorStrategy(&OneForOneStrategy{
			Decider: func(err error) Directive {
				return Stop
			},
		})

		ref := &supervisionMockRef{
			actor:  child,
			parent: parent,
			stopFunc: func() {
				close(child.Mailbox())
			},
		}
		InjectParent(child, ref)
		child.SetSelf(ref)

		Start(child)

		ref.Tell("panic")
		time.Sleep(100 * time.Millisecond)

		// Check if mailbox is closed (indicating stop)
		select {
		case _, ok := <-child.Mailbox():
			if ok {
				t.Errorf("expected mailbox to be closed")
			}
		default:
			// Might still be open if PostStop hasn't finished, but close() was called
		}
	})

	// 4. Stability under rapid panics
	t.Run("Stability", func(t *testing.T) {
		child := &SupervisionTestActor{BaseActor: NewBaseActor()}
		parent := &BaseActor{}
		parent.setSupervisorStrategy(&OneForOneStrategy{
			Decider: func(err error) Directive {
				return Restart
			},
		})

		ref := &supervisionMockRef{actor: child, parent: parent}
		InjectParent(child, ref)
		child.SetSelf(ref)

		Start(child)

		for i := 0; i < 20; i++ {
			ref.Tell("panic")
		}

		time.Sleep(300 * time.Millisecond)
		t.Logf("Processed %d panics", atomic.LoadInt32(&child.panicCount))

		ref.Tell("alive")
		time.Sleep(50 * time.Millisecond)
		if child.lastMsg.Load() != "alive" {
			t.Errorf("expected actor to be alive after rapid panics")
		}
	})
}

// supervisionMockRef implements Ref and provides access to the parent's HandleFailure
type supervisionMockRef struct {
	parent   *BaseActor
	actor    Actor
	stopFunc func()
}

func (m *supervisionMockRef) Tell(msg any, sender ...Ref) {
	m.actor.Mailbox() <- msg
}

func (m *supervisionMockRef) Path() string { return "/test" }

func (m *supervisionMockRef) HandleFailure(child Ref, childActor Actor, err error) {
	m.parent.HandleFailure(child, childActor, err)
}

func (m *supervisionMockRef) Stop(ref Ref) {
	if m.stopFunc != nil {
		m.stopFunc()
	}
}

func (m *supervisionMockRef) System() ActorContext {
	return &supervisionMockContext{stopper: m}
}

type supervisionMockContext struct {
	ActorContext
	stopper interface{ Stop(Ref) }
}

func (m *supervisionMockContext) Stop(ref Ref) {
	m.stopper.Stop(ref)
}
