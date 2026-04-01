/*
 * supervision_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"fmt"
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

// multiStopContext implements ActorContext with a per-ref Stop dispatch table.
// Used by AllForOneStrategy Stop tests where each child needs its own handler.
type multiStopContext struct {
	ActorContext
	stopFuncs map[Ref]func()
}

func (m *multiStopContext) Stop(ref Ref) {
	if f, ok := m.stopFuncs[ref]; ok {
		f()
	}
}

// ── AllForOneStrategy tests ───────────────────────────────────────────────────

func TestAllForOneStrategy(t *testing.T) {
	t.Run("Restart_AllChildren", func(t *testing.T) {
		// Create three sibling actors managed by a common parent.
		children := make([]*SupervisionTestActor, 3)
		refs := make([]*supervisionMockRef, 3)
		parent := &BaseActor{}
		parent.setSupervisorStrategy(&AllForOneStrategy{
			Decider: func(err error) Directive { return Restart },
		})

		// parentRef is the mock ref that delegates HandleFailure to the parent.
		var parentRef *supervisionMockRef

		for i := range children {
			children[i] = &SupervisionTestActor{BaseActor: NewBaseActor()}
		}

		// parentRef must be created before registering children so that its
		// HandleFailure can call parent.HandleFailure with the children already
		// registered.
		stopFunc := func() {} // no-op: Restart, not Stop
		parentRef = &supervisionMockRef{
			parent:   parent,
			actor:    children[0], // placeholder; only HandleFailure matters
			stopFunc: stopFunc,
		}

		for i, child := range children {
			name := fmt.Sprintf("child%d", i)
			refs[i] = &supervisionMockRef{actor: child, parent: parent}
			InjectParent(child, parentRef)
			child.SetSelf(refs[i])
			// Register this child in the parent so AllForOne can iterate them.
			parent.AddChild(name, refs[i], Props{New: func() Actor {
				return &SupervisionTestActor{BaseActor: NewBaseActor()}
			}})
			Start(child)
		}

		// child[0] panics — all three should receive a restartSignal.
		refs[0].Tell("panic")
		time.Sleep(200 * time.Millisecond)

		for i, child := range children {
			rc := atomic.LoadInt32(&child.restartCount)
			if rc != 1 {
				t.Errorf("child[%d]: expected 1 restart (AllForOne), got %d", i, rc)
			}
		}

		// All actors must still process messages after the mass restart.
		for i, ref := range refs {
			ref.Tell("alive")
			time.Sleep(50 * time.Millisecond)
			if children[i].lastMsg.Load() != "alive" {
				t.Errorf("child[%d]: expected to be alive after AllForOne restart", i)
			}
		}
	})

	t.Run("Stop_AllChildren", func(t *testing.T) {
		children := make([]*SupervisionTestActor, 3)
		refs := make([]*supervisionMockRef, 3)
		parent := &BaseActor{}
		stopCalled := make([]int32, 3)

		for i := range children {
			children[i] = &SupervisionTestActor{BaseActor: NewBaseActor()}
			refs[i] = &supervisionMockRef{actor: children[i]}
		}

		// multiStopCtx routes sys.Stop(ref) → the correct child's close func.
		stopCtx := &multiStopContext{stopFuncs: make(map[Ref]func())}
		for i := range refs {
			idx := i
			stopCtx.stopFuncs[refs[i]] = func() {
				atomic.AddInt32(&stopCalled[idx], 1)
				close(children[idx].Mailbox())
			}
		}
		parent.SetSystem(stopCtx)

		parent.setSupervisorStrategy(&AllForOneStrategy{
			Decider: func(err error) Directive { return Stop },
		})

		parentRef := &supervisionMockRef{parent: parent, actor: children[0]}

		for i, child := range children {
			name := fmt.Sprintf("child%d", i)
			refs[i].parent = parent
			InjectParent(child, parentRef)
			child.SetSelf(refs[i])
			parent.AddChild(name, refs[i], Props{New: func() Actor {
				return &SupervisionTestActor{BaseActor: NewBaseActor()}
			}})
			Start(child)
		}

		refs[1].Tell("panic")
		time.Sleep(200 * time.Millisecond)

		for i := range children {
			if atomic.LoadInt32(&stopCalled[i]) == 0 {
				t.Errorf("child[%d]: expected Stop to be called (AllForOne)", i)
			}
		}
	})

	t.Run("OneForOne_DoesNotAffectSiblings", func(t *testing.T) {
		// Baseline: OneForOneStrategy must NOT restart siblings.
		children := make([]*SupervisionTestActor, 3)
		refs := make([]*supervisionMockRef, 3)
		parent := &BaseActor{}
		parent.setSupervisorStrategy(&OneForOneStrategy{
			Decider: func(err error) Directive { return Restart },
		})

		var parentRef *supervisionMockRef
		for i := range children {
			children[i] = &SupervisionTestActor{BaseActor: NewBaseActor()}
		}
		parentRef = &supervisionMockRef{parent: parent, actor: children[0]}

		for i, child := range children {
			name := fmt.Sprintf("child%d", i)
			refs[i] = &supervisionMockRef{actor: child, parent: parent}
			InjectParent(child, parentRef)
			child.SetSelf(refs[i])
			parent.AddChild(name, refs[i], Props{New: func() Actor {
				return &SupervisionTestActor{BaseActor: NewBaseActor()}
			}})
			Start(child)
		}

		refs[0].Tell("panic")
		time.Sleep(200 * time.Millisecond)

		// Only child[0] should restart.
		if atomic.LoadInt32(&children[0].restartCount) != 1 {
			t.Errorf("child[0]: expected 1 restart, got %d", children[0].restartCount)
		}
		for i := 1; i < 3; i++ {
			if rc := atomic.LoadInt32(&children[i].restartCount); rc != 0 {
				t.Errorf("child[%d]: OneForOne must not restart siblings, got %d restarts", i, rc)
			}
		}
	})
}
