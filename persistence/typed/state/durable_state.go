/*
 * durable_state.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package state

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/persistence"
)

// DurableStateBehavior defines a behavior for a state-persistent actor.
type DurableStateBehavior[Command any, State any] struct {
	PersistenceID string
	EmptyState    State
	StateStore    persistence.DurableStateStore
	OnCommand     func(typed.TypedContext[Command], State, Command) Effect[State]
	Tag           string
}

// Effect represents the result of processing a command in DurableState.
type Effect[State any] interface {
	isEffect()
}

type persistEffect[State any] struct {
	state State
	then  func(State)
}

func (*persistEffect[State]) isEffect() {}

// Persist creates an effect that updates the state.
func Persist[State any](state State) Effect[State] {
	return &persistEffect[State]{state: state}
}

// PersistThen creates an effect that updates the state and then runs a callback.
func PersistThen[State any](then func(State), state State) Effect[State] {
	return &persistEffect[State]{state: state, then: then}
}

type noneEffect[State any] struct{}

func (*noneEffect[State]) isEffect() {}

// None represents an effect that does nothing.
func None[State any]() Effect[State] {
	return &noneEffect[State]{}
}

type stopEffect[State any] struct{}

func (*stopEffect[State]) isEffect() {}

// Stop returns an effect that stops the actor.
func Stop[State any]() Effect[State] {
	return &stopEffect[State]{}
}

type unhandledEffect[State any] struct{}

func (*unhandledEffect[State]) isEffect() {}

// Unhandled returns an effect indicating the command was not handled.
func Unhandled[State any]() Effect[State] {
	return &unhandledEffect[State]{}
}

// durableStateActor is the internal implementation of a state-persistent actor.
type durableStateActor[Command any, State any] struct {
	actor.BaseActor
	behavior   *DurableStateBehavior[Command, State]
	state      State
	revision   uint64
	recovering bool
	tctx       typed.TypedContext[Command]
	timers            *actor.TimerSchedulerImpl[Command]
	userStash         *actor.StashBufferImpl[Command]
	userStashPending  []Command
	drainingUserStash bool
	stash             []Command // internal recovery stash
}

func NewDurableStateActor[Command any, State any](b *DurableStateBehavior[Command, State]) actor.Actor {
	p := &durableStateActor[Command, State]{
		BaseActor:  actor.NewBaseActor(),
		behavior:   b,
		state:      b.EmptyState,
		recovering: true,
	}
	p.tctx = &durableStateTypedContext[Command, State]{actor: p}
	return p
}

type durableStateTypedContext[C any, S any] struct {
	actor *durableStateActor[C, S]
}

func (c *durableStateTypedContext[C, S]) Self() typed.TypedActorRef[C] {
	return typed.NewTypedActorRef[C](c.actor.Self())
}

func (c *durableStateTypedContext[C, S]) System() actor.ActorContext {
	return c.actor.System()
}

func (c *durableStateTypedContext[C, S]) Log() *slog.Logger {
	return c.actor.Log().Logger()
}

func (c *durableStateTypedContext[C, S]) Watch(target actor.Ref) {
	c.actor.System().Watch(c.actor.Self(), target)
}

func (c *durableStateTypedContext[C, S]) Unwatch(target actor.Ref) {
	if sys, ok := c.actor.System().(interface {
		Unwatch(watcher actor.Ref, target actor.Ref)
	}); ok {
		sys.Unwatch(c.actor.Self(), target)
	}
}

func (c *durableStateTypedContext[C, S]) Stop(target actor.Ref) {
	if sys, ok := c.actor.System().(interface {
		Stop(target actor.Ref)
	}); ok {
		sys.Stop(target)
	}
}

func (c *durableStateTypedContext[C, S]) Passivate() {
	if parent := c.actor.Parent(); parent != nil {
		parent.Tell(actor.Passivate{Entity: c.actor.Self()}, c.actor.Self())
	}
}

func (c *durableStateTypedContext[C, S]) Timers() typed.TimerScheduler[C] {
	return c.actor.timers
}

func (c *durableStateTypedContext[C, S]) Stash() typed.StashBuffer[C] {
	return c.actor.userStash
}

func (c *durableStateTypedContext[C, S]) Sender() actor.Ref {
	return c.actor.Sender()
}

var askCounter atomic.Uint64

func (c *durableStateTypedContext[C, S]) Ask(target actor.Ref, msgFactory func(actor.Ref) any, transform func(any, error) C) {
	askID := askCounter.Add(1)
	timerKey := fmt.Sprintf("ask-timeout-%d", askID)
	timeout := 3 * time.Second

	completed := &atomic.Bool{}
	timeoutMsg := transform(nil, actor.ErrAskTimeout)
	c.Timers().StartSingleTimer(timerKey, timeoutMsg, timeout)

	responder := &contextAskResponder[C]{
		self:      c.Self(),
		transform: transform,
		timerKey:  timerKey,
		timers:    c.Timers(),
		completed: completed,
	}
	target.Tell(msgFactory(responder))
}

func (c *durableStateTypedContext[C, S]) Spawn(behavior any, name string) (actor.Ref, error) {
	return c.actor.System().Spawn(behavior, name)
}

func (c *durableStateTypedContext[C, S]) SpawnAnonymous(behavior any) (actor.Ref, error) {
	return c.actor.System().SpawnAnonymous(behavior)
}

func (c *durableStateTypedContext[C, S]) SystemActorOf(behavior any, name string) (actor.Ref, error) {
	return c.actor.System().SystemActorOf(behavior, name)
}

type contextAskResponder[T any] struct {
	self      typed.TypedActorRef[T]
	transform func(any, error) T
	timerKey  string
	timers    typed.TimerScheduler[T]
	completed *atomic.Bool
}

func (r *contextAskResponder[T]) Tell(msg any, sender ...actor.Ref) {
	if r.completed.CompareAndSwap(false, true) {
		r.timers.Cancel(r.timerKey)
		transformed := r.transform(msg, nil)
		r.self.Tell(transformed)
	}
}

func (r *contextAskResponder[T]) Path() string {
	return "/temp/context-ask"
}

func (p *durableStateActor[Command, State]) PreStart() {
	p.timers = actor.NewTimerScheduler[Command](p.Self())
	p.userStash = actor.NewStashBuffer[Command](actor.DefaultStashCapacity, func(cmd Command) {
		p.userStashPending = append(p.userStashPending, cmd)
	})
	p.recover()
}

func (p *durableStateActor[Command, State]) PostStop() {
	p.timers.CancelAll()
}

func (p *durableStateActor[Command, State]) Receive(msg any) {
	if p.recovering {
		if cmd, ok := msg.(Command); ok {
			p.stash = append(p.stash, cmd)
		}
		return
	}

	if cmd, ok := msg.(Command); ok {
		effect := p.behavior.OnCommand(p.tctx, p.state, cmd)
		switch e := effect.(type) {
		case *persistEffect[State]:
			p.persist(e.state, e.then)
		case *stopEffect[State]:
			p.System().Stop(p.Self())
		case *unhandledEffect[State]:
			p.Log().Warn("Unhandled command", "type", fmt.Sprintf("%T", cmd))
		}
		p.drainUserStash()
		return
	}
}

func (p *durableStateActor[Command, State]) drainUserStash() {
	if p.drainingUserStash {
		return
	}
	p.drainingUserStash = true
	defer func() { p.drainingUserStash = false }()

	for len(p.userStashPending) > 0 {
		next := p.userStashPending[0]
		p.userStashPending = p.userStashPending[1:]
		p.Receive(next)
	}
}

func (p *durableStateActor[Command, State]) recover() {
	ctx := context.Background()
	state, revision, err := p.behavior.StateStore.Get(ctx, p.behavior.PersistenceID)
	if err != nil {
		p.Log().Error("Recovery failed", "error", err)
		p.System().Stop(p.Self())
		return
	}

	if state != nil {
		if s, ok := state.(State); ok {
			p.state = s
			p.revision = revision
		}
	}

	p.recovering = false
	p.Log().Info("Recovery completed", "revision", p.revision)

	for _, cmd := range p.stash {
		p.Receive(cmd)
	}
	p.stash = nil
}

func (p *durableStateActor[Command, State]) persist(state State, then func(State)) {
	ctx := context.Background()
	p.revision++
	err := p.behavior.StateStore.Upsert(ctx, p.behavior.PersistenceID, p.revision, state, p.behavior.Tag)
	if err != nil {
		p.Log().Error("Failed to upsert state", "error", err)
		p.revision--
		// For robustness, maybe we should stop here too if we can't persist
		return
	}

	p.state = state
	if then != nil {
		then(p.state)
	}
}
