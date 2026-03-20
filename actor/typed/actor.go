/*
 * typed_actor.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

// Behavior is the definition of how an actor reacts to a message.
// It is a function that takes a TypedContext and a message of type T,
// and returns the next Behavior for the actor.
//
// Returning Same[T]() (or nil) indicates that the actor should keep its
// current behavior for the next message.
// Returning Stopped[T]() indicates that the actor should stop.
type Behavior[T any] func(ctx TypedContext[T], msg T) Behavior[T]

// TypedContext provides access to the actor's contextual information and
// allows for interaction with the actor system in a type-safe manner.
type TypedContext[T any] interface {
	// Self returns the type-safe actor reference for this actor.
	Self() TypedActorRef[T]

	// System returns the untyped actor context, providing access to
	// system-level operations like spawning children or watching other actors.
	System() actor.ActorContext

	// Log returns the structured logger for this actor.
	Log() *slog.Logger

	// Watch registers this actor to receive a TerminatedMessage when target stops.
	Watch(target actor.Ref)

	// Unwatch removes a previously established watch.
	Unwatch(target actor.Ref)

	// Stop gracefully terminates the target actor.
	Stop(target actor.Ref)

	// Passivate requests the parent actor to stop this actor.
	Passivate()

	// Timers returns the TimerScheduler for scheduling time-based messages
	// to this actor. All timers are automatically cancelled on actor stop.
	Timers() TimerScheduler[T]

	// Stash returns the StashBuffer for temporarily holding messages that
	// should not be processed in the actor's current behavior.
	Stash() StashBuffer[T]

	// Sender returns the sender of the current message.
	Sender() actor.Ref

	// Ask sends a message to another actor and adapts the response.
	// target is the recipient of the message.
	// msgFactory creates the message to be sent, providing a temporary reply-to reference.
	// transform converts the response (or error) into a message of type T for this actor.
	Ask(target actor.Ref, msgFactory func(replyTo actor.Ref) any, transform func(res any, err error) T)

	// Spawn creates a new typed actor with the given behavior and name.
	Spawn(behavior any, name string) (actor.Ref, error)

	// SpawnAnonymous creates a new typed actor with an automatically generated name.
	SpawnAnonymous(behavior any) (actor.Ref, error)

	// SystemActorOf creates a new actor under the /system guardian.
	SystemActorOf(behavior any, name string) (actor.Ref, error)
}

// typedContext is the internal implementation of TypedContext[T].
type typedContext[T any] struct {
	actor *TypedActor[T]
}

var askCounter atomic.Uint64

func (c *typedContext[T]) Self() TypedActorRef[T] {
	return NewTypedActorRef[T](c.actor.Self())
}

func (c *typedContext[T]) System() actor.ActorContext {
	return c.actor.System()
}

func (c *typedContext[T]) Log() *slog.Logger {
	return c.actor.Log().Logger()
}

func (c *typedContext[T]) Watch(target actor.Ref) {
	c.actor.System().Watch(c.actor.Self(), target)
}

func (c *typedContext[T]) Unwatch(target actor.Ref) {
	if sys, ok := c.actor.System().(interface {
		Unwatch(watcher actor.Ref, target actor.Ref)
	}); ok {
		sys.Unwatch(c.actor.Self(), target)
	}
}

func (c *typedContext[T]) Stop(target actor.Ref) {
	if sys, ok := c.actor.System().(interface {
		Stop(target actor.Ref)
	}); ok {
		sys.Stop(target)
	}
}

func (c *typedContext[T]) Passivate() {
	if parent := c.actor.Parent(); parent != nil {
		parent.Tell(actor.Passivate{Entity: c.actor.Self()}, c.actor.Self())
	}
}

func (c *typedContext[T]) Timers() TimerScheduler[T] {
	return c.actor.timers
}

func (c *typedContext[T]) Stash() StashBuffer[T] {
	return c.actor.stash
}

func (c *typedContext[T]) Sender() actor.Ref {
	return c.actor.Sender()
}

func (c *typedContext[T]) Spawn(behavior any, name string) (actor.Ref, error) {
	return c.actor.System().Spawn(behavior, name)
}

func (c *typedContext[T]) SpawnAnonymous(behavior any) (actor.Ref, error) {
	return c.actor.System().SpawnAnonymous(behavior)
}

func (c *typedContext[T]) SystemActorOf(behavior any, name string) (actor.Ref, error) {
	return c.actor.System().SystemActorOf(behavior, name)
}

func (c *typedContext[T]) Ask(target actor.Ref, msgFactory func(actor.Ref) any, transform func(any, error) T) {
	askID := askCounter.Add(1)
	timerKey := fmt.Sprintf("ask-timeout-%d", askID)
	timeout := 3 * time.Second // Default timeout

	completed := &atomic.Bool{}

	// 1. Create transformed message for timeout
	timeoutMsg := transform(nil, actor.ErrAskTimeout)

	// 2. Schedule timeout using existing TimerScheduler
	c.Timers().StartSingleTimer(timerKey, timeoutMsg, timeout)

	// 3. Create temporary responder
	responder := &contextAskResponder[T]{
		self:      c.Self(),
		transform: transform,
		timerKey:  timerKey,
		timers:    c.Timers(),
		completed: completed,
	}

	// 4. Send the message
	msg := msgFactory(responder)
	target.Tell(msg)
}

type contextAskResponder[T any] struct {
	self      TypedActorRef[T]
	transform func(any, error) T
	timerKey  string
	timers    TimerScheduler[T]
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

// TypedActor is the internal bridge between the untyped actor system and typed behaviors.
type TypedActor[T any] struct {
	actor.BaseActor
	behavior Behavior[T]
	ctx      *typedContext[T]
	timers   *timerScheduler[T]
	stash    *stashBuffer[T]
	stopped  bool
}

// NewTypedActorInternal creates a new TypedActor instance with the given initial behavior.
func NewTypedActorInternal[T any](behavior Behavior[T]) *TypedActor[T] {
	a := &TypedActor[T]{
		BaseActor: actor.NewBaseActor(),
		behavior:  behavior,
	}
	a.ctx = &typedContext[T]{actor: a}
	return a
}

// PreStart initialises the timer scheduler and stash buffer once the actor's
// self reference has been injected by the actor system.
func (a *TypedActor[T]) PreStart() {
	a.timers = newTimerScheduler[T](a.Self())
	a.stash = newStashBuffer[T](a.Self(), actor.DefaultStashCapacity)
}

// PostStop cancels all active timers so their goroutines exit cleanly.
func (a *TypedActor[T]) PostStop() {
	a.timers.CancelAll()
}

// NewTypedActor creates a new Actor that handles messages of type T using the given behavior.
func NewTypedActor[T any](behavior Behavior[T]) actor.Actor {
	return NewTypedActorInternal(behavior)
}

// behaviorWrapper is a non-generic interface to allow calling behavior from untyped context.
type behaviorWrapper interface {
	Receive(ctx any, msg any) any // returns next behavior wrapper
}

type behaviorImpl[T any] struct {
	fn Behavior[T]
}

func (b *behaviorImpl[T]) Receive(ctx any, msg any) any {
	next := b.fn(ctx.(TypedContext[T]), msg.(T))
	if next == nil {
		return nil
	}
	return &behaviorImpl[T]{fn: next}
}

// NewTypedActorGeneric creates a new Actor from any behavior type.
func NewTypedActorGeneric(behavior any) actor.Actor {
	// We use reflection to find the message type T and wrap the behavior.
	val := reflect.ValueOf(behavior)
	if val.Kind() != reflect.Func {
		panic(fmt.Sprintf("typed: behavior must be a function, got %T", behavior))
	}
	
	// Extraction of T from func(TypedContext[T], T) Behavior[T]
	tType := val.Type().In(1)
	
	// Create a generic TypedActor using reflection to instantiate the generic type.
	// Since TypedActor[T] is a struct, we need to use reflect.New and some magic.
	// A simpler way is to have a factory function.
	
	return createTypedActorReflection(behavior, tType)
}

func createTypedActorReflection(behavior any, tType reflect.Type) actor.Actor {
	// We use a specialized non-generic actor that handles the type conversion.
	return &genericTypedActor{
		BaseActor: actor.NewBaseActor(),
		behavior:  reflect.ValueOf(behavior),
		tType:     tType,
	}
}

type genericTypedActor struct {
	actor.BaseActor
	behavior reflect.Value // Behavior[T]
	tType    reflect.Type
	ctx      any                  // TypedContext[T]
	timers   TimerScheduler[any] // Use local interface
	stash    StashBuffer[any]    // Use local interface
	stopped  bool
}

func (a *genericTypedActor) PreStart() {
	// We need a TypedContext[T]. We can't easily create one without generics.
	// But we can create a proxy context that implements the interface.
	a.ctx = createProxyContext(a)
	
	// Initialize timers/stash
	a.timers = actor.NewTimerScheduler[any](a.Self())
	a.stash = actor.NewStashBuffer[any](a.Self(), actor.DefaultStashCapacity)
}

func (a *genericTypedActor) PostStop() {
	if a.timers != nil {
		a.timers.CancelAll()
	}
}

func (a *genericTypedActor) Receive(msg any) {
	if a.stopped {
		return
	}
	
	// Check if msg is of type T
	mVal := reflect.ValueOf(msg)
	if !mVal.Type().AssignableTo(a.tType) {
		return
	}
	
	// Call behavior(ctx, msg)
	results := a.behavior.Call([]reflect.Value{
		reflect.ValueOf(a.ctx),
		mVal,
	})
	
	next := results[0]
	if !next.IsNil() {
		// Check if next is Stopped[T]
		// Stopped[T] is a stable pointer per type.
		if isStoppedGeneric(next.Interface(), a.tType) {
			a.Log().Debug("TypedActor: behavior stopped, stopping actor")
			a.stopped = true
			if s, ok := a.System().(interface{ Stop(actor.Ref) }); ok {
				s.Stop(a.Self())
			}
		} else {
			a.behavior = next
		}
	}
}

func isStoppedGeneric(behavior any, tType reflect.Type) bool {
	// Logic to detect Stopped[T] via reflection
	return false // Placeholder
}

// createProxyContext creates a TypedContext[T] proxy using reflection.
func createProxyContext(a *genericTypedActor) any {
	// This is complex. Let's provide a simpler implementation for now
	// that works for the most common cases or rethink the generic spawner.
	return nil 
}

// Receive implements the Actor interface for TypedActor[T].
func (a *TypedActor[T]) Receive(msg any) {
	if a.stopped {
		return
	}
	if m, ok := msg.(T); ok {
		next := a.behavior(a.ctx, m)
		if next != nil {
			if isStopped(next) {
				a.Log().Debug("TypedActor: behavior stopped, stopping actor")
				a.stopped = true
				a.ctx.Stop(a.Self())
			} else {
				a.behavior = next
			}
		}
	}
}

var stoppedSentinels sync.Map

// isStopped returns true if the behavior is the Stopped sentinel.
func isStopped[T any](b Behavior[T]) bool {
	if b == nil {
		return false
	}
	return reflect.ValueOf(b).Pointer() == reflect.ValueOf(Stopped[T]()).Pointer()
}

// Stopped returns a sentinel behavior indicating that the actor should stop.
func Stopped[T any]() Behavior[T] {
	t := reflect.TypeFor[T]()
	if s, ok := stoppedSentinels.Load(t); ok {
		return s.(Behavior[T])
	}
	// Stable pointer for each type T
	var s Behavior[T]
	s = func(ctx TypedContext[T], msg T) Behavior[T] {
		return s
	}
	stoppedSentinels.Store(t, s)
	return s
}

// Same returns a sentinel behavior indicating that the actor should keep its current behavior.
// In this implementation, Same is represented by a nil Behavior.
func Same[T any]() Behavior[T] {
	return nil
}

// Setup is a behavior decorator that allows for initialization of the actor.
// The factory function is called once when the actor starts, before it
// processes its first message.
func Setup[T any](factory func(TypedContext[T]) Behavior[T]) Behavior[T] {
	var inner Behavior[T]
	return func(ctx TypedContext[T], msg T) Behavior[T] {
		if inner == nil {
			ctx.Log().Debug("TypedActor: executing setup behavior")
			inner = factory(ctx)
			if inner == nil {
				inner = Same[T]()
			}
			ctx.Log().Debug("TypedActor: setup returned new behavior", "type", reflect.TypeOf(inner))
		}
		next := inner(ctx, msg)
		if next != nil {
			// If inner returned Stopped, we must return Stopped to the actor loop.
			if isStopped(next) {
				return Stopped[T]()
			}
			inner = next
		}
		return next
	}
}
