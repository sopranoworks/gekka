/*
 * base_actor.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"fmt"
	"log/slog"
	"sync"
)

// Actor is the interface that user-defined actors must implement.
//
// Receive is called for each message delivered to the actor.
// Mailbox returns the channel used to push messages into the actor.
//
// Embed BaseActor in your struct to get a default mailbox implementation:
//
//	type MyActor struct {
//	    actor.BaseActor
//	}
//
//	func (a *MyActor) Receive(msg any) {
//	    switch m := msg.(type) {
//	    case []byte:
//	        log.Printf("got: %s", m)
//	    }
//	}
type Actor interface {
	// Receive is called once per message, in the actor's dedicated goroutine.
	Receive(msg any)
	// Mailbox returns the channel on which callers should push messages.
	Mailbox() chan any

	// context). It returns nil until the actor has been registered via
	// SpawnActor or ActorOf.
	System() ActorContext

	// Self returns this actor's own reference.
	Self() Ref

	// Death Watch internals
	AddWatcher(watcher any)
	RemoveWatcher(watcher any)
	Watchers() []any
	SetOnStop(f func())

	// Lifecycle hooks
	PreStart()
	PostStop()
	PreRestart(reason error, message any)

	// Internals for Supervision
	SupervisorStrategy() SupervisorStrategy
	HandleFailure(child Ref, childActor Actor, err error)
}

// BaseActor provides a default Mailbox implementation and should be embedded
// in every user-defined actor struct.  The default buffer size is 256; call
// NewBaseActorWithSize to override it.
type BaseActor struct {
	mailbox            chan any
	currentSender      Ref          // set for the duration of each Receive call; nil otherwise
	selfRef            Ref          // this actor's own reference, injected by SpawnActor/ActorOf
	systemRef          ActorContext // injected by SpawnActor/ActorOf
	parentRef          Ref          // injected by SpawnActor/ActorOf for children
	supervisorStrategy SupervisorStrategy
	actorLog           ActorLogger
	watchersMu         sync.Mutex
	watchers           []any
	children           map[string]Ref   // children spawned by this actor
	childProps         map[string]Props // props used to spawn children, for Restart
	onStop             func()
}

// Sender returns the actor reference that sent the currently-processed message.
//
// Sender is only meaningful inside Receive; it is set before each Receive call
// and cleared immediately after. Accessing it from outside Receive (e.g. from
// a spawned goroutine) is not thread-safe and will return nil.
//
// For messages delivered by Artery (from Pekko or remote Go nodes) Sender
// is the remote ActorRef extracted from the Artery envelope. For local
// Tell(msg, senderRef) calls it is the explicitly-supplied senderRef.
// For Tell(msg) with no sender, Sender returns nil (actor.NoSender).
func (b *BaseActor) Sender() Ref { return b.currentSender }

// Self returns this actor's own ActorRef.
//
// Self is injected by SpawnActor / ActorOf during actor construction and is
// safe to read at any time after the actor starts. It is typically used to
// pass the actor's own reference as the sender in a Tell reply:
//
//	a.Sender().Tell([]byte("ack"), a.Self())
func (b *BaseActor) Self() Ref { return b.selfRef }

// SetSelf injects this actor's own reference. Called once by SpawnActor/ActorOf.
func (b *BaseActor) SetSelf(r Ref) { b.selfRef = r }

// System returns the ActorContext for this actor's owning node.
//
// It is safe to call at any time after the actor has been registered (i.e.
// after SpawnActor or ActorOf returns). Before registration it returns nil.
//
// Common uses inside Receive:
//
//	// Spawn a peer actor:
//	ref, err := a.System().ActorOf(actor.Props{New: func() actor.Actor {
//	    return &ChildActor{BaseActor: actor.NewBaseActor()}
//	}}, "child")
//
//	// Tie a background goroutine to the node's lifecycle:
//	go doWork(a.System().Context())
func (b *BaseActor) System() ActorContext { return b.systemRef }

// setSystem is called once by SpawnActor/ActorOf to inject the ActorContext.
func (b *BaseActor) setSystem(s ActorContext) { b.systemRef = s }

// setSender is called by Start before each Receive invocation.
func (b *BaseActor) setSender(r Ref) { b.currentSender = r }

// Log returns the actor-aware structured logger for this actor.
//
// Every log entry automatically includes the actor path, system name, and —
// when called inside Receive — the current sender path.
//
//	func (a *MyActor) Receive(msg any) {
//	    a.Log().Info("received", "type", fmt.Sprintf("%T", msg))
//	    // → level=INFO actor=pekko://Sys@host:port/user/myActor system=Sys sender=… msg=received type=…
//	}
//
// Before the logger is initialised (i.e. before SpawnActor/ActorOf is called),
// Log returns a no-op ActorLogger backed by the default slog handler.
func (b *BaseActor) Log() ActorLogger {
	if b.actorLog.base == nil {
		// Not yet initialised — return a temporary logger backed by the default
		// slog handler with a placeholder actor attribute.
		h := slog.Default().Handler()
		return ActorLogger{base: slog.New(h).With("actor", "(uninitialised)"), getRef: nil}
	}
	return b.actorLog
}

// initLog is called by SpawnActor / ActorOf once the actor has a known path.
// h is the slog.Handler configured on the GekkaNode (may be nil → use default).
func (b *BaseActor) initLog(h slog.Handler, self Ref) {
	if h == nil {
		h = slog.Default().Handler()
	}
	b.actorLog = newActorLogger(h, self, func() Ref { return b.currentSender })
}

// NewBaseActor returns a BaseActor with a mailbox channel buffered to 256.
func NewBaseActor() BaseActor {
	return BaseActor{mailbox: make(chan any, 256)}
}

// NewBaseActorWithSize returns a BaseActor whose mailbox channel is buffered
// to size.
func NewBaseActorWithSize(size int) BaseActor {
	return BaseActor{mailbox: make(chan any, size)}
}

// Mailbox satisfies the Actor interface and returns the embedded channel.
func (b *BaseActor) Mailbox() chan any {
	return b.mailbox
}

// AddWatcher tracks an actor that is monitoring this actor's lifecycle.
func (b *BaseActor) AddWatcher(w any) {
	b.watchersMu.Lock()
	defer b.watchersMu.Unlock()
	for _, x := range b.watchers {
		if x == w {
			return
		}
	}
	b.watchers = append(b.watchers, w)
}

// RemoveWatcher stops tracking an actor.
func (b *BaseActor) RemoveWatcher(w any) {
	b.watchersMu.Lock()
	defer b.watchersMu.Unlock()
	for i, x := range b.watchers {
		if x == w {
			b.watchers = append(b.watchers[:i], b.watchers[i+1:]...)
			return
		}
	}
}

// Watchers returns a copy of the current watchers.
func (b *BaseActor) Watchers() []any {
	b.watchersMu.Lock()
	defer b.watchersMu.Unlock()
	res := make([]any, len(b.watchers))
	copy(res, b.watchers)
	return res
}

// SetOnStop registers a callback to be invoked when the actor's mailbox closes.
func (b *BaseActor) SetOnStop(f func()) {
	b.watchersMu.Lock()
	defer b.watchersMu.Unlock()
	b.onStop = f
}

// triggerStop is called internally when the receive loop exits.
func (b *BaseActor) triggerStop() {
	b.watchersMu.Lock()
	f := b.onStop
	b.watchersMu.Unlock()
	if f != nil {
		f()
	}
}

// initMailbox lazily initialises the mailbox if it was not set via one of the
// constructors (i.e. the struct was created with a zero-value literal).
func (b *BaseActor) initMailbox() {
	if b.mailbox == nil {
		b.mailbox = make(chan any, 256)
	}
	if b.children == nil {
		b.children = make(map[string]Ref)
	}
}

// Lifecycle hooks default implementations

func (b *BaseActor) PreStart() {}
func (b *BaseActor) PostStop() {}
func (b *BaseActor) PreRestart(reason error, message any) {
	b.PostStop()
}

// Parent/Child tracking

func (b *BaseActor) Parent() Ref     { return b.parentRef }
func (b *BaseActor) setParent(p Ref) { b.parentRef = p }

func (b *BaseActor) SupervisorStrategy() SupervisorStrategy     { return b.supervisorStrategy }
func (b *BaseActor) setSupervisorStrategy(s SupervisorStrategy) { b.supervisorStrategy = s }

func (b *BaseActor) AddChild(name string, ref Ref, props Props) {
	b.watchersMu.Lock()
	defer b.watchersMu.Unlock()
	if b.children == nil {
		b.children = make(map[string]Ref)
	}
	if b.childProps == nil {
		b.childProps = make(map[string]Props)
	}
	b.children[name] = ref
	b.childProps[name] = props
}

func (b *BaseActor) RemoveChild(name string) {
	b.watchersMu.Lock()
	defer b.watchersMu.Unlock()
	if b.children != nil {
		delete(b.children, name)
	}
	if b.childProps != nil {
		delete(b.childProps, name)
	}
}

func (b *BaseActor) HandleFailure(child Ref, childActor Actor, err error) {
	if child == nil {
		return
	}
	strategy := b.SupervisorStrategy()
	if strategy == nil {
		strategy = DefaultSupervisorStrategy
	}

	directive := strategy.Decide(err)
	switch directive {
	case Resume:
		child.Tell(resumeSignal{})
	case Restart:
		child.Tell(restartSignal{reason: err})
	case Stop:
		// We need to stop the child. GekkaNode.Stop takes ActorRef.
		// Since we are in the actor package, we might need a bridge.
		// For now, let's just close the mailbox if we can.
		type stopper interface{ Stop(Ref) }
		if s, ok := b.System().(stopper); ok {
			s.Stop(child)
		}
	case Escalate:
		panic(err)
	}
}

type resumeSignal struct{}
type restartSignal struct{ reason error }

func (b *BaseActor) Children() map[string]Ref {
	b.watchersMu.Lock()
	defer b.watchersMu.Unlock()
	res := make(map[string]Ref)
	for k, v := range b.children {
		res[k] = v
	}
	return res
}

// InjectSystem sets the ActorContext on any actor that embeds BaseActor.
// It is called by SpawnActor/ActorOf after the actor has been registered.
//
// The interface is discovered with a package-local type assertion so that the
// unexported setSystem method (defined in this package) is reachable regardless
// of the calling package.
func InjectSystem(a Actor, ctx ActorContext) {
	type setter interface{ setSystem(ActorContext) }
	if s, ok := a.(setter); ok {
		s.setSystem(ctx)
	}
}

// InjectLog initialises the actor-aware structured logger on any actor that
// embeds BaseActor. It is called by SpawnActor/ActorOf once the actor has a
// known path. Same package-local type-assertion approach as InjectSystem.
func InjectLog(a Actor, h slog.Handler, self Ref) {
	type logIniter interface{ initLog(slog.Handler, Ref) }
	if li, ok := a.(logIniter); ok {
		li.initLog(h, self)
	}
}

// InjectSupervisorStrategy sets the supervisor strategy for an actor.
func InjectSupervisorStrategy(a Actor, s SupervisorStrategy) {
	type strategySetter interface{ setSupervisorStrategy(SupervisorStrategy) }
	if ss, ok := a.(strategySetter); ok {
		ss.setSupervisorStrategy(s)
	}
}

// InjectParent sets the parent reference for an actor.
func InjectParent(a Actor, parent Ref) {
	type parentSetter interface{ setParent(Ref) }
	if ps, ok := a.(parentSetter); ok {
		ps.setParent(parent)
	}
}

// Start runs a dedicated goroutine that reads from a.Mailbox() and calls
// a.Receive for each message.  The goroutine exits when the channel is closed.
//
// Call Start once after constructing the actor, before registering it with
// GekkaNode.RegisterActor:
//
//	a := &MyActor{BaseActor: actor.NewBaseActor()}
//	actor.Start(a)
//	node.RegisterActor("/user/myActor", a)
func Start(a Actor) {
	// Lazy initialisation: if the actor was created with a zero-value struct
	// literal (no NewBaseActor()), the embedded BaseActor.mailbox may be nil.
	// We detect this via the unexported initer interface.
	type initer interface{ initMailbox() }
	if b, ok := any(a).(initer); ok {
		b.initMailbox()
	}

	type senderSetter interface{ setSender(Ref) }
	ss, hasSS := any(a).(senderSetter)

	type parentGetter interface{ Parent() Ref }
	pg, hasParent := any(a).(parentGetter)

	go func() {
		defer func() {
			if trig, ok := any(a).(interface{ triggerStop() }); ok {
				trig.triggerStop()
			}
			a.PostStop()
		}()

		a.PreStart()

		for {
			var shouldContinue bool
			func() {
				defer func() {
					if r := recover(); r != nil {
						err, ok := r.(error)
						if !ok {
							err = fmt.Errorf("%v", r)
						}
						if hasParent {
							if parent := pg.Parent(); parent != nil {
								if p, ok := parent.(interface {
									HandleFailure(Ref, Actor, error)
								}); ok {
									p.HandleFailure(a.Self(), a, err)
								} else {
									parent.Tell(Failure{Actor: a.Self(), Reason: err})
								}
							}
						}
						shouldContinue = true
					}
				}()

				for raw := range a.Mailbox() {
					switch m := raw.(type) {
					case resumeSignal:
						shouldContinue = true
						return
					case restartSignal:
						a.PreRestart(m.reason, nil)
						shouldContinue = true
						return
					case Envelope:
						if hasSS {
							ss.setSender(m.Sender)
						}
						a.Receive(m.Payload)
						if hasSS {
							ss.setSender(nil)
						}
					default:
						if hasSS {
							ss.setSender(nil)
						}
						a.Receive(m)
					}
				}
			}()
			if !shouldContinue {
				break
			}
		}
	}()
}

// Failure is sent to the parent actor when a child actor panics.
type Failure struct {
	Actor  Ref
	Reason error
}
