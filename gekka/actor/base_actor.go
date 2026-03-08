/*
 * base_actor.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
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

	// System returns the ActorContext for this actor's node, giving access to
	// ActorOf (to spawn peers/children) and Context (the node's lifecycle
	// context). It returns nil until the actor has been registered via
	// SpawnActor or ActorOf.
	System() ActorContext

	// Death Watch internals
	AddWatcher(watcher any)
	RemoveWatcher(watcher any)
	Watchers() []any
	SetOnStop(f func())
}

// BaseActor provides a default Mailbox implementation and should be embedded
// in every user-defined actor struct.  The default buffer size is 256; call
// NewBaseActorWithSize to override it.
type BaseActor struct {
	mailbox       chan any
	currentSender Ref          // set for the duration of each Receive call; nil otherwise
	selfRef       Ref          // this actor's own reference, injected by SpawnActor/ActorOf
	systemRef     ActorContext // injected by SpawnActor/ActorOf
	actorLog      ActorLogger
	watchersMu    sync.Mutex
	watchers      []any
	onStop        func()
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

	go func() {
		for raw := range a.Mailbox() {
			// Unwrap Envelope to extract sender and payload.
			msg := raw
			var sender Ref
			if env, ok := raw.(Envelope); ok {
				msg = env.Payload
				sender = env.Sender
			}
			if hasSS {
				ss.setSender(sender)
			}
			a.Receive(msg)
			if hasSS {
				ss.setSender(nil)
			}
		}
		if trig, ok := any(a).(interface{ triggerStop() }); ok {
			trig.triggerStop()
		}
	}()
}
