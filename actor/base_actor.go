/*
 * base_actor.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sopranoworks/gekka/telemetry"
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
//
// In addition to the user-message mailbox, every BaseActor owns a separate
// systemMailbox channel (priority). Start's dispatch loop drains
// systemMailbox ahead of mailbox on every iteration via a drain-first select
// so PoisonPill, Kill, and other system signals cannot be starved by a
// saturated user mailbox — Pekko's actor-cell-level priority semantics.
type BaseActor struct {
	mailbox            chan any
	systemMailbox      chan any       // priority channel for PoisonPill, Kill, supervisor signals
	systemCloseOnce    sync.Once      // guards close(systemMailbox)
	mbSend             func(any) bool // non-nil when a custom mailbox is installed
	mbClose            func()         // non-nil when a custom mailbox needs special teardown
	currentSender      Ref             // set for the duration of each Receive call; nil otherwise
	currentCtx         context.Context // trace context for the current message; nil outside Receive
	selfRef            Ref             // this actor's own reference, injected by SpawnActor/ActorOf
	systemRef          ActorContext    // injected by SpawnActor/ActorOf
	parentRef          Ref             // injected by SpawnActor/ActorOf for children
	supervisorStrategy SupervisorStrategy
	actorLog           ActorLogger
	watchersMu         sync.Mutex
	watchers           []any
	children           map[string]Ref   // children spawned by this actor
	childProps         map[string]Props // props used to spawn children, for Restart
	onStop             func()
	receiveStack       []func(msg any)  // behavior stack for become/unbecome
	receiveTimeout     *receiveTimeoutConfig // classic receive timeout state

	// Classic stash support
	classicStash        *StashBufferImpl[any] // nil until first Stash() call
	classicStashPending []any                 // populated by redeliver callback
	drainingClassicStash bool                 // prevents recursive drain
	currentMessage      any                   // set during Receive so Stash() can grab it
	drainDispatch       func(any) bool        // set by Start(); used by drain to re-enter dispatch
}

// systemMailboxBufferSize is the fixed buffer for the priority channel.
// 256 slots is large enough that a healthy supervision tree never saturates
// it — system messages are infrequent compared to user traffic. SendSystem
// returns false on the rare overflow rather than blocking the caller.
const systemMailboxBufferSize = 256

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

// SetSystem is called once by SpawnActor/ActorOf to inject the ActorContext.
func (b *BaseActor) SetSystem(s ActorContext) { b.systemRef = s }

// setSender is called by Start before each Receive invocation.
func (b *BaseActor) setSender(r Ref) { b.currentSender = r }

// CurrentContext returns the trace context associated with the message
// currently being processed by this actor's Receive method.
//
// It is safe to read from within Receive; outside Receive it returns
// context.Background(). Pass it to TellCtx or to child spans so they are
// recorded as children of the current trace.
func (b *BaseActor) CurrentContext() context.Context {
	if b.currentCtx == nil {
		return context.Background()
	}
	return b.currentCtx
}

// setCurrentCtx is called by Start before/after each Receive invocation.
func (b *BaseActor) setCurrentCtx(ctx context.Context) { b.currentCtx = ctx }

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
// h is the slog.Handler configured on the Cluster (may be nil → use default).
func (b *BaseActor) initLog(h slog.Handler, self Ref) {
	if h == nil {
		h = slog.Default().Handler()
	}
	b.actorLog = newActorLogger(h, self, func() Ref { return b.currentSender })
}

// NewBaseActor returns a BaseActor with a mailbox channel buffered to 256
// and a separate priority channel for system messages (also buffered).
func NewBaseActor() BaseActor {
	return BaseActor{
		mailbox:       make(chan any, 256),
		systemMailbox: make(chan any, systemMailboxBufferSize),
	}
}

// NewBaseActorWithSize returns a BaseActor whose mailbox channel is buffered
// to size. The priority channel for system messages keeps its default size.
func NewBaseActorWithSize(size int) BaseActor {
	return BaseActor{
		mailbox:       make(chan any, size),
		systemMailbox: make(chan any, systemMailboxBufferSize),
	}
}

// Mailbox satisfies the Actor interface and returns the embedded channel.
func (b *BaseActor) Mailbox() chan any {
	return b.mailbox
}

// SystemMailbox returns the priority channel used by Start to dispatch
// system messages ahead of user messages. The framework routes PoisonPill,
// Kill, and internal supervisor signals through this channel via SendSystem
// so they cannot be starved by a saturated user mailbox.
//
// User code rarely interacts with this channel directly; see SendSystem for
// the supported enqueue path.
func (b *BaseActor) SystemMailbox() chan any {
	return b.systemMailbox
}

// SendSystem enqueues msg on the priority channel using a non-blocking
// send. Returns true on success, false when the system mailbox is full or
// closed. The buffered channel is sized so healthy supervision trees never
// reach saturation; a false return signals an unrecoverable backlog and
// the caller may fall back to the user mailbox if appropriate.
func (b *BaseActor) SendSystem(msg any) (ok bool) {
	if b.systemMailbox == nil {
		return false
	}
	defer func() {
		if r := recover(); r != nil {
			ok = false // send on closed channel
		}
	}()
	select {
	case b.systemMailbox <- msg:
		return true
	default:
		return false
	}
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
// The priority systemMailbox is initialised here too so every actor — even
// those constructed via zero-value struct literals — gets system-message
// priority dispatch.
func (b *BaseActor) initMailbox() {
	if b.mailbox == nil {
		b.mailbox = make(chan any, 256)
	}
	if b.systemMailbox == nil {
		b.systemMailbox = make(chan any, systemMailboxBufferSize)
	}
	if b.children == nil {
		b.children = make(map[string]Ref)
	}
}

// Send delivers a message using the installed mailbox's enqueue semantics.
// When a MailboxFactory was provided via Props.Mailbox, this method applies
// the configured drop strategy or back-pressure. Without a custom mailbox it
// falls back to non-blocking channel send (DropNewest semantics).
//
// Returns true if the message was accepted, false if it was dropped.
func (b *BaseActor) Send(msg any) bool {
	if b.mbSend != nil {
		return b.mbSend(msg)
	}
	select {
	case b.mailbox <- msg:
		return true
	default:
		return false
	}
}

// CloseMailbox terminates the mailbox. For the default channel mailbox it
// closes the underlying channel. For priority mailboxes it signals the drain
// goroutine to stop (which then closes the channel, ending Start's range loop).
//
// The priority systemMailbox is closed independently and idempotently via
// systemCloseOnce so external callers (e.g. test helpers calling
// close(child.Mailbox())) and PoisonPill-driven closure both terminate the
// dispatch loop cleanly.
func (b *BaseActor) CloseMailbox() {
	// Cancel any pending receive timeout timer first to prevent the timer
	// callback from racing with the mailbox close (which would cause a
	// "send on closed channel" panic when the timer Tells ReceiveTimeout).
	b.CancelReceiveTimeout()
	if b.mbClose != nil {
		b.mbClose()
	} else {
		close(b.mailbox)
	}
	b.closeSystemMailbox()
}

// closeSystemMailbox closes the priority channel idempotently. Called by
// CloseMailbox and exposed for paths that close the user mailbox directly
// (e.g. close(child.Mailbox()) in tests) so the dispatch loop also drains
// the system side and exits.
func (b *BaseActor) closeSystemMailbox() {
	if b.systemMailbox == nil {
		return
	}
	b.systemCloseOnce.Do(func() { close(b.systemMailbox) })
}

// baseActor returns the receiver's *BaseActor pointer. Used by InjectMailbox
// to reach the embedded BaseActor of any actor struct through a type assertion.
func (b *BaseActor) baseActor() *BaseActor { return b }

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

	// AllForOneStrategy applies the directive to every child, not just the
	// one that failed. Collect all siblings first, then act on them all.
	targets := []Ref{child}
	if _, ok := strategy.(allForOneSupervisor); ok {
		all := b.Children()
		targets = make([]Ref, 0, len(all))
		for _, ref := range all {
			targets = append(targets, ref)
		}
		// Guarantee the failing child is included even if it was already
		// removed from the children map during the failure path.
		found := false
		for _, ref := range targets {
			if ref == child {
				found = true
				break
			}
		}
		if !found {
			targets = append(targets, child)
		}
	}

	type stopper interface{ Stop(Ref) }
	sys, hasStopper := b.System().(stopper)

	for _, target := range targets {
		switch directive {
		case Resume:
			target.Tell(resumeSignal{})
		case Restart:
			target.Tell(restartSignal{reason: err})
		case Stop:
			if hasStopper {
				sys.Stop(target)
			}
		case Escalate:
			panic(err)
		}
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

// ── Become / Unbecome ──────────────────────────────────────────────────────

// Become pushes a new message handler onto the behavior stack. Subsequent
// messages will be dispatched to this handler until Unbecome is called.
func (b *BaseActor) Become(receive func(msg any)) {
	b.receiveStack = append(b.receiveStack, receive)
}

// BecomeStacked is an alias for Become (Pekko compatibility).
func (b *BaseActor) BecomeStacked(receive func(msg any)) {
	b.Become(receive)
}

// Unbecome pops the top handler from the behavior stack. If the stack is
// empty after the pop, the actor reverts to its original Receive method.
func (b *BaseActor) Unbecome() {
	if len(b.receiveStack) > 0 {
		b.receiveStack = b.receiveStack[:len(b.receiveStack)-1]
	}
}

// currentReceive returns the active message handler: either the top of the
// behavior stack or nil (meaning use the original Receive method).
func (b *BaseActor) currentReceive() func(msg any) {
	if len(b.receiveStack) > 0 {
		return b.receiveStack[len(b.receiveStack)-1]
	}
	return nil
}

// ── Classic Stash ─────────────────────────────────────────────────────────

// initClassicStash lazily creates the stash buffer on first use.
func (b *BaseActor) initClassicStash() {
	if b.classicStash != nil {
		return
	}
	b.classicStash = NewStashBuffer[any](DefaultStashCapacity, func(msg any) {
		b.classicStashPending = append(b.classicStashPending, msg)
	})
}

// Stash stashes the currently-processed message for later redelivery.
// It must be called from within Receive; calling it outside Receive returns
// an error because there is no current message to stash.
func (b *BaseActor) Stash() error {
	if b.currentMessage == nil {
		return fmt.Errorf("actor.Stash: no current message (must be called inside Receive)")
	}
	b.initClassicStash()
	return b.classicStash.Stash(b.currentMessage)
}

// UnstashAll triggers redelivery of all stashed messages in FIFO order.
// Each message re-enters the actor's dispatch path (including the become
// stack). It is safe to call on an empty or uninitialised stash.
func (b *BaseActor) UnstashAll() {
	if b.classicStash == nil {
		return
	}
	b.classicStash.UnstashAll()
	b.drainClassicStash()
}

// StashBuffer returns the underlying stash buffer for inspection. Returns
// nil if Stash() has never been called.
func (b *BaseActor) StashBuffer() *StashBufferImpl[any] {
	return b.classicStash
}

// drainClassicStash pops from classicStashPending and re-enters dispatch.
// Guards against recursive drain (e.g. when a drained message calls
// UnstashAll again).
func (b *BaseActor) drainClassicStash() {
	if b.drainingClassicStash {
		return
	}
	b.drainingClassicStash = true
	defer func() { b.drainingClassicStash = false }()

	for len(b.classicStashPending) > 0 {
		next := b.classicStashPending[0]
		b.classicStashPending = b.classicStashPending[1:]
		if b.drainDispatch != nil {
			b.currentMessage = next
			b.drainDispatch(next)
			b.currentMessage = nil
		}
	}
}

// setCurrentMessage is called by Start() before/after each dispatch to track
// the message currently being processed.
func (b *BaseActor) setCurrentMessage(msg any) { b.currentMessage = msg }

// setDrainDispatch is called by Start() to wire the dispatch callback used
// by drainClassicStash to re-enter the actor's message handling path.
func (b *BaseActor) setDrainDispatch(fn func(any) bool) { b.drainDispatch = fn }

// ── Classic ReceiveTimeout ─────────────────────────────────────────────────

type receiveTimeoutConfig struct {
	mu       sync.Mutex
	duration time.Duration
	timer    *time.Timer
	active   bool
}

// SetReceiveTimeout configures the classic actor to receive a ReceiveTimeout{}
// message if no messages arrive within d. Resets on every message delivery.
func (b *BaseActor) SetReceiveTimeout(d time.Duration) {
	if b.receiveTimeout == nil {
		b.receiveTimeout = &receiveTimeoutConfig{}
	}
	rt := b.receiveTimeout
	rt.mu.Lock()
	defer rt.mu.Unlock()

	if rt.timer != nil {
		rt.timer.Stop()
	}
	rt.duration = d
	rt.active = true

	self := b.selfRef
	rt.timer = time.AfterFunc(d, func() {
		rt.mu.Lock()
		defer rt.mu.Unlock()
		if rt.active && self != nil {
			self.Tell(ReceiveTimeout{})
		}
	})
}

// CancelReceiveTimeout cancels a previously set receive timeout.
func (b *BaseActor) CancelReceiveTimeout() {
	if b.receiveTimeout == nil {
		return
	}
	rt := b.receiveTimeout
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.active = false
	if rt.timer != nil {
		rt.timer.Stop()
		rt.timer = nil
	}
}

// resetReceiveTimeout resets the timer. Called after each message delivery.
func (b *BaseActor) resetReceiveTimeout() {
	if b.receiveTimeout == nil {
		return
	}
	rt := b.receiveTimeout
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if !rt.active {
		return
	}
	if rt.timer != nil {
		rt.timer.Stop()
	}
	self := b.selfRef
	rt.timer = time.AfterFunc(rt.duration, func() {
		rt.mu.Lock()
		defer rt.mu.Unlock()
		if rt.active && self != nil {
			self.Tell(ReceiveTimeout{})
		}
	})
}

// InjectSystem sets the ActorContext on any actor that embeds BaseActor.
// It is called by SpawnActor/ActorOf after the actor has been registered.
//
// The interface is discovered with a package-local type assertion so that the
// unexported setSystem method (defined in this package) is reachable regardless
// of the calling package.
func InjectSystem(a Actor, ctx ActorContext) {
	type setter interface{ SetSystem(ActorContext) }
	if s, ok := a.(setter); ok {
		s.SetSystem(ctx)
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

// InjectSender sets the current sender for an actor. This should only be used
// when manually invoking Receive (e.g. from a typed behavior wrapper).
func InjectSender(a Actor, sender Ref) {
	type senderSetter interface{ setSender(Ref) }
	if ss, ok := a.(senderSetter); ok {
		ss.setSender(sender)
	}
}

// Start runs a dedicated goroutine that reads from a.Mailbox() and calls
// a.Receive for each message.  The goroutine exits when the channel is closed.
//
// Each iteration uses a drain-first select between the priority systemMailbox
// (PoisonPill, Kill, supervisor signals) and the user mailbox so that system
// messages always win over user traffic, regardless of which Mailbox impl is
// configured. This is Pekko's actor-cell-level priority semantics.
//
// Call Start once after constructing the actor, before registering it with
// Cluster.RegisterActor:
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

	type ctxSetter interface{ setCurrentCtx(context.Context) }
	cs, hasCS := any(a).(ctxSetter)

	type parentGetter interface{ Parent() Ref }
	pg, hasParent := any(a).(parentGetter)

	// Detect become/unbecome support (BaseActor provides it).
	type becomeSupport interface {
		currentReceive() func(msg any)
		resetReceiveTimeout()
		CancelReceiveTimeout()
	}
	bs, hasBecomeSupport := any(a).(becomeSupport)

	// Detect classic stash support (BaseActor provides it).
	type classicStashSupport interface {
		setCurrentMessage(any)
		setDrainDispatch(func(any) bool)
		drainClassicStash()
	}
	css, hasClassicStash := any(a).(classicStashSupport)

	// Detect priority system-mailbox support (BaseActor provides it). Actors
	// that don't expose SystemMailbox fall back to single-channel dispatch
	// for backward compatibility.
	type systemMailboxer interface{ SystemMailbox() chan any }
	var sysCh chan any
	if smb, ok := any(a).(systemMailboxer); ok {
		sysCh = smb.SystemMailbox()
	}

	// Acquire metric instruments once for this actor's lifetime.
	mailboxGauge, processDuration := initActorMetrics()

	go func() {
		defer func() {
			// Cancel receive timeout on stop
			if hasBecomeSupport {
				bs.CancelReceiveTimeout()
			}
			if trig, ok := any(a).(interface{ triggerStop() }); ok {
				trig.triggerStop()
			}
			a.PostStop()
		}()

		a.PreStart()

		// Resolve actor path once for telemetry labels. Safe even when Self
		// is not yet injected (returns empty string, which is harmless).
		actorPath := ""
		if self := a.Self(); self != nil {
			actorPath = self.Path()
		}
		pathAttr := telemetry.StringAttr("actor.path", actorPath)

		// dispatchMsg handles system messages (PoisonPill, Kill, Identify)
		// and delegates to the become stack or Receive. Returns false if the
		// actor should stop (PoisonPill).
		dispatchMsg := func(msg any) bool {
			switch msg.(type) {
			case PoisonPill:
				return false // signal stop
			case Kill:
				panic(&ActorKilledException{Actor: a.Self()})
			}
			if id, ok := msg.(Identify); ok {
				if sender := a.Self(); sender != nil {
					if hasSS {
						if s := any(a).(interface{ Sender() Ref }); s != nil {
							if senderRef := s.Sender(); senderRef != nil {
								senderRef.Tell(ActorIdentity{MessageID: id.MessageID, Ref: a.Self()})
								return true
							}
						}
					}
				}
				return true
			}
			// Reset receive timeout on each message
			if hasBecomeSupport {
				bs.resetReceiveTimeout()
			}
			// Dispatch via become stack or Receive
			if hasBecomeSupport {
				if handler := bs.currentReceive(); handler != nil {
					handler(msg)
					return true
				}
			}
			a.Receive(msg)
			return true
		}

		// Wire classic stash dispatch callback now that dispatchMsg is defined.
		if hasClassicStash {
			css.setDrainDispatch(dispatchMsg)
		}

		// processRaw runs the full per-message pipeline (telemetry, sender
		// setup, dispatch). Returns:
		//   - shouldContinue=true → restart inner loop after PreRestart-style
		//     control signal (resumeSignal/restartSignal).
		//   - exit=true → goroutine should terminate (PoisonPill or stop).
		processRaw := func(raw any) (shouldContinue, exit bool) {
			mailboxGauge.Add(context.Background(), -1, pathAttr)

			switch m := raw.(type) {
			case resumeSignal:
				return true, true
			case restartSignal:
				a.PreRestart(m.reason, nil)
				return true, true
			case Envelope:
				if hasSS {
					ss.setSender(m.Sender)
				}
				msgCtx := context.Background()
				if len(m.TraceContext) > 0 {
					msgCtx = actorTracer().Extract(msgCtx, m.TraceContext)
				}
				msgCtx, span := actorTracer().Start(msgCtx, "actor.Receive")
				span.SetAttribute("actor.path", actorPath)
				if hasCS {
					cs.setCurrentCtx(msgCtx)
				}
				start := time.Now()
				if hasClassicStash {
					css.setCurrentMessage(m.Payload)
				}
				if !dispatchMsg(m.Payload) {
					span.End()
					if closer, ok := any(a).(interface{ CloseMailbox() }); ok {
						closer.CloseMailbox()
					}
					return false, true
				}
				if hasClassicStash {
					css.setCurrentMessage(nil)
					css.drainClassicStash()
				}
				processDuration.Record(msgCtx, time.Since(start).Seconds(), pathAttr)
				span.End()
				if hasCS {
					cs.setCurrentCtx(context.Background())
				}
				if hasSS {
					ss.setSender(nil)
				}
				return false, false
			default:
				if hasSS {
					ss.setSender(nil)
				}
				msgCtx, span := actorTracer().Start(context.Background(), "actor.Receive")
				span.SetAttribute("actor.path", actorPath)
				if hasCS {
					cs.setCurrentCtx(msgCtx)
				}
				start := time.Now()
				if hasClassicStash {
					css.setCurrentMessage(m)
				}
				if !dispatchMsg(m) {
					span.End()
					if closer, ok := any(a).(interface{ CloseMailbox() }); ok {
						closer.CloseMailbox()
					}
					return false, true
				}
				if hasClassicStash {
					css.setCurrentMessage(nil)
					css.drainClassicStash()
				}
				processDuration.Record(msgCtx, time.Since(start).Seconds(), pathAttr)
				span.End()
				if hasCS {
					cs.setCurrentCtx(context.Background())
				}
				return false, false
			}
		}

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

				userCh := a.Mailbox()
				localSysCh := sysCh

				for {
					var raw any

					// Drain-first: try the priority channel non-blocking
					// before any blocking select. Naive 2-case select picks
					// randomly when both are ready and would let user traffic
					// starve system messages.
					if localSysCh != nil {
						select {
						case msg, ok := <-localSysCh:
							if !ok {
								localSysCh = nil
							} else {
								sc, exit := processRaw(msg)
								if sc {
									shouldContinue = true
									return
								}
								if exit {
									return
								}
								continue
							}
						default:
						}
					}

					// Block until either channel produces.
					if localSysCh == nil {
						msg, ok := <-userCh
						if !ok {
							return
						}
						raw = msg
					} else {
						select {
						case msg, ok := <-localSysCh:
							if !ok {
								localSysCh = nil
								continue
							}
							raw = msg
						case msg, ok := <-userCh:
							if !ok {
								// User mailbox closed: drain any remaining
								// system messages so PoisonPill-driven
								// shutdowns don't lose tail signals, then exit.
								for localSysCh != nil {
									sm, sok := <-localSysCh
									if !sok {
										return
									}
									sc, exit := processRaw(sm)
									if sc {
										shouldContinue = true
										return
									}
									if exit {
										return
									}
								}
								return
							}
							raw = msg
						}
					}

					sc, exit := processRaw(raw)
					if sc {
						shouldContinue = true
						return
					}
					if exit {
						return
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
