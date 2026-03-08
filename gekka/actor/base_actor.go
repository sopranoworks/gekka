/*
 * base_actor.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

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
}

// BaseActor provides a default Mailbox implementation and should be embedded
// in every user-defined actor struct.  The default buffer size is 256; call
// NewBaseActorWithSize to override it.
type BaseActor struct {
	mailbox chan any
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

// initMailbox lazily initialises the mailbox if it was not set via one of the
// constructors (i.e. the struct was created with a zero-value literal).
func (b *BaseActor) initMailbox() {
	if b.mailbox == nil {
		b.mailbox = make(chan any, 256)
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
	go func() {
		for msg := range a.Mailbox() {
			a.Receive(msg)
		}
	}()
}
