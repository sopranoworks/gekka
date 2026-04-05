/*
 * actor.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package actor provides type-safe representations of Pekko/Akka actor addresses
// and paths, equivalent to org.apache.pekko.actor.{Address,ActorPath}.
//
// Usage:
//
//	addr := actor.Address{Protocol: "pekko", System: "ClusterSystem", Host: "127.0.0.1", Port: 2552}
//	path := addr.WithRoot("user").Child("myActor")
//	fmt.Println(path) // → pekko://ClusterSystem@127.0.0.1:2552/user/myActor
package actor

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// Ref is the interface satisfied by all actor references, both local and remote.
//
// It enables location-transparent messaging: the caller does not need to know
// where the target actor lives.
//
// gekka.ActorRef implements Ref. Use actor.Ref as the parameter/return type
// inside actor code (e.g. BaseActor.Sender / Self) to avoid import cycles.
type Ref interface {
	// Tell delivers msg to the actor without waiting for a reply.
	// sender is the originating actor reference; pass nil (or actor.NoSender)
	// when no reply is expected.
	Tell(msg any, sender ...Ref)
	// Path returns the full actor-path URI for this reference,
	// e.g. "pekko://ClusterSystem@127.0.0.1:2552/user/myActor".
	Path() string
}

// Directive defines the action a supervisor takes in response to a child's failure.
type Directive int

const (
	// Resume continues processing the next message, keeping the current state.
	Resume Directive = iota
	// Restart stops the actor and starts it again, resetting its state.
	Restart
	// Stop permanently terminates the actor.
	Stop
	// Escalate notifies the parent's supervisor about the failure.
	Escalate
)

var (
	// ErrAskTimeout is returned by Ask operations when the response does not
	// arrive within the expected duration.
	ErrAskTimeout = fmt.Errorf("ask timed out")
)

// SupervisorStrategy defines how to handle failures in child actors.
type SupervisorStrategy interface {
	// Decide returns the Directive for a given error.
	Decide(err error) Directive
}

// NoSender is the nil Ref value — the default sender when no explicit origin
// is provided. Pass it (or omit the sender argument) to Tell when the
// recipient should not reply to the caller.
var NoSender Ref

// Envelope wraps a message together with its sender reference for internal
// delivery through the actor mailbox.
//
// Tell creates an Envelope when a non-nil sender is supplied; Start detects
// Envelopes and sets BaseActor.currentSender before calling Receive, then
// clears it afterwards. User code never needs to create or inspect Envelope
// directly.
type Envelope struct {
	Payload      any               // the actual message delivered to Receive
	Sender       Ref               // nil ↔ NoSender
	TraceContext map[string]string // W3C trace headers for span propagation (nil = no tracing)
}

// Props is a factory specification for creating an actor instance.
//
// New is called once per actor creation; dependencies (node references,
// configuration, etc.) should be captured by the closure rather than embedded
// in Props, keeping Props itself data-free.
//
// Props lives in the actor package (not in gekka) so that ActorContext can
// reference it without introducing an import cycle.
//
//	props := actor.Props{New: func() actor.Actor {
//	    return &MyActor{BaseActor: actor.NewBaseActor()}
//	}}
//	ref, err := self.System().ActorOf(props, "child")
type Props struct {
	// New must not be nil; it is called exactly once during actor creation.
	New func() Actor

	// SupervisorStrategy defines how failures in children spawned by this
	// actor should be handled. If nil, a default strategy (usually Restart)
	// is used.
	SupervisorStrategy SupervisorStrategy

	// Mailbox overrides the default unbounded channel mailbox. Use
	// NewBoundedMailbox or NewPriorityMailbox to set a custom mailbox.
	// Nil means the default 256-element buffered channel.
	Mailbox MailboxFactory
}

// TerminatedMessage is implemented by actor-stopped notifications.
//
// PoolRouter and other lifecycle-aware actors type-assert incoming messages
// against this interface so they can react to child termination without
// importing the gekka package (which would create a circular dependency).
//
// gekka.Terminated satisfies this interface.
type TerminatedMessage interface {
	// TerminatedActor returns the Ref of the actor that stopped.
	TerminatedActor() Ref
}

// Passivate is a signal sent to the parent actor (e.g. Shard) to request
// graceful termination of the sender.
type Passivate struct {
	Entity Ref
}

// StateTimeout is a message sent when an FSM state duration expires.
type StateTimeout struct{}

func (StateTimeout) String() string {
	return "FSM.StateTimeout"
}

// ActorContext is the subset of the ActorSystem API that is safe to use from
// within actor code (i.e. from the actor package) without introducing an import
// cycle.
//
// Obtain it inside Receive via BaseActor.System():
//
//	func (a *MyActor) Receive(msg any) {
//	    // Spawn a child actor:
//	    ref, _ := a.System().ActorOf(actor.Props{New: func() actor.Actor {
//	        return &ChildActor{BaseActor: actor.NewBaseActor()}
//	    }}, "child")
//	    // Watch the child so Receive gets a TerminatedMessage when it stops:
//	    a.System().Watch(a.Self(), ref)
//
//	    // Tie a background goroutine to the node's lifecycle:
//	    go doWork(a.System().Context())
//	}
//
// gekka.ActorSystem embeds all ActorContext methods and additionally exposes
// richer return types (e.g. gekka.ActorRef instead of actor.Ref).
type ActorContext interface {
	// ActorOf creates a new actor, registers it at /user/<name>, starts its
	// receive goroutine, and returns a location-transparent Ref.
	ActorOf(props Props, name string) (Ref, error)

	// Spawn creates a new typed actor with the given behavior and name.
	// behavior must be a typed.Behavior[T].
	Spawn(behavior any, name string) (Ref, error)

	// SpawnAnonymous creates a new typed actor with an automatically generated name.
	SpawnAnonymous(behavior any) (Ref, error)

	// SystemActorOf creates a new actor under the /system guardian.
	SystemActorOf(behavior any, name string) (Ref, error)

	// Context returns the root context of the node that owns this system.
	// It is cancelled when the node shuts down, making it suitable as the
	// parent context for background goroutines started by actors.
	Context() context.Context

	// Watch registers watcher to receive a TerminatedMessage when target
	// stops. Use it inside PreStart or Receive to monitor children:
	//
	//	a.System().Watch(a.Self(), childRef)
	Watch(watcher Ref, target Ref)

	// Resolve looks up and returns the Ref for an already-registered actor at
	// path. For local paths (starting with "/") the actor must be registered;
	// for remote absolute URIs the ref is returned immediately.
	// Returns an error when the local actor is not found.
	//
	// Used by GroupRouter.PreStart to convert routee path strings into live
	// Refs without importing the gekka package.
	Resolve(path string) (Ref, error)

	// Stop stops the actor identified by ref. The actor's PostStop lifecycle
	// hook is called after all queued messages have been processed.
	// It is a no-op when ref does not point to a local actor.
	Stop(ref Ref)

	// ActorSelection returns a handle to one or more actors, local or remote,
	// identified by path.
	ActorSelection(path string) ActorSelection
}

// Selection represents a single path element in an ActorSelection.
type Selection struct {
	Type    int32  // 0: Parent, 1: ChildName, 2: ChildPattern
	Matcher string // Name or pattern
}

// ActorSelection identifies one or more actors by a path pattern.
type ActorSelection struct {
	Anchor Ref
	Path   []Selection
	// System allows the selection to perform delivery without an import cycle.
	System interface {
		DeliverSelection(s ActorSelection, msg any, sender ...Ref)
		ResolveSelection(s ActorSelection, ctx context.Context) (Ref, error)
		AskSelection(s ActorSelection, ctx context.Context, msg any) (any, error)
	}
}

// Tell delivers msg to the actors identified by this selection.
func (s ActorSelection) Tell(msg any, sender ...Ref) {
	if s.System != nil {
		s.System.DeliverSelection(s, msg, sender...)
	}
}

// Ask sends msg and blocks until a reply is received or ctx is cancelled.
func (s ActorSelection) Ask(ctx context.Context, msg any) (any, error) {
	if s.System != nil {
		return s.System.AskSelection(s, ctx, msg)
	}
	return nil, fmt.Errorf("actor: selection system is nil")
}

// Resolve returns a concrete Ref for this selection.
func (s ActorSelection) Resolve(ctx context.Context) (Ref, error) {
	if s.System != nil {
		return s.System.ResolveSelection(s, ctx)
	}
	return nil, fmt.Errorf("actor: selection system is nil")
}

// ParseSelectionElements converts a path string into a slice of Selection elements.
func ParseSelectionElements(path string) []Selection {
	if path == "" {
		return nil
	}
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return nil
	}
	parts := strings.Split(trimmed, "/")
	res := make([]Selection, len(parts))
	for i, p := range parts {
		if p == ".." {
			res[i] = Selection{Type: 0}
		} else if strings.ContainsAny(p, "*?") {
			res[i] = Selection{Type: 2, Matcher: p}
		} else {
			res[i] = Selection{Type: 1, Matcher: p}
		}
	}
	return res
}

// Address identifies an actor system on the network.
// It is the Go equivalent of org.apache.pekko.actor.Address.
type Address struct {
	Protocol string // "pekko" or "akka"
	System   string // actor system name, e.g. "ClusterSystem"
	Host     string // hostname or IP, e.g. "127.0.0.1"
	Port     int    // TCP port, e.g. 2552
}

// ToProto converts the Address into a Protobuf Address.
func (a Address) ToProto() *gproto_remote.Address {
	return &gproto_remote.Address{
		Protocol: proto.String(a.Protocol),
		System:   proto.String(a.System),
		Hostname: proto.String(a.Host),
		Port:     proto.Uint32(uint32(a.Port)),
	}
}

// String returns the address in URI form: "protocol://system@host:port".
func (a Address) String() string {
	return fmt.Sprintf("%s://%s@%s:%d", a.Protocol, a.System, a.Host, a.Port)
}

// Root returns an ActorPath at the root guardian of this address.
func (a Address) Root() ActorPath {
	return ActorPath{Address: a}
}

// WithRoot returns an ActorPath rooted at the named top-level guardian.
// Pekko has two top-level guardians: "user" (for user-created actors) and
// "system" (for system actors such as cluster daemons).
//
//	addr.WithRoot("user").Child("myActor")
//	// → "pekko://ClusterSystem@127.0.0.1:2552/user/myActor"
func (a Address) WithRoot(name string) ActorPath {
	return ActorPath{Address: a, elements: []string{name}}
}

// ParseAddress parses a URI of the form "protocol://system@host:port" into an Address.
//
//	addr, err := actor.ParseAddress("pekko://ClusterSystem@127.0.0.1:2552")
func ParseAddress(uri string) (Address, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return Address{}, fmt.Errorf("actor: parse address %q: %w", uri, err)
	}
	if u.User == nil || u.User.Username() == "" {
		return Address{}, fmt.Errorf("actor: parse address %q: missing actor system name", uri)
	}
	host, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		return Address{}, fmt.Errorf("actor: parse address %q: %w", uri, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return Address{}, fmt.Errorf("actor: parse address %q: invalid port: %w", uri, err)
	}
	return Address{
		Protocol: u.Scheme,
		System:   u.User.Username(),
		Host:     host,
		Port:     port,
	}, nil
}

// ActorPath identifies a specific actor within a remote actor system.
// It is the Go equivalent of org.apache.pekko.actor.ActorPath.
//
// ActorPath is immutable: Child and Parent return new values.
type ActorPath struct {
	// Address is the actor system this path belongs to.
	Address Address

	elements []string // ordered path segments, e.g. ["user", "myActor"]
}

// ParseActorPath parses a full actor path URI into an ActorPath.
//
//	path, err := actor.ParseActorPath("pekko://ClusterSystem@127.0.0.1:2552/user/myActor")
func ParseActorPath(uri string) (ActorPath, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return ActorPath{}, fmt.Errorf("actor: parse path %q: %w", uri, err)
	}
	addrURI := u.Scheme + "://" + u.Host
	if u.User != nil {
		addrURI = u.Scheme + "://" + u.User.String() + "@" + u.Host
	}
	addr, err := ParseAddress(addrURI)
	if err != nil {
		return ActorPath{}, err
	}
	elems := strings.FieldsFunc(u.Path, func(r rune) bool { return r == '/' })
	return ActorPath{Address: addr, elements: elems}, nil
}

// String returns the full actor path URI:
// "protocol://system@host:port/elem1/elem2/…"
func (p ActorPath) String() string {
	if len(p.elements) == 0 {
		return p.Address.String() + "/"
	}
	return p.Address.String() + "/" + strings.Join(p.elements, "/")
}

// ToAddress converts the system address of this path into a Protobuf Address.
func (p ActorPath) ToAddress() *gproto_remote.Address {
	return p.Address.ToProto()
}

// ToUniqueAddress converts the parsed path into a Protobuf UniqueAddress (UID unknown).
func (p ActorPath) ToUniqueAddress(uid uint64) *gproto_remote.UniqueAddress {
	return &gproto_remote.UniqueAddress{
		Address: p.ToAddress(),
		Uid:     proto.Uint64(uid),
	}
}

// Path returns the path part of the actor path, e.g. "/user/myActor".
func (p ActorPath) Path() string {
	if len(p.elements) == 0 {
		return "/"
	}
	return "/" + strings.Join(p.elements, "/")
}

// Child appends name as a child element and returns the new ActorPath.
// The receiver is not modified.
//
//	path.Child("myActor")           // → …/user/myActor
//	path.Child("myActor").Child("a") // → …/user/myActor/a
func (p ActorPath) Child(name string) ActorPath {
	elems := make([]string, len(p.elements)+1)
	copy(elems, p.elements)
	elems[len(p.elements)] = name
	return ActorPath{Address: p.Address, elements: elems}
}

// Parent returns the parent path, removing the last element.
// Calling Parent on a root path (no elements) returns the same path.
func (p ActorPath) Parent() ActorPath {
	if len(p.elements) == 0 {
		return p
	}
	elems := make([]string, len(p.elements)-1)
	copy(elems, p.elements[:len(p.elements)-1])
	return ActorPath{Address: p.Address, elements: elems}
}

// Name returns the last path element (the actor's local name).
// Returns an empty string for a root path.
func (p ActorPath) Name() string {
	if len(p.elements) == 0 {
		return ""
	}
	return p.elements[len(p.elements)-1]
}

// Elements returns a copy of the ordered path segments.
func (p ActorPath) Elements() []string {
	return append([]string(nil), p.elements...)
}

// Depth returns the number of path elements.
func (p ActorPath) Depth() int {
	return len(p.elements)
}
