/*
 * actor_ref.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"fmt"
	"strings"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/telemetry"
)

// ActorRef is a location-transparent reference to a specific actor. It
// supports fire-and-forget delivery (Tell) and request-reply (Ask) regardless
// of whether the target actor is on this node or on a remote node.
//
// ActorRef implements actor.Ref, so it can be passed wherever that interface
// is expected (e.g. BaseActor.Sender(), BaseActor.Self()).
//
// Obtain an ActorRef from:
//   - node.ActorSelection("...").Resolve(ctx) — discovery by path
//   - node.SpawnActor("/user/myActor", a)     — register a new local actor
//   - node.System.ActorOf(props, "name")      — create + register in one step
//
// An ActorRef is a lightweight value — it is safe to copy and share across
// goroutines.
type ActorRef struct {
	fullPath string         // full actor-path URI, e.g. "pekko://System@host:port/user/foo"
	sys      internalSystem // the underlying system (Cluster or localActorSystem)
	local    actor.Actor    // non-nil when the target is registered locally on this node
}

// NoSender is the zero-value ActorRef used when no specific sender is attached
// to a message. Pass it (or omit the sender argument) to Tell when the recipient
// should not reply to a particular actor.
//
//	ref.Tell([]byte("fire and forget"), gekka.NoSender)
//	ref.Tell([]byte("fire and forget")) // equivalent
var NoSender ActorRef

// Terminated is sent to watching actors when the target actor stops.
// For remote actors, this is triggered when the hosting node leaves the cluster
// or becomes unreachably severed.
type Terminated struct {
	Actor ActorRef
}

// TerminatedActor implements actor.TerminatedMessage so that Terminated can be
// handled by actors in the actor package (e.g. PoolRouter) without an import
// cycle.
func (t Terminated) TerminatedActor() actor.Ref { return t.Actor }

// Path returns the full actor-path URI for this reference.
func (r ActorRef) Path() string { return r.fullPath }

// System returns the underlying ActorSystem.
func (r ActorRef) System() ActorSystem {
	return r.sys
}

// String implements fmt.Stringer so an ActorRef can be passed directly to
// node.Send, node.Ask, or any API that accepts a path string.
func (r ActorRef) String() string { return r.fullPath }

// Tell delivers msg to the actor. For local actors the message is placed
// directly into the actor's mailbox (no serialisation). For remote actors
// it is serialised and sent over Artery TCP.
//
// Tell is fire-and-forget: it returns as soon as the message is accepted by
// the mailbox or the Artery outbox. It does not wait for the actor to process
// the message or for any network acknowledgment.
//
// sender is the actor reference to embed as the message origin so that the
// recipient can reply via its Sender() method. Omit sender (or pass
// gekka.NoSender / nil) for anonymous fire-and-forget messages.
//
//	// Simple fire-and-forget
//	ref.Tell([]byte("ping"))
//
//	// With explicit sender so the recipient can reply
//	ref.Tell([]byte("ping"), self)
//
// Tell satisfies the actor.Ref interface.
func (r ActorRef) Tell(msg any, sender ...actor.Ref) {
	// Resolve sender: nil means NoSender.
	var s actor.Ref
	if len(sender) > 0 && sender[0] != nil && sender[0].Path() != "" {
		s = sender[0]
	}

	if r.local != nil {
		defer func() {
			if r := recover(); r != nil {
				_ = r // Mailbox closed or other panic — drop silently
			}
		}()
		env := actor.Envelope{Payload: msg, Sender: s}
		select {
		case r.local.Mailbox() <- env:
		default:
			// Mailbox full — drop silently (analogous to Pekko dead letters).
		}
		return
	}

	if r.sys == nil {
		return
	}

	// Remote: serialise and send via Artery TCP.
	if s != nil {
		_ = r.sys.SendWithSender(context.Background(), r.fullPath, s.Path(), msg)
	} else {
		_ = r.sys.Send(context.Background(), r.fullPath, msg)
	}
}

// Ask sends msg and blocks until the actor replies or ctx is cancelled.
//
// The reply is deserialized via the node's SerializationRegistry when a
// matching serializer is registered; otherwise the raw []byte payload is
// returned.
//
// A context deadline or timeout is strongly recommended — Ask blocks until
// the remote actor replies:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	defer cancel()
//	reply, err := ref.Ask(ctx, []byte("ping"))
//
// TellCtx delivers msg with W3C trace-context propagation extracted from ctx.
//
// For local actors the trace headers are embedded in the Envelope so the
// recipient's receive loop can start a child span. For remote actors the
// context is passed through to the underlying Send call.
//
// Use TellCtx from within an actor's Receive method by passing
// BaseActor.CurrentContext() as ctx to ensure automatic parent-child linking:
//
//	func (a *MyActor) Receive(msg any) {
//	    a.Sender().TellCtx(a.CurrentContext(), "ack", a.Self())
//	}
func (r ActorRef) TellCtx(ctx context.Context, msg any, sender ...actor.Ref) {
	var s actor.Ref
	if len(sender) > 0 && sender[0] != nil && sender[0].Path() != "" {
		s = sender[0]
	}

	if r.local != nil {
		defer func() {
			if r := recover(); r != nil {
				_ = r // Mailbox closed or other panic — drop silently
			}
		}()
		// Inject current span into the trace-context map.
		var tc map[string]string
		tracer := telemetry.GetTracer("github.com/sopranoworks/gekka")
		tmp := make(map[string]string, 2)
		tracer.Inject(ctx, tmp)
		if len(tmp) > 0 {
			tc = tmp
		}
		env := actor.Envelope{Payload: msg, Sender: s, TraceContext: tc}
		select {
		case r.local.Mailbox() <- env:
		default:
		}
		return
	}

	if r.sys == nil {
		return
	}
	if s != nil {
		_ = r.sys.SendWithSender(ctx, r.fullPath, s.Path(), msg)
	} else {
		_ = r.sys.Send(ctx, r.fullPath, msg)
	}
}

func (r ActorRef) Ask(ctx context.Context, msg any) (any, error) {
	tracer := telemetry.GetTracer("github.com/sopranoworks/gekka")
	ctx, span := tracer.Start(ctx, "actor.Ask")
	span.SetAttribute("actor.path", r.fullPath)
	defer span.End()

	reply, err := r.sys.Ask(ctx, r.fullPath, msg)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if reply.DeserializedMessage != nil {
		return reply.DeserializedMessage, nil
	}
	if reg := r.sys.SerializationRegistry(); reg != nil {
		obj, err := reg.DeserializePayload(reply.SerializerId, reply.Manifest, reply.Payload)
		if err == nil {
			return obj, nil
		}
	}
	return reply.Payload, nil
}

// ActorSelection is a lazily-resolved handle to one or more actors, local or
// remote, identified by a path string.
//
// A path may be:
//   - A local suffix   "/user/myActor"            (relative to this node)
//   - A remote URI     "pekko://Sys@host:port/user/myActor"
//
// Obtain an ActorSelection from node.ActorSelection and call Resolve to get a
// concrete ActorRef:
//
//	sel := node.ActorSelection("pekko://ClusterSystem@10.0.0.1:2552/user/echo")
//	ref, err := sel.Resolve(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	ref.Tell([]byte("Hello"))
//
// For remote URIs Resolve always succeeds — the TCP connection is established
// lazily on the first Tell or Ask.
type ActorSelection struct {
	rawPath string         // path as given by the caller
	sys     internalSystem // the underlying system (Cluster or localActorSystem)
}

// Resolve returns a concrete ActorRef for this selection.
//
// For local paths (starting with "/"), the actor must already be registered
// via node.RegisterActor or node.SpawnActor; an error is returned if not.
//
// For remote absolute URIs an ActorRef is returned immediately — no network
// round-trip occurs during Resolve.
//
// Passing nil for ctx is valid: the method falls back to the node's own root
// context (equivalent to node.Context()), which is cancelled when the node
// shuts down. This is a convenient default when no per-call deadline is needed:
//
//	ref, err := node.ActorSelection("/user/myActor").Resolve(nil)
func (s ActorSelection) Resolve(_ context.Context) (ActorRef, error) {

	// ── Absolute URI ──────────────────────────────────────────────────────
	if strings.Contains(s.rawPath, "://") {
		ap, err := actor.ParseActorPath(s.rawPath)
		if err != nil {
			return ActorRef{}, fmt.Errorf("actorselection: invalid path %q: %w", s.rawPath, err)
		}

		// Check whether this URI addresses a local actor on this node.
		self := s.sys.SelfAddress()
		if ap.Address.System == self.System && ap.Address.Host == self.Host && ap.Address.Port == self.Port {
			localPath := ap.Path()
			a, found := s.sys.GetLocalActor(localPath)
			if found {
				return ActorRef{fullPath: s.rawPath, sys: s.sys, local: a}, nil
			}
			// Local URI but actor not registered — fall through to remote ref.
			// The message will reach the dead-letter queue on delivery.
		}
		return ActorRef{fullPath: s.rawPath, sys: s.sys}, nil
	}

	// ── Local relative path ───────────────────────────────────────────────
	if strings.HasPrefix(s.rawPath, "/") {
		a, found := s.sys.GetLocalActor(s.rawPath)

		fullPath := s.sys.SelfPathURI(s.rawPath)
		if found {
			return ActorRef{fullPath: fullPath, sys: s.sys, local: a}, nil
		}
		return ActorRef{}, fmt.Errorf("actorselection: no actor registered at %q", s.rawPath)
	}

	return ActorRef{}, fmt.Errorf("actorselection: invalid path format %q (must be absolute URI or start with /)", s.rawPath)
}

// Tell resolves the selection and delivers msg. For unresolved local actors an
// error is logged and the message is dropped. sender is forwarded to the
// resolved ActorRef.Tell — see ActorRef.Tell for semantics.
func (s ActorSelection) Tell(msg any, sender ...actor.Ref) {
	ref, err := s.Resolve(context.Background())
	if err != nil {
		if !strings.HasPrefix(s.rawPath, "/") {
			// Remote path: send even if local lookup failed (actor may be remote).
			_ = s.sys.Send(context.Background(), s.rawPath, msg)
		}
		return
	}
	ref.Tell(msg, sender...)
}

// Ask resolves the selection and blocks until the actor replies or ctx is
// cancelled. See ActorRef.Ask for semantics and return-value behaviour.
//
// Passing nil for ctx is valid: the method falls back to the node's own root
// context (node.Context()), which is cancelled when the node shuts down.
// Note: without a deadline, Ask may block indefinitely if the remote actor
// never replies. A context with timeout is strongly recommended for production.
func (s ActorSelection) Ask(ctx context.Context, msg any) (any, error) {
	if ctx == nil {
		ctx = s.sys.Context()
	}
	ref, err := s.Resolve(ctx)
	if err != nil {
		return nil, err
	}
	return ref.Ask(ctx, msg)
}

// ActorSelection returns a handle to one or more actors, local or remote,
// identified by path.
//
// ToTyped converts an untyped Ref to a TypedActorRef[T].
func ToTyped[T any](ref actor.Ref) typed.TypedActorRef[T] {
	return typed.ToTyped[T](ref)
}

// ToUntyped converts a TypedActorRef[T] to an untyped Ref.
func ToUntyped[T any](ref typed.TypedActorRef[T]) actor.Ref {
	return typed.ToUntyped(ref)
}
