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

	"gekka/gekka/actor"
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
	fullPath string // full actor-path URI, e.g. "pekko://System@host:port/user/foo"
	node     *GekkaNode
	local    actor.Actor // non-nil when the target is registered locally on this node
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

// Path returns the full actor-path URI for this reference.
func (r ActorRef) Path() string { return r.fullPath }

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
		env := actor.Envelope{Payload: msg, Sender: s}
		select {
		case r.local.Mailbox() <- env:
		default:
			// Mailbox full — drop silently (analogous to Pekko dead letters).
		}
		return
	}

	if r.node == nil {
		return
	}

	// Remote: serialise and send via Artery TCP.
	if s != nil {
		_ = r.node.router.SendWithSender(context.Background(), r.fullPath, s.Path(), msg)
	} else {
		_ = r.node.Send(context.Background(), r.fullPath, msg)
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
func (r ActorRef) Ask(ctx context.Context, msg any) (any, error) {
	reply, err := r.node.Ask(ctx, r.fullPath, msg)
	if err != nil {
		return nil, err
	}
	if reg := r.node.nm.SerializerRegistry; reg != nil {
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
	rawPath string // path as given by the caller
	node    *GekkaNode
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
		self := s.node.SelfAddress()
		if ap.Address.System == self.System && ap.Address.Host == self.Host && ap.Address.Port == self.Port {
			localPath := "/" + strings.Join(ap.Elements(), "/")
			s.node.actorsMu.RLock()
			a, found := s.node.actors[localPath]
			s.node.actorsMu.RUnlock()
			if found {
				return ActorRef{fullPath: s.rawPath, node: s.node, local: a}, nil
			}
			// Local URI but actor not registered — fall through to remote ref.
			// The message will reach the dead-letter queue on delivery.
		}
		return ActorRef{fullPath: s.rawPath, node: s.node}, nil
	}

	// ── Local relative path ───────────────────────────────────────────────
	if strings.HasPrefix(s.rawPath, "/") {
		s.node.actorsMu.RLock()
		a, found := s.node.actors[s.rawPath]
		s.node.actorsMu.RUnlock()

		fullPath := s.node.selfPathURI(s.rawPath)
		if found {
			return ActorRef{fullPath: fullPath, node: s.node, local: a}, nil
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
			_ = s.node.Send(context.Background(), s.rawPath, msg)
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
		ctx = s.node.ctx
	}
	ref, err := s.Resolve(ctx)
	if err != nil {
		return nil, err
	}
	return ref.Ask(ctx, msg)
}

// selfPathURI converts a local path suffix such as "/user/myActor" into the
// full actor-path URI for this node. If path is already absolute it is
// returned unchanged.
func (n *GekkaNode) selfPathURI(path string) string {
	if len(path) > 0 && path[0] == '/' {
		self := n.SelfAddress()
		return fmt.Sprintf("%s://%s@%s:%d%s",
			self.Protocol, self.System, self.Host, self.Port, path)
	}
	return path
}

// ActorSelection returns a handle to one or more actors, local or remote,
// identified by path.
//
// path can be a local suffix ("/user/myActor") or a full remote URI
// ("pekko://System@host:port/user/myActor"). Call Resolve on the returned
// ActorSelection to obtain a concrete ActorRef:
//
//	ref, err := node.ActorSelection("/user/myActor").Resolve(ctx)
//	ref.Tell("Hello")
func (n *GekkaNode) ActorSelection(path string) ActorSelection {
	return ActorSelection{rawPath: path, node: n}
}

// SpawnActor starts a and registers it at path, then returns an ActorRef for
// that actor. It is a convenient alternative to the manual three-step sequence
// of actor.Start / node.RegisterActor / building an ActorRef:
//
//	ref := node.SpawnActor("/user/myActor", &MyActor{BaseActor: actor.NewBaseActor()})
//	ref.Tell("Hello, local actor!")
//
// path must be the full path suffix as used in Artery envelopes, e.g.
// "/user/myActor". Do NOT call actor.Start yourself before SpawnActor — that
// would launch two receive goroutines.
func (n *GekkaNode) SpawnActor(path string, a actor.Actor, props actor.Props) ActorRef {
	ref := ActorRef{fullPath: n.selfPathURI(path), node: n, local: a}

	// Inject the actor's own reference so it can use Self() inside Receive.
	type selfSetter interface{ SetSelf(actor.Ref) }
	if ss, ok := a.(selfSetter); ok {
		ss.SetSelf(ref)
	}

	// Resolve the parent path (e.g., "/user/parent/child" -> "/user/parent")
	parentPath := "/user"
	lastSlash := strings.LastIndex(path, "/")
	if lastSlash > 0 {
		parentPath = path[:lastSlash]
	}

	// Inject the ActorContext so actors can spawn peers and access the
	// node lifecycle context via a.System(). Uses actor.InjectSystem so that
	// the package-local type assertion reaches the unexported setSystem method.
	if nas, ok := n.System.(*nodeActorSystem); ok {
		actor.InjectSystem(a, nas.asActorContext(path))
	}

	// Inject SupervisorStrategy from Props
	actor.InjectSupervisorStrategy(a, props.SupervisorStrategy)

	// Inject parent reference if this is a child actor.
	if parentPath != "/user" {
		if parentActor, found := n.actors[parentPath]; found {
			if parentRef, err := n.ActorSelection(n.selfPathURI(parentPath)).Resolve(context.TODO()); err == nil {
				actor.InjectParent(a, parentRef)
				// Also register this child with the parent
				type childAdder interface {
					AddChild(string, actor.Ref, actor.Props)
				}
				if ca, ok := parentActor.(childAdder); ok {
					ca.AddChild(path[lastSlash+1:], ref, props)
				}
			}
		}
	}

	// Initialise the actor-aware logger. Uses actor.InjectLog for the same
	// package-locality reason.
	actor.InjectLog(a, n.logHandler, ref)

	a.SetOnStop(func() {
		// Stop all children recursively
		type childrenGetter interface{ Children() map[string]actor.Ref }
		if cg, ok := any(a).(childrenGetter); ok {
			for _, child := range cg.Children() {
				if childRef, ok := child.(ActorRef); ok {
					n.System.Stop(childRef)
				}
			}
		}

		n.UnregisterActor(path)
		terminatedMsg := Terminated{Actor: ref}
		for _, w := range a.Watchers() {
			if watcherRef, ok := w.(ActorRef); ok {
				watcherRef.Tell(terminatedMsg)
			}
		}

		// Remove from parent's children list
		if parentPath != "/user" {
			if parentActor, found := n.actors[parentPath]; found {
				type childRemover interface{ RemoveChild(string) }
				if cr, ok := parentActor.(childRemover); ok {
					cr.RemoveChild(path[lastSlash+1:])
				}
			}
		}
	})
	actor.Start(a)
	n.RegisterActor(path, a)
	return ref
}
