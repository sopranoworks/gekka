/*
 * actor_system_local.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"

	hocon "github.com/sopranoworks/gekka-config"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/internal/core"
	"github.com/sopranoworks/gekka/persistence"
	"github.com/sopranoworks/gekka/stream"
	"github.com/sopranoworks/gekka/telemetry"
)

// localActorSystem implements ActorSystem for local-only use without networking.
type localActorSystem struct {
	name          string
	ctx           context.Context
	cancel        context.CancelFunc
	actors        map[string]actor.Actor
	actorsMu      sync.RWMutex
	logHandler    slog.Handler
	sched         *systemScheduler
	journal       persistence.Journal
	snapshotStore persistence.SnapshotStore
}

// NewActorSystem creates and returns a local-only ActorSystem.
// It manages actors within the same process without networking or cluster features.
//
// A Journal and SnapshotStore are provisioned automatically from the plugin
// registry. The default plugin is "in-memory" (stdlib only, no external deps).
// Override via the HOCON key persistence.journal.plugin / persistence.snapshot-store.plugin:
//
//	gekka {
//	  persistence {
//	    journal.plugin       = "postgres"
//	    snapshot-store.plugin = "postgres"
//	  }
//	}
func NewActorSystem(name string, config ...*hocon.Config) (ActorSystem, error) {
	journalPlugin := "in-memory"
	snapshotPlugin := "in-memory"
	if len(config) > 0 && config[0] != nil {
		if v, err := config[0].GetString("persistence.journal.plugin"); err == nil && v != "" {
			journalPlugin = v
		}
		if v, err := config[0].GetString("persistence.snapshot-store.plugin"); err == nil && v != "" {
			snapshotPlugin = v
		}
	}

	j, err := persistence.NewJournal(journalPlugin)
	if err != nil {
		return nil, fmt.Errorf("actor system: provision journal %q: %w", journalPlugin, err)
	}
	ss, err := persistence.NewSnapshotStore(snapshotPlugin)
	if err != nil {
		return nil, fmt.Errorf("actor system: provision snapshot store %q: %w", snapshotPlugin, err)
	}

	telemetryPlugin := "no-op"
	if len(config) > 0 && config[0] != nil {
		if v, err := config[0].GetString("telemetry.provider.plugin"); err == nil && v != "" {
			telemetryPlugin = v
		}
	}
	if tp, err := telemetry.GetProvider(telemetryPlugin); err == nil {
		telemetry.SetProvider(tp)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &localActorSystem{
		name:          name,
		ctx:           ctx,
		cancel:        cancel,
		actors:        make(map[string]actor.Actor),
		sched:         newSystemScheduler(),
		journal:       j,
		snapshotStore: ss,
	}
	// Terminate the scheduler when the system context is cancelled.
	go func() {
		<-ctx.Done()
		s.sched.terminate()
	}()
	return s, nil
}

// Journal implements ActorSystem.
func (s *localActorSystem) Journal() persistence.Journal { return s.journal }

// SnapshotStore implements ActorSystem.
func (s *localActorSystem) SnapshotStore() persistence.SnapshotStore { return s.snapshotStore }

// ProvideJournal replaces the journal backend at runtime.  Call before spawning
// any persistent actors.  Idiomatic for tests that want a real SQL backend:
//
//	db, _ := sql.Open("pgx", dsn)
//	journal, _ := persistence.NewJournalFromDB("postgres", db)
//	sys.(*localActorSystem).ProvideJournal(journal)
func (s *localActorSystem) ProvideJournal(j persistence.Journal) {
	s.journal = j
}

// ProvideSnapshotStore replaces the snapshot store backend at runtime.
func (s *localActorSystem) ProvideSnapshotStore(ss persistence.SnapshotStore) {
	s.snapshotStore = ss
}

// Scheduler implements ActorSystem.
func (s *localActorSystem) Scheduler() Scheduler {
	return s.sched
}

// Materializer implements ActorSystem.
func (s *localActorSystem) Materializer() stream.Materializer {
	return stream.ActorMaterializer{}
}

// Receptionist implements ActorSystem.
func (s *localActorSystem) Receptionist() typed.TypedActorRef[any] {
	return typed.TypedActorRef[any]{} // Not available in local-only system
}

// SubscribeToReceptionist implements internalSystem.
func (s *localActorSystem) SubscribeToReceptionist(keyID string, subscriber typed.TypedActorRef[any], callback func([]string)) {
	// No-op in local-only system
}

// ActorOf implements ActorSystem.
func (s *localActorSystem) ActorOf(props Props, name string) (ActorRef, error) {
	return s.ActorOfHierarchical(props, name, "/user")
}

// Spawn implements ActorSystem.
func (s *localActorSystem) Spawn(behavior any, name string) (ActorRef, error) {
	props := Props{
		New: func() actor.Actor { return typed.NewTypedActorGeneric(behavior) },
	}
	return s.ActorOf(props, name)
}

// SpawnAnonymous implements ActorSystem.
func (s *localActorSystem) SpawnAnonymous(behavior any) (ActorRef, error) {
	return s.Spawn(behavior, "")
}

// SystemActorOf implements ActorSystem.
func (s *localActorSystem) SystemActorOf(behavior any, name string) (ActorRef, error) {
	props := Props{
		New: func() actor.Actor { return typed.NewTypedActorGeneric(behavior) },
	}
	return s.ActorOfHierarchical(props, name, "/system")
}

// ActorOfHierarchical creates a new actor as a child of parentPath.
func (s *localActorSystem) ActorOfHierarchical(props Props, name string, parentPath string) (ActorRef, error) {
	// Generate a unique name when none is supplied.
	if name == "" {
		n := autoNameCounter.Add(1)
		name = fmt.Sprintf("$%d", n)
	}

	// Reject names that contain '/' — they would silently create nested paths.
	if strings.ContainsRune(name, '/') {
		return ActorRef{}, fmt.Errorf("actorOf: name %q must not contain '/'", name)
	}

	if parentPath == "" {
		parentPath = "/user"
	}
	path := parentPath + "/" + name

	// Check for duplicates before constructing the actor.
	s.actorsMu.RLock()
	_, exists := s.actors[path]
	s.actorsMu.RUnlock()
	if exists {
		return ActorRef{}, fmt.Errorf("actorOf: actor already registered at %q", path)
	}

	// Plain actor — Props.New is required.
	if props.New == nil {
		return ActorRef{}, fmt.Errorf("actorOf: Props.New must not be nil")
	}
	a := props.New()
	return s.SpawnActor(path, a, props).(ActorRef), nil
}

// Context implements ActorSystem.
func (s *localActorSystem) Context() context.Context {
	return s.ctx
}

// Terminate implements ActorSystem.
func (s *localActorSystem) Terminate() {
	s.cancel()
}

// WhenTerminated implements ActorSystem.
func (s *localActorSystem) WhenTerminated() <-chan struct{} {
	return s.ctx.Done()
}

// Watch implements ActorSystem.
func (s *localActorSystem) Watch(watcher ActorRef, target ActorRef) {
	if target.local != nil {
		target.local.AddWatcher(watcher)
	}
}

// Unwatch implements ActorSystem.
func (s *localActorSystem) Unwatch(watcher ActorRef, target ActorRef) {
	if target.local != nil {
		target.local.RemoveWatcher(watcher)
	}
}

// Stop implements ActorSystem.
func (s *localActorSystem) Stop(target ActorRef) {
	if target.local != nil {
		close(target.local.Mailbox())
	}
}

// RemoteActorOf implements ActorSystem.
func (s *localActorSystem) RemoteActorOf(address actor.Address, path string) ActorRef {
	fullPath := address.String()
	if !strings.HasPrefix(path, "/") {
		fullPath += "/"
	}
	fullPath += path
	return ActorRef{fullPath: fullPath, sys: s}
}

// Ask implements ActorSystem.
func (s *localActorSystem) Ask(ctx context.Context, dst interface{}, msg any) (*IncomingMessage, error) {
	var pathStr string
	switch d := dst.(type) {
	case string:
		pathStr = d
	case actor.ActorPath:
		pathStr = d.String()
	case fmt.Stringer:
		pathStr = d.String()
	default:
		return nil, fmt.Errorf("gekka: Ask: unsupported destination type %T", dst)
	}

	ap, err := actor.ParseActorPath(pathStr)
	if err != nil {
		// Fallback for local path suffix
		if strings.HasPrefix(pathStr, "/") {
			s.actorsMu.RLock()
			a, found := s.actors[pathStr]
			s.actorsMu.RUnlock()
			if found {
				return AskLocal(ctx, a, msg)
			}
			return nil, fmt.Errorf("gekka: Ask: actor not found: %s", pathStr)
		}
		return nil, fmt.Errorf("gekka: Ask: parse: %w", err)
	}

	// Check if local
	if ap.Address.System == s.name {
		localPath := ap.Path()
		s.actorsMu.RLock()
		a, found := s.actors[localPath]
		s.actorsMu.RUnlock()
		if found {
			return AskLocal(ctx, a, msg)
		}
	}

	return nil, fmt.Errorf("gekka: Ask: target %s is not local and networking is disabled", pathStr)
}

// Send implements ActorSystem.
func (s *localActorSystem) Send(ctx context.Context, dst interface{}, msg any) error {
	var pathStr string
	switch d := dst.(type) {
	case string:
		pathStr = d
	case actor.ActorPath:
		pathStr = d.String()
	case fmt.Stringer:
		pathStr = d.String()
	default:
		return fmt.Errorf("gekka: Send: unsupported destination type %T", dst)
	}

	s.actorsMu.RLock()
	a, found := s.actors[pathStr]
	s.actorsMu.RUnlock()
	if found {
		a.Mailbox() <- msg
		return nil
	}
	return fmt.Errorf("gekka: Send: actor not found: %s", pathStr)
}

// RegisterType implements ActorSystem.
func (s *localActorSystem) RegisterType(manifest string, typ reflect.Type) {
	// Local-only system doesn't use serialization for delivery.
}

// GetTypeByManifest implements ActorSystem.
func (s *localActorSystem) GetTypeByManifest(manifest string) (reflect.Type, bool) {
	return nil, false
}

// ActorSelection implements ActorSystem.
func (s *localActorSystem) ActorSelection(path string) ActorSelection {
	return ActorSelection{rawPath: path, sys: s}
}

// SendWithSender implements internalSystem.
func (s *localActorSystem) SendWithSender(ctx context.Context, path string, senderPath string, msg any) error {
	s.actorsMu.RLock()
	a, found := s.actors[path]
	s.actorsMu.RUnlock()
	if found {
		// We need to resolve senderPath to a Ref
		var sender actor.Ref
		if senderPath != "" {
			// Try to resolve locally
			s.actorsMu.RLock()
			sa, sfound := s.actors[senderPath]
			s.actorsMu.RUnlock()
			if sfound {
				sender = ActorRef{fullPath: s.SelfPathURI(senderPath), sys: s, local: sa}
			} else {
				sender = ActorRef{fullPath: senderPath, sys: s}
			}
		}
		a.Mailbox() <- actor.Envelope{Payload: msg, Sender: sender}
		return nil
	}
	return fmt.Errorf("gekka: SendWithSender: actor not found: %s", path)
}

// SelfAddress implements internalSystem.
func (s *localActorSystem) SelfAddress() actor.Address {
	return actor.Address{
		Protocol: "gekka",
		System:   s.name,
		Host:     "local",
		Port:     0,
	}
}

// SerializationRegistry implements internalSystem.
func (s *localActorSystem) SerializationRegistry() *core.SerializationRegistry {
	return nil
}

// GetLocalActor implements internalSystem.
func (s *localActorSystem) GetLocalActor(path string) (actor.Actor, bool) {
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	a, ok := s.actors[path]
	return a, ok
}

// SelfPathURI implements internalSystem.
func (s *localActorSystem) SelfPathURI(path string) string {
	if len(path) > 0 && path[0] == '/' {
		self := s.SelfAddress()
		return fmt.Sprintf("%s://%s@%s:%d%s",
			self.Protocol, self.System, self.Host, self.Port, path)
	}
	return path
}

// LookupDeployment implements internalSystem.
func (s *localActorSystem) LookupDeployment(path string) (core.DeploymentConfig, bool) {
	return core.DeploymentConfig{}, false
}

// SpawnActor implements internalSystem.
func (s *localActorSystem) SpawnActor(path string, a actor.Actor, props actor.Props) actor.Ref {
	ref := ActorRef{fullPath: s.SelfPathURI(path), sys: s, local: a}

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
	// node lifecycle context via a.System().
	actor.InjectSystem(a, asActorContext(s, path))

	// Inject SupervisorStrategy from Props
	actor.InjectSupervisorStrategy(a, props.SupervisorStrategy)

	// Inject parent reference if this is a child actor.
	if parentPath != "/user" {
		if parentActor, found := s.actors[parentPath]; found {
			if parentRef, err := (ActorSelection{rawPath: s.SelfPathURI(parentPath), sys: s}).Resolve(context.TODO()); err == nil {
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

	// Initialise the actor-aware logger.
	actor.InjectLog(a, s.logHandler, ref)

	a.SetOnStop(func() {
		// Stop all children recursively
		type childrenGetter interface{ Children() map[string]actor.Ref }
		if cg, ok := any(a).(childrenGetter); ok {
			for _, child := range cg.Children() {
				if childRef, ok := child.(ActorRef); ok {
					s.Stop(childRef)
				}
			}
		}

		s.actorsMu.Lock()
		delete(s.actors, path)
		s.actorsMu.Unlock()

		terminatedMsg := Terminated{Actor: ref}
		for _, w := range a.Watchers() {
			if watcherRef, ok := w.(ActorRef); ok {
				watcherRef.Tell(terminatedMsg)
			}
		}

		// Remove from parent's children list
		if parentPath != "/user" {
			if parentActor, found := s.actors[parentPath]; found {
				type childRemover interface{ RemoveChild(string) }
				if cr, ok := parentActor.(childRemover); ok {
					cr.RemoveChild(path[lastSlash+1:])
				}
			}
		}
	})
	actor.Start(a)

	s.actorsMu.Lock()
	s.actors[path] = a
	s.actorsMu.Unlock()

	return ref
}
