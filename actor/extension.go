/*
 * extension.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"sync"
)

// Extension is a marker interface for actor system extensions.  Extensions
// are lazily initialized on first access and guaranteed to be singletons
// within an actor system.
type Extension interface {
	isExtension()
}

// ExtensionId identifies and creates an [Extension].  Implement this interface
// to define a new extension type:
//
//	type MyExtId struct{}
//	func (MyExtId) CreateExtension(sys ActorContext) Extension { return &myExt{} }
type ExtensionId interface {
	// CreateExtension is called once per actor system to create the extension
	// instance.
	CreateExtension(system ActorContext) Extension
}

// ExtensionRegistry manages extension instances for an actor system.
// Extensions are lazily initialized and thread-safe.
type ExtensionRegistry struct {
	mu         sync.Mutex
	extensions map[ExtensionId]Extension
	initOnce   map[ExtensionId]*sync.Once
	system     ActorContext
}

// NewExtensionRegistry creates a new registry bound to the given system.
func NewExtensionRegistry(system ActorContext) *ExtensionRegistry {
	return &ExtensionRegistry{
		extensions: make(map[ExtensionId]Extension),
		initOnce:   make(map[ExtensionId]*sync.Once),
		system:     system,
	}
}

// RegisterExtension initializes and registers an extension if not already
// present.  Returns the extension instance.  Concurrent calls with the same
// id are safe — only one call to CreateExtension is made.
func (r *ExtensionRegistry) RegisterExtension(id ExtensionId) Extension {
	r.mu.Lock()
	once, exists := r.initOnce[id]
	if !exists {
		once = &sync.Once{}
		r.initOnce[id] = once
	}
	r.mu.Unlock()

	once.Do(func() {
		ext := id.CreateExtension(r.system)
		r.mu.Lock()
		r.extensions[id] = ext
		r.mu.Unlock()
	})

	r.mu.Lock()
	defer r.mu.Unlock()
	return r.extensions[id]
}

// Extension returns the extension for the given id, or (nil, false) if not
// registered.
func (r *ExtensionRegistry) Extension(id ExtensionId) (Extension, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ext, ok := r.extensions[id]
	return ext, ok
}
