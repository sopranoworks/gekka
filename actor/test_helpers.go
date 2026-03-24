/*
 * test_helpers.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"testing"
)

// FunctionalMockRef is a mock actor reference that calls a handler function on Tell.
type FunctionalMockRef struct {
	Ref
	Handler func(any)
	PathURI string
}

func (r *FunctionalMockRef) Tell(msg any, sender ...Ref) { r.Handler(msg) }
func (r *FunctionalMockRef) Path() string                { return r.PathURI }

// NewFunctionalMockRef creates a new FunctionalMockRef.
func NewFunctionalMockRef(path string, handler func(any)) *FunctionalMockRef {
	return &FunctionalMockRef{PathURI: path, Handler: handler}
}

// ScatterGatherTestSystem is a mock ActorContext for testing complex routers and supervisors.
type ScatterGatherTestSystem struct {
	ActorContext
	Received chan string
	T        *testing.T
}

func (s *ScatterGatherTestSystem) ActorOf(props Props, name string) (Ref, error) {
	s.T.Log("System spawning actor...")

	p := props.New()

	// Use FunctionalMockRef for spawned actors so they can receive replies
	ref := &FunctionalMockRef{
		PathURI: "/temp/agg",
		Handler: func(m any) {
			s.T.Logf("Mock actor received message: %T", m)
			p.Receive(m)
		},
	}

	if a, ok := p.(interface{ SetSelf(Ref) }); ok {
		a.SetSelf(ref)
	}
	InjectSystem(p, s)

	Start(p)
	return ref, nil
}

func (s *ScatterGatherTestSystem) Stop(ref Ref) {
	// No-op for mock
}
