/*
 * supervisor.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

// OneForOneStrategy is a supervisor strategy that applies the decider's
// directive only to the child actor that failed.
type OneForOneStrategy struct {
	Decider func(err error) Directive
}

// Decide implements SupervisorStrategy.
func (s *OneForOneStrategy) Decide(err error) Directive {
	if s.Decider == nil {
		return Restart
	}
	return s.Decider(err)
}

// AllForOneStrategy is a supervisor strategy that applies the decider's
// directive to ALL child actors when any one of them fails.
//
// This mirrors Pekko's AllForOneStrategy: if one child fails, the same
// directive (Restart, Stop, Resume, or Escalate) is applied to every sibling
// in the parent's children set, not just to the failing child.
//
// Use this strategy when your children are tightly coupled and a failure in
// one makes the state of the others suspect.
type AllForOneStrategy struct {
	Decider func(err error) Directive
}

// Decide implements SupervisorStrategy.
func (s *AllForOneStrategy) Decide(err error) Directive {
	if s.Decider == nil {
		return Restart
	}
	return s.Decider(err)
}

// allForOneSupervisor is the marker interface used by BaseActor.HandleFailure
// to distinguish AllForOneStrategy from OneForOneStrategy without exposing the
// concrete type across packages.
type allForOneSupervisor interface {
	allForOne()
}

// allForOne is the unexported marker method that satisfies allForOneSupervisor.
func (s *AllForOneStrategy) allForOne() {}

// DefaultSupervisorStrategy is the default strategy used when none is supplied.
// It restarts the actor for any error.
var DefaultSupervisorStrategy = &OneForOneStrategy{
	Decider: func(err error) Directive {
		return Restart
	},
}

// StoppingSupervisorStrategy stops the actor for any error.
var StoppingSupervisorStrategy = &OneForOneStrategy{
	Decider: func(err error) Directive {
		return Stop
	},
}
