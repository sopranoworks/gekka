/*
 * supervisor.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import ()

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
