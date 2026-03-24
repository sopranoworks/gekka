/*
 * circuit_breaker.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"context"
	"errors"
	"sync"
	"time"
)

// CircuitBreakerState defines the possible states of a circuit breaker.
type CircuitBreakerState int

const (
	Closed CircuitBreakerState = iota
	Open
	HalfOpen
)

var (
	ErrCircuitOpen = errors.New("circuit breaker is open")
	ErrCallTimeout = errors.New("circuit breaker call timeout")
)

// CircuitBreakerSettings configures the behavior of a circuit breaker.
type CircuitBreakerSettings struct {
	MaxFailures  int
	CallTimeout  time.Duration
	ResetTimeout time.Duration
}

// CircuitBreaker protects actor communications by failing fast when errors occur.
type CircuitBreaker struct {
	mu            sync.RWMutex
	state         CircuitBreakerState
	failures      int
	lastFailure   time.Time
	settings      CircuitBreakerSettings
	onStateChange func(state CircuitBreakerState)
}

// NewCircuitBreaker creates a new CircuitBreaker with the given settings.
func NewCircuitBreaker(settings CircuitBreakerSettings) *CircuitBreaker {
	return &CircuitBreaker{
		state:    Closed,
		settings: settings,
	}
}

// OnStateChange registers a callback for state transitions.
func (cb *CircuitBreaker) OnStateChange(f func(state CircuitBreakerState)) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.onStateChange = f
}

// Call executes a function through the circuit breaker.
func (cb *CircuitBreaker) Call(f func() error) error {
	state := cb.getState()

	if state == Open {
		return ErrCircuitOpen
	}

	// For HalfOpen or Closed, we attempt the call.
	var err error
	done := make(chan struct{})

	go func() {
		err = f()
		close(done)
	}()

	select {
	case <-done:
		if err != nil {
			cb.recordFailure()
			return err
		}
		cb.recordSuccess()
		return nil
	case <-time.After(cb.settings.CallTimeout):
		cb.recordFailure()
		return ErrCallTimeout
	}
}

func (cb *CircuitBreaker) getState() CircuitBreakerState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	if cb.state == Open {
		if time.Since(cb.lastFailure) > cb.settings.ResetTimeout {
			// Auto-transition to HalfOpen after reset timeout
			cb.mu.RUnlock()
			cb.mu.Lock()
			if cb.state == Open {
				cb.changeState(HalfOpen)
			}
			cb.mu.Unlock()
			cb.mu.RLock()
		}
	}
	return cb.state
}

func (cb *CircuitBreaker) recordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailure = time.Now()

	if cb.state == Closed && cb.failures >= cb.settings.MaxFailures {
		cb.changeState(Open)
	} else if cb.state == HalfOpen {
		cb.changeState(Open)
	}
}

func (cb *CircuitBreaker) recordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == HalfOpen || cb.failures > 0 {
		cb.failures = 0
		cb.changeState(Closed)
	}
}

func (cb *CircuitBreaker) changeState(newState CircuitBreakerState) {
	if cb.state != newState {
		cb.state = newState
		if cb.onStateChange != nil {
			go cb.onStateChange(newState)
		}
	}
}

// Wrapper for Tell (fire-and-forget). Note that Tell doesn't usually return errors
// or have a timeout, so this wrapper primarily checks if the circuit is open.
func (cb *CircuitBreaker) Tell(ref Ref, msg any, sender ...Ref) error {
	if cb.getState() == Open {
		return ErrCircuitOpen
	}
	ref.Tell(msg, sender...)
	return nil
}

// Wrapper for Ask (request-response).
func (cb *CircuitBreaker) Ask(ctx context.Context, ref Ref, msg any, askFn func(context.Context, Ref, any) (any, error)) (any, error) {
	var result any
	err := cb.Call(func() error {
		var callErr error
		result, callErr = askFn(ctx, ref, msg)
		return callErr
	})
	return result, err
}
