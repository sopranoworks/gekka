/*
 * circuit_breaker_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCircuitBreaker_Lifecycle(t *testing.T) {
	settings := CircuitBreakerSettings{
		MaxFailures:  3,
		CallTimeout:  100 * time.Millisecond,
		ResetTimeout: 200 * time.Millisecond,
	}
	cb := NewCircuitBreaker(settings)

	// 1. Initially Closed
	assert.Equal(t, Closed, cb.state)

	// 2. Record 2 failures - still Closed
	errFail := errors.New("fail")
	_ = cb.Call(func() error { return errFail })
	_ = cb.Call(func() error { return errFail })
	assert.Equal(t, Closed, cb.state)

	// 3. 3rd failure - transitions to Open
	_ = cb.Call(func() error { return errFail })
	assert.Equal(t, Open, cb.state)

	// 4. While Open, calls fail fast
	err := cb.Call(func() error { return nil })
	assert.Equal(t, ErrCircuitOpen, err)

	// 5. Wait for ResetTimeout
	time.Sleep(250 * time.Millisecond)
	// state should be Open until next Call
	assert.Equal(t, Open, cb.state)

	// 6. Next call - transitions to HalfOpen
	// We use cb.Call to trigger the state check
	_ = cb.Call(func() error { return nil })
	assert.Equal(t, Closed, cb.state) // Should transition HalfOpen -> Closed if successful
}

func TestCircuitBreaker_HalfOpenToOpen(t *testing.T) {
	settings := CircuitBreakerSettings{
		MaxFailures:  1,
		CallTimeout:  100 * time.Millisecond,
		ResetTimeout: 100 * time.Millisecond,
	}
	cb := NewCircuitBreaker(settings)

	_ = cb.Call(func() error { return errors.New("fail") })
	assert.Equal(t, Open, cb.state)

	time.Sleep(150 * time.Millisecond)

	// Fail in HalfOpen -> should go back to Open
	_ = cb.Call(func() error { return errors.New("fail") })
	assert.Equal(t, Open, cb.state)
}

func TestCircuitBreaker_CallTimeout(t *testing.T) {
	settings := CircuitBreakerSettings{
		MaxFailures:  1,
		CallTimeout:  50 * time.Millisecond,
		ResetTimeout: 100 * time.Millisecond,
	}
	cb := NewCircuitBreaker(settings)

	err := cb.Call(func() error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	assert.Equal(t, ErrCallTimeout, err)
	assert.Equal(t, Open, cb.state)
}

func TestCircuitBreaker_AskWrapper(t *testing.T) {
	settings := CircuitBreakerSettings{
		MaxFailures:  1,
		CallTimeout:  100 * time.Millisecond,
		ResetTimeout: 100 * time.Millisecond,
	}
	cb := NewCircuitBreaker(settings)
	ref := &FunctionalMockRef{PathURI: "/user/target"}

	mockAsk := func(ctx context.Context, r Ref, msg any) (any, error) {
		return "pong", nil
	}

	res, err := cb.Ask(context.Background(), ref, "ping", mockAsk)
	assert.NoError(t, err)
	assert.Equal(t, "pong", res)
}
