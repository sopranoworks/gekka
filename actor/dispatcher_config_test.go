/*
 * dispatcher_config_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResolveDispatcherKey_Default(t *testing.T) {
	// "default-dispatcher" is pre-registered.
	d := ResolveDispatcherKey("default-dispatcher")
	assert.Equal(t, DispatcherDefault, d)
}

func TestResolveDispatcherKey_Pinned(t *testing.T) {
	d := ResolveDispatcherKey("pinned-dispatcher")
	assert.Equal(t, DispatcherPinned, d)
}

func TestResolveDispatcherKey_CustomRegistered(t *testing.T) {
	RegisterDispatcherConfig("my-pinned", DispatcherConfig{
		Type:       "pinned-dispatcher",
		Throughput: 1,
	})
	d := ResolveDispatcherKey("my-pinned")
	assert.Equal(t, DispatcherPinned, d)
}

func TestResolveDispatcherKey_CallingThread(t *testing.T) {
	RegisterDispatcherConfig("test-calling-thread", DispatcherConfig{
		Type:       "calling-thread-dispatcher",
		Throughput: 1,
	})
	d := ResolveDispatcherKey("test-calling-thread")
	assert.Equal(t, DispatcherCallingThread, d)
}

func TestResolveDispatcherKey_EmptyReturnsDefault(t *testing.T) {
	d := ResolveDispatcherKey("")
	assert.Equal(t, DispatcherDefault, d)
}

func TestResolveDispatcherKey_UnknownReturnsDefault(t *testing.T) {
	d := ResolveDispatcherKey("non-existent-dispatcher")
	assert.Equal(t, DispatcherDefault, d)
}

func TestDispatcherTypeFromString(t *testing.T) {
	tests := []struct {
		input    string
		expected DispatcherType
	}{
		{"default-dispatcher", DispatcherDefault},
		{"pinned-dispatcher", DispatcherPinned},
		{"calling-thread-dispatcher", DispatcherCallingThread},
		{"Pinned-Dispatcher", DispatcherPinned},
		{"CALLING-THREAD-dispatcher", DispatcherCallingThread},
		{"unknown", DispatcherDefault},
		{"", DispatcherDefault},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			assert.Equal(t, tc.expected, dispatcherTypeFromString(tc.input))
		})
	}
}

func TestPropsDispatcherKey(t *testing.T) {
	RegisterDispatcherConfig("test-pool-pinned", DispatcherConfig{
		Type: "pinned-dispatcher",
	})

	props := Props{
		New:           func() Actor { return nil },
		DispatcherKey: "test-pool-pinned",
	}

	// DispatcherKey should resolve to pinned.
	resolved := ResolveDispatcherKey(props.DispatcherKey)
	assert.Equal(t, DispatcherPinned, resolved)
}
