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

func TestGetDispatcherConfig(t *testing.T) {
	cfg, ok := GetDispatcherConfig("default-dispatcher")
	assert.True(t, ok)
	assert.Equal(t, "default-dispatcher", cfg.Type)

	_, ok = GetDispatcherConfig("nonexistent")
	assert.False(t, ok)

	_, ok = GetDispatcherConfig("")
	assert.False(t, ok)
}

func TestDispatcherConfig_MailboxType(t *testing.T) {
	RegisterDispatcherConfig("ctrl-dispatcher", DispatcherConfig{
		Type:        "default-dispatcher",
		MailboxType: "org.apache.pekko.dispatch.UnboundedControlAwareMailbox",
	})

	cfg, ok := GetDispatcherConfig("ctrl-dispatcher")
	assert.True(t, ok)
	assert.Equal(t, "org.apache.pekko.dispatch.UnboundedControlAwareMailbox", cfg.MailboxType)

	// ResolveMailbox should return a non-nil factory (registered via init())
	mf := cfg.ResolveMailbox()
	assert.NotNil(t, mf, "registered FQCN should resolve to a MailboxFactory")
}

func TestDispatcherConfig_UnknownMailboxType(t *testing.T) {
	RegisterDispatcherConfig("unknown-mb-dispatcher", DispatcherConfig{
		Type:        "default-dispatcher",
		MailboxType: "com.example.NonExistentMailbox",
	})

	cfg, _ := GetDispatcherConfig("unknown-mb-dispatcher")
	mf := cfg.ResolveMailbox()
	assert.Nil(t, mf, "unregistered FQCN should return nil")
}

func TestLookupMailboxType(t *testing.T) {
	// Pekko FQCN should be registered
	mf := LookupMailboxType("org.apache.pekko.dispatch.UnboundedControlAwareMailbox")
	assert.NotNil(t, mf)

	// Akka FQCN should also be registered
	mf = LookupMailboxType("akka.dispatch.UnboundedControlAwareMailbox")
	assert.NotNil(t, mf)

	// Unknown should return nil
	mf = LookupMailboxType("com.example.Nothing")
	assert.Nil(t, mf)
}
