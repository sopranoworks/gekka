/*
 * typed_stash_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"testing"

	"github.com/sopranoworks/gekka/actor"
	"github.com/stretchr/testify/assert"
)

func TestStashBuffer_Basic(t *testing.T) {
	self := &typedMockRef{path: "/user/test"}
	stash := newStashBuffer[string](self, 10)

	assert.Equal(t, 0, len(stash.Messages))

	// Note: Stash() is currently a placeholder in typed_stash.go
	err := stash.Stash()
	assert.NoError(t, err)

	stash.Clear()
	assert.Equal(t, 0, len(stash.Messages))
}

func TestStashBuffer_Capacity(t *testing.T) {
	self := &typedMockRef{path: "/user/test"}
	stash := newStashBuffer[string](self, 2)
	
	// Manually fill
	stash.Messages = append(stash.Messages, actor.Envelope{Payload: "1"})
	stash.Messages = append(stash.Messages, actor.Envelope{Payload: "2"})
	
	err := stash.Stash()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "capacity exceeded")
}
