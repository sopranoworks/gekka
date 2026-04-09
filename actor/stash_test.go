/*
 * stash_test.go
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

func TestStashBuffer_StashAppends(t *testing.T) {
	var delivered []string
	s := NewStashBuffer[string](10, func(msg string) { delivered = append(delivered, msg) })

	assert.NoError(t, s.Stash("a"))
	assert.NoError(t, s.Stash("b"))
	assert.NoError(t, s.Stash("c"))

	assert.Equal(t, 3, s.Size())
	assert.False(t, s.IsFull())
	assert.Empty(t, delivered, "redeliver must not fire until UnstashAll")
}

func TestStashBuffer_UnstashAllFIFOOrder(t *testing.T) {
	var delivered []int
	s := NewStashBuffer[int](10, func(msg int) { delivered = append(delivered, msg) })

	for i := 1; i <= 5; i++ {
		assert.NoError(t, s.Stash(i))
	}

	assert.NoError(t, s.UnstashAll())
	assert.Equal(t, []int{1, 2, 3, 4, 5}, delivered)
	assert.Equal(t, 0, s.Size(), "buffer must be empty after UnstashAll")
}

func TestStashBuffer_CapacityExceeded(t *testing.T) {
	s := NewStashBuffer[string](2, func(string) {})

	assert.NoError(t, s.Stash("a"))
	assert.NoError(t, s.Stash("b"))
	assert.True(t, s.IsFull())

	err := s.Stash("c")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "capacity exceeded")
	assert.Equal(t, 2, s.Size(), "failed Stash must not modify buffer")
}

func TestStashBuffer_ClearDropsMessages(t *testing.T) {
	var delivered []string
	s := NewStashBuffer[string](10, func(msg string) { delivered = append(delivered, msg) })

	_ = s.Stash("a")
	_ = s.Stash("b")
	s.Clear()

	assert.Equal(t, 0, s.Size())
	assert.NoError(t, s.UnstashAll())
	assert.Empty(t, delivered, "Clear must drop messages without delivering them")
}

func TestStashBuffer_UnstashAllOnEmpty(t *testing.T) {
	s := NewStashBuffer[string](10, func(string) { t.Fatal("must not be called") })
	assert.NoError(t, s.UnstashAll())
}

func TestStashBuffer_ReuseAfterUnstash(t *testing.T) {
	var delivered []int
	s := NewStashBuffer[int](10, func(msg int) { delivered = append(delivered, msg) })

	_ = s.Stash(1)
	_ = s.Stash(2)
	_ = s.UnstashAll()

	_ = s.Stash(3)
	_ = s.Stash(4)
	_ = s.UnstashAll()

	assert.Equal(t, []int{1, 2, 3, 4}, delivered)
}
