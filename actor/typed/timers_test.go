/*
 * typed_timers_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/stretchr/testify/assert"
)

func TestTimerScheduler_SingleTimer(t *testing.T) {
	var count atomic.Int32
	self := &actor.FunctionalMockRef{
		Handler: func(m any) {
			count.Add(1)
		},
	}

	timers := newTimerScheduler[string](self)
	timers.StartSingleTimer("key1", "msg1", 100*time.Millisecond)

	assert.True(t, timers.IsTimerActive("key1"))

	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, int32(1), count.Load())
	assert.False(t, timers.IsTimerActive("key1"))
}

func TestTimerScheduler_PeriodicTimer(t *testing.T) {
	var count atomic.Int32
	self := &actor.FunctionalMockRef{
		Handler: func(m any) {
			count.Add(1)
		},
	}

	timers := newTimerScheduler[string](self)
	timers.StartPeriodicTimer("key1", "msg1", 100*time.Millisecond)

	time.Sleep(350 * time.Millisecond) // Should fire ~3 times (0.1, 0.2, 0.3)
	assert.GreaterOrEqual(t, count.Load(), int32(3))

	timers.Cancel("key1")
	lastCount := count.Load()
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, lastCount, count.Load())
}

func TestTimerScheduler_Cancel(t *testing.T) {
	self := &typedMockRef{}
	timers := newTimerScheduler[string](self)
	
	timers.StartSingleTimer("key1", "msg1", 1*time.Second)
	assert.True(t, timers.IsTimerActive("key1"))
	
	timers.Cancel("key1")
	assert.False(t, timers.IsTimerActive("key1"))
}
