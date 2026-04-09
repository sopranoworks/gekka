/*
 * source_constructors_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"errors"
	"testing"
	"time"
)

func TestSingle(t *testing.T) {
	src := Single(42)
	got, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != 42 {
		t.Fatalf("want [42], got %v", got)
	}
}

func TestEmpty(t *testing.T) {
	src := Empty[int]()
	got, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("want [], got %v", got)
	}
}

func TestRange(t *testing.T) {
	src := Range(1, 5)
	got, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatal(err)
	}
	want := []int{1, 2, 3, 4, 5}
	if len(got) != len(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
	for i, v := range want {
		if got[i] != v {
			t.Fatalf("want %v, got %v", want, got)
		}
	}
}

func TestRange_Empty(t *testing.T) {
	src := Range(5, 3)
	got, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("want [], got %v", got)
	}
}

func TestTick(t *testing.T) {
	src := Tick[int](50*time.Millisecond, 1).Take(3)
	start := time.Now()
	got, err := RunWith(src, Collect[int](), SyncMaterializer{})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 elements, got %d", len(got))
	}
	for i, v := range got {
		if v != 1 {
			t.Fatalf("element %d: want 1, got %d", i, v)
		}
	}
	// First element is immediate, then 2 intervals of 50ms = ~100ms minimum
	if elapsed < 80*time.Millisecond {
		t.Fatalf("too fast: %v (expected >= 80ms)", elapsed)
	}
}

func TestFromFuture_Success(t *testing.T) {
	src := FromFuture(func() (string, error) {
		return "hello", nil
	})
	got, err := RunWith(src, Collect[string](), SyncMaterializer{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "hello" {
		t.Fatalf("want [hello], got %v", got)
	}
}

func TestFromFuture_Error(t *testing.T) {
	boom := errors.New("boom")
	src := FromFuture(func() (string, error) {
		return "", boom
	})
	_, err := RunWith(src, Collect[string](), SyncMaterializer{})
	if !errors.Is(err, boom) {
		t.Fatalf("want boom error, got %v", err)
	}
}
