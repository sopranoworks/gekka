/*
 * zip_latest_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"errors"
	"testing"

	"github.com/sopranoworks/gekka/stream"
)

func TestZipLatest_BothSidesUpdate(t *testing.T) {
	result, err := stream.RunWith(
		stream.ZipLatest(
			stream.FromSlice([]int{1, 2, 3}),
			stream.FromSlice([]int{10, 20}),
		),
		stream.Collect[stream.Pair[int, int]](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) < 2 {
		t.Fatalf("expected at least 2 pairs, got %d", len(result))
	}
	last := result[len(result)-1]
	if last.First != 3 || last.Second != 20 {
		t.Fatalf("expected last pair {3, 20}, got %v", last)
	}
}

func TestZipLatest_DropsBeforeBothReady(t *testing.T) {
	result, err := stream.RunWith(
		stream.ZipLatest(
			stream.FromSlice([]int{1}),
			stream.Empty[int](),
		),
		stream.Collect[stream.Pair[int, int]](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 pairs (right never ready), got %d: %v", len(result), result)
	}
}

func TestZipLatest_CompletesWhenBothDone(t *testing.T) {
	result, err := stream.RunWith(
		stream.ZipLatest(
			stream.FromSlice([]int{1}),
			stream.FromSlice([]int{10}),
		),
		stream.Collect[stream.Pair[int, int]](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 pair, got %d", len(result))
	}
	if result[0] != (stream.Pair[int, int]{First: 1, Second: 10}) {
		t.Fatalf("got %v, want {1, 10}", result[0])
	}
}

func TestZipLatest_PropagatesError(t *testing.T) {
	boom := errors.New("boom")
	_, err := stream.RunWith(
		stream.ZipLatest(
			stream.Failed[int](boom),
			stream.FromSlice([]int{1}),
		),
		stream.Collect[stream.Pair[int, int]](),
		stream.ActorMaterializer{},
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected boom, got %v", err)
	}
}

func TestCombineLatest_IsAlias(t *testing.T) {
	result, err := stream.RunWith(
		stream.CombineLatest(
			stream.FromSlice([]int{1}),
			stream.FromSlice([]int{10}),
		),
		stream.Collect[stream.Pair[int, int]](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 || result[0] != (stream.Pair[int, int]{First: 1, Second: 10}) {
		t.Fatalf("unexpected result: %v", result)
	}
}
