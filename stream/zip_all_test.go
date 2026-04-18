/*
 * zip_all_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"testing"

	"github.com/sopranoworks/gekka/stream"
)

func TestZipAll_PadsShortRight(t *testing.T) {
	result, err := stream.RunWith(
		stream.ZipAll(
			stream.FromSlice([]int{1, 2, 3}),
			stream.FromSlice([]int{10}),
			0, 0,
		),
		stream.Collect[stream.Pair[int, int]](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []stream.Pair[int, int]{
		{First: 1, Second: 10},
		{First: 2, Second: 0},
		{First: 3, Second: 0},
	}
	if len(result) != len(expected) {
		t.Fatalf("expected %d pairs, got %d: %v", len(expected), len(result), result)
	}
	for i, p := range result {
		if p != expected[i] {
			t.Errorf("pair[%d]: expected %v, got %v", i, expected[i], p)
		}
	}
}

func TestZipAll_PadsShortLeft(t *testing.T) {
	result, err := stream.RunWith(
		stream.ZipAll(
			stream.FromSlice([]int{1}),
			stream.FromSlice([]int{10, 20, 30}),
			0, 0,
		),
		stream.Collect[stream.Pair[int, int]](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []stream.Pair[int, int]{
		{First: 1, Second: 10},
		{First: 0, Second: 20},
		{First: 0, Second: 30},
	}
	if len(result) != len(expected) {
		t.Fatalf("expected %d pairs, got %d: %v", len(expected), len(result), result)
	}
	for i, p := range result {
		if p != expected[i] {
			t.Errorf("pair[%d]: expected %v, got %v", i, expected[i], p)
		}
	}
}

func TestZipAll_EqualLength(t *testing.T) {
	result, err := stream.RunWith(
		stream.ZipAll(
			stream.FromSlice([]int{1, 2}),
			stream.FromSlice([]int{10, 20}),
			0, 0,
		),
		stream.Collect[stream.Pair[int, int]](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []stream.Pair[int, int]{
		{First: 1, Second: 10},
		{First: 2, Second: 20},
	}
	if len(result) != len(expected) {
		t.Fatalf("expected %d pairs, got %d: %v", len(expected), len(result), result)
	}
	for i, p := range result {
		if p != expected[i] {
			t.Errorf("pair[%d]: expected %v, got %v", i, expected[i], p)
		}
	}
}

func TestZipAllWith_CustomCombine(t *testing.T) {
	result, err := stream.RunWith(
		stream.ZipAllWith(
			stream.FromSlice([]int{1, 2}),
			stream.FromSlice([]int{10, 20, 30}),
			0, 0,
			func(a, b int) int { return a + b },
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []int{11, 22, 30}
	if len(result) != len(expected) {
		t.Fatalf("expected %d elements, got %d: %v", len(expected), len(result), result)
	}
	for i, v := range result {
		if v != expected[i] {
			t.Errorf("elem[%d]: expected %d, got %d", i, expected[i], v)
		}
	}
}
