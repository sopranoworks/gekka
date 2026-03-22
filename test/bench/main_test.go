/*
 * main_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package bench contains performance benchmarks for the Gekka framework.
//
// # Running benchmarks
//
//	go test -bench=. -benchmem ./test/bench/
//
// # CPU + heap profiling
//
//	go test -bench=. -benchmem -cpuprofile=cpu.prof -memprofile=mem.prof ./test/bench/
//	go tool pprof -http=:8080 cpu.prof
//
// # Flamegraphs (requires go-torch or pprof built-in)
//
//	go tool pprof -http=:8080 -flame cpu.prof
package bench

import (
	"flag"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
)

var (
	cpuProfile = flag.String("bench.cpuprofile", "", "write CPU profile to file")
	memProfile = flag.String("bench.memprofile", "", "write heap profile to file")
)

// TestMain wires CPU and heap profiling around the benchmark suite.
//
// Usage:
//
//	go test -bench=. -benchmem ./test/bench/ \
//	    -args -bench.cpuprofile=cpu.prof -bench.memprofile=mem.prof
func TestMain(m *testing.M) {
	flag.Parse()

	// ── CPU profile ──────────────────────────────────────────────────────────
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatalf("bench: could not create CPU profile %q: %v", *cpuProfile, err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("bench: could not start CPU profile: %v", err)
		}
		defer func() {
			pprof.StopCPUProfile()
			_ = f.Close()
			log.Printf("bench: CPU profile written to %s", *cpuProfile)
		}()
	}

	code := m.Run()

	// ── Heap (memory) profile ─────────────────────────────────────────────────
	if *memProfile != "" {
		f, err := os.Create(*memProfile)
		if err != nil {
			log.Fatalf("bench: could not create memory profile %q: %v", *memProfile, err)
		}
		runtime.GC() // flush any finalizers before sampling
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatalf("bench: could not write memory profile: %v", err)
		}
		_ = f.Close()
		log.Printf("bench: memory profile written to %s", *memProfile)
	}

	os.Exit(code)
}
