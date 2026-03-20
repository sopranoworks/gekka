/*
 * metrics_collector.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"runtime"
	"sync"
	"time"
)

// MailboxLengthProvider is implemented by the actor system to provide
// visibility into local mailbox depths for pressure calculation.
type MailboxLengthProvider interface {
	GetMailboxLengths() map[string]int
}

var (
	mailboxProvider   MailboxLengthProvider
	mailboxProviderMu sync.RWMutex
)

// SetMailboxLengthProvider installs the provider used by the MetricsCollector.
func SetMailboxLengthProvider(p MailboxLengthProvider) {
	mailboxProviderMu.Lock()
	defer mailboxProviderMu.Unlock()
	mailboxProvider = p
}

// NodePressure represents the normalized load status of a node.
type NodePressure struct {
	CPUUsage    float64 `json:"cpu_usage"`    // 0.0 to 1.0
	HeapMemory  uint64  `json:"heap_memory"`  // bytes
	MailboxSize int     `json:"mailbox_size"` // total pending messages
	Score       float64 `json:"score"`        // normalized pressure score (0.0 to 1.0)
}

// MetricsCollector gathers local node metrics and calculates a pressure score.
type MetricsCollector struct {
	lastSample  time.Time
}

// NewMetricsCollector creates a new MetricsCollector.
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		lastSample: time.Now(),
	}
}

// Collect returns a snapshot of the current node pressure.
func (c *MetricsCollector) Collect() NodePressure {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	cpu := c.calculateCPUUsage()
	mailboxSize := c.collectMailboxSize()

	return NodePressure{
		CPUUsage:    cpu,
		HeapMemory:  ms.HeapAlloc,
		MailboxSize: mailboxSize,
		Score:       c.calculateScore(cpu, ms.HeapAlloc, mailboxSize),
	}
}

func (c *MetricsCollector) calculateCPUUsage() float64 {
	// Simplified CPU calculation using goroutine count as a proxy
	// since cross-platform raw CPU % is heavy without gopsutil.
	// A high number of goroutines relative to CPU cores indicates pressure.
	numCPU := runtime.NumCPU()
	numG := runtime.NumGoroutine()

	usage := float64(numG) / float64(numCPU*100)
	if usage > 1.0 {
		usage = 1.0
	}
	return usage
}

func (c *MetricsCollector) collectMailboxSize() int {
	mailboxProviderMu.RLock()
	p := mailboxProvider
	mailboxProviderMu.RUnlock()

	if p == nil {
		return 0
	}

	total := 0
	for _, size := range p.GetMailboxLengths() {
		total += size
	}
	return total
}

func (c *MetricsCollector) calculateScore(cpu float64, heap uint64, mailbox int) float64 {
	// Weighted average of normalized metrics.
	// 40% CPU, 30% Heap, 30% Mailbox
	
	// Normalize Heap (assume 1GB is 'full' for scoring purposes if not configured)
	const maxHeap = 1024 * 1024 * 1024
	heapNorm := float64(heap) / maxHeap
	if heapNorm > 1.0 {
		heapNorm = 1.0
	}

	// Normalize Mailbox (assume 10000 total pending messages is 'full')
	const maxMailbox = 10000
	mailboxNorm := float64(mailbox) / maxMailbox
	if mailboxNorm > 1.0 {
		mailboxNorm = 1.0
	}

	score := (cpu * 0.4) + (heapNorm * 0.3) + (mailboxNorm * 0.3)
	return score
}
