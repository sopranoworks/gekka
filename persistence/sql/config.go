/*
 * config.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sqlstore

// Config holds table-name configuration shared by persistence components.
type Config struct {
	// JournalTable is the name of the events table.
	// Defaults to "journal".
	JournalTable string

	// SnapshotTable is the name of the snapshots table.
	// Defaults to "snapshots".
	SnapshotTable string

	// StateTable is the name of the durable state table.
	// Defaults to "durable_state".
	StateTable string
}

// DefaultConfig returns a Config with the default table names.
func DefaultConfig() Config {
	return Config{
		JournalTable:  "journal",
		SnapshotTable: "snapshots",
		StateTable:    "durable_state",
	}
}

func (c Config) journalTable() string {
	if c.JournalTable == "" {
		return "journal"
	}
	return c.JournalTable
}

func (c Config) snapshotTable() string {
	if c.SnapshotTable == "" {
		return "snapshots"
	}
	return c.SnapshotTable
}

func (c Config) stateTable() string {
	if c.StateTable == "" {
		return "durable_state"
	}
	return c.StateTable
}
