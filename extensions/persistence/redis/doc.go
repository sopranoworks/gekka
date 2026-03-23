/*
 * doc.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package redisstore provides a Redis-backed implementation of the
// persistence.Journal and persistence.SnapshotStore interfaces for the
// gekka actor framework.
//
// This extension module is intentionally separate from the core gekka module
// so that applications that do not use Redis incur zero dependency cost.
//
// # Journal
//
// Events are stored in a Redis Stream (XADD) keyed by persistence ID:
//
//	{keyPrefix}j:{persistenceId}
//
// Each stream entry records the sequence number, JSON-encoded payload, type
// manifest, and optional sender path.  Deletion (AsyncDeleteMessagesTo) reads
// the stream and issues XDEL for matching entry IDs.
//
// # SnapshotStore
//
// Snapshots are stored as individual JSON strings:
//
//	{keyPrefix}s:{persistenceId}:{seqNr}
//
// A sorted set per persistence ID acts as a sequence-number index:
//
//	{keyPrefix}si:{persistenceId}   (score = seqNr)
//
// LoadSnapshot queries the index with ZRevRangeByScore and applies
// timestamp criteria in Go after retrieving candidate keys.
//
// # Quick start
//
// Blank-import the package in your application to activate the "redis"
// provider in the persistence registry:
//
//	import _ "github.com/sopranoworks/gekka-extensions-persistence-redis"
//
// Then configure via HOCON:
//
//	gekka.persistence.journal.plugin        = "redis"
//	gekka.persistence.snapshot-store.plugin = "redis"
//	gekka.persistence.journal.settings {
//	  address    = "localhost:6379"
//	  password   = ""
//	  db         = 0
//	  key-prefix = "myapp:"
//	}
//
// Register your domain types with DefaultCodec so that replayed events are
// returned as the correct Go types rather than json.RawMessage:
//
//	redisstore.DefaultCodec.Register(OrderPlaced{})
//	redisstore.DefaultCodec.Register(OrderShipped{})
package redisstore
