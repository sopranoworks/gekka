/*
 * doc.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package spannerstore will provide a Cloud Spanner-backed implementation of
// the gekka persistence.Journal and persistence.SnapshotStore interfaces.
//
// This extension module is intentionally separate from the core gekka module
// so that applications that do not use Cloud Spanner incur zero dependency
// cost from the spanner SDK.
//
// The source code currently lives in the core repository at
// persistence/spanner/ and will be migrated here in v0.13.1-dev.
//
// Usage (future):
//
//	import spannerstore "github.com/sopranoworks/gekka-extensions-persistence-spanner"
//
//	client, _ := spanner.NewClient(ctx, db)
//	journal  := spannerstore.NewSpannerJournal(client, codec)
//	snapshot := spannerstore.NewSpannerSnapshotStore(client, codec)
package spannerstore
