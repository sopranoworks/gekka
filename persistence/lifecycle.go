/*
 * lifecycle.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import "context"

// Lifecycle is an optional interface that Journal and SnapshotStore plugins
// may implement to participate in the ActorSystem lifecycle.
//
// When the ActorSystem starts it checks whether the provisioned Journal and
// SnapshotStore implement Lifecycle and, if so, calls Start with the system
// lifetime context. Plugins should use that context to cancel background
// goroutines and to release connections when the system shuts down.
//
// Example — a Spanner journal that closes its client on shutdown:
//
//	func (j *SpannerJournal) Start(ctx context.Context) error {
//	    go func() {
//	        <-ctx.Done()
//	        j.client.Close()
//	    }()
//	    return nil
//	}
type Lifecycle interface {
	// Start is called once, before any Journal or SnapshotStore operations,
	// with the context that is cancelled when the ActorSystem terminates.
	// A non-nil error aborts ActorSystem creation.
	Start(ctx context.Context) error
}

// StartLifecycle calls Start(ctx) on p if it implements Lifecycle.
// It is called by the ActorSystem immediately after a plugin is provisioned.
func StartLifecycle(ctx context.Context, p any) error {
	if l, ok := p.(Lifecycle); ok {
		return l.Start(ctx)
	}
	return nil
}
