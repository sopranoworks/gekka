/*
 * persist_async.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import "context"

// persistAsyncAck is an internal message delivered to the actor's mailbox
// after a background journal write batch completes. PersistentFSM.Receive
// intercepts this type before user dispatch.
type persistAsyncAck struct {
	handlers []func() // pre-bound closures: calls handler(event)
	err      error
}

// asyncWriteTask is enqueued by PersistAsync / PersistAllAsync.
type asyncWriteTask struct {
	reprs    []PersistentRepr
	handlers []func() // one per repr, in matching order
}

// asyncWriteLoop runs in a dedicated goroutine started by the first PersistAsync
// call. It drains tasks from ch, writes each batch to the journal
// sequentially, then delivers a persistAsyncAck to the actor's mailbox.
// The goroutine exits when ch is closed (actor PostStop).
func asyncWriteLoop(j Journal, ch <-chan asyncWriteTask, mailbox chan<- any) {
	for task := range ch {
		err := j.AsyncWriteMessages(context.Background(), task.reprs)
		mailbox <- persistAsyncAck{handlers: task.handlers, err: err}
	}
}
