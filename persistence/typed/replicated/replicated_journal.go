/*
 * replicated_journal.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package replicated

import (
	"context"

	"github.com/sopranoworks/gekka/persistence"
)

// ReplicatedJournal wraps a Journal and tags each persisted event with the
// origin ReplicaId. This enables querying events by replica origin.
type ReplicatedJournal struct {
	inner     persistence.Journal
	replicaId ReplicaId
}

// NewReplicatedJournal creates a journal adapter that tags all writes with the
// given replica ID.
func NewReplicatedJournal(inner persistence.Journal, replicaId ReplicaId) *ReplicatedJournal {
	return &ReplicatedJournal{inner: inner, replicaId: replicaId}
}

func (j *ReplicatedJournal) ReplayMessages(ctx context.Context, persistenceId string, fromSequenceNr, toSequenceNr uint64, max uint64, callback func(persistence.PersistentRepr)) error {
	return j.inner.ReplayMessages(ctx, persistenceId, fromSequenceNr, toSequenceNr, max, callback)
}

func (j *ReplicatedJournal) ReadHighestSequenceNr(ctx context.Context, persistenceId string, fromSequenceNr uint64) (uint64, error) {
	return j.inner.ReadHighestSequenceNr(ctx, persistenceId, fromSequenceNr)
}

func (j *ReplicatedJournal) AsyncWriteMessages(ctx context.Context, messages []persistence.PersistentRepr) error {
	tagged := make([]persistence.PersistentRepr, len(messages))
	for i, m := range messages {
		tagged[i] = m
		tagged[i].Tags = appendIfMissing(m.Tags, string(j.replicaId))
	}
	return j.inner.AsyncWriteMessages(ctx, tagged)
}

func (j *ReplicatedJournal) AsyncDeleteMessagesTo(ctx context.Context, persistenceId string, toSequenceNr uint64) error {
	return j.inner.AsyncDeleteMessagesTo(ctx, persistenceId, toSequenceNr)
}

// EventsByReplicaId replays events for the given persistence ID that were
// tagged with the specified replica origin.
func (j *ReplicatedJournal) EventsByReplicaId(ctx context.Context, persistenceId string, replicaId ReplicaId, fromSeqNr uint64, callback func(persistence.PersistentRepr)) error {
	return j.inner.ReplayMessages(ctx, persistenceId, fromSeqNr, ^uint64(0), 0, func(repr persistence.PersistentRepr) {
		for _, tag := range repr.Tags {
			if tag == string(replicaId) {
				callback(repr)
				return
			}
		}
	})
}

func appendIfMissing(slice []string, s string) []string {
	for _, existing := range slice {
		if existing == s {
			return slice
		}
	}
	return append(slice, s)
}
