/*
 * journal.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package redisstore

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
	"github.com/sopranoworks/gekka/persistence"
)

// RedisJournal implements persistence.Journal using Redis Streams.
//
// Each persistence ID maps to its own stream:
//
//	{keyPrefix}j:{persistenceId}
//
// Stream entries carry the fields: seqnr, payload (JSON), manifest, sender.
type RedisJournal struct {
	client *redis.Client
	prefix string
	codec  PayloadCodec
}

// NewRedisJournal creates a RedisJournal.
func NewRedisJournal(client *redis.Client, prefix string, codec PayloadCodec) *RedisJournal {
	return &RedisJournal{client: client, prefix: prefix, codec: codec}
}

func (j *RedisJournal) streamKey(pid string) string {
	return j.prefix + "j:" + pid
}

// AsyncWriteMessages appends each message to the persistence-ID stream via XADD.
func (j *RedisJournal) AsyncWriteMessages(ctx context.Context, messages []persistence.PersistentRepr) error {
	for _, msg := range messages {
		manifest, data, err := j.codec.Encode(msg.Payload)
		if err != nil {
			return fmt.Errorf("redisjournal: encode %s/%d: %w", msg.PersistenceID, msg.SequenceNr, err)
		}
		args := &redis.XAddArgs{
			Stream: j.streamKey(msg.PersistenceID),
			ID:     "*", // auto-generate stream entry ID
			Values: map[string]any{
				"seqnr":    strconv.FormatUint(msg.SequenceNr, 10),
				"payload":  string(data),
				"manifest": manifest,
				"sender":   msg.SenderPath,
			},
		}
		if err := j.client.XAdd(ctx, args).Err(); err != nil {
			return fmt.Errorf("redisjournal: XADD %s/%d: %w", msg.PersistenceID, msg.SequenceNr, err)
		}
	}
	return nil
}

// ReadHighestSequenceNr returns the highest sequence number stored for pid
// that is >= fromSequenceNr.  Returns 0 if no such entry exists.
func (j *RedisJournal) ReadHighestSequenceNr(ctx context.Context, pid string, fromSequenceNr uint64) (uint64, error) {
	// XREVRANGE returns entries in descending stream-ID order; the first entry
	// has the highest sequence number because events are always appended in order.
	entries, err := j.client.XRevRangeN(ctx, j.streamKey(pid), "+", "-", 1).Result()
	if err != nil {
		return 0, fmt.Errorf("redisjournal: XREVRANGE for %q: %w", pid, err)
	}
	if len(entries) == 0 {
		return 0, nil
	}
	seqNr, err := parseSeqNr(entries[0])
	if err != nil {
		return 0, fmt.Errorf("redisjournal: parse seqnr for %q: %w", pid, err)
	}
	if seqNr < fromSequenceNr {
		return 0, nil
	}
	return seqNr, nil
}

// ReplayMessages reads all events in the stream for pid and invokes callback
// for each event whose sequence number falls within [fromSequenceNr, toSequenceNr].
// At most max events are delivered (0 means unlimited).
func (j *RedisJournal) ReplayMessages(
	ctx context.Context,
	pid string,
	fromSequenceNr, toSequenceNr uint64,
	max uint64,
	callback func(persistence.PersistentRepr),
) error {
	entries, err := j.client.XRange(ctx, j.streamKey(pid), "-", "+").Result()
	if err != nil {
		return fmt.Errorf("redisjournal: XRANGE for %q: %w", pid, err)
	}

	var delivered uint64
	for _, entry := range entries {
		seqNr, err := parseSeqNr(entry)
		if err != nil {
			return fmt.Errorf("redisjournal: parse seqnr in replay for %q: %w", pid, err)
		}
		if seqNr < fromSequenceNr || seqNr > toSequenceNr {
			continue
		}

		manifest, _ := entry.Values["manifest"].(string)
		rawPayload, _ := entry.Values["payload"].(string)
		sender, _ := entry.Values["sender"].(string)

		payload, err := j.codec.Decode(manifest, []byte(rawPayload))
		if err != nil {
			return fmt.Errorf("redisjournal: decode %q/%d (manifest=%q): %w", pid, seqNr, manifest, err)
		}

		callback(persistence.PersistentRepr{
			PersistenceID: pid,
			SequenceNr:    seqNr,
			Payload:       payload,
			SenderPath:    sender,
		})

		delivered++
		if max > 0 && delivered >= max {
			break
		}
	}
	return nil
}

// AsyncDeleteMessagesTo deletes all stream entries for pid with seqNr <= toSequenceNr.
func (j *RedisJournal) AsyncDeleteMessagesTo(ctx context.Context, pid string, toSequenceNr uint64) error {
	entries, err := j.client.XRange(ctx, j.streamKey(pid), "-", "+").Result()
	if err != nil {
		return fmt.Errorf("redisjournal: XRANGE for deletion of %q: %w", pid, err)
	}

	var ids []string
	for _, entry := range entries {
		seqNr, err := parseSeqNr(entry)
		if err != nil {
			return fmt.Errorf("redisjournal: parse seqnr for delete %q: %w", pid, err)
		}
		if seqNr <= toSequenceNr {
			ids = append(ids, entry.ID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	if err := j.client.XDel(ctx, j.streamKey(pid), ids...).Err(); err != nil {
		return fmt.Errorf("redisjournal: XDEL for %q: %w", pid, err)
	}
	return nil
}

// Ensure RedisJournal satisfies Journal at compile time.
var _ persistence.Journal = (*RedisJournal)(nil)

// parseSeqNr extracts the "seqnr" field from a stream entry.
func parseSeqNr(entry redis.XMessage) (uint64, error) {
	raw, ok := entry.Values["seqnr"].(string)
	if !ok {
		return 0, fmt.Errorf("seqnr field missing or wrong type in entry %s", entry.ID)
	}
	return strconv.ParseUint(raw, 10, 64)
}
