/*
 * source.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package kafka

import (
	"context"
	"sync"

	kafkaconn "github.com/segmentio/kafka-go"
	"github.com/sopranoworks/gekka/stream"
)

// SourceConfig configures the Kafka consumer source.
type SourceConfig struct {
	Brokers  []string
	Topic    string
	GroupID  string
	MinBytes int             // min bytes to fetch per call (0 = 1)
	MaxBytes int             // max bytes per call (0 = 10 MiB)
	Ctx      context.Context // context for cancellation; nil defaults to Background
}

// Source returns a stream.Source that emits kafka-go Messages from a Kafka topic.
// Commit is automatic after each element is emitted downstream.
// Cancel Ctx to stop the source cleanly.
func Source(cfg SourceConfig) stream.Source[kafkaconn.Message, stream.NotUsed] {
	ctx := cfg.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	minBytes := cfg.MinBytes
	if minBytes == 0 {
		minBytes = 1
	}
	maxBytes := cfg.MaxBytes
	if maxBytes == 0 {
		maxBytes = 10 * 1024 * 1024
	}

	var (
		r    *kafkaconn.Reader
		once sync.Once
	)

	return stream.FromIteratorFunc(func() (kafkaconn.Message, bool, error) {
		once.Do(func() {
			r = kafkaconn.NewReader(kafkaconn.ReaderConfig{
				Brokers:  cfg.Brokers,
				Topic:    cfg.Topic,
				GroupID:  cfg.GroupID,
				MinBytes: minBytes,
				MaxBytes: maxBytes,
			})
		})
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			r.Close()
			if ctx.Err() != nil {
				// Clean shutdown via context cancellation.
				return kafkaconn.Message{}, false, nil
			}
			return kafkaconn.Message{}, false, err
		}
		if cErr := r.CommitMessages(ctx, msg); cErr != nil {
			return kafkaconn.Message{}, false, cErr
		}
		return msg, true, nil
	})
}
