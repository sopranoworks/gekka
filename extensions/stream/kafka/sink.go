/*
 * sink.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package kafka

import (
	"context"

	kafkaconn "github.com/segmentio/kafka-go"
	"github.com/sopranoworks/gekka/stream"
)

// SinkConfig configures the Kafka producer sink.
type SinkConfig struct {
	Brokers []string
	Topic   string
	// Async enables fire-and-forget writes (no delivery guarantees).
	Async bool
	// Ctx is used for write calls; nil defaults to context.Background().
	Ctx context.Context
}

// Sink returns a stream.Sink that publishes each kafkaconn.Message to Kafka.
// The writer is created once and closed when the stream completes or errors.
func Sink(cfg SinkConfig) stream.Sink[kafkaconn.Message, stream.NotUsed] {
	return stream.SinkFromFunc(func(pull func() (kafkaconn.Message, bool, error)) error {
		ctx := cfg.Ctx
		if ctx == nil {
			ctx = context.Background()
		}
		w := &kafkaconn.Writer{
			Addr:  kafkaconn.TCP(cfg.Brokers...),
			Topic: cfg.Topic,
			Async: cfg.Async,
		}
		defer w.Close()
		for {
			msg, ok, err := pull()
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			if wErr := w.WriteMessages(ctx, msg); wErr != nil {
				return wErr
			}
		}
	})
}
