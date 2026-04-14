/*
 * source.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package amqp provides AMQP/RabbitMQ Source and Sink connectors for gekka streams.
package amqp

import (
	"fmt"
	"sync"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/sopranoworks/gekka/stream"
)

// InboundMessage is a received AMQP message.
type InboundMessage struct {
	Body        []byte
	ContentType string
	Exchange    string
	RoutingKey  string
	DeliveryTag uint64
	AckFunc     func() error // call to acknowledge; nil when AutoAck=true
}

// SourceConfig configures an AMQP Source.
type SourceConfig struct {
	URL       string
	QueueName string
	AutoAck   bool
	Prefetch  int // 0 = unlimited
}

// Source reads messages from an AMQP queue.
// Lazily dials on first pull using sync.Once.
//
// NOTE: the AMQP connection is closed only when the broker closes the deliveries
// channel (broker disconnect or stream.Take limit reached via channel cancel). If
// the downstream abandons the source mid-stream without draining it to completion,
// the connection and channel will be leaked. Pass a stream.Take to bound consumption.
func Source(cfg SourceConfig) stream.Source[InboundMessage, stream.NotUsed] {
	var (
		once       sync.Once
		initErr    error
		deliveries <-chan amqp091.Delivery
		conn       *amqp091.Connection
		ch         *amqp091.Channel
	)
	return stream.FromIteratorFunc(func() (InboundMessage, bool, error) {
		once.Do(func() {
			conn, initErr = amqp091.Dial(cfg.URL)
			if initErr != nil {
				return
			}
			ch, initErr = conn.Channel()
			if initErr != nil {
				conn.Close()
				return
			}
			if cfg.Prefetch > 0 {
				if initErr = ch.Qos(cfg.Prefetch, 0, false); initErr != nil {
					ch.Close()
					conn.Close()
					return
				}
			}
			deliveries, initErr = ch.Consume(cfg.QueueName, "", cfg.AutoAck, false, false, false, nil)
			if initErr != nil {
				ch.Close()
				conn.Close()
			}
		})
		if initErr != nil {
			return InboundMessage{}, false, fmt.Errorf("amqp source: %w", initErr)
		}
		d, ok := <-deliveries
		if !ok {
			ch.Close()
			conn.Close()
			return InboundMessage{}, false, nil
		}
		var ackFn func() error
		if !cfg.AutoAck {
			ackFn = func() error { return d.Ack(false) }
		}
		return InboundMessage{
			Body:        d.Body,
			ContentType: d.ContentType,
			Exchange:    d.Exchange,
			RoutingKey:  d.RoutingKey,
			DeliveryTag: d.DeliveryTag,
			AckFunc:     ackFn,
		}, true, nil
	})
}
