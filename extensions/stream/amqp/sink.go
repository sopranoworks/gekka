/*
 * sink.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package amqp

import (
	"fmt"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/sopranoworks/gekka/stream"
)

// OutboundMessage is a message to publish to AMQP.
type OutboundMessage struct {
	Body        []byte
	ContentType string
	Headers     amqp091.Table
}

// SinkConfig configures an AMQP Sink.
type SinkConfig struct {
	URL        string
	Exchange   string // "" = default exchange (direct to queue)
	RoutingKey string
	Mandatory  bool
	Immediate  bool
}

// Sink publishes messages to an AMQP exchange.
// Dials once at start, then drains the upstream via the pull function.
func Sink(cfg SinkConfig) stream.Sink[OutboundMessage, stream.NotUsed] {
	return stream.SinkFromFunc(func(pull func() (OutboundMessage, bool, error)) error {
		conn, err := amqp091.Dial(cfg.URL)
		if err != nil {
			return fmt.Errorf("amqp sink: dial: %w", err)
		}
		defer conn.Close()
		ch, err := conn.Channel()
		if err != nil {
			return fmt.Errorf("amqp sink: channel: %w", err)
		}
		defer ch.Close()

		for {
			msg, ok, err := pull()
			if err != nil || !ok {
				return err
			}
			ct := msg.ContentType
			if ct == "" {
				ct = "application/octet-stream"
			}
			if pubErr := ch.Publish(cfg.Exchange, cfg.RoutingKey,
				cfg.Mandatory, cfg.Immediate,
				amqp091.Publishing{ContentType: ct, Body: msg.Body, Headers: msg.Headers},
			); pubErr != nil {
				return fmt.Errorf("amqp sink: publish: %w", pubErr)
			}
		}
	})
}
