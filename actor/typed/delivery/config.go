/*
 * actor/typed/delivery/config.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package delivery

import "time"

// ProducerControllerConfig corresponds to
// pekko.reliable-delivery.producer-controller.* in Pekko's reference.conf.
type ProducerControllerConfig struct {
	// ChunkLargeMessages is the byte threshold above which outgoing
	// payloads are split into chunked SequencedMessages. 0 disables
	// chunking. Pekko default: 0 (off).
	ChunkLargeMessages int

	// DurableQueueRequestTimeout is the timeout used by the producer
	// when waiting for a reply from the durable queue. Pekko default: 3s.
	DurableQueueRequestTimeout time.Duration

	// DurableQueueRetryAttempts is the number of times the producer
	// retries a request to the durable queue before failing.
	// Pekko default: 10.
	DurableQueueRetryAttempts int

	// DurableQueueResendFirstInterval is the interval the producer uses
	// to retry sending the first message until it has been confirmed.
	// Pekko default: 1s.
	DurableQueueResendFirstInterval time.Duration
}

// ConsumerControllerConfig corresponds to
// pekko.reliable-delivery.consumer-controller.* in Pekko's reference.conf.
type ConsumerControllerConfig struct {
	// FlowControlWindow is the number of in-flight messages the consumer
	// requests at a time. The consumer asks for more once half the window
	// has been consumed. Pekko default: 50.
	FlowControlWindow int

	// ResendIntervalMin is the lower bound for the exponential resend of
	// flow-control messages. Pekko default: 2s.
	ResendIntervalMin time.Duration

	// ResendIntervalMax is the upper bound for the exponential resend of
	// flow-control messages while idle. Pekko default: 30s.
	ResendIntervalMax time.Duration

	// OnlyFlowControl, when true, suppresses the gap-filling Resend the
	// consumer normally sends on out-of-order messages. Lost messages
	// will not be redelivered, but flow control still operates.
	// Pekko default: false.
	OnlyFlowControl bool
}

// WorkPullingProducerControllerConfig corresponds to
// pekko.reliable-delivery.work-pulling.producer-controller.* in Pekko's
// reference.conf.
type WorkPullingProducerControllerConfig struct {
	// BufferSize caps the number of pending work items the controller
	// will buffer when no idle workers are available. Items beyond this
	// limit are dropped (Pekko stops the controller; gekka logs and
	// drops to keep degraded operation observable in tests).
	// Pekko default: 1000.
	BufferSize int

	// InternalAskTimeout is the timeout used when the controller asks a
	// worker for an Ack. Pekko default: 60s.
	InternalAskTimeout time.Duration

	// ChunkLargeMessages is the byte threshold above which outgoing
	// payloads are split into chunked SequencedMessages. 0 disables
	// chunking. Pekko reference overrides the parent producer-controller
	// default to keep chunking off for work-pulling.
	ChunkLargeMessages int
}

// Config is the top-level pekko.reliable-delivery.* settings struct.
// Defaults match Pekko's reference.conf.
type Config struct {
	ProducerController            ProducerControllerConfig
	ConsumerController            ConsumerControllerConfig
	WorkPullingProducerController WorkPullingProducerControllerConfig
}

// DefaultConfig returns the Pekko reference.conf defaults for
// pekko.reliable-delivery.*. Used by gekka when HOCON is silent on
// these keys.
func DefaultConfig() Config {
	return Config{
		ProducerController: ProducerControllerConfig{
			ChunkLargeMessages:              0,
			DurableQueueRequestTimeout:      3 * time.Second,
			DurableQueueRetryAttempts:       10,
			DurableQueueResendFirstInterval: 1 * time.Second,
		},
		ConsumerController: ConsumerControllerConfig{
			FlowControlWindow: 50,
			ResendIntervalMin: 2 * time.Second,
			ResendIntervalMax: 30 * time.Second,
			OnlyFlowControl:   false,
		},
		WorkPullingProducerController: WorkPullingProducerControllerConfig{
			BufferSize:         1000,
			InternalAskTimeout: 60 * time.Second,
			ChunkLargeMessages: 0,
		},
	}
}
