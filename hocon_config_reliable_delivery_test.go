/*
 * hocon_config_reliable_delivery_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"testing"
	"time"
)

// Verifies pekko.reliable-delivery.* keys parse into ClusterConfig.ReliableDelivery.
// Sets every key listed in actor-typed/src/main/resources/reference.conf to a
// non-default value and asserts each lands in the corresponding sub-config.
func TestHOCON_ReliableDelivery_AllKeysParsed(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote {
    artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  }
  cluster.seed-nodes = []
  reliable-delivery {
    producer-controller {
      chunk-large-messages = 65536
      durable-queue {
        request-timeout       = 7s
        retry-attempts        = 25
        resend-first-interval = 2500ms
      }
    }
    consumer-controller {
      flow-control-window  = 200
      resend-interval-min  = 5s
      resend-interval-max  = 90s
      only-flow-control    = true
    }
    work-pulling {
      producer-controller {
        buffer-size           = 4096
        internal-ask-timeout  = 120s
        chunk-large-messages  = 0
      }
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	rd := cfg.ReliableDelivery

	if rd.ProducerController.ChunkLargeMessages != 65536 {
		t.Errorf("ProducerController.ChunkLargeMessages = %d, want 65536", rd.ProducerController.ChunkLargeMessages)
	}
	if rd.ProducerController.DurableQueueRequestTimeout != 7*time.Second {
		t.Errorf("DurableQueueRequestTimeout = %v, want 7s", rd.ProducerController.DurableQueueRequestTimeout)
	}
	if rd.ProducerController.DurableQueueRetryAttempts != 25 {
		t.Errorf("DurableQueueRetryAttempts = %d, want 25", rd.ProducerController.DurableQueueRetryAttempts)
	}
	if rd.ProducerController.DurableQueueResendFirstInterval != 2500*time.Millisecond {
		t.Errorf("DurableQueueResendFirstInterval = %v, want 2500ms", rd.ProducerController.DurableQueueResendFirstInterval)
	}

	if rd.ConsumerController.FlowControlWindow != 200 {
		t.Errorf("FlowControlWindow = %d, want 200", rd.ConsumerController.FlowControlWindow)
	}
	if rd.ConsumerController.ResendIntervalMin != 5*time.Second {
		t.Errorf("ResendIntervalMin = %v, want 5s", rd.ConsumerController.ResendIntervalMin)
	}
	if rd.ConsumerController.ResendIntervalMax != 90*time.Second {
		t.Errorf("ResendIntervalMax = %v, want 90s", rd.ConsumerController.ResendIntervalMax)
	}
	if !rd.ConsumerController.OnlyFlowControl {
		t.Errorf("OnlyFlowControl = false, want true")
	}

	if rd.WorkPullingProducerController.BufferSize != 4096 {
		t.Errorf("WorkPulling.BufferSize = %d, want 4096", rd.WorkPullingProducerController.BufferSize)
	}
	if rd.WorkPullingProducerController.InternalAskTimeout != 120*time.Second {
		t.Errorf("WorkPulling.InternalAskTimeout = %v, want 120s", rd.WorkPullingProducerController.InternalAskTimeout)
	}
	if rd.WorkPullingProducerController.ChunkLargeMessages != 0 {
		t.Errorf("WorkPulling.ChunkLargeMessages = %d, want 0", rd.WorkPullingProducerController.ChunkLargeMessages)
	}
}

// Verifies that with an empty HOCON block (silent on reliable-delivery keys),
// ClusterConfig.ReliableDelivery falls back to the Pekko reference defaults
// — i.e., DefaultConfig() is applied, not zero-values.
func TestHOCON_ReliableDelivery_DefaultsWhenSilent(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote { artery { canonical { hostname = "127.0.0.1", port = 2552 } } }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	rd := cfg.ReliableDelivery

	// Pekko reference defaults.
	if rd.ProducerController.DurableQueueRequestTimeout != 3*time.Second {
		t.Errorf("default DurableQueueRequestTimeout = %v, want 3s", rd.ProducerController.DurableQueueRequestTimeout)
	}
	if rd.ConsumerController.FlowControlWindow != 50 {
		t.Errorf("default FlowControlWindow = %d, want 50", rd.ConsumerController.FlowControlWindow)
	}
	if rd.WorkPullingProducerController.BufferSize != 1000 {
		t.Errorf("default BufferSize = %d, want 1000", rd.WorkPullingProducerController.BufferSize)
	}
	if rd.ConsumerController.OnlyFlowControl {
		t.Errorf("default OnlyFlowControl = true, want false")
	}
}

// Verifies the chunk-large-messages key accepts the literal string "off"
// (Pekko's spelling) and HOCON byte-size strings like "256 KiB". The parser
// must not crash and must produce the expected int value.
func TestHOCON_ReliableDelivery_ChunkLargeMessages_OffAndByteSize(t *testing.T) {
	t.Run("off keyword", func(t *testing.T) {
		cfg, err := parseHOCONString(`
pekko {
  remote { artery { canonical { hostname = "127.0.0.1", port = 2552 } } }
  cluster.seed-nodes = []
  reliable-delivery.producer-controller.chunk-large-messages = off
}
`)
		if err != nil {
			t.Fatalf("parseHOCONString: %v", err)
		}
		if cfg.ReliableDelivery.ProducerController.ChunkLargeMessages != 0 {
			t.Errorf("chunk-large-messages=off → got %d, want 0",
				cfg.ReliableDelivery.ProducerController.ChunkLargeMessages)
		}
	})

	t.Run("byte size", func(t *testing.T) {
		cfg, err := parseHOCONString(`
pekko {
  remote { artery { canonical { hostname = "127.0.0.1", port = 2552 } } }
  cluster.seed-nodes = []
  reliable-delivery.producer-controller.chunk-large-messages = "128 KiB"
}
`)
		if err != nil {
			t.Fatalf("parseHOCONString: %v", err)
		}
		got := cfg.ReliableDelivery.ProducerController.ChunkLargeMessages
		if got != 128*1024 {
			t.Errorf(`chunk-large-messages="128 KiB" → got %d, want %d`, got, 128*1024)
		}
	})
}
