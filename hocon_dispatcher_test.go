/*
 * hocon_dispatcher_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"testing"

	"github.com/sopranoworks/gekka/actor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHOCONDispatcherConfig(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 2551
  }
  dispatchers {
    my-blocking-dispatcher {
      type = "pinned-dispatcher"
      throughput = 1
    }
    my-fast-dispatcher {
      type = "default-dispatcher"
      throughput = 10
    }
    my-test-dispatcher {
      type = "calling-thread-dispatcher"
      throughput = 1
    }
  }
}
`
	_, err := ParseHOCONString(hocon)
	require.NoError(t, err)

	// Verify dispatchers were registered.
	assert.Equal(t, actor.DispatcherPinned, actor.ResolveDispatcherKey("my-blocking-dispatcher"))
	assert.Equal(t, actor.DispatcherDefault, actor.ResolveDispatcherKey("my-fast-dispatcher"))
	assert.Equal(t, actor.DispatcherCallingThread, actor.ResolveDispatcherKey("my-test-dispatcher"))
}

func TestHOCONDispatcherConfig_ReferenceConf(t *testing.T) {
	// Parse reference.conf which contains default and pinned dispatchers.
	hocon := `
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 2551
  }
  dispatchers {
    default-dispatcher {
      type = "default-dispatcher"
      throughput = 5
    }
    pinned-dispatcher {
      type = "pinned-dispatcher"
      throughput = 1
    }
  }
}
`
	_, err := ParseHOCONString(hocon)
	require.NoError(t, err)

	assert.Equal(t, actor.DispatcherDefault, actor.ResolveDispatcherKey("default-dispatcher"))
	assert.Equal(t, actor.DispatcherPinned, actor.ResolveDispatcherKey("pinned-dispatcher"))
}
