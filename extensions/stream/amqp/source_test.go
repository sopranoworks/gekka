//go:build integration

// extensions/stream/amqp/source_test.go
// Requires RabbitMQ: amqp://guest:guest@localhost:5672/
// The test declares the queue itself; no pre-existing setup is required.
package amqp_test

import (
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	amqpext "github.com/sopranoworks/gekka-extensions-stream-amqp"
	"github.com/sopranoworks/gekka/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const amqpURL = "amqp://guest:guest@localhost:5672/"

// declareQueue ensures "gekka-test" exists on the default exchange before the test runs.
func declareQueue(t *testing.T) {
	t.Helper()
	conn, err := amqp091.Dial(amqpURL)
	require.NoError(t, err, "connect to RabbitMQ")
	defer conn.Close()
	ch, err := conn.Channel()
	require.NoError(t, err, "open channel")
	defer ch.Close()
	_, err = ch.QueueDeclare("gekka-test", false, true, false, false, nil)
	require.NoError(t, err, "declare queue")
}

func TestAMQP_RoundTrip(t *testing.T) {
	declareQueue(t)

	msgs := []amqpext.OutboundMessage{
		{Body: []byte("alpha")},
		{Body: []byte("beta")},
		{Body: []byte("gamma")},
	}
	err := stream.RunWith(
		stream.FromSlice(msgs),
		amqpext.Sink(amqpext.SinkConfig{URL: amqpURL, RoutingKey: "gekka-test"}),
		stream.SyncMaterializer{},
	)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	results, err := stream.RunWith(
		stream.Take(amqpext.Source(amqpext.SourceConfig{
			URL: amqpURL, QueueName: "gekka-test", AutoAck: true,
		}), 3),
		stream.Collect[amqpext.InboundMessage](),
		stream.SyncMaterializer{},
	)
	require.NoError(t, err)
	require.Len(t, results, 3)

	bodies := make([]string, 3)
	for i, r := range results {
		bodies[i] = string(r.Body)
	}
	assert.ElementsMatch(t, []string{"alpha", "beta", "gamma"}, bodies)
}
