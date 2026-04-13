//go:build integration

package kafka_test

import (
	"context"
	"testing"
	"time"

	kafkaconn "github.com/segmentio/kafka-go"
	gkafka "github.com/sopranoworks/gekka-extensions-stream-kafka"
	"github.com/sopranoworks/gekka/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sinkTestTopic = "gekka-sink-test-topic"

func TestKafkaSink_Produce(t *testing.T) {
	ctx := context.Background()

	// Produce via Sink
	msgs := []kafkaconn.Message{
		{Value: []byte("msg-1")},
		{Value: []byte("msg-2")},
	}
	src := stream.FromSlice(msgs)
	sink := gkafka.Sink(gkafka.SinkConfig{
		Brokers: []string{testBroker},
		Topic:   sinkTestTopic,
		Ctx:     ctx,
	})
	_, err := src.To(sink).Run(stream.SyncMaterializer{})
	require.NoError(t, err)

	// Read back to verify
	ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	r := kafkaconn.NewReader(kafkaconn.ReaderConfig{
		Brokers:  []string{testBroker},
		Topic:    sinkTestTopic,
		GroupID:  "gekka-sink-verify",
		MinBytes: 1,
		MaxBytes: 10 * 1024 * 1024,
	})
	defer r.Close()

	var got []string
	for i := 0; i < 2; i++ {
		m, fetchErr := r.FetchMessage(ctx2)
		require.NoError(t, fetchErr)
		got = append(got, string(m.Value))
		_ = r.CommitMessages(ctx2, m)
	}
	assert.Contains(t, got, "msg-1")
	assert.Contains(t, got, "msg-2")
}
