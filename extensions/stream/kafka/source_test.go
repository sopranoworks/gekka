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

const testBroker = "localhost:9092"
const testTopic = "gekka-test-topic"

func TestKafkaSource_Consume(t *testing.T) {
	// Pre-produce one message.
	writer := &kafkaconn.Writer{Addr: kafkaconn.TCP(testBroker), Topic: testTopic}
	err := writer.WriteMessages(context.Background(), kafkaconn.Message{Value: []byte("hello-kafka")})
	require.NoError(t, err)
	_ = writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	src := gkafka.Source(gkafka.SourceConfig{
		Brokers: []string{testBroker},
		Topic:   testTopic,
		GroupID: "gekka-test-group",
		Ctx:     ctx,
	})

	var received []string
	sink := stream.ForeachErr[kafkaconn.Message](func(msg kafkaconn.Message) error {
		received = append(received, string(msg.Value))
		cancel() // got one message, stop the source via context cancellation
		return nil
	})

	// Run blocks until the source ends (context cancelled after first message).
	_, _ = src.To(sink).Run(stream.SyncMaterializer{})

	assert.Contains(t, received, "hello-kafka")
}
