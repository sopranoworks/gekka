//go:build integration

// extensions/stream/sqs/sqs_test.go
// Requires LocalStack at http://localhost:4566
package sqs_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsext "github.com/sopranoworks/gekka-extensions-stream-sqs"
	"github.com/sopranoworks/gekka/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func localClient() *sqs.Client {
	return sqs.New(sqs.Options{
		BaseEndpoint: aws.String("http://localhost:4566"),
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		Region:       "us-east-1",
	})
}

func TestSQS_BatchedRoundTrip(t *testing.T) {
	client := localClient()
	qout, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{
		QueueName: aws.String("gekka-test-sqs"),
	})
	require.NoError(t, err)
	qURL := aws.ToString(qout.QueueUrl)

	msgs := make([]string, 12)
	for i := range msgs {
		msgs[i] = "msg-" + string(rune('a'+i))
	}
	err = stream.RunWith(
		stream.FromSlice(msgs),
		sqsext.Sink(sqsext.SinkConfig{Client: client, QueueURL: qURL, BatchSize: 6}),
		stream.SyncMaterializer{},
	)
	require.NoError(t, err)

	received, err := stream.RunWith(
		stream.Take(sqsext.Source(sqsext.SourceConfig{
			Client: client, QueueURL: qURL,
			MaxMessages: 10, WaitTimeSeconds: 1, AutoDelete: true,
		}), 12),
		stream.Collect[sqsext.Message](),
		stream.SyncMaterializer{},
	)
	require.NoError(t, err)
	require.Len(t, received, 12)

	bodies := make([]string, len(received))
	for i, m := range received {
		bodies[i] = m.Body
	}
	assert.ElementsMatch(t, msgs, bodies)
}
