//go:build integration

package kinesis_test

import (
	"context"
	"testing"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	gkinesis "github.com/sopranoworks/gekka-extensions-stream-kinesis"
	"github.com/sopranoworks/gekka/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	localstackEndpoint = "http://localhost:4566"
	testStreamName     = "gekka-test-stream"
)

func newLocalStackClient(t *testing.T) *awskinesis.Client {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awsconfig.WithBaseEndpoint(localstackEndpoint),
	)
	require.NoError(t, err)
	return awskinesis.NewFromConfig(cfg)
}

func TestKinesisSource_Consume(t *testing.T) {
	client := newLocalStackClient(t)
	ctx := context.Background()

	// Create stream (ignore error if already exists).
	_, _ = client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: &[]string{testStreamName}[0],
		ShardCount: &[]int32{1}[0],
	})

	// Wait for stream to become ACTIVE.
	waiter := awskinesis.NewStreamExistsWaiter(client)
	require.NoError(t, waiter.Wait(ctx, &awskinesis.DescribeStreamInput{
		StreamName: &[]string{testStreamName}[0],
	}, 30*time.Second))

	// Put a test record.
	partKey := "test-key"
	_, err := client.PutRecord(ctx, &awskinesis.PutRecordInput{
		StreamName:   &[]string{testStreamName}[0],
		Data:         []byte("hello-kinesis"),
		PartitionKey: &partKey,
	})
	require.NoError(t, err)

	// Read back via Source.
	srcCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	src := gkinesis.Source(gkinesis.SourceConfig{
		Client:            client,
		StreamName:        testStreamName,
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
		PollInterval:      500 * time.Millisecond,
		Ctx:               srcCtx,
	})

	var received []string
	sink := stream.ForeachErr[gkinesis.Record](func(rec gkinesis.Record) error {
		received = append(received, string(rec.Data))
		cancel() // got one message, stop via context cancellation
		return nil
	})

	_, _ = src.To(sink).Run(stream.SyncMaterializer{})

	assert.Contains(t, received, "hello-kinesis")
}
