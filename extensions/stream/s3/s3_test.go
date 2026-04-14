//go:build integration

// extensions/stream/s3/s3_test.go
// Requires MinIO at http://localhost:9000 (access: minioadmin/minioadmin)
// Bucket "gekka-test" must exist before running.
package s3ext_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3ext "github.com/sopranoworks/gekka-extensions-stream-s3"
	"github.com/sopranoworks/gekka/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func minioClient() *s3.Client {
	return s3.New(s3.Options{
		BaseEndpoint: aws.String("http://localhost:9000"),
		Credentials:  credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", ""),
		Region:       "us-east-1",
		UsePathStyle: true,
	})
}

func ensureBucket(t *testing.T, client *s3.Client, bucket string) {
	t.Helper()
	_, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	// Ignore BucketAlreadyOwnedByYou error
	if err != nil {
		t.Logf("CreateBucket: %v (may already exist, continuing)", err)
	}
}

func TestS3_UploadAndDownload(t *testing.T) {
	client := minioClient()
	ensureBucket(t, client, "gekka-test")

	err := stream.RunWith(
		stream.FromSlice([][]byte{[]byte("hello "), []byte("world")}),
		s3ext.Sink(s3ext.SinkConfig{Client: client, Bucket: "gekka-test", Key: "test.txt"}),
		stream.SyncMaterializer{},
	)
	require.NoError(t, err)

	chunks, err := stream.RunWith(
		s3ext.Source(s3ext.SourceConfig{Client: client, Bucket: "gekka-test", Key: "test.txt"}),
		stream.Collect[[]byte](),
		stream.SyncMaterializer{},
	)
	require.NoError(t, err)
	var combined []byte
	for _, c := range chunks {
		combined = append(combined, c...)
	}
	assert.Equal(t, "hello world", string(combined))
}

func TestS3_ListBucket(t *testing.T) {
	client := minioClient()
	ensureBucket(t, client, "gekka-test")

	keys, err := stream.RunWith(
		s3ext.ListBucket(s3ext.ListConfig{Client: client, Bucket: "gekka-test"}),
		stream.Collect[s3ext.ObjectInfo](),
		stream.SyncMaterializer{},
	)
	require.NoError(t, err)
	require.NotEmpty(t, keys)
	var found bool
	for _, k := range keys {
		if k.Key == "test.txt" {
			found = true
		}
	}
	assert.True(t, found, "uploaded test.txt should appear in listing")
}
