// extensions/stream/sqs/sink.go
package sqs

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/sopranoworks/gekka/stream"
)

// SinkConfig configures an SQS Sink.
type SinkConfig struct {
	Client    *sqs.Client
	QueueURL  string
	BatchSize int // 1–10 messages per SendMessageBatch call; default 10
}

// Sink sends string messages to SQS using batched SendMessageBatch for efficiency.
func Sink(cfg SinkConfig) stream.Sink[string, stream.NotUsed] {
	batchSize := cfg.BatchSize
	if batchSize <= 0 || batchSize > 10 {
		batchSize = 10
	}
	return stream.SinkFromFunc(func(pull func() (string, bool, error)) error {
		var batch []sqstypes.SendMessageBatchRequestEntry
		seq := 0
		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			out, err := cfg.Client.SendMessageBatch(context.Background(), &sqs.SendMessageBatchInput{
				QueueUrl: aws.String(cfg.QueueURL),
				Entries:  batch,
			})
			if err != nil {
				return fmt.Errorf("sqs sink: SendMessageBatch: %w", err)
			}
			if len(out.Failed) > 0 {
				return fmt.Errorf("sqs sink: %d messages failed in batch", len(out.Failed))
			}
			batch = batch[:0]
			return nil
		}
		for {
			body, ok, err := pull()
			if err != nil {
				return err
			}
			if !ok {
				return flush()
			}
			batch = append(batch, sqstypes.SendMessageBatchRequestEntry{
				Id:          aws.String(strconv.Itoa(seq)),
				MessageBody: aws.String(body),
			})
			seq++
			if len(batch) >= batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	})
}
