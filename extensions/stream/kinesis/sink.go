/*
 * sink.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package kinesis

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/sopranoworks/gekka/stream"
)

// SinkRecord is a record to be published to Kinesis.
type SinkRecord struct {
	Data         []byte
	PartitionKey string
}

// SinkConfig configures the Kinesis producer sink.
type SinkConfig struct {
	Client     *awskinesis.Client
	StreamName string
	// BatchSize is the maximum number of records per PutRecords call (Kinesis max: 500).
	// Zero defaults to 500.
	BatchSize int
	// Ctx is used for PutRecords calls; nil defaults to context.Background().
	Ctx context.Context
}

// Sink returns a stream.Sink that publishes SinkRecords to a Kinesis stream via PutRecords.
// Records are batched up to BatchSize. Any remaining records are flushed when the upstream ends.
func Sink(cfg SinkConfig) stream.Sink[SinkRecord, stream.NotUsed] {
	return stream.SinkFromFunc(func(pull func() (SinkRecord, bool, error)) error {
		ctx := cfg.Ctx
		if ctx == nil {
			ctx = context.Background()
		}
		batchSize := cfg.BatchSize
		if batchSize == 0 {
			batchSize = 500
		}

		batch := make([]types.PutRecordsRequestEntry, 0, batchSize)

		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			n := len(batch)
			out, err := cfg.Client.PutRecords(ctx, &awskinesis.PutRecordsInput{
				StreamName: aws.String(cfg.StreamName),
				Records:    batch,
			})
			batch = batch[:0]
			if err != nil {
				return err
			}
			if out.FailedRecordCount != nil && *out.FailedRecordCount > 0 {
				return fmt.Errorf("kinesis PutRecords: %d of %d records failed", *out.FailedRecordCount, n)
			}
			return nil
		}

		for {
			rec, ok, err := pull()
			if err != nil {
				return err
			}
			if !ok {
				return flush()
			}
			batch = append(batch, types.PutRecordsRequestEntry{
				Data:         rec.Data,
				PartitionKey: aws.String(rec.PartitionKey),
			})
			if len(batch) >= batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	})
}
