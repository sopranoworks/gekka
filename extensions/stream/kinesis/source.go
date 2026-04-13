/*
 * source.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package kinesis

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/sopranoworks/gekka/stream"
)

// Record aliases the SDK record type.
type Record = types.Record

// SourceConfig configures the Kinesis consumer source.
type SourceConfig struct {
	Client            *awskinesis.Client
	StreamName        string
	ShardIteratorType types.ShardIteratorType // default: TRIM_HORIZON
	PollInterval      time.Duration           // default: 1s
	Ctx               context.Context         // nil defaults to Background
}

// Source returns a stream.Source that emits Kinesis Records from all shards of a stream.
// Shards are discovered once on the first pull. Records are emitted sequentially across all shards.
// Cancel Ctx to stop the source cleanly.
func Source(cfg SourceConfig) stream.Source[Record, stream.NotUsed] {
	ctx := cfg.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	iterType := cfg.ShardIteratorType
	if iterType == "" {
		iterType = types.ShardIteratorTypeTrimHorizon
	}
	pollInterval := cfg.PollInterval
	if pollInterval == 0 {
		pollInterval = time.Second
	}

	var (
		once      sync.Once
		initErr   error
		iterators []string // one per shard; nil entry = shard closed
		buf       []Record // buffered records not yet emitted
	)

	init := func() {
		// List all shards.
		listOut, err := cfg.Client.ListShards(ctx, &awskinesis.ListShardsInput{
			StreamName: aws.String(cfg.StreamName),
		})
		if err != nil {
			initErr = err
			return
		}
		iterators = make([]string, len(listOut.Shards))
		for i, shard := range listOut.Shards {
			out, err := cfg.Client.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{
				StreamName:        aws.String(cfg.StreamName),
				ShardId:           shard.ShardId,
				ShardIteratorType: iterType,
			})
			if err != nil {
				initErr = err
				return
			}
			if out.ShardIterator != nil {
				iterators[i] = *out.ShardIterator
			}
		}
	}

	return stream.FromIteratorFunc(func() (Record, bool, error) {
		once.Do(init)
		if initErr != nil {
			return Record{}, false, initErr
		}

		for {
			// Check context cancellation.
			select {
			case <-ctx.Done():
				return Record{}, false, nil
			default:
			}

			// Drain buffered records first.
			if len(buf) > 0 {
				rec := buf[0]
				buf = buf[1:]
				return rec, true, nil
			}

			// Check if all shards are exhausted.
			allDone := true
			for _, it := range iterators {
				if it != "" {
					allDone = false
					break
				}
			}
			if allDone {
				return Record{}, false, nil
			}

			// Fetch a batch from each active shard.
			fetched := 0
			for i, it := range iterators {
				if it == "" {
					continue
				}
				out, err := cfg.Client.GetRecords(ctx, &awskinesis.GetRecordsInput{
					ShardIterator: aws.String(it),
				})
				if err != nil {
					if ctx.Err() != nil {
						return Record{}, false, nil
					}
					return Record{}, false, err
				}
				buf = append(buf, out.Records...)
				fetched += len(out.Records)
				if out.NextShardIterator == nil || *out.NextShardIterator == "" {
					iterators[i] = "" // shard closed
				} else {
					iterators[i] = *out.NextShardIterator
				}
			}

			// If no records arrived, sleep before polling again.
			if fetched == 0 {
				select {
				case <-ctx.Done():
					return Record{}, false, nil
				case <-time.After(pollInterval):
				}
			}
		}
	})
}
