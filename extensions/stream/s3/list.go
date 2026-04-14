/*
 * list.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package s3ext

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sopranoworks/gekka/stream"
)

// ObjectInfo holds metadata for a single S3 object.
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
}

// ListConfig configures a ListBucket source.
type ListConfig struct {
	Client    *s3.Client
	Bucket    string
	Prefix    string // optional key prefix filter
	Delimiter string // optional delimiter for common-prefix grouping
}

// ListBucket streams ObjectInfo for every object in Bucket matching Prefix.
// Uses ListObjectsV2 with automatic continuation-token pagination.
func ListBucket(cfg ListConfig) stream.Source[ObjectInfo, stream.NotUsed] {
	var (
		once      sync.Once
		initErr   error
		buf       []ObjectInfo
		nextToken *string
		done      bool
	)
	fetch := func() error {
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(cfg.Bucket),
			ContinuationToken: nextToken,
		}
		if cfg.Prefix != "" {
			input.Prefix = aws.String(cfg.Prefix)
		}
		if cfg.Delimiter != "" {
			input.Delimiter = aws.String(cfg.Delimiter)
		}
		out, err := cfg.Client.ListObjectsV2(context.Background(), input)
		if err != nil {
			return fmt.Errorf("s3 list: ListObjectsV2 %s: %w", cfg.Bucket, err)
		}
		for _, obj := range out.Contents {
			info := ObjectInfo{
				Key:  aws.ToString(obj.Key),
				Size: aws.ToInt64(obj.Size),
				ETag: aws.ToString(obj.ETag),
			}
			if obj.LastModified != nil {
				info.LastModified = *obj.LastModified
			}
			buf = append(buf, info)
		}
		if out.IsTruncated != nil && *out.IsTruncated {
			nextToken = out.NextContinuationToken
		} else {
			done = true
		}
		return nil
	}
	return stream.FromIteratorFunc(func() (ObjectInfo, bool, error) {
		once.Do(func() { initErr = fetch() })
		if initErr != nil {
			return ObjectInfo{}, false, initErr
		}
		for len(buf) == 0 {
			if done {
				return ObjectInfo{}, false, nil
			}
			if err := fetch(); err != nil {
				return ObjectInfo{}, false, err
			}
		}
		v := buf[0]
		buf = buf[1:]
		return v, true, nil
	})
}
