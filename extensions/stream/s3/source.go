/*
 * source.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package s3ext provides S3/MinIO Source, Sink, and ListBucket connectors for gekka streams.
package s3ext

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sopranoworks/gekka/stream"
)

// SourceConfig configures an S3 Source.
type SourceConfig struct {
	Client    *s3.Client
	Bucket    string
	Key       string
	ChunkSize int // bytes per chunk; default 64 KiB
}

// Source downloads an S3 object and emits its content as []byte chunks.
// The object is fetched lazily on first pull. Chunks are 64 KiB by default.
//
// NOTE: connection is not closed if stream is abandoned mid-download.
// Use stream.Take with a known size or drain the source to completion.
func Source(cfg SourceConfig) stream.Source[[]byte, stream.NotUsed] {
	chunkSize := cfg.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 64 * 1024
	}
	var (
		once    sync.Once
		initErr error
		body    io.ReadCloser
		buf     = make([]byte, chunkSize)
		closed  bool
	)
	return stream.FromIteratorFunc(func() ([]byte, bool, error) {
		once.Do(func() {
			out, err := cfg.Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: aws.String(cfg.Bucket),
				Key:    aws.String(cfg.Key),
			})
			if err != nil {
				initErr = fmt.Errorf("s3 source: GetObject %s/%s: %w", cfg.Bucket, cfg.Key, err)
				return
			}
			body = out.Body
		})
		if initErr != nil {
			return nil, false, initErr
		}
		if closed {
			return nil, false, nil
		}
		n, err := body.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			return chunk, true, nil
		}
		if err == io.EOF {
			body.Close()
			closed = true
			return nil, false, nil
		}
		return nil, false, err
	})
}
