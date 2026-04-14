/*
 * sink.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package s3ext

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sopranoworks/gekka/stream"
)

// SinkConfig configures an S3 Sink.
type SinkConfig struct {
	Client      *s3.Client
	Bucket      string
	Key         string
	ContentType string // default: "application/octet-stream"
}

// Sink collects all upstream []byte chunks and uploads them as a single PutObject call.
func Sink(cfg SinkConfig) stream.Sink[[]byte, stream.NotUsed] {
	ct := cfg.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	return stream.SinkFromFunc(func(pull func() ([]byte, bool, error)) error {
		var combined []byte
		for {
			chunk, ok, err := pull()
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			combined = append(combined, chunk...)
		}
		_, err := cfg.Client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket:      aws.String(cfg.Bucket),
			Key:         aws.String(cfg.Key),
			Body:        bytes.NewReader(combined),
			ContentType: aws.String(ct),
		})
		if err != nil {
			return fmt.Errorf("s3 sink: PutObject %s/%s: %w", cfg.Bucket, cfg.Key, err)
		}
		return nil
	})
}
