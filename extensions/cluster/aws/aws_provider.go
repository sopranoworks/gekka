/*
 * aws_provider.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package aws provides an AWS EC2-based discovery extension for the Gekka cluster.
//
// AWSProvider implements discovery.SeedProvider by calling the EC2
// DescribeInstances API to locate running instances whose tags match a
// user-defined filter.  Import this package for its side effect — the init
// function registers the "aws-ec2" provider with the discovery registry:
//
//	import _ "github.com/sopranoworks/gekka-extensions-cluster-aws"
package aws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	gkdiscovery "github.com/sopranoworks/gekka/discovery"
)

func init() {
	gkdiscovery.Register("aws-ec2", Factory)
}

// Factory creates an AWSProvider from a generic DiscoveryConfig.
//
// Recognised config keys:
//
//	"region"      string  AWS region (default: SDK default resolution).
//	"tag-key"     string  EC2 instance tag key used to filter instances.
//	"tag-value"   string  EC2 instance tag value used to filter instances.
//	"port"        int     Port appended to each discovered private IP.
func Factory(cfg gkdiscovery.DiscoveryConfig) (gkdiscovery.SeedProvider, error) {
	region, _ := cfg.Config["region"].(string)
	tagKey, _ := cfg.Config["tag-key"].(string)
	tagValue, _ := cfg.Config["tag-value"].(string)
	port, _ := cfg.Config["port"].(int)

	opts := []func(*config.LoadOptions) error{}
	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("aws-ec2: failed to load AWS config: %w", err)
	}

	client := ec2.NewFromConfig(awsCfg)
	return NewAWSProvider(client, tagKey, tagValue, port), nil
}

// EC2DescribeInstancesAPI is the subset of the EC2 API used by AWSProvider.
// It is satisfied by *ec2.Client and can be replaced with a mock in tests.
type EC2DescribeInstancesAPI interface {
	DescribeInstances(ctx context.Context, params *ec2.DescribeInstancesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error)
}

// AWSProvider implements discovery.SeedProvider using the AWS EC2
// DescribeInstances API.
type AWSProvider struct {
	client   EC2DescribeInstancesAPI
	tagKey   string
	tagValue string
	port     int
}

// NewAWSProvider creates an AWSProvider with an injected EC2 client.  Use
// NewAWSProvider in production code (or via Factory) and pass a mock client
// in unit tests.
func NewAWSProvider(client EC2DescribeInstancesAPI, tagKey, tagValue string, port int) *AWSProvider {
	return &AWSProvider{
		client:   client,
		tagKey:   tagKey,
		tagValue: tagValue,
		port:     port,
	}
}

// FetchSeedNodes calls EC2 DescribeInstances and returns "privateIP:port" for
// every running instance that matches the configured tag filter.
//
// Only instances in the "running" state are included.  The private IP address
// is used (suitable for intra-VPC cluster communication).
func (p *AWSProvider) FetchSeedNodes() ([]string, error) {
	input := &ec2.DescribeInstancesInput{
		Filters: p.buildFilters(),
	}

	var seeds []string
	paginator := ec2.NewDescribeInstancesPaginator(p.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("aws-ec2: DescribeInstances failed: %w", err)
		}
		for _, reservation := range page.Reservations {
			for _, instance := range reservation.Instances {
				if instance.State == nil || instance.State.Name != types.InstanceStateNameRunning {
					continue
				}
				if instance.PrivateIpAddress == nil {
					continue
				}
				seeds = append(seeds, fmt.Sprintf("%s:%d", aws.ToString(instance.PrivateIpAddress), p.port))
			}
		}
	}

	if len(seeds) == 0 {
		return nil, fmt.Errorf("aws-ec2: no running instances found with tag %s=%s", p.tagKey, p.tagValue)
	}
	return seeds, nil
}

// buildFilters constructs the EC2 filter list. Adds a tag filter when both
// tagKey and tagValue are non-empty; always restricts to running instances.
func (p *AWSProvider) buildFilters() []types.Filter {
	filters := []types.Filter{
		{
			Name:   aws.String("instance-state-name"),
			Values: []string{"running"},
		},
	}
	if p.tagKey != "" && p.tagValue != "" {
		filters = append(filters, types.Filter{
			Name:   aws.String(fmt.Sprintf("tag:%s", p.tagKey)),
			Values: []string{p.tagValue},
		})
	}
	return filters
}

// Ensure AWSProvider implements SeedProvider at compile time.
var _ gkdiscovery.SeedProvider = (*AWSProvider)(nil)
