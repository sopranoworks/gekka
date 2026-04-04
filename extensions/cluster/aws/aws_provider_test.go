/*
 * aws_provider_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package aws

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	gkdiscovery "github.com/sopranoworks/gekka/discovery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockEC2Client implements EC2DescribeInstancesAPI for testing.
type mockEC2Client struct {
	output *ec2.DescribeInstancesOutput
	err    error
}

func (m *mockEC2Client) DescribeInstances(_ context.Context, _ *ec2.DescribeInstancesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	return m.output, m.err
}

func runningInstance(privateIP string) types.Instance {
	return types.Instance{
		InstanceId:       aws.String("i-" + privateIP),
		PrivateIpAddress: aws.String(privateIP),
		State: &types.InstanceState{
			Name: types.InstanceStateNameRunning,
		},
	}
}

func stoppedInstance(privateIP string) types.Instance {
	return types.Instance{
		InstanceId:       aws.String("i-stopped-" + privateIP),
		PrivateIpAddress: aws.String(privateIP),
		State: &types.InstanceState{
			Name: types.InstanceStateNameStopped,
		},
	}
}

func TestAWSProvider_FetchSeedNodes(t *testing.T) {
	client := &mockEC2Client{
		output: &ec2.DescribeInstancesOutput{
			Reservations: []types.Reservation{
				{
					Instances: []types.Instance{
						runningInstance("10.0.0.1"),
						runningInstance("10.0.0.2"),
						stoppedInstance("10.0.0.3"), // must be excluded
					},
				},
			},
		},
	}

	p := NewAWSProvider(client, "cluster", "gekka", 2552)
	seeds, err := p.FetchSeedNodes()

	require.NoError(t, err)
	assert.Len(t, seeds, 2)
	assert.Contains(t, seeds, "10.0.0.1:2552")
	assert.Contains(t, seeds, "10.0.0.2:2552")
	assert.NotContains(t, seeds, "10.0.0.3:2552")
}

func TestAWSProvider_NoRunningInstances(t *testing.T) {
	client := &mockEC2Client{
		output: &ec2.DescribeInstancesOutput{
			Reservations: []types.Reservation{
				{
					Instances: []types.Instance{
						stoppedInstance("10.0.0.1"),
					},
				},
			},
		},
	}

	p := NewAWSProvider(client, "cluster", "gekka", 2552)
	_, err := p.FetchSeedNodes()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no running instances found")
}

func TestAWSProvider_EmptyReservations(t *testing.T) {
	client := &mockEC2Client{
		output: &ec2.DescribeInstancesOutput{
			Reservations: []types.Reservation{},
		},
	}

	p := NewAWSProvider(client, "cluster", "gekka", 2552)
	_, err := p.FetchSeedNodes()

	assert.Error(t, err)
}

func TestAWSProvider_BuildFilters_WithTag(t *testing.T) {
	p := NewAWSProvider(nil, "Name", "my-cluster", 2552)
	filters := p.buildFilters()

	assert.Len(t, filters, 2)
	assert.Equal(t, "instance-state-name", aws.ToString(filters[0].Name))
	assert.Equal(t, "tag:Name", aws.ToString(filters[1].Name))
	assert.Equal(t, []string{"my-cluster"}, filters[1].Values)
}

func TestAWSProvider_BuildFilters_NoTag(t *testing.T) {
	p := NewAWSProvider(nil, "", "", 2552)
	filters := p.buildFilters()

	// Only the running-state filter when no tag is specified
	assert.Len(t, filters, 1)
	assert.Equal(t, "instance-state-name", aws.ToString(filters[0].Name))
}

func TestFactory_MissingRegion_UsesDefault(t *testing.T) {
	// Factory with only required fields — SDK picks up region from env/config.
	// We cannot call Factory directly in unit tests (it calls AWS APIs), so we
	// just verify that the Factory function is registered.
	names := gkdiscovery.ProviderNames()
	assert.Contains(t, names, "aws-ec2")
}
