/*
 * spanner_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

//go:build spanner

package spannerstore_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/sopranoworks/gekka/persistence"
	spannerstore "github.com/sopranoworks/gekka-extensions-persistence-spanner"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
)

const (
	testProject  = "test-project"
	testInstance = "test-instance"
	testDatabase = "test-db"
)

type emulatorFixture struct {
	endpoint string
	dbName   string
}

var (
	fixtureOnce sync.Once
	fixture     *emulatorFixture
	fixtureErr  error
)

func getFixture(t *testing.T) *emulatorFixture {
	t.Helper()
	fixtureOnce.Do(func() {
		ctx := context.Background()

		ctr, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
			ContainerRequest: tc.ContainerRequest{
				Image:        "gcr.io/cloud-spanner-emulator/emulator:latest",
				ExposedPorts: []string{"9010/tcp"},
				WaitingFor:   wait.ForListeningPort("9010/tcp").WithStartupTimeout(60 * time.Second),
			},
			Started: true,
		})
		if err != nil {
			fixtureErr = fmt.Errorf("start emulator: %w", err)
			return
		}

		host, err := ctr.Host(ctx)
		if err != nil {
			fixtureErr = fmt.Errorf("emulator host: %w", err)
			return
		}
		port, err := ctr.MappedPort(ctx, "9010/tcp")
		if err != nil {
			fixtureErr = fmt.Errorf("emulator port: %w", err)
			return
		}
		endpoint := fmt.Sprintf("%s:%s", host, port.Port())

		opts := []option.ClientOption{
			option.WithEndpoint(endpoint),
			option.WithoutAuthentication(),
		}

		instAdmin, err := instance.NewInstanceAdminClient(ctx, opts...)
		if err != nil {
			fixtureErr = fmt.Errorf("instance admin client: %w", err)
			return
		}
		defer instAdmin.Close()

		instOp, err := instAdmin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
			Parent:     fmt.Sprintf("projects/%s", testProject),
			InstanceId: testInstance,
			Instance: &instancepb.Instance{
				Config:      fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", testProject),
				DisplayName: "Test Instance",
				NodeCount:   1,
			},
		})
		if err != nil {
			fixtureErr = fmt.Errorf("create instance: %w", err)
			return
		}
		if _, err := instOp.Wait(ctx); err != nil {
			fixtureErr = fmt.Errorf("wait for instance: %w", err)
			return
		}

		dbAdmin, err := database.NewDatabaseAdminClient(ctx, opts...)
		if err != nil {
			fixtureErr = fmt.Errorf("database admin client: %w", err)
			return
		}
		defer dbAdmin.Close()

		dbName := fmt.Sprintf("projects/%s/instances/%s/databases/%s",
			testProject, testInstance, testDatabase)
		dbOp, err := dbAdmin.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
			Parent:          fmt.Sprintf("projects/%s/instances/%s", testProject, testInstance),
			CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", testDatabase),
			ExtraStatements: []string{
				spannerstore.JournalDDL,
				spannerstore.EventTagDDL,
				spannerstore.SnapshotsDDL,
				spannerstore.OffsetsDDL,
			},
		})
		if err != nil {
			fixtureErr = fmt.Errorf("create database: %w", err)
			return
		}
		if _, err := dbOp.Wait(ctx); err != nil {
			fixtureErr = fmt.Errorf("wait for database: %w", err)
			return
		}

		fixture = &emulatorFixture{endpoint: endpoint, dbName: dbName}
	})

	if fixtureErr != nil {
		t.Skipf("spanner emulator unavailable: %v", fixtureErr)
	}
	return fixture
}

func newClient(t *testing.T, f *emulatorFixture) *spanner.Client {
	t.Helper()
	opts := []option.ClientOption{
		option.WithEndpoint(f.endpoint),
		option.WithoutAuthentication(),
	}
	client, err := spanner.NewClient(context.Background(), f.dbName, opts...)
	if err != nil {
		t.Fatalf("spanner.NewClient: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

type OrderPlaced struct{ Item string }
type Evt struct{ V int }

func TestSpanner_Journal_WriteAndReplay(t *testing.T) {
	f := getFixture(t)
	client := newClient(t, f)

	codec := spannerstore.NewJSONCodec()
	codec.Register(OrderPlaced{})

	j := spannerstore.NewSpannerJournal(client, codec)
	ctx := context.Background()
	const pid = "spanner-order-1"

	events := []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: OrderPlaced{Item: "apple"}},
		{PersistenceID: pid, SequenceNr: 2, Payload: OrderPlaced{Item: "banana"}},
	}
	if err := j.AsyncWriteMessages(ctx, events); err != nil {
		t.Fatalf("AsyncWriteMessages: %v", err)
	}

	highest, err := j.ReadHighestSequenceNr(ctx, pid, 0)
	if err != nil {
		t.Fatalf("ReadHighestSequenceNr: %v", err)
	}
	if highest != 2 {
		t.Errorf("HighestSequenceNr = %d, want 2", highest)
	}

	var replayed []persistence.PersistentRepr
	if err := j.ReplayMessages(ctx, pid, 1, 2, 0, func(r persistence.PersistentRepr) {
		replayed = append(replayed, r)
	}); err != nil {
		t.Fatalf("ReplayMessages: %v", err)
	}
	if len(replayed) != 2 {
		t.Fatalf("replayed %d events, want 2", len(replayed))
	}
	if ev, ok := replayed[0].Payload.(OrderPlaced); !ok || ev.Item != "apple" {
		t.Errorf("replayed[0] = %v, want OrderPlaced{apple}", replayed[0].Payload)
	}
	if ev, ok := replayed[1].Payload.(OrderPlaced); !ok || ev.Item != "banana" {
		t.Errorf("replayed[1] = %v, want OrderPlaced{banana}", replayed[1].Payload)
	}
}

type State struct{ Counter int }

func TestSpanner_SnapshotStore_SaveAndLoad(t *testing.T) {
	f := getFixture(t)
	client := newClient(t, f)

	codec := spannerstore.NewJSONCodec()
	codec.Register(State{})

	ss := spannerstore.NewSpannerSnapshotStore(client, codec)
	ctx := context.Background()
	const pid = "spanner-snap-actor"

	meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: 5}
	if err := ss.SaveSnapshot(ctx, meta, State{Counter: 99}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	snap, err := ss.LoadSnapshot(ctx, pid, persistence.LatestSnapshotCriteria())
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if snap == nil {
		t.Fatal("LoadSnapshot returned nil, want snapshot")
	}
	if snap.Metadata.SequenceNr != 5 {
		t.Errorf("SequenceNr = %d, want 5", snap.Metadata.SequenceNr)
	}
	st, ok := snap.Snapshot.(State)
	if !ok {
		t.Fatalf("Snapshot type = %T, want State", snap.Snapshot)
	}
	if st.Counter != 99 {
		t.Errorf("Counter = %d, want 99", st.Counter)
	}
}
