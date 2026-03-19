/*
 * main.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package main

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor/typed"
	ptyped "github.com/sopranoworks/gekka/persistence/typed"
	"github.com/sopranoworks/gekka/persistence"
	"github.com/sopranoworks/gekka/sharding"
)

// --- Messages ---

type Command interface{}
type AddItem struct {
	CartId string
	Item   string
}
type GetItems struct {
	CartId  string
	ReplyTo typed.TypedActorRef[[]string]
}

type Event struct {
	Item string
}

type State struct {
	Items []string
}

// --- Sharding Setup ---

func ExtractCartEntityId(msg any) (sharding.EntityId, sharding.ShardId, any) {
	switch m := msg.(type) {
	case AddItem:
		// Simple sharding: cartId is entityId, shardId is first char of cartId
		shardId := "shard-" + string(m.CartId[0])
		return m.CartId, shardId, m
	case GetItems:
		shardId := "shard-" + string(m.CartId[0])
		return m.CartId, shardId, m
	case sharding.ShardingEnvelope:
		return m.EntityId, m.ShardId, m.Message
	}
	return "", "", msg
}

func ShoppingCartBehavior(journal persistence.Journal) func(id string) *gekka.EventSourcedBehavior[Command, Event, State] {
	return func(id string) *gekka.EventSourcedBehavior[Command, Event, State] {
		return &gekka.EventSourcedBehavior[Command, Event, State]{
			PersistenceID: "cart-" + id,
			Journal:       journal,
			InitialState:  State{Items: []string{}},
			CommandHandler: func(ctx typed.TypedContext[Command], state State, cmd Command) ptyped.Effect[Event, State] {
				switch m := cmd.(type) {
				case AddItem:
					return ptyped.Persist[Event, State](Event{Item: m.Item})
				case GetItems:
					m.ReplyTo.Tell(state.Items)
					return ptyped.None[Event, State]()
				}
				return ptyped.None[Event, State]()
			},
			EventHandler: func(state State, event Event) State {
				state.Items = append(state.Items, event.Item)
				return state
			},
		}
	}
}

func main() {
	// 1. Setup persistence
	journal := persistence.NewInMemoryJournal()

	// 2. Start two nodes (simulated)
	system1, _ := gekka.NewCluster(gekka.ClusterConfig{SystemName: "ShardingSystem", Port: 2551})
	system2, _ := gekka.NewCluster(gekka.ClusterConfig{SystemName: "ShardingSystem", Port: 2552})

	system2.Join("127.0.0.1", 2551)

	// Register user types for sharding serialization
	system1.RegisterType("main.AddItem", reflect.TypeOf(AddItem{}))
	system1.RegisterType("main.GetItems", reflect.TypeOf(GetItems{}))
	system1.RegisterType("[]string", reflect.TypeOf([]string{}))
	system2.RegisterType("main.AddItem", reflect.TypeOf(AddItem{}))
	system2.RegisterType("main.GetItems", reflect.TypeOf(GetItems{}))
	system2.RegisterType("[]string", reflect.TypeOf([]string{}))

	fmt.Println("Waiting for cluster to form...")
	start := time.Now()
	for {
		if system1.IsUp() && system2.IsUp() {
			break
		}
		if time.Since(start) > 20*time.Second {
			log.Fatal("Timed out waiting for cluster to form")
		}
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Println("Cluster is UP")

	// 3. Spawn sharded entities on both nodes
	settings := gekka.ShardingSettings{NumberOfShards: 10}

	factory := ShoppingCartBehavior(journal)

	cartRegion1, err := gekka.StartSharding(system1, "ShoppingCart", factory, ExtractCartEntityId, settings)
	if err != nil {
		log.Fatal(err)
	}

	_, err = gekka.StartSharding(system2, "ShoppingCart", factory, ExtractCartEntityId, settings)
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	// 4. Send messages to entities via regions
	fmt.Println("--- Sending messages to carts ---")

	// cartA should go to one shard, cartB to another
	cartRegion1.Tell(AddItem{CartId: "cartA", Item: "Apple"})
	cartRegion1.Tell(AddItem{CartId: "cartB", Item: "Banana"})

	time.Sleep(5 * time.Second)

	// 5. Query carts
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	fmt.Println("--- Querying cartA ---")
	itemsA, err := gekka.Ask(ctx, gekka.ToTyped[Command](cartRegion1.Region), 3*time.Second, func(replyTo typed.TypedActorRef[[]string]) Command {
		return GetItems{CartId: "cartA", ReplyTo: replyTo}
	})
	if err != nil {
		fmt.Printf("Query cartA failed: %v\n", err)
	} else {
		fmt.Printf("CartA items: %v\n", itemsA)
	}

	fmt.Println("--- Querying cartB ---")
	itemsB, err := gekka.Ask(ctx, gekka.ToTyped[Command](cartRegion1.Region), 3*time.Second, func(replyTo typed.TypedActorRef[[]string]) Command {
		return GetItems{CartId: "cartB", ReplyTo: replyTo}
	})
	if err != nil {
		fmt.Printf("Query cartB failed: %v\n", err)
	} else {
		fmt.Printf("CartB items: %v\n", itemsB)
	}

	fmt.Println("\nExample completed.")
	system1.Shutdown()
	system2.Shutdown()
}
