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
	"time"

	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/persistence"
)

// --- Messages ---

type Command interface{}
type Increment struct{}
type GetValue struct {
	ReplyTo actor.TypedActorRef[int]
}

type Event struct {
	Delta int
}

type State struct {
	Value int
}

// --- Persistent Actor ---

func Counter(persistenceID string, journal persistence.Journal, snaps persistence.SnapshotStore) *gekka.EventSourcedBehavior[Command, Event, State] {
	return &gekka.EventSourcedBehavior[Command, Event, State]{
		PersistenceID: persistenceID,
		Journal:       journal,
		SnapshotStore: snaps,
		InitialState:  State{Value: 0},
		CommandHandler: func(ctx actor.TypedContext[Command], state State, cmd Command) actor.Effect[Event, State] {
			switch m := cmd.(type) {
			case Increment:
				fmt.Println("Counter: persisting increment event")
				return actor.Persist[Event, State](Event{Delta: 1})
			case GetValue:
				m.ReplyTo.Tell(state.Value)
				return actor.None[Event, State]()
			}
			return actor.None[Event, State]()
		},
		EventHandler: func(state State, event Event) State {
			state.Value += event.Delta
			fmt.Printf("Counter: event applied, current value: %d\n", state.Value)
			return state
		},
		SnapshotInterval: 3,
	}
}

func main() {
	// 1. Setup persistence backends
	journal := persistence.NewInMemoryJournal()
	snaps := persistence.NewInMemorySnapshotStore()

	// 2. Initialize actor system
	system, err := gekka.NewActorSystem("PersistenceSystem")
	if err != nil {
		log.Fatal(err)
	}

	persistenceID := "my-counter"

	// 3. Spawn persistent actor
	fmt.Println("--- Spawning Counter ---")
	counter, err := gekka.SpawnPersistent(system, Counter(persistenceID, journal, snaps), "counter")
	if err != nil {
		log.Fatal(err)
	}

	// 4. Send some increments
	counter.Tell(Increment{})
	counter.Tell(Increment{})

	time.Sleep(100 * time.Millisecond)

	// 5. Query value
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	val, _ := gekka.Ask(ctx, counter, 3*time.Second, func(replyTo actor.TypedActorRef[int]) Command {
		return GetValue{ReplyTo: replyTo}
	})
	fmt.Printf("Current value: %d\n", val)

	// 6. Demonstrate recovery: stop and restart
	fmt.Println("\n--- Restarting Counter (Recovery) ---")
	system.Stop(counter.Untyped().(gekka.ActorRef))
	time.Sleep(100 * time.Millisecond)

	counter2, _ := gekka.SpawnPersistent(system, Counter(persistenceID, journal, snaps), "counter-new")
	
	// Increment again (this will trigger snapshot if it reaches 3)
	counter2.Tell(Increment{})
	time.Sleep(100 * time.Millisecond)

	val2, _ := gekka.Ask(ctx, counter2, 3*time.Second, func(replyTo actor.TypedActorRef[int]) Command {
		return GetValue{ReplyTo: replyTo}
	})
	fmt.Printf("Value after recovery and increment: %d\n", val2)

	fmt.Println("\nExample completed.")
}
