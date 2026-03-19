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
	"github.com/sopranoworks/gekka/actor/typed"
)

// --- Messages ---

// Greet is the message sent to the Greeter actor.
type Greet struct {
	Whom    string
	ReplyTo typed.TypedActorRef[Greeting]
}

// Greeting is the reply message from the Greeter actor.
type Greeting struct {
	Message string
}

// --- Greeter Actor ---

// Greeter behavior handles Greet messages and replies with a Greeting.
func Greeter() typed.Behavior[Greet] {
	return func(ctx typed.TypedContext[Greet], msg Greet) typed.Behavior[Greet] {
		ctx.Log().Info("Greeter received Greet", "whom", msg.Whom)
		msg.ReplyTo.Tell(Greeting{
			Message: fmt.Sprintf("Hello %s!", msg.Whom),
		})
		return typed.Same[Greet]()
	}
}

// --- GreeterBot Actor ---

// GreeterBot behavior initiates greetings and receives replies.
func GreeterBot(max int, greeter typed.TypedActorRef[Greet]) typed.Behavior[Greeting] {
	return typed.Setup(func(ctx typed.TypedContext[Greeting]) typed.Behavior[Greeting] {
		// Start the first greeting
		ctx.Log().Info("GreeterBot starting", "max", max)
		greeter.Tell(Greet{Whom: "Gekka", ReplyTo: ctx.Self()})
		return bot(0, max, greeter)
	})
}

func bot(greetingCount, max int, greeter typed.TypedActorRef[Greet]) typed.Behavior[Greeting] {
	return func(ctx typed.TypedContext[Greeting], msg Greeting) typed.Behavior[Greeting] {
		n := greetingCount + 1
		ctx.Log().Info("GreeterBot received Greeting", "count", n, "message", msg.Message)
		if n >= max {
			ctx.Log().Info("GreeterBot reached max greetings, stopping")
			return typed.Stopped[Greeting]()
		} else {
			greeter.Tell(Greet{Whom: "Gekka", ReplyTo: ctx.Self()})
			return bot(n, max, greeter)
		}
	}
}

func main() {
	// 1. Create the actor system
	system, err := gekka.NewActorSystem("TypedGreeterSystem")
	if err != nil {
		log.Fatal(err)
	}

	// 2. Spawn the Greeter actor using Spawn
	greeter, err := gekka.Spawn(system, Greeter(), "greeter")
	if err != nil {
		log.Fatal(err)
	}

	// 3. Demonstrate gekka.Ask for request-reply from outside the actor system
	fmt.Println("--- Demonstrating gekka.Ask ---")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	reply, err := gekka.Ask(ctx, greeter, 3*time.Second, func(replyTo typed.TypedActorRef[Greeting]) Greet {
		return Greet{Whom: "Typed Ask", ReplyTo: replyTo}
	})
	if err != nil {
		log.Fatalf("Ask failed: %v", err)
	}
	fmt.Printf("Ask reply: %s\n", reply.Message)

	// 4. Spawn the GreeterBot actor to demonstrate actor-to-actor communication
	fmt.Println("\n--- Demonstrating GreeterBot ---")
	_, err = gekka.Spawn(system, GreeterBot(3, greeter), "bot")
	if err != nil {
		log.Fatal(err)
	}

	// Wait for the bot to finish (it stops after 3 greetings)
	time.Sleep(1 * time.Second)
	fmt.Println("\nExample completed.")
}
