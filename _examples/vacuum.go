package main

import (
	"fmt"
	"github.com/modfin/delta"
	"time"
)

func main() {
	// Example 1: Vacuum based on age (remove messages older than 5 seconds)
	mq, err := delta.New(
		delta.URITemp(),
		delta.DBRemoveOnClose(),
		delta.WithVacuum(delta.VacuumOnAge(5*time.Second), 2*time.Second),
	)
	if err != nil {
		panic(err)
	}

	// Publish some messages
	for i := 0; i < 3; i++ {
		mq.Publish("data", []byte(fmt.Sprintf("old-message-%d", i)))
	}

	fmt.Println("Published 3 messages, waiting 6 seconds for them to age...")
	time.Sleep(6 * time.Second)

	// Publish more messages after the vacuum interval
	for i := 0; i < 2; i++ {
		mq.Publish("data", []byte(fmt.Sprintf("new-message-%d", i)))
	}

	fmt.Println("Published 2 new messages")

	// Try to subscribe from the beginning - should only see new messages
	sub, err := mq.SubscribeFrom("data", time.Now().Add(-10*time.Second))
	if err != nil {
		panic(err)
	}

	count := 0
	go func() {
		for msg := range sub.Chan() {
			count++
			fmt.Printf("Received: %s\n", string(msg.Payload))
		}
	}()

	time.Sleep(3 * time.Second)
	fmt.Printf("Total messages received: %d (old messages were vacuumed)\n", count)

	sub.Unsubscribe()
	mq.Close()
	time.Sleep(100 * time.Millisecond)

	// Example 2: Vacuum keeping only N most recent messages
	fmt.Println("\n--- VacuumKeepN Example ---")
	mq2, err := delta.New(
		delta.URITemp(),
		delta.DBRemoveOnClose(),
		delta.WithVacuum(delta.VacuumKeepN(5), 1*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer mq2.Close()

	// Publish 10 messages
	for i := 0; i < 10; i++ {
		mq2.Publish("limited", []byte(fmt.Sprintf("msg-%d", i)))
	}

	fmt.Println("Published 10 messages, vacuum will keep only 5 most recent")
	time.Sleep(2 * time.Second)

	// Subscribe to see which messages remain
	sub2, _ := mq2.SubscribeFrom("limited", time.Now().Add(-1*time.Hour))
	go func() {
		for msg := range sub2.Chan() {
			fmt.Printf("Kept: %s\n", string(msg.Payload))
		}
	}()

	time.Sleep(1 * time.Second)
	sub2.Unsubscribe()
}
