package main

import (
	"fmt"
	"github.com/modfin/delta"
	"time"
)

func main() {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	if err != nil {
		panic(err)
	}
	defer mq.Close()

	// Subscribe to receive messages
	sub, err := mq.Subscribe("updates")
	if err != nil {
		panic(err)
	}

	received := 0
	go func() {
		for msg := range sub.Chan() {
			received++
			fmt.Printf("Received %d: %s\n", received, string(msg.Payload))
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Async publishing - returns immediately without waiting
	fmt.Println("Starting async publishes...")
	start := time.Now()

	var pubs []*delta.Publication
	for i := 0; i < 100; i++ {
		pub := mq.PublishAsync("updates", []byte(fmt.Sprintf("message-%d", i)))
		pubs = append(pubs, pub)
	}

	fmt.Printf("Published 100 messages in %v (async)\n", time.Since(start))

	// Wait for all publishes to complete
	fmt.Println("Waiting for publishes to complete...")
	for i, pub := range pubs {
		<-pub.Done()
		if pub.Err != nil {
			fmt.Printf("Publish %d failed: %v\n", i, pub.Err)
		}
	}

	fmt.Printf("All publishes completed in %v\n", time.Since(start))

	// Wait for all messages to be received
	time.Sleep(500 * time.Millisecond)
	fmt.Printf("Total messages received: %d\n", received)

	sub.Unsubscribe()
	time.Sleep(100 * time.Millisecond)
}
