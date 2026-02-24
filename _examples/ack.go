package main

import (
	"fmt"
	"github.com/modfin/delta"
	"time"
)

func main() {
	// Create MQ with VacuumOnReadAck strategy
	// Messages will only be deleted after Ack() is called
	mq, err := delta.New(
		delta.URITemp(),
		delta.DBRemoveOnClose(),
		delta.WithVacuum(delta.VacuumOnReadAck, 2*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer mq.Close()

	// Create a queue group for reliable processing
	sub1, err := mq.Queue("jobs", "workers")
	if err != nil {
		panic(err)
	}

	sub2, err := mq.Queue("jobs", "workers")
	if err != nil {
		panic(err)
	}

	// Worker 1: Ack successful messages
	go func() {
		for msg := range sub1.Chan() {
			fmt.Printf("Worker 1 processing: %s\n", string(msg.Payload))

			// Simulate processing
			time.Sleep(100 * time.Millisecond)

			// Acknowledge successful processing
			// This marks the message for vacuum cleanup
			if err := msg.Ack(); err != nil {
				fmt.Printf("Worker 1 failed to ack: %v\n", err)
			} else {
				fmt.Printf("Worker 1 acknowledged: %s\n", string(msg.Payload))
			}
		}
	}()

	// Worker 2: Ack all messages
	go func() {
		for msg := range sub2.Chan() {
			fmt.Printf("Worker 2 processing: %s\n", string(msg.Payload))

			// Acknowledge
			if err := msg.Ack(); err != nil {
				fmt.Printf("Worker 2 failed to ack: %v\n", err)
			}
		}
	}()

	// Publish some jobs
	for i := 0; i < 5; i++ {
		mq.Publish("jobs", []byte(fmt.Sprintf("job-%d", i)))
	}

	fmt.Println("Published 5 jobs")

	// Wait for processing
	time.Sleep(3 * time.Second)

	// Publish more jobs after vacuum should have run
	fmt.Println("\nPublishing more jobs (previous ack'd messages should be vacuumed)...")
	for i := 5; i < 8; i++ {
		mq.Publish("jobs", []byte(fmt.Sprintf("job-%d", i)))
	}

	time.Sleep(3 * time.Second)

	fmt.Println("\nDone - ack'd messages are periodically vacuumed")

	sub1.Unsubscribe()
	sub2.Unsubscribe()
	time.Sleep(100 * time.Millisecond)
}
