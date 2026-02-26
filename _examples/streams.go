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

	// Create separate streams for different concerns
	auditMQ, err := mq.Stream("audit")
	if err != nil {
		panic(err)
	}

	metricsMQ, err := mq.Stream("metrics")
	if err != nil {
		panic(err)
	}

	eventsMQ, err := mq.Stream("events")
	if err != nil {
		panic(err)
	}

	// Subscribe on each stream
	auditSub, err := auditMQ.Subscribe("user.action")
	if err != nil {
		panic(err)
	}

	metricsSub, err := metricsMQ.Subscribe("cpu.usage")
	if err != nil {
		panic(err)
	}

	eventsSub, err := eventsMQ.Subscribe("order.created")
	if err != nil {
		panic(err)
	}

	// Start goroutines to receive messages
	go func() {
		for msg := range auditSub.Chan() {
			fmt.Printf("[AUDIT] %s: %s\n", msg.Topic, string(msg.Payload))
		}
	}()

	go func() {
		for msg := range metricsSub.Chan() {
			fmt.Printf("[METRICS] %s: %s\n", msg.Topic, string(msg.Payload))
		}
	}()

	go func() {
		for msg := range eventsSub.Chan() {
			fmt.Printf("[EVENTS] %s: %s\n", msg.Topic, string(msg.Payload))
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Publish to each stream
	auditMQ.Publish("user.action", []byte("user login"))
	metricsMQ.Publish("cpu.usage", []byte("45%"))
	eventsMQ.Publish("order.created", []byte(`{"order_id": 123}`))

	time.Sleep(500 * time.Millisecond)

	// Messages are isolated per stream
	// The default stream won't receive these messages
	sub, _ := mq.Subscribe("user.action")
	go func() {
		for msg := range sub.Chan() {
			fmt.Printf("[DEFAULT] %s: %s\n", msg.Topic, string(msg.Payload))
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// This only goes to the default stream, not auditMQ
	mq.Publish("user.action", []byte("default stream action"))

	time.Sleep(500 * time.Millisecond)

	auditSub.Unsubscribe()
	metricsSub.Unsubscribe()
	eventsSub.Unsubscribe()
	sub.Unsubscribe()

	time.Sleep(100 * time.Millisecond)
}
