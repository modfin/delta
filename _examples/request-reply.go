package main

import (
	"context"
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

	// subscribe to topic a.b.c with queue name "main-queue".
	// queue name is used to identify the queue, if multiple subscribe to the same topic with the same queue name
	// a message will be delivered to only one of the subscribers.
	//
	// Having a classic request-reply pattern with multiple subscribers and only wanting response should use a queue
	sub0, err := mq.Queue("a.b.c", "main-queue")
	if err != nil {
		panic(err)
	}

	sub1, err := mq.Queue("a.b.c", "main-queue")
	if err != nil {
		panic(err)
	}

	sub2, err := mq.Queue("a.b.c", "main-queue")
	if err != nil {
		panic(err)
	}

	for i, sub := range []*delta.Subscription{sub0, sub1, sub2} {
		i := i
		sub := sub

		go func(i int, sub *delta.Subscription) {
			for {
				msg, ok := sub.Next()
				if !ok {
					fmt.Println("queue-sub", i, "is closed")
					break
				}
				fmt.Printf("QueueSub %d got message, replying...\n", i)
				txt := fmt.Sprintf("QueueSub %d> %s ~> %s", i, msg.Topic, string(msg.Payload))
				_, err := msg.Reply([]byte(txt))
				if err != nil {
					panic(err)
				}
			}
		}(i, sub)
	}

	time.Sleep(time.Second) // let the subscribers start

	ctx := context.Background()

	for i := 0; i < 4; i++ {
		start := time.Now()
		sub, err := mq.Request(ctx, "a.b.c", []byte(fmt.Sprintf("request %d", i)))
		if err != nil {
			panic(err)
		}
		req := time.Since(start)
		start = time.Now()
		msg, ok := sub.Next()
		if !ok {
			panic("no response")
		}
		rep := time.Since(start)
		fmt.Printf("Response %d> %s (req: %v, rep: %v)\n", i, string(msg.Payload), req, rep)
	}
	time.Sleep(time.Second)
	// Output:
	//  QueueSub 0 got message, replying...
	//  Response 0> QueueSub 0> a.b.c ~> request 0 (req: 299.268µs, rep: 2.195039ms)
	//
	//  QueueSub 2 got message, replying...
	//  Response 1> QueueSub 2> a.b.c ~> request 1 (req: 298.019µs, rep: 1.00283ms)
	//
	//  QueueSub 0 got message, replying...
	//  Response 2> QueueSub 0> a.b.c ~> request 2 (req: 282.532µs, rep: 463.578µs)
	//
	//  QueueSub 0 got message, replying...
	//  Response 3> QueueSub 0> a.b.c ~> request 3 (req: 71.356µs, rep: 270.517µs)

	sub0.Unsubscribe()
	sub1.Unsubscribe()
	sub2.Unsubscribe()

	time.Sleep(time.Second)
	// Output:
	//  queue-sub 0 is closed
	//  queue-sub 2 is closed
	//  queue-sub 1 is closed
}
