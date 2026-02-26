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

	// subscribe to topic a.b.c with queue name "main-queue".
	// queue name is used to identify the queue, if multiple subscribe to the same topic with the same queue name
	// a message will be delivered to only one of the subscribers.
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

	// start a go routine for each subscription
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
				fmt.Printf("QueueSub %d> %s ~> %s\n", i, msg.Topic, string(msg.Payload))
			}
		}(i, sub)
	}

	time.Sleep(time.Second) // let the subscribers start

	_, err = mq.Publish("a.b.c", []byte("hello 1"))
	if err != nil {
		panic(err)
	}
	_, err = mq.Publish("a.b.c", []byte("hello 2"))
	if err != nil {
		panic(err)
	}
	_, err = mq.Publish("a.b.c", []byte("hello 3"))
	if err != nil {
		panic(err)
	}
	_, err = mq.Publish("a.b.c", []byte("hello 4"))
	if err != nil {
		panic(err)
	}

	time.Sleep(time.Second)
	// Output:
	//  QueueSub 1> a.b.c ~> hello 1
	//  QueueSub 0> a.b.c ~> hello 4
	//  QueueSub 2> a.b.c ~> hello 2
	//  QueueSub 2> a.b.c ~> hello 3

	sub0.Unsubscribe()
	sub1.Unsubscribe()
	sub2.Unsubscribe()

	time.Sleep(time.Second)
	// Output:
	//  queue-sub 2 is closed
	//  queue-sub 0 is closed
	//  queue-sub 1 is closed
}
