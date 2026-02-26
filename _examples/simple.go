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

	// subscribe to topic a.b.c
	sub0, err := mq.Subscribe("a.b.c")
	if err != nil {
		panic(err)
	}

	// subscribe to all topics starting with a. (prefix matching)
	sub1, err := mq.Subscribe("a.**")
	if err != nil {
		panic(err)
	}

	// subscribe to all topics starting with a.
	// and have exactly one more level, eg.
	//   a.b, a.c and a.d
	//   but not a.b.c
	sub2, err := mq.Subscribe("a.*")
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
					fmt.Println("subscription", i, "is closed")
					break
				}
				fmt.Printf("Sub %d> %s ~> %s\n", i, msg.Topic, string(msg.Payload))
			}
		}(i, sub)
	}

	time.Sleep(time.Second) // let the subscribers start

	_, err = mq.Publish("a.b", []byte("hello a.b"))
	if err != nil {
		panic(err)
	}
	_, err = mq.Publish("a.b.c", []byte("hello a.b.c"))
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second)
	// Output:
	//  Sub 1> a.b ~> hello a.b
	//  Sub 2> a.b ~> hello a.b
	//  Sub 1> a.b.c ~> hello a.b.c
	//  Sub 0> a.b.c ~> hello a.b.c

	sub0.Unsubscribe()
	sub1.Unsubscribe()
	sub2.Unsubscribe()
	time.Sleep(time.Second)
	// Output:
	//  subscription 2 is closed
	//  subscription 0 is closed
	//  subscription 1 is closed
}
