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

	size := 5

	for i := range size {
		_, err = mq.Publish("a.b.c", []byte(fmt.Sprintf("pre-sub-1 msg:%d", i+size*0)))
		if err != nil {
			panic(err)
		}
	}

	from := time.Now() // get the current time, all message written before this will be ignored
	fmt.Println("from:", from.In(time.UTC).Format(time.StampNano))
	// Output:
	//  from: Nov  1 12:57:43.958881440

	for i := range size {
		_, err = mq.Publish("a.b.c", []byte(fmt.Sprintf("pre-sub-2 msg:%d", i+size*1)))
		if err != nil {
			panic(err)
		}
	}

	// subscribe to topic a.b.c but reads historic writes first from [from-
	sub, err := mq.SubscribeFrom("a.b.c", from)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			msg, ok := sub.Next()
			if !ok {
				fmt.Println("subscription is closed")
				break
			}
			fmt.Printf("Sub > %s ~> %s, at: %s\n", msg.Topic, string(msg.Payload), msg.At.In(time.UTC).Format(time.StampNano))
		}
	}()

	time.Sleep(time.Second) // let the subscribers start and read the historic writes
	// Output:
	//  Sub > a.b.c ~> pre-sub-2 msg:5, at: Nov  1 12:57:43.958967102
	//  Sub > a.b.c ~> pre-sub-2 msg:6, at: Nov  1 12:57:43.959046218
	//  Sub > a.b.c ~> pre-sub-2 msg:7, at: Nov  1 12:57:43.959123491
	//  Sub > a.b.c ~> pre-sub-2 msg:8, at: Nov  1 12:57:43.959196468
	//  Sub > a.b.c ~> pre-sub-2 msg:9, at: Nov  1 12:57:43.959238839

	fmt.Println()
	for i := range size { // write some more messages
		_, err = mq.Publish("a.b.c", []byte(fmt.Sprintf("post-sub msg:%d", i+size*2)))
		if err != nil {
			panic(err)
		}
	}
	time.Sleep(time.Second) // let the subscribers read the just written messages
	// Output:
	//  Sub > a.b.c ~> post-sub msg:10, at: Nov  1 12:57:44.960341329
	//  Sub > a.b.c ~> post-sub msg:12, at: Nov  1 12:57:44.960957248
	//  Sub > a.b.c ~> post-sub msg:13, at: Nov  1 12:57:44.961161357
	//  Sub > a.b.c ~> post-sub msg:14, at: Nov  1 12:57:44.961301796
	//  Sub > a.b.c ~> post-sub msg:11, at: Nov  1 12:57:44.960793975

	sub.Unsubscribe()
	time.Sleep(time.Second) // let the subscribers close
	// Output:
	//  subscription is closed
}
