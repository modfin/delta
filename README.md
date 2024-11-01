# Delta

[![goreportcard.com](https://goreportcard.com/badge/github.com/modfin/delta)](https://goreportcard.com/report/github.com/modfin/delta)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/modfin/delta)](https://pkg.go.dev/github.com/modfin/delta)

Delta is a message queue backed by SQLite for persistence. It provides a simple and efficient way to handle message queuing with the durability and reliability of SQLite.

It is somewhat inspired by nats.io, but is single instance and persistence from the start.

## Features

- **Persistence**: Messages are stored in an SQLite database, ensuring durability.
- **Pub/Sub**: Supports publish/subscribe messaging pattern.
- **Queue**: Supports message queuing with load balancing.
- **Request/Reply**: Supports request/reply messaging pattern.
- **Multiple Streams**: Allows creating multiple streams for different namespaces.
- **Globs**: Supports subscribing on a Glob for pattern-based subscriptions.

## Installation

To install Delta, use `go get`:

```sh
go get github.com/modfin/delta
```

## Usage

### Creating a Message Queue Instance

```go
package main

import (
	"github.com/modfin/delta"
	"log/slog"
)

func main() {
	mq, err := delta.New("file:delta.db", delta.WithLogger(slog.Default()))
	if err != nil {
		panic(err)
	}
	defer mq.Close()
}
```


### Pub/Sub

The publish/subscribe messaging pattern allows multiple subscribers to receive messages from a topic, "One to many" or broadcasting

[![](https://mermaid.ink/img/pako:eNptjrEKgzAQhl8l3FRBB9vNoVC0nTqU2jFLjKcGEiMxoZTouzdSCxV6t9x9_8dxHriuETJopH7yjhlLrnfak1An72-ukmLs0MzzykiSHMmkxjadSL7bPfQgeBR9wvw3LLwvXTVyIyo0JP0e2DjnjbP_61w2zmFxliYEYlBoFBN1-N4vhILtUCGFLIw1NsxJS4H2c1CZs7p89RwyaxzGYLRrO8gaJsewuaFmFgvBWsPUSuc3JgZclg?type=png)](https://mermaid.live/edit#pako:eNptjrEKgzAQhl8l3FRBB9vNoVC0nTqU2jFLjKcGEiMxoZTouzdSCxV6t9x9_8dxHriuETJopH7yjhlLrnfak1An72-ukmLs0MzzykiSHMmkxjadSL7bPfQgeBR9wvw3LLwvXTVyIyo0JP0e2DjnjbP_61w2zmFxliYEYlBoFBN1-N4vhILtUCGFLIw1NsxJS4H2c1CZs7p89RwyaxzGYLRrO8gaJsewuaFmFgvBWsPUSuc3JgZclg)



### Publishing Messages

Publish a message to a specific topic using the `Publish` method. No need to create the topic on beforehand or declare it.

```go
pub, err := mq.Publish("a.b.c.d", []byte("hello world"))
if err != nil {
    panic(err)
}
```

### Subscribing to Messages

Subscribe to a specific topic using the `Subscribe` method. The subscription can use a glob pattern to match multiple topics.
Example might be `location.*` will match `location.us`, `location.eu` etc. but not `location.us.new-york`. For this we 
can use `**` which basically translates to prefix match, eg `location.**` will match `location.us`, `location.us.new-york`, `location.us.new-york.new-york-city` and so on.


This can be done using a chan or by using the `Next` method which will block until a message is received or subscription is closed .

```go
sub, err := mq.Subscribe("a.b.*.d")
if err != nil {
    panic(err)
}

go func() {
    for msg := range sub.Chan() { // or 'msg, ok := sub.Next()' can be used
        fmt.Printf("[Chan] Received message: %s <- %s\n", msg.Topic, string(msg.Payload))
    }
}()


```


### Using Queues

A queue is a group of subscribers that receive messages from a topic. Each message is delivered to _only one_ subscriber.
It does not put any constraint how things are being published to the queue, it is only the consumption that is load balanced.

[![](https://mermaid.ink/img/pako:eNplzz0LgzAQBuC_ctxkwQ7azaFQtJ06lLZjlhhPDRhTYkIp0f_eSL-Q5uC4e3k4iEehK8IM607fRcuNheOZ9RDezvuTKzs5tGSm6Z3Ber2FUQ1NEoeWzm0zQh5FV32TYrV6sfzL0hEK7y-uHISRJRlIPqd-JhzYL0z6b5IRDguzmc1cABijIqO4rMI__JwwtC0pYpiFsaKau84yZP0UKHdWXx69wMwaRzEa7ZoWs5p3Q9jcreKWCskbw9U7nZ7ap1_k?type=png)](https://mermaid.live/edit#pako:eNplzz0LgzAQBuC_ctxkwQ7azaFQtJ06lLZjlhhPDRhTYkIp0f_eSL-Q5uC4e3k4iEehK8IM607fRcuNheOZ9RDezvuTKzs5tGSm6Z3Ber2FUQ1NEoeWzm0zQh5FV32TYrV6sfzL0hEK7y-uHISRJRlIPqd-JhzYL0z6b5IRDguzmc1cABijIqO4rMI__JwwtC0pYpiFsaKau84yZP0UKHdWXx69wMwaRzEa7ZoWs5p3Q9jcreKWCskbw9U7nZ7ap1_k)

```go

for i := 0; i < 3; i++ {
	i:= i
    sub, err := mq.Queue("a.b.*.d", "queue1") // queue1 is the name of the queue which is used to identify it
    if err != nil {
        panic(err)
    }

    go func() {
        for msg := range sub.Chan() {
            fmt.Printf("Received message from queue by worker %d: %s\n", 
				i, 
				string(msg.Payload),
			)
        }
    }()
}

```

### Request/Reply

The request/reply messaging pattern allows a client to send a request to a service and receive a response.

[![](https://mermaid.ink/img/pako:eNpVj08LwjAMxb9KyGkDFdTbDoL45-RBpsdeui7bCu06uhYZ3b67VSdicsl7v5dAAgpTEmZYKfMQDbcOLjlrIdY-hKsvlOwbstM0e7Bc7mDUfT3CIUnuppMiTT_sAKvIjiHcfNELKwuysP4ufuDpD25-8Hf1_BfZfiPndyRPkpw6NaTpW-5Z-2oAXKAmq7ks4yfh5TB0DWlimMWxpIp75RiydopR7p25Da3AzFlPC7TG1w1mFVd9VL4ruaOj5LXlenanJxw1XZc?type=png)](https://mermaid.live/edit#pako:eNpVj08LwjAMxb9KyGkDFdTbDoL45-RBpsdeui7bCu06uhYZ3b67VSdicsl7v5dAAgpTEmZYKfMQDbcOLjlrIdY-hKsvlOwbstM0e7Bc7mDUfT3CIUnuppMiTT_sAKvIjiHcfNELKwuysP4ufuDpD25-8Hf1_BfZfiPndyRPkpw6NaTpW-5Z-2oAXKAmq7ks4yfh5TB0DWlimMWxpIp75RiydopR7p25Da3AzFlPC7TG1w1mFVd9VL4ruaOj5LXlenanJxw1XZc)


```go

// test is the name of the queue which is used to identify it
// it is important to use a queue group to ensure that the request is 
//not hadled by multiple workers
for i := 0; i < 3; i++ {
    i:= i
    sub, err := mq.Queue("greet.*", "test") 
    if err != nil {
        panic(err)
    }
    go func() {
        for msg := range sub.Chan() {
            _, name, _ := strings.Cut(msg.Topic, ".")
            _, err := msg.Reply([]byte(fmt.Sprintf("from worker %d > hello %s, ", i, name))
            if err != nil {
                panic(err)
            }
        }
    }()
}


resp, err := mq.Request(context.Background(), "greet.alice", nil)
if err != nil {
    panic(err)
}
msg, ok := resp.Next()
if ok {
    fmt.Printf("Received reply: %s\n", string(msg.Payload))
}
```



### Subscription From

The `SubscribeFrom` method allows you to subscribe to messages from a specific topic starting from a given historical time. This is useful when you want to process messages that were published after a certain point in time or you want to re-process messages.

#### Example Usage

```go

// Publish some messages
for i := 0; i < 10; i++ {
    payload := []byte("message " + strconv.Itoa(i))
    _, err := mq.Publish("example.topic", payload)
    if err != nil {
        panic(err)
    }
}

// Subscribe from a specific time
from := time.Now()

// Publish more messages
for i := 10; i < 20; i++ {
    payload := []byte("message " + strconv.Itoa(i))
    _, err := mq.Publish("example.topic", payload)
    if err != nil {
        panic(err)
    }
}

sub, err := mq.SubscribeFrom("example.topic", from)
if err != nil {
    panic(err)
}
// Read messages from the subscription
for i := 10; i < 20; i++ {
    msg, ok := sub.Next()
    if !ok {
        panic("failed to read message")
    }
    fmt.Println("Received historic message:", string(msg.Payload))
}

```

## Benchmarks

Some benchmarks, but remember, generic benchmarks are shit :) Anyway, it seems it performs decently.
It fans out with very little overhead and seems and writes per second seems to be pretty stable (since its singe threaded.).  

```text

// Performed with waking up readers, which takes a performance hit on writers.

BenchmarkMultipleSubscribers/1-22                     8_242 read-msg/s    9_196 write-msg/s
BenchmarkMultipleSubscribers/4-22                    31_810 read-msg/s    8_037 write-msg/s
BenchmarkMultipleSubscribers/num-cpu_(22)-22        212_433 read-msg/s    9_774 write-msg/s
BenchmarkMultipleSubscribers/2x_num-cpu_(44)-22     411_696 read-msg/s    9_631 write-msg/s


BenchmarkMultipleSubscribersSize/_0.1mb-22   1_313 read-MB/s     13_130 read-msg/s     59.7 write-MB/s     597.3 write-msg/s
BenchmarkMultipleSubscribersSize/_1.0mb-22   3_517 read-MB/s      3_517 read-msg/s    160.6 write-MB/s     160.6 write-msg/s
BenchmarkMultipleSubscribersSize/10.0mb-22   3_856 read-MB/s        386 read-msg/s    184.5 write-MB/s      18.5 write-msg/s

```


## License

This project is licensed under the MIT License. See the `LICENSE` file for details.