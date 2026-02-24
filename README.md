# Delta

[![goreportcard.com](https://goreportcard.com/badge/github.com/modfin/delta)](https://goreportcard.com/report/github.com/modfin/delta)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/modfin/delta)](https://pkg.go.dev/github.com/modfin/delta)

Delta is a lightweight, SQLite-backed message queue for Go. It provides Pub/Sub, load-balanced queues, request/reply patterns, and historical message replay—all with the durability and reliability of SQLite storage.

Inspired by NATS.io but designed for single-instance deployments with built-in persistence.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
- [API Reference](#api-reference)
- [Examples](#examples)
- [Configuration](#configuration)
- [Topic Patterns](#topic-patterns)
- [Benchmarks](#benchmarks)
- [License](#license)

## Features

- **Persistence**: All messages stored in SQLite with WAL mode for durability and performance
- **Pub/Sub**: Broadcast messages to multiple subscribers (one-to-many)
- **Queue**: Load-balanced message distribution across consumer groups (one-to-one)
- **Request/Reply**: Synchronous request/response pattern with automatic inbox routing
- **Multiple Streams**: Isolated namespaces within a single database
- **Historical Replay**: Subscribe from a specific point in time to re-process messages
- **Glob Patterns**: Flexible topic matching with `*` (single level) and `**` (multi-level) wildcards
- **Message Acknowledgment**: Manual acknowledgment for reliable message processing
- **Configurable Vacuuming**: Automatic cleanup with age-based, count-based, or ack-based strategies
- **Async Publishing**: Non-blocking publish operations for high-throughput scenarios

## Installation

```sh
go get github.com/modfin/delta
```

**Note**: Delta requires CGo due to the `github.com/mattn/go-sqlite3` dependency. Ensure you have a C compiler installed.

## Quick Start

```go
package main

import (
	"fmt"
	"github.com/modfin/delta"
)

func main() {
	// Create a temporary MQ (removed on close)
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	if err != nil {
		panic(err)
	}
	defer mq.Close()

	// Subscribe to a topic
	sub, err := mq.Subscribe("hello.world")
	if err != nil {
		panic(err)
	}
	defer sub.Unsubscribe()

	// Publish a message
	_, err = mq.Publish("hello.world", []byte("Hello, Delta!"))
	if err != nil {
		panic(err)
	}

	// Receive the message
	msg, ok := sub.Next()
	if !ok {
		panic("subscription closed")
	}

	fmt.Printf("Received: %s = %s\n", msg.Topic, string(msg.Payload))
	// Output: Received: hello.world = Hello, Delta!
}
```

## Core Concepts

### Topics

Topics are dot-separated strings (e.g., `orders.created`, `users.123.profile`). They support glob patterns:

- `*` matches a single level: `orders.*` matches `orders.created` but not `orders.123.processed`
- `**` matches multiple levels: `orders.**` matches `orders.created`, `orders.123.processed`, etc.

### Streams

Streams provide isolated namespaces within a single database. Each stream has its own message table:

```go
// Default stream
mq, _ := delta.New("file:myapp.db")

// Create a separate "audit" stream
auditMQ, _ := mq.Stream("audit")
auditMQ.Publish("user.login", []byte("alice logged in"))
```

### Message Delivery Guarantees

- **Pub/Sub**: At-least-once delivery to all active subscribers
- **Queue**: Exactly-one delivery within a consumer group
- **Request/Reply**: Exactly-one response from a queue group

## API Reference

### Constructor

#### `New(uri string, ops ...Op) (*MQ, error)`

Creates a new message queue instance. The `uri` parameter accepts SQLite URI formats:

- `file:path/to/db.sqlite` - File-based database
- `file::memory:` or `file::memory:?cache=shared` - In-memory database

**Options** (via `Op` functional options):

| Option | Description |
|--------|-------------|
| `DBSyncOff()` | Sets `synchronous = off` for better write performance (sacrifices durability) |
| `DBRemoveOnClose()` | Removes database files when MQ is closed (useful for temp databases) |
| `WithLogger(log *slog.Logger)` | Sets a custom logger (uses discard logger if nil) |
| `WithVacuum(fn VacuumFunc, interval)` | Configures automatic message cleanup |

### Database URI Helpers

#### `URITemp() string`

Creates a temporary database in the system temp directory. Returns a `file:` URI with `?tmp=true` suffix.

```go
mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
```

#### `URIFromPath(path string) (string, error)`

Creates a `file:` URI from a filesystem path, creating parent directories as needed.

```go
uri, err := delta.URIFromPath("/data/myapp/messages.db")
if err != nil {
    panic(err)
}
mq, err := delta.New(uri)
```

#### `RemoveStore(uri string, logger *slog.Logger) error`

Removes database files (including `-shm` and `-wal` files) for a file-based URI.

```go
err := delta.RemoveStore("file:/data/myapp.db", slog.Default())
```

### MQ Methods

#### Lifecycle

| Method | Description |
|--------|-------------|
| `Close() error` | Closes the MQ and all its streams |
| `CurrentStream() string` | Returns the current stream name |

#### Stream Management

| Method | Description |
|--------|-------------|
| `Stream(name string, ops ...Op) (*MQ, error)` | Creates or returns an existing named stream |

#### Publishing

| Method | Description |
|--------|-------------|
| `Publish(topic string, payload []byte) (*Publication, error)` | Synchronously publishes a message |
| `PublishAsync(topic string, payload []byte) *Publication` | Asynchronously publishes a message (non-blocking) |

#### Subscribing

| Method | Description |
|--------|-------------|
| `Subscribe(topic string) (*Subscription, error)` | Creates a Pub/Sub subscription (supports glob patterns) |
| `SubscribeFrom(topic string, from time.Time) (*Subscription, error)` | Subscribes and replays messages from a specific time |
| `Queue(topic string, key string) (*Subscription, error)` | Creates a load-balanced queue subscription |
| `Request(ctx context.Context, topic string, payload []byte) (*Subscription, error)` | Publishes a request and returns a subscription for the reply |

### Message Types

#### `Msg`

Represents a message in the queue:

```go
type Msg struct {
    MessageId uint64    // Unique message identifier
    Topic     string    // Topic the message was published to
    Payload   []byte    // Message data
    At        time.Time // Timestamp when message was published
}
```

**Methods:**

| Method | Description |
|--------|-------------|
| `Reply(payload []byte) (Msg, error)` | Replies to this message (publishes to `_inbox.{MessageId}`) |
| `Ack() error` | Acknowledges the message, allowing `VacuumOnReadAck` to clean it up |

#### `Publication`

Returned by `Publish` and `PublishAsync`:

```go
type Publication struct {
    Msg               // Embedded message with ID, topic, payload, timestamp
    Err  error        // Set if publish failed (only for async)
}
```

**Methods:**

| Method | Description |
|--------|-------------|
| `Done() <-chan struct{}` | Channel that closes when publish completes (useful for async) |

#### `Subscription`

Represents a subscription to a topic:

**Methods:**

| Method | Description |
|--------|-------------|
| `Topic() string` | Returns the subscription's topic pattern |
| `Id() string` | Returns the unique subscription ID |
| `Chan() <-chan Msg` | Returns the message channel for range loops |
| `Next() (Msg, bool)` | Blocks until next message (returns false if closed) |

**Fields:**

| Field | Description |
|-------|-------------|
| `Unsubscribe func()` | Call to close the subscription |

### Vacuum Strategies

Vacuum functions clean up old messages. Configure with `WithVacuum(fn, interval)`.

| Function | Description |
|----------|-------------|
| `VacuumOnAge(maxAge time.Duration) VacuumFunc` | Removes messages older than the specified duration |
| `VacuumKeepN(n int) VacuumFunc` | Keeps only the N most recent messages |
| `VacuumOnReadAck(mq *MQ)` | Removes acknowledged messages (must be paired with `Msg.Ack()`) |

### Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `DEFAULT_STREAM` | `"default"` | Default stream name |
| `OptimizeLatency` | `0` | Optimization mode for low latency |
| `OptimizeThroughput` | `1` | Optimization mode for high throughput |

## Examples

### Pub/Sub (Broadcast)

Multiple subscribers receive the same message:

```go
mq, _ := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
defer mq.Close()

// Multiple subscribers on the same topic
sub1, _ := mq.Subscribe("notifications")
sub2, _ := mq.Subscribe("notifications")

go func() {
    for msg := range sub1.Chan() {
        fmt.Printf("Subscriber 1: %s\n", string(msg.Payload))
    }
}()

go func() {
    for msg := range sub2.Chan() {
        fmt.Printf("Subscriber 2: %s\n", string(msg.Payload))
    }
}()

// Both subscribers receive this message
mq.Publish("notifications", []byte("New alert!"))
```

### Queue (Load Balancing)

Only one subscriber in a group receives each message:

```go
mq, _ := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
defer mq.Close()

// Create 3 workers in the same queue group
for i := 0; i < 3; i++ {
    worker := i
    sub, _ := mq.Queue("tasks", "worker-pool")
    
    go func() {
        for msg := range sub.Chan() {
            fmt.Printf("Worker %d processing: %s\n", worker, string(msg.Payload))
        }
    }()
}

// Each message goes to exactly one worker
for i := 0; i < 10; i++ {
    mq.Publish("tasks", []byte(fmt.Sprintf("task-%d", i)))
}
```

### Request/Reply

Synchronous request-response pattern:

```go
mq, _ := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
defer mq.Close()

// Create a service that responds to requests
service, _ := mq.Queue("greet.*", "greeting-service")
go func() {
    for msg := range service.Chan() {
        // Extract name from topic (greet.{name})
        parts := strings.Split(msg.Topic, ".")
        name := parts[1]
        
        // Reply to the request
        msg.Reply([]byte(fmt.Sprintf("Hello, %s!", name)))
    }
}()

// Send a request and wait for response
ctx := context.Background()
replySub, _ := mq.Request(ctx, "greet.alice", nil)

msg, ok := replySub.Next()
if ok {
    fmt.Printf("Response: %s\n", string(msg.Payload))
    // Output: Response: Hello, alice!
}
```

### Historical Replay

Subscribe from a specific point in time:

```go
mq, _ := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
defer mq.Close()

// Publish some messages now
for i := 0; i < 5; i++ {
    mq.Publish("events", []byte(fmt.Sprintf("msg-%d", i)))
}

// Record the time
from := time.Now()

// Publish more messages
for i := 5; i < 10; i++ {
    mq.Publish("events", []byte(fmt.Sprintf("msg-%d", i)))
}

// Subscribe from the recorded time (replays msg-5 through msg-9)
sub, _ := mq.SubscribeFrom("events", from)
for msg := range sub.Chan() {
    fmt.Printf("Replayed: %s\n", string(msg.Payload))
}
```

### Async Publishing

Non-blocking publish for high throughput:

```go
mq, _ := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
defer mq.Close()

// Start async publishes
var publications []*delta.Publication
for i := 0; i < 1000; i++ {
    pub := mq.PublishAsync("metrics", []byte(fmt.Sprintf("metric-%d", i)))
    publications = append(publications, pub)
}

// Wait for all to complete
for _, pub := range publications {
    <-pub.Done()
    if pub.Err != nil {
        log.Printf("Publish failed: %v", pub.Err)
    }
}
```

### Streams (Namespaces)

Isolated streams within one database:

```go
mq, _ := delta.New("file:myapp.db")
defer mq.Close()

// Default stream for application events
mq.Publish("user.signup", []byte(`{"user": "alice"}`))

// Create separate streams
auditMQ, _ := mq.Stream("audit")
metricsMQ, _ := mq.Stream("metrics")

// Each stream is isolated
eventsMQ, _ := mq.Stream("events")
eventsMQ.Publish("order.created", []byte(`{"order": 123}`))

// Messages don't cross streams
sub, _ := mq.Subscribe("order.created")     // Won't see eventsMQ messages
eventsSub, _ := eventsMQ.Subscribe("order.created") // Will see them
```

### Message Acknowledgment

Manual acknowledgment for reliable processing:

```go
mq, _ := delta.New(
    delta.URITemp(),
    delta.DBRemoveOnClose(),
    delta.WithVacuum(delta.VacuumOnReadAck, time.Minute),
)
defer mq.Close()

sub, _ := mq.Queue("jobs", "workers")
go func() {
    for msg := range sub.Chan() {
        // Process the message
        if processJob(msg.Payload) {
            // Acknowledge successful processing
            msg.Ack()
        }
    }
}()
```

### Vacuum Strategies

Automatic message cleanup:

```go
// Age-based: Remove messages older than 24 hours
mq, _ := delta.New(
    "file:myapp.db",
    delta.WithVacuum(delta.VacuumOnAge(24*time.Hour), time.Hour),
)

// Count-based: Keep only the 10000 most recent messages
mq, _ := delta.New(
    "file:myapp.db",
    delta.WithVacuum(delta.VacuumKeepN(10000), 30*time.Minute),
)

// Ack-based: Remove only acknowledged messages
mq, _ := delta.New(
    "file:myapp.db",
    delta.WithVacuum(delta.VacuumOnReadAck, 5*time.Minute),
)
```

## Configuration

### Database Options

```go
// High durability (default)
mq, _ := delta.New("file:myapp.db")

// High performance (synchronous=off)
mq, _ := delta.New("file:myapp.db", delta.DBSyncOff())

// Temporary database (auto-cleanup)
mq, _ := delta.New(delta.URITemp(), delta.DBRemoveOnClose())

// Custom path with auto-created directories
uri, _ := delta.URIFromPath("/var/lib/myapp/data.db")
mq, _ := delta.New(uri)
```

### Logging

```go
// With default logger
mq, _ := delta.New("file:myapp.db", delta.WithLogger(slog.Default()))

// Discard all logs (default behavior)
mq, _ := delta.New("file:myapp.db", delta.WithLogger(nil))
```

## Topic Patterns

Delta supports flexible topic matching with glob patterns:

| Pattern | Matches | Doesn't Match |
|---------|---------|---------------|
| `foo.bar` | `foo.bar` | `foo.baz`, `foo.bar.baz` |
| `foo.*` | `foo.bar`, `foo.baz` | `foo.bar.baz`, `foo` |
| `foo.**` | `foo.bar`, `foo.bar.baz`, `foo.a.b.c` | `bar.foo` |
| `*.bar` | `foo.bar`, `baz.bar` | `foo.baz.bar` |
| `orders.*.status` | `orders.123.status`, `orders.abc.status` | `orders.123.created` |
| `events.**` | `events.user.login`, `events.system.disk.full` | `logs.events` |

### Brace Groups

Topics can include brace-enclosed groups for special characters:

```go
// Email addresses with dots
mq.Publish("users.{alice@example.com}.profile", []byte("..."))

// Complex keys
mq.Subscribe("users.{complex-key.with.dots}.events")
```

## Benchmarks

Run on `Intel Core Ultra 9 185H` (2026-02-24):

**Parallel Publishing:**
```
BenchmarkParPub/simple-22           63,416      26,542 ns/op      37,681 msg/s     2,273 B/op      64 allocs/op
BenchmarkParPub/no-sync-22          66,301      16,307 ns/op      61,338 msg/s     1,188 B/op      26 allocs/op
```

**Pub/Sub (Multiple Subscribers):**
```
BenchmarkMultipleSubscribers/1-22                  119,448      26,937 ns/op      36,726 read-msg/s    37,123 write-msg/s    2,540 B/op      71 allocs/op
BenchmarkMultipleSubscribers/4-22                   61,191      24,093 ns/op     106,783 read-msg/s    41,506 write-msg/s    2,298 B/op      57 allocs/op
BenchmarkMultipleSubscribers/num-cpu_(22)-22        92,359      40,103 ns/op     504,170 read-msg/s    24,936 write-msg/s    4,927 B/op      91 allocs/op
BenchmarkMultipleSubscribers/2x_num-cpu_(44)-22    111,342      32,275 ns/op     726,985 read-msg/s    30,983 write-msg/s    4,759 B/op      75 allocs/op
```

**Variable Message Sizes:**
```
BenchmarkMultipleSubscribersSize/_0.1mb-22   5,748     187,816 ns/op  11,668 read-MB/s   116,684 read-msg/s    532.4 write-MB/s   5,324 write-msg/s   218,112 B/op   108 allocs/op
BenchmarkMultipleSubscribersSize/_1.0mb-22     684   1,585,876 ns/op  13,832 read-MB/s    13,832 read-msg/s    630.6 write-MB/s     630.6 write-msg/s 2,017,489 B/op   138 allocs/op
BenchmarkMultipleSubscribersSize/10.0mb-22      68  15,121,519 ns/op  14,333 read-MB/s     1,433 read-msg/s    661.3 write-MB/s      66.13 write-msg/s 19,719,668 B/op   142 allocs/op
```

**UID Generation:**
```
BenchmarkUID-22               3,189,724        360.6 ns/op         144 B/op        8 allocs/op
```

Performance characteristics:
- Single-writer throughput: ~37K msg/s (durability) to ~61K msg/s (no-sync)
- Read throughput scales linearly with subscriber count (up to 727K msg/s with 44 subscribers)
- Large message handling maintains ~11-14 GB/s aggregate read throughput
- Low allocation overhead for typical workloads

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
