# AGENTS.md

## Project Overview

Go library (`github.com/modfin/delta`) -- an SQLite-backed message queue providing Pub/Sub, Queue (load-balanced), Request/Reply, multiple streams, and glob-based topic matching. Single flat package `delta` with no subdirectories for source code. Requires CGo (C compiler) due to `github.com/mattn/go-sqlite3`.

## Public API Reference

This section documents all exported symbols that constitute the public API.

### Core Types

| Type | File | Description |
|------|------|-------------|
| `MQ` | mq.go:115 | Main message queue instance with Pub/Sub, Queue, Request/Reply functionality |
| `Msg` | mq.go:404 | Represents a message with ID, topic, payload, and timestamp |
| `Publication` | mq.go:638 | Result of Publish/PublishAsync operations with embedded Msg |
| `Subscription` | mq.go:684 | Active subscription to a topic pattern |
| `Op` | mq.go:22 | Functional option type for configuring MQ during construction |
| `VacuumFunc` | mq.go:82 | Function signature for message cleanup strategies |

### Constructor

| Function | File | Signature | Description |
|----------|------|-----------|-------------|
| `New` | mq.go:155 | `New(uri string, op ...Op) (*MQ, error)` | Creates a new MQ instance with SQLite URI and options |

### Configuration Options (Op)

| Function | File | Signature | Description |
|----------|------|-----------|-------------|
| `DBSyncOff` | mq.go:37 | `DBSyncOff() Op` | Sets SQLite synchronous=off for write performance |
| `DBRemoveOnClose` | mq.go:44 | `DBRemoveOnClose() Op` | Removes database files when MQ is closed |
| `WithLogger` | mq.go:52 | `WithLogger(log *slog.Logger) Op` | Sets a custom structured logger |
| `WithVacuum` | mq.go:63 | `WithVacuum(vacuum VacuumFunc, interval time.Duration) Op` | Configures automatic message cleanup |

### Database URI Helpers

| Function | File | Signature | Description |
|----------|------|-----------|-------------|
| `URITemp` | util.go:22 | `URITemp() string` | Creates a temporary database URI in system temp directory |
| `URIFromPath` | util.go:31 | `URIFromPath(path string) (string, error)` | Creates file URI from filesystem path |
| `RemoveStore` | mq.go:357 | `RemoveStore(uri string, logger *slog.Logger) error` | Removes database files (including -shm, -wal) |

### Vacuum Strategies

| Function | File | Signature | Description |
|----------|------|-----------|-------------|
| `VacuumOnAge` | util.go:181 | `VacuumOnAge(maxAge time.Duration) VacuumFunc` | Removes messages older than specified duration |
| `VacuumKeepN` | util.go:203 | `VacuumKeepN(n int) VacuumFunc` | Keeps only N most recent messages |
| `VacuumOnReadAck` | util.go:220 | `VacuumOnReadAck(mq *MQ)` | Removes acknowledged messages (must call `Msg.Ack()`) |

### MQ Methods

#### Lifecycle

| Method | File | Signature | Description |
|--------|------|-----------|-------------|
| `Close` | mq.go:315 | `(c *MQ) Close() error` | Closes the MQ and all its streams |
| `CurrentStream` | mq.go:1016 | `(c *MQ) CurrentStream() string` | Returns the current stream name |

#### Stream Management

| Method | File | Signature | Description |
|--------|------|-----------|-------------|
| `Stream` | mq.go:243 | `(c *MQ) Stream(stream string, ops ...Op) (*MQ, error)` | Creates or returns an existing named stream |

#### Publishing

| Method | File | Signature | Description |
|--------|------|-----------|-------------|
| `Publish` | mq.go:648 | `(mq *MQ) Publish(topic string, payload []byte) (*Publication, error)` | Synchronously publishes a message |
| `PublishAsync` | mq.go:670 | `(mq *MQ) PublishAsync(topic string, payload []byte) *Publication` | Asynchronously publishes a message |

#### Subscribing

| Method | File | Signature | Description |
|--------|------|-----------|-------------|
| `Subscribe` | mq.go:756 | `(mq *MQ) Subscribe(topic string) (*Subscription, error)` | Creates a Pub/Sub subscription |
| `SubscribeFrom` | mq.go:950 | `(mq *MQ) SubscribeFrom(topic string, from time.Time) (*Subscription, error)` | Subscribes with historical replay |
| `Queue` | mq.go:789 | `(mq *MQ) Queue(topic string, key string) (*Subscription, error)` | Creates a load-balanced queue subscription |
| `Request` | mq.go:887 | `(mq *MQ) Request(ctx context.Context, topic string, payload []byte) (*Subscription, error)` | Publishes request and subscribes to reply |

### Msg Methods

| Method | File | Signature | Description |
|--------|------|-----------|-------------|
| `Reply` | mq.go:412 | `(m *Msg) Reply(payload []byte) (Msg, error)` | Replies to a message (publishes to `_inbox.{MessageId}`) |
| `Ack` | mq.go:427 | `(m *Msg) Ack() error` | Acknowledges the message for `VacuumOnReadAck` |

### Publication Methods

| Method | File | Signature | Description |
|--------|------|-----------|-------------|
| `Done` | mq.go:644 | `(p *Publication) Done() <-chan struct{}` | Returns channel that closes when publish completes |

### Subscription Methods

| Method | File | Signature | Description |
|--------|------|-----------|-------------|
| `Topic` | mq.go:711 | `(s *Subscription) Topic() string` | Returns the subscription's topic pattern |
| `Id` | mq.go:714 | `(s *Subscription) Id() string` | Returns the unique subscription ID |
| `Chan` | mq.go:747 | `(s *Subscription) Chan() <-chan Msg` | Returns the message channel |
| `Next` | mq.go:751 | `(s *Subscription) Next() (Msg, bool)` | Blocks until next message |

**Subscription Fields:**

| Field | File | Type | Description |
|-------|------|------|-------------|
| `Unsubscribe` | mq.go:695 | `func()` | Call to close the subscription |

### Exported Constants

| Constant | File | Type | Value | Description |
|----------|------|------|-------|-------------|
| `DEFAULT_STREAM` | util.go:20 | `string` | `"default"` | Default stream name |
| `OptimizeLatency` | util.go:16 | `int` | `0` | Optimization mode for low latency |
| `OptimizeThroughput` | util.go:17 | `int` | `1` | Optimization mode for high throughput |

### Summary

**Total Public API Surface:**
- 6 Types: `MQ`, `Msg`, `Publication`, `Subscription`, `Op`, `VacuumFunc`
- 12 Functions: `New`, `URITemp`, `URIFromPath`, `RemoveStore`, `DBSyncOff`, `DBRemoveOnClose`, `WithLogger`, `WithVacuum`, `VacuumOnAge`, `VacuumKeepN`, `VacuumOnReadAck`
- 17 Methods: 10 on MQ, 2 on Msg, 1 on Publication, 4 on Subscription
- 3 Constants: `DEFAULT_STREAM`, `OptimizeLatency`, `OptimizeThroughput`

## Build / Test / Lint Commands

```sh
# Build
go build ./...

# Run all tests
go test ./...

# Run all tests (verbose)
go test -v ./...

# Run a single test by name
go test -v -run TestSimplePubSub ./...

# Run a single subtest (table-driven)
go test -v -run TestSplitTopic/input_name ./...

# Run benchmarks
go test -bench=. -benchmem ./...

# Run a specific benchmark
go test -bench=BenchmarkParPub -benchmem ./...

# Format code (standard Go formatter)
go fmt ./...

# Vet code (standard Go static analysis)
go vet ./...
```

No Makefile, no CI/CD pipeline, no custom linter config. The project relies solely on `gofmt` and `go vet`.

## Project Structure

```
delta/
  mq.go          -- Core MQ types, constructor, pub/sub/queue/request API, read loop, vacuum loop (~830 lines)
  db.go          -- SQLite database layer: schema, persist, query, vacuum, message iteration (~238 lines)
  glob.go        -- Generic glob-matching trie for topic subscriptions (~148 lines)
  util.go        -- Utilities: UID generation, topic validation, URI helpers, vacuum strategies (~214 lines)
  log.go         -- No-op slog.Handler (discardLogger) (~24 lines)
  mq_test.go           -- Integration tests (package delta_test, black-box)
  glob_test.go         -- Unit tests for glob trie (package delta, white-box)
  util_test.go         -- Unit tests for util functions (package delta, white-box)
  x_benchmark_test.go  -- Benchmarks (package delta_test, black-box)
  _examples/            -- Standalone example programs (simple, queue, request-reply, sub-from)
```

## Dependencies

- **Runtime:** `github.com/mattn/go-sqlite3` v1.14.24 (CGo SQLite3 driver)
- **Test:** `github.com/stretchr/testify` v1.9.0 (assertions via `assert` sub-package)

## Code Style Guidelines

### Formatting

- Standard `gofmt` formatting: tabs for indentation, no semicolons.
- No enforced line length limit (some lines exceed 150 chars).
- Double quotes for strings; backticks for raw strings (SQL queries, schema constants).

### Imports

- All imports in a single `import (...)` block.
- Standard library and third-party packages are mixed together in **lexicographic order** without blank-line separation between groups. Follow this existing convention.

```go
import (
    "context"
    "database/sql"
    "fmt"
    "github.com/mattn/go-sqlite3"
    "log/slog"
    "sync"
    "time"
)
```

### Naming Conventions

- **Package name:** `delta` (single lowercase word).
- **Exported types:** PascalCase -- `MQ`, `Msg`, `Publication`, `Subscription`, `Op`, `VacuumFunc`.
- **Abbreviations stay uppercase:** `MQ`, `DB` (in `DBSyncOff`, `DBRemoveOnClose`), `URI` (in `URITemp`, `URIFromPath`).
- **Exported functions:** PascalCase -- `New`, `Publish`, `Subscribe`, `Queue`, `Request`.
- **Unexported functions:** camelCase -- `dbPragma`, `splitTopic`, `checkTopic`, `persist`, `uid`.
- **Unexported struct types:** trailing underscore convention -- `base_`, `stream_` (project-specific pattern).
- **Constants:** mixed conventions exist:
  - `DEFAULT_STREAM` -- SCREAMING_SNAKE_CASE for the default stream name.
  - `OptimizeLatency`, `OptimizeThroughput` -- PascalCase for iota constants.
  - `base_schema`, `base_schema_idx` -- snake_case for unexported SQL schema constants.
- **Method receivers:** short names (`c`, `mq`, `m`, `s`, `t`, `p`, `g`, `d`). Note: `*MQ` uses both `c` and `mq` as receiver names across different methods.
- **Local variables:** short, often single-letter (`m`, `s`, `i`, `q`, `r`, `b`).
- **File names:** snake_case (`mq.go`, `glob_test.go`). Benchmark file prefixed with `x_` to sort last.

### Types and Generics

- Core types are concrete structs, not interfaces.
- Two unexported interfaces: `query` (database operations) and `globbable` (for glob trie items).
- Generics used for the glob trie: `globber[T globbable]`.
- Go 1.23 features: `iter.Seq[Msg]` (range-over-function), `range N` syntax.
- Functional options pattern: `type Op func(*MQ) error`.

### Error Handling

- Return `error` as the last return value (standard Go).
- Wrap errors with `fmt.Errorf("description, %w", err)` -- note **comma** before `%w` (not colon).
- Combine errors with `errors.Join(...)` when multiple operations may fail.
- Compare errors with `errors.Is(err, sql.ErrNoRows)`.
- No custom error types -- all errors are string-based via `fmt.Errorf`.
- Goroutine errors are logged via `slog.Error` rather than returned.
- Intentionally discarded errors use explicit `_ = ...`.

```go
// Error wrapping pattern used in this project:
return nil, fmt.Errorf("could not open db, %w", err)

// Combining multiple errors:
return errors.Join(
    dbPragma("journal_mode = WAL")(c),
    dbPragma("synchronous = normal")(c),
)
```

### Comments

- Godoc-style `//` comments on some exported symbols (many are undocumented).
- `// TODO` comments for known issues/improvements.
- Inline comments for performance observations and rationale.
- Some legacy references to "cache" and "cove" from project evolution -- do not propagate these.

### Testing Conventions

- **Black-box tests** (`package delta_test`) for public API in `mq_test.go`.
- **White-box tests** (`package delta`) for internal functions in `glob_test.go`, `util_test.go`.
- Assertions use `github.com/stretchr/testify/assert` exclusively (`assert.NoError`, `assert.Equal`, `assert.True`, `assert.Len`, `assert.ElementsMatch`).
- **Table-driven tests** for input validation (see `TestSplitTopic`, `Test_checkTopicSub`).
- Test names: `TestPascalCase` or `TestPascalCase_with_underscores` for variants.
- Tests create temporary DBs via `delta.URITemp()` with `delta.DBRemoveOnClose()` for cleanup.
- Concurrency-heavy tests use `sync.WaitGroup` and `select` with `time.After` for timeouts.
- Output in tests uses `fmt.Println` (not always `t.Log`).

### Concurrency Patterns

- Heavy use of goroutines, `sync.WaitGroup`, `sync.Mutex`, `sync.RWMutex`, `sync/atomic`.
- Channels used for message delivery to subscribers.
- Context-based cancellation throughout (`context.Context`).
- The MQ read loop and vacuum loop run as background goroutines.

### Database Patterns

- SQLite via `database/sql` with the `go-sqlite3` driver.
- Custom driver registered in `init()` as `"sqlite3_delta-v"` with a `match_glob` SQL function.
- WAL journal mode, normal synchronous by default.
- Schema uses parameterized table names via `fmt.Sprintf` (not SQL parameters).
- Vacuum strategies: `VacuumOnAge`, `VacuumKeepN`, `VacuumOnReadAck`.
