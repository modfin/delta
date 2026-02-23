# AGENTS.md

## Project Overview

Go library (`github.com/modfin/delta`) -- an SQLite-backed message queue providing Pub/Sub, Queue (load-balanced), Request/Reply, multiple streams, and glob-based topic matching. Single flat package `delta` with no subdirectories for source code. Requires CGo (C compiler) due to `github.com/mattn/go-sqlite3`.

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
  examples/            -- Standalone example programs (simple, queue, request-reply, sub-from)
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
