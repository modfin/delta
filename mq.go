package delta

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

func dbPragma(pragma string) Op {
	return func(c *MQ) error {
		return exec(c.base.db,
			fmt.Sprintf(`pragma %s;`, pragma),
		)
	}
}

// Op configures an MQ instance during New or Stream creation.
//
// Returning an error aborts construction.
type Op func(*MQ) error

// DBSyncOff configures SQLite with synchronous=off.
//
// This improves write throughput at the cost of durability and crash safety.
func DBSyncOff() Op {
	return func(c *MQ) error {
		return dbPragma("synchronous = off")(c)
	}
}

// DBRemoveOnClose removes backing database files when MQ.Close is called.
func DBRemoveOnClose() Op {
	return func(mq *MQ) error {
		mq.base.removeOnClose = true
		return nil
	}
}

// WithLogger sets the logger used by MQ background loops and internals.
//
// Passing nil disables logging by using a discard logger.
func WithLogger(log *slog.Logger) Op {
	return func(c *MQ) error {
		if log == nil {
			log = slog.New(discardLogger{})
		}
		c.base.log = log
		return nil
	}
}

// WithVacuum enables periodic message cleanup.
//
// vacuum is executed every interval in the background vacuum loop.
func WithVacuum(vacuum VacuumFunc, interval time.Duration) Op {
	return func(c *MQ) error {
		c.base.vacuum = vacuum
		c.base.vacuumInterval = interval
		return nil
	}
}

func dbDefault() Op {
	return func(c *MQ) error {
		return errors.Join(
			dbPragma("journal_mode = WAL")(c),
			dbPragma("synchronous = normal")(c),
			dbPragma(`auto_vacuum = incremental`)(c),
			dbPragma(`incremental_vacuum`)(c),
		)
	}
}

// VacuumFunc is a cleanup strategy run by the vacuum loop.
//
// Implementations can remove old messages based on stream policy.
type VacuumFunc func(*MQ)
type base_ struct {
	//shards
	uri string

	db            *sql.DB
	closeOnce     sync.Once
	closed        chan struct{}
	removeOnClose bool
	log           *slog.Logger
	streamMu      sync.RWMutex
	streams       map[string]*MQ

	vacuumInterval time.Duration
	vacuum         VacuumFunc
}

type stream_ struct {
	name string // aka namespace, shard or whatever

	written uint64
	read    uint64

	writeMu sync.Mutex

	inform chan struct{}

	subs *globber[*Subscription]

	groupMu sync.Mutex
	groups  map[string]*group
}

// MQ is a SQLite-backed message queue stream.
//
// It supports publish/subscribe, queue groups, request/reply, and replay.
type MQ struct {
	base   *base_
	stream stream_
}
