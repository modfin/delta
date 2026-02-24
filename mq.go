package delta

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type Op func(*MQ) error

func dbPragma(pragma string) Op {
	return func(c *MQ) error {
		return exec(c.base.db,
			fmt.Sprintf(`pragma %s;`, pragma),
		)
	}
}

// DBSyncOff is a helper function to set
//
//	synchronous = off
//
// this is useful for write performance but effects read performance and durability
func DBSyncOff() Op {
	return func(c *MQ) error {
		return dbPragma("synchronous = off")(c)
	}
}

// DBRemoveOnClose is a helper function to remove the database files on close
func DBRemoveOnClose() Op {
	return func(mq *MQ) error {
		mq.base.removeOnClose = true
		return nil
	}
}

// WithLogger sets the logger for the cache
func WithLogger(log *slog.Logger) Op {
	return func(c *MQ) error {
		if log == nil {
			log = slog.New(discardLogger{})
		}
		c.base.log = log
		return nil
	}
}

// WithVacuum sets the logger for the cache
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

type MQ struct {
	base   *base_
	stream stream_
}
