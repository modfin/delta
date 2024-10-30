package delta

import (
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"os"
	"strings"
	"time"
)

type query interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Exec(query string, args ...any) (sql.Result, error)
}

const base_schema = `CREATE TABLE IF NOT EXISTS %s (
    			message_id BIGINT PRIMARY KEY,
    			topic TEXT, 
    			payload BLOB,
    			created_at BIGINT
		 );`
const base_schema_idx = `CREATE INDEX IF NOT EXISTS %s__topic_idx ON %s (topic);`

const base_metadata_schema = `CREATE TABLE IF NOT EXISTS _mq_delta_metadata (
    			key TEXT,
    			value TEXT,
    			created_at BIGINT
		 );`

func exec(db query, qs ...string) error {
	for _, q := range qs {
		_, err := db.Exec(q)
		if err != nil {
			return fmt.Errorf("could not exec, %s, err, %w", q, err)
		}
	}
	return nil
}

func serial(db query, tbl func() string) (uint64, error) {
	q := fmt.Sprintf(`SELECT coalesce(MAX(message_id), 0) FROM %s`, tbl())
	r := db.QueryRow(q)
	var s uint64
	err := r.Scan(&s)

	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}

	return s, err

}

func persist(db query, m Msg, tbl func() string) error {

	q := fmt.Sprintf(`INSERT INTO %s (message_id, topic, payload, created_at) VALUES ($1, $2, $3, $4)`, tbl())

	_, err := db.Exec(q, m.MessageId, m.Topic, m.Payload, m.At.UnixNano())

	return err
}

func message(db *sql.DB, id uint64, tbl func() string) (Msg, error) {
	var m Msg

	var ts int64

	err := db.QueryRow(fmt.Sprintf(`SELECT message_id, topic, payload, created_at FROM %s WHERE message_id = $1`, tbl()), id).Scan(&m.MessageId, &m.Topic, &m.Payload, &ts)
	if err != nil {
		return m, fmt.Errorf("could not get message, %w", err)
	}

	m.At = time.Unix(0, ts)

	return m, nil

}

func iterMessage(db query, topic string, from time.Time, tbl func() string, log *slog.Logger) iter.Seq[Msg] {

	glob := strings.Contains(topic, "*")

	var rows *sql.Rows
	var err error
	if !glob {
		rows, err = db.Query(fmt.Sprintf(`
				SELECT message_id, topic, payload, created_at 
				FROM %s 
				WHERE topic = $1 
				  AND created_at >= $2 
				ORDER BY created_at`, tbl()), topic, from.UnixNano())
	}

	if glob {
		start, _, _ := strings.Cut(topic, "*")
		start = start + "%"
		rows, err = db.Query(fmt.Sprintf(`
				SELECT message_id, topic, payload, created_at 
				FROM %s 
				WHERE topic like $1 
				  AND match_glob(topic, $2) 
				  AND created_at >= $3 
				ORDER BY created_at`, tbl()), start, topic, from.UnixNano())
	}

	if err != nil {
		if log != nil {
			log.Error("[cove] iterKV, could not query in iter", "err", err)
			return func(yield func(msg Msg) bool) {}
		}
		_, _ = fmt.Fprintf(os.Stderr, "[cove] iterKV, could not query in iter, %v", err)
		return func(yield func(msg Msg) bool) {}

	}

	return func(yield func(msg Msg) bool) {
		defer rows.Close()
		for rows.Next() {
			var m Msg
			var ts int64
			err := rows.Scan(&m.MessageId, &m.Topic, &m.Payload, &ts)
			m.At = time.Unix(0, ts)

			if err != nil {
				if log != nil {
					log.Error("[cove] iterKV, could not scan in iter,", "err", err)
					return
				}
				_, _ = fmt.Fprintf(os.Stderr, "cove: iterKV, could not scan in iter, %v", err)
				return
			}
			if !yield(m) {
				return
			}
		}
	}
}
