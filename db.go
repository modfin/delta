package delta

import (
	"database/sql"
	"errors"
	"fmt"
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
    			Payload BLOB,
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

	q := fmt.Sprintf(`INSERT INTO %s (message_id, topic, Payload, created_at) VALUES ($1, $2, $3, $4)`, tbl())

	_, err := db.Exec(q, m.MessageId, m.Topic, m.Payload, m.At.UnixNano())

	return err
}

func message(db *sql.DB, id uint64, tbl func() string) (Msg, error) {
	var m Msg

	var ts int64

	err := db.QueryRow(fmt.Sprintf(`SELECT message_id, topic, Payload, created_at FROM %s WHERE message_id = $1`, tbl()), id).Scan(&m.MessageId, &m.Topic, &m.Payload, &ts)
	if err != nil {
		return m, fmt.Errorf("could not get message, %w", err)
	}

	m.At = time.Unix(0, ts)

	return m, nil

}
