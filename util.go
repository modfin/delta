package delta

import (
	"encoding/base32"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

const (
	OptimizeLatency = iota
	OptimizeThroughput
)

const DEFAULT_STREAM = "default"

func URITemp() string {
	d := fmt.Sprintf("%d-delta", time.Now().UnixNano())
	uri := filepath.Join(os.TempDir(), d, "delta.db")
	os.MkdirAll(filepath.Dir(uri), 0755)
	return fmt.Sprintf("file:%s?tmp=true", uri)
}

func URIFromPath(path string) string {
	_ = os.MkdirAll(filepath.Dir(path), 0755)
	return fmt.Sprintf("file:%s", path)
}

func checkStreamName(stream string) (string, error) {
	stream = strings.TrimSpace(stream)
	stream = strings.ToLower(stream)
	stream = strings.ReplaceAll(stream, " ", "_")
	stream = strings.ReplaceAll(stream, "-", "_")
	stream = strings.ReplaceAll(stream, ".", "_")

	for _, r := range stream {
		if 'a' <= r && r <= 'z' {
			continue
		}
		if '0' <= r && r <= '9' {
			continue
		}
		switch r {
		case '_':
			continue
		}
		return "", fmt.Errorf("stream_ contains invalid character, only a-z, 0-9 and _ are allowed, name contains '%c'", r)
	}
	return stream, nil
}

func splitTopic(topic string) []string {
	var groups []string
	var currentGroup strings.Builder
	var depth int
	var inGroup bool

	// Extract groups by iterating through characters
	for _, char := range topic {
		if char == '{' {
			depth++
			if depth == 1 {
				inGroup = true
				continue // Skip the opening brace for the group content
			}
		}
		if inGroup && char == '}' {
			depth--
			if depth == 0 {
				groups = append(groups, currentGroup.String())
				currentGroup.Reset()
				inGroup = false
				continue // Skip the closing brace
			}
		}
		if inGroup {
			currentGroup.WriteRune(char)
		}
	}

	modifiedTopic := topic

	if inGroup { // Add the last unclosed group
		groups = append(groups, currentGroup.String())
		currentGroup.Reset()
		inGroup = false
		depth = 0
		modifiedTopic += "}"
	}

	// Replace groups with placeholders
	for _, group := range groups {
		modifiedTopic = strings.Replace(modifiedTopic, "{"+group+"}", "{}", 1)
	}

	// Split by dots
	parts := strings.Split(modifiedTopic, ".")

	// Replace placeholders with actual groups
	if len(groups) > 0 {
		groupIndex := 0
		for i := range parts {
			for strings.Contains(parts[i], "{}") && groupIndex < len(groups) {
				parts[i] = strings.Replace(parts[i], "{}", groups[groupIndex], 1)
				groupIndex++
			}
		}
	}

	return parts
}

func checkTopic(topic string) (string, error) {
	topic = strings.TrimSpace(topic)
	topic = strings.Trim(topic, ".")
	topic = strings.ToLower(topic)
	if len(topic) == 0 {
		return "", fmt.Errorf("topic is empty")
	}
	// TODO check that there are other things then .
	//return topic, nil

	parts := splitTopic(topic)

	for i := range parts {
		if strings.Contains(parts[i], ".") {
			parts[i] = "{" + parts[i] + "}"
		}
	}
	return strings.Join(parts, "."), nil
}

var uid_count uint32

func uid() string {

	enc := base32.StdEncoding.WithPadding(base32.NoPadding)
	t := time.Now().UnixNano()
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(t))
	ts := enc.EncodeToString(b)

	countInt := atomic.AddUint32(&uid_count, 1)
	b = make([]byte, 4)
	binary.BigEndian.PutUint32(b, countInt)
	count := enc.EncodeToString(b)

	randInt := rand.Uint64()
	b = make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(randInt))
	rand := enc.EncodeToString(b)

	return strings.ToLower(fmt.Sprintf("%s-%s-%s", ts, count, rand))
}

func VacuumOnAge(maxAge time.Duration) VacuumFunc {
	if maxAge < 0 {
		maxAge = -maxAge
	}
	return func(mq *MQ) {
		removeBefore := time.Now().Add(-maxAge)

		l := mq.base.log.With("type", "max-age")

		l.Info("[delta] vacuuming", "older_then", removeBefore, "in", mq.CurrentStream())

		removed, err := vacuumBefore(mq.base.db, removeBefore, mq.tbl)
		if err != nil {
			l.Error("[delta] vacuuming error", "err", err)
		}
		if removed > 0 {
			l.Info("[delta] vacuuming result", "removed", removed, "in", mq.CurrentStream())
		}

	}
}

func VacuumKeepN(n int) VacuumFunc {
	return func(mq *MQ) {
		l := mq.base.log.With("type", "keep-n")
		l.Info("[delta] vacuuming", "top", n, "in", mq.CurrentStream())

		removed, err := vacuumKeep(mq.base.db, n, mq.tbl)
		if err != nil {
			l.Error("[delta] vacuuming error", "err", err)
			return
		}
		if removed > 0 {
			l.Info("[delta] vacuuming result", "removed", removed, "in", mq.CurrentStream())
		}

	}
}

func VacuumOnReadAck(mq *MQ) {
	l := mq.base.log.With("type", "read-ack")

	l.Info("[delta] vacuuming", "stream", mq.CurrentStream())

	removed, err := vacuumReadAck(mq.base.db, mq.tbl)
	if err != nil {
		l.Error("[delta] vacuuming error", "err", err)
		return
	}
	if removed > 0 {
		l.Info("[delta] vacuuming result", "removed", removed, "in", mq.CurrentStream())
	}

}
