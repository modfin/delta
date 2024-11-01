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

func checkTopicPub(topic string) (string, error) {
	topic = strings.TrimSpace(topic)
	topic = strings.ToLower(topic)
	if len(topic) == 0 {
		return "", fmt.Errorf("topic is empty")
	}
	for _, r := range topic {
		if 'a' <= r && r <= 'z' {
			continue
		}
		if '0' <= r && r <= '9' {
			continue
		}
		switch r {
		case '-', '_', '.':
			continue
		}
		return "", fmt.Errorf("topic contains invalid character, only a-z, -, _, . are allowed, got %c", r)
	}
	return topic, nil
}

func checkTopicSub(topic string) (string, error) {
	topic = strings.TrimSpace(topic)
	topic = strings.ToLower(topic)
	if len(topic) == 0 {
		return "", fmt.Errorf("topic is empty")
	}
	for _, r := range topic {
		if 'a' <= r && r <= 'z' {
			continue
		}
		if '0' <= r && r <= '9' {
			continue
		}
		switch r {
		case '-', '_', '.', '*':
			continue
		}
		return "", fmt.Errorf("topic contains invalid character, only a-z, -, _, ., * are allowed, got %c", r)
	}
	return topic, nil
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
