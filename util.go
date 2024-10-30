package delta

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
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
		return "", fmt.Errorf("stream contains invalid character, only a-z, 0-9 and _ are allowed, name contains '%c'", r)
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
