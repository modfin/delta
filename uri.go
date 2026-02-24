package delta

import (
	"fmt"
	"os"
	"path/filepath"
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
	if err := os.MkdirAll(filepath.Dir(uri), 0700); err != nil {
		panic(fmt.Sprintf("delta: URITemp could not create temp directory: %v", err))
	}
	return fmt.Sprintf("file:%s?tmp=true", uri)
}

func URIFromPath(path string) (string, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return "", fmt.Errorf("could not create directory for db path, %w", err)
	}
	return fmt.Sprintf("file:%s", path), nil
}
