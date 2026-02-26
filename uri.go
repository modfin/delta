package delta

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	// OptimizeLatency indicates optimization for lower latency.
	OptimizeLatency = iota
	// OptimizeThroughput indicates optimization for higher throughput.
	OptimizeThroughput
)

// DEFAULT_STREAM is the default stream name used by New.
const DEFAULT_STREAM = "default"

// URITemp returns a temporary SQLite file: URI for a new queue store.
//
// The path is created under os.TempDir and tagged with tmp=true so
// DBRemoveOnClose/RemoveStore can remove the temporary directory as well.
func URITemp() string {
	d := fmt.Sprintf("%d-delta", time.Now().UnixNano())
	uri := filepath.Join(os.TempDir(), d, "delta.db")
	if err := os.MkdirAll(filepath.Dir(uri), 0700); err != nil {
		panic(fmt.Sprintf("delta: URITemp could not create temp directory: %v", err))
	}
	return fmt.Sprintf("file:%s?tmp=true", uri)
}

// URIFromPath converts a filesystem path into a SQLite file: URI.
//
// Parent directories are created when missing.
func URIFromPath(path string) (string, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return "", fmt.Errorf("could not create directory for db path, %w", err)
	}
	return fmt.Sprintf("file:%s", path), nil
}
