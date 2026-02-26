package delta_test

// uri_test.go — regression tests for issues #12 and #13:
//   #12: URITemp() creates directories with 0755; should use 0700.
//   #13: URITemp() and URIFromPath() discard os.MkdirAll errors; errors
//        should be propagated to the caller.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/modfin/delta"
	"github.com/stretchr/testify/assert"
)

// TestURITemp_DirectoryPermissions verifies that URITemp() creates its
// temporary directory with mode 0700 (owner-only), not the overly-broad 0755.
// Issue #12: other system users can read directory listings with 0755.
func TestURITemp_DirectoryPermissions(t *testing.T) {
	uri := delta.URITemp()

	// Extract the file path from the URI (strip "file:" prefix and "?..." query).
	raw := strings.TrimPrefix(uri, "file:")
	path, _, _ := strings.Cut(raw, "?")
	dir := filepath.Dir(path)

	info, err := os.Stat(dir)
	assert.NoError(t, err, "URITemp() directory must exist")
	if err != nil {
		return
	}

	perm := info.Mode().Perm()
	assert.Equal(t, os.FileMode(0700), perm,
		"URITemp() must create temp directory with mode 0700, got %04o", perm)

	// Cleanup: remove the temp dir created by URITemp.
	_ = os.RemoveAll(dir)
}

// TestURIFromPath_DirectoryPermissions verifies that URIFromPath() creates
// parent directories with mode 0700, not the overly-broad 0755.
// Issue #12.
func TestURIFromPath_DirectoryPermissions(t *testing.T) {
	base := t.TempDir()
	// Use a nested path so URIFromPath must create the parent.
	path := filepath.Join(base, "secure_subdir", "delta.db")

	_, err := delta.URIFromPath(path)
	assert.NoError(t, err, "URIFromPath() must not return an error for a valid path")

	info, err := os.Stat(filepath.Dir(path))
	assert.NoError(t, err, "URIFromPath() parent directory must exist")
	if err != nil {
		return
	}

	perm := info.Mode().Perm()
	assert.Equal(t, os.FileMode(0700), perm,
		"URIFromPath() must create parent directory with mode 0700, got %04o", perm)
}

// TestURIFromPath_PropagatesError verifies that URIFromPath() returns a
// non-nil error when the directory cannot be created (e.g. because the parent
// exists as a file, making MkdirAll fail).
// Issue #13: previously the error was silently discarded.
func TestURIFromPath_PropagatesError(t *testing.T) {
	base := t.TempDir()
	// Create a regular file where we want a directory — MkdirAll must fail.
	blocker := filepath.Join(base, "notadir")
	err := os.WriteFile(blocker, []byte("block"), 0600)
	assert.NoError(t, err)

	// Try to use a path whose parent requires "notadir" to be a directory.
	path := filepath.Join(blocker, "sub", "delta.db")
	_, err = delta.URIFromPath(path)
	assert.Error(t, err,
		"URIFromPath() must return an error when MkdirAll fails")
}
