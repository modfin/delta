package delta

// lifecycle_test.go — white-box tests (package delta) for lifecycle helpers
// that cannot be exercised from the public API alone.

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestRemoveStore_NotClosed verifies that removeStore() returns an error when
// called on an MQ whose base.closed channel has not been signalled (i.e. the
// MQ has not been closed yet).  The "db is not closed" guard (lifecycle.go:292)
// protects against accidental data loss.
func TestRemoveStore_NotClosed(t *testing.T) {
	mq, err := New(URITemp(), DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	// removeStore() must refuse to act because the MQ is still open.
	err = mq.removeStore()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "db is not closed")
}
