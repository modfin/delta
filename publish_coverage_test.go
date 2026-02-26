package delta_test

import (
	"fmt"
	"testing"

	"github.com/modfin/delta"
	"github.com/stretchr/testify/assert"
)

// TestPublish_PeriodicWrittenCursorCheckpoint verifies that publishing crosses the
// 1000-message boundary and triggers the periodic written-cursor checkpoint call
// without error (publish.go:37-39).
func TestPublish_PeriodicWrittenCursorCheckpoint(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	// Publish 1005 messages to cross the 999 boundary.
	for i := range 1005 {
		_, err := mq.Publish("periodic.ack.test", []byte(fmt.Sprintf("msg%d", i)))
		assert.NoError(t, err)
	}
}
