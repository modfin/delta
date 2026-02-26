package delta

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSubscription_TryNotifyClosed verifies the branch in tryNotify where
// s.closed == true (types.go:102-104).
func TestSubscription_TryNotifyClosed(t *testing.T) {
	s := &Subscription{
		id:         "test",
		topic:      "test.topic",
		notifyChan: make(chan Msg, 1),
		doneChan:   make(chan struct{}),
	}

	// Close it explicitly
	s.close()

	// tryNotify should return false because it is closed
	written := s.tryNotify(Msg{Payload: []byte("test")})
	assert.False(t, written, "tryNotify should return false on a closed subscription")
}
