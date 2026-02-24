package delta

import (
	"encoding/base32"
	"encoding/binary"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"
)

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
