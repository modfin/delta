package delta

import (
	"runtime"
	"sync/atomic"
	"time"
)

func (mq *MQ) vacuumloop() {
	go func() {
		if mq.base.vacuum == nil {
			mq.base.log.Warn("[delta] no vacuum strategy configured; messages will accumulate in the database without bound")
			return
		}
		if mq.base.vacuumInterval < 100*time.Millisecond {
			mq.base.vacuumInterval = 100 * time.Millisecond
		}
		sleep := mq.base.vacuumInterval
		vacuum := mq.base.vacuum

		mq.base.log.Info("[delta] starting vacuum loop", "interval", sleep)
		for {
			func() {
				defer func() {
					if r := recover(); r != nil {
						mq.base.log.Error("[delta] panic in vacuum loop, recovering", "panic", r)
					}
				}()
				select {
				case <-mq.base.closed:
					return
				case <-time.After(sleep):
					vacuum(mq)
					mq.base.streamMu.RLock()
					streams := make([]*MQ, 0, len(mq.base.streams))
					for _, s := range mq.base.streams {
						streams = append(streams, s)
					}
					mq.base.streamMu.RUnlock()
					for _, s := range streams {
						vacuum(s)
					}
				}
			}()

			// Exit the outer loop only after the MQ is closed. The inner func()
			// returns both on normal ticks and on panic recovery, so we must check.
			select {
			case <-mq.base.closed:
				return
			default:
			}
		}
	}()
}

func (mq *MQ) readloop() {
	mq.base.log.Debug("[delta] starting loop", "stream_", mq.CurrentStream())
	var size = max(uint64(runtime.NumCPU()), 4)

	var jobs = make(chan uint64)

	producer := func() {
		var dirty bool
		for {
			func() {
				defer func() {
					if r := recover(); r != nil {
						mq.base.log.Error("[delta] panic in read-loop producer, recovering", "panic", r)
					}
				}()

				if dirty {
					dirty = false
					err := checkpointReadCursor(mq.base.db, atomic.LoadUint64(&mq.stream.read), mq.tbl)
					if err != nil {
						mq.base.log.Error("[delta] could not checkpoint read cursor", "err", err, "stream_", mq.CurrentStream())
					}
				}

				select {
				case <-mq.base.closed:
					mq.base.log.Debug("[delta] reader, stopping loop", "stream_", mq.CurrentStream())
					return
				case <-mq.stream.inform: // probably faster to remove this and have a tighter sleep
				case <-time.After(100 * time.Millisecond):
				}

				for {
					written := atomic.LoadUint64(&mq.stream.written)
					read := atomic.LoadUint64(&mq.stream.read)

					// if we have read all messages, need to wait for new messages
					if read > written {
						break
					}

					written = min(written, read+size)
					for ; read <= written; read++ {
						jobs <- read
					}
					atomic.StoreUint64(&mq.stream.read, read)
					dirty = true
				}
			}()

			// Exit the outer loop once the MQ has been closed.
			select {
			case <-mq.base.closed:
				return
			default:
			}
		}
	}

	consumer := func(i int) {
		for {
			func() {
				defer func() {
					if r := recover(); r != nil {
						mq.base.log.Error("[delta] panic in read-loop consumer, recovering", "panic", r)
					}
				}()

				if h := getTestPanicHook(); h != nil {
					h()
				}

				var messageId uint64
				select {
				case <-mq.base.closed:
					return
				case messageId = <-jobs:
				}
				m, err := message(mq.base.db, messageId, mq.tbl)
				if err != nil {
					mq.base.log.Error("[delta] could not get message", "id", messageId, "err", err, "stream_", mq.CurrentStream())
					return
				}

				subs := mq.stream.subs.Match(m.Topic)
				for _, s := range subs {
					s := s
					m := m
					m.mq = mq
					go func() { // TODO deap copy message? should it not be a go routine? then a pool of go routines?
						defer func() {
							if r := recover(); r != nil {
								mq.base.log.Error("[delta] panic in subscriber notify goroutine, recovering", "panic", r)
							}
						}()
						s.notify(m)
					}()
				}
			}()

			// Exit the outer loop once the MQ has been closed.
			select {
			case <-mq.base.closed:
				return
			default:
			}
		}
	}

	go producer()

	for i := 0; i < int(size); i++ {
		i := i
		go consumer(i)
	}

}
