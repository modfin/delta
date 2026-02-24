package delta

import (
	"time"
)

func VacuumOnAge(maxAge time.Duration) VacuumFunc {
	if maxAge < 0 {
		maxAge = -maxAge
	}
	return func(mq *MQ) {
		removeBefore := time.Now().Add(-maxAge)

		l := mq.base.log.With("type", "max-age")

		l.Info("[delta] vacuuming", "older_then", removeBefore, "in", mq.CurrentStream())

		removed, err := vacuumBefore(mq.base.db, removeBefore, mq.tbl)
		if err != nil {
			l.Error("[delta] vacuuming error", "err", err)
		}
		if removed > 0 {
			l.Info("[delta] vacuuming result", "removed", removed, "in", mq.CurrentStream())
		}

	}
}

func VacuumKeepN(n int) VacuumFunc {
	return func(mq *MQ) {
		l := mq.base.log.With("type", "keep-n")
		l.Info("[delta] vacuuming", "top", n, "in", mq.CurrentStream())

		removed, err := vacuumKeep(mq.base.db, n, mq.tbl)
		if err != nil {
			l.Error("[delta] vacuuming error", "err", err)
			return
		}
		if removed > 0 {
			l.Info("[delta] vacuuming result", "removed", removed, "in", mq.CurrentStream())
		}

	}
}

func VacuumOnReadAck(mq *MQ) {
	l := mq.base.log.With("type", "read-ack")

	l.Info("[delta] vacuuming", "stream", mq.CurrentStream())

	removed, err := vacuumReadAck(mq.base.db, mq.tbl)
	if err != nil {
		l.Error("[delta] vacuuming error", "err", err)
		return
	}
	if removed > 0 {
		l.Info("[delta] vacuuming result", "removed", removed, "in", mq.CurrentStream())
	}

}
