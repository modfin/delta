package delta

import (
	"time"
)

// VacuumOnAge returns a vacuum strategy that removes messages older than maxAge.
//
// Negative maxAge values are treated as their absolute value.
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

// VacuumKeepN returns a vacuum strategy that keeps the N newest messages.
//
// Older messages are removed when they are below the persisted read cursor.
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
