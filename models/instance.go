package models

import (
	"database/sql"
	"time"
)

// Instance .
type Instance struct {
	ID              int64 `gorm:"primaryKey"`
	Workers         int
	Migrate         bool
	RunsStarted     int // Job runs started
	RunsFinished    int // Job runs finished
	RunsTimedOut    int // Job runs timed out
	RunsErroredOut  int // Job runs that have returned an error
	RunsScheduled   int // Job runs rescheduled after finishing
	RunsResurrected int // Job runs resurrected from an unrecoverable timeout (crash)
	PullInterval    time.Duration
	PullLimit       int
	LastSeenAt      time.Time // Last time instance was alive
	StoppedAt       sql.NullTime
	StartedAt       time.Time
}
