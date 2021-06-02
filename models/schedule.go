package models

import (
	"database/sql"
	"time"
)

// Schedule .
type Schedule struct {
	ID    int64 `gorm:"primaryKey"`
	JobID int64 `gorm:"unique"`
	Args  RunArgs

	ScheduleAt time.Time
	NextRunAt  time.Time

	ScheduleCount int
	ScheduleLimit int // 0 = Unlimited

	LastScheduledAt sql.NullTime
	LastScheduledBy sql.NullInt64

	CreatedAt time.Time
	CreatedBy int64
}

// TableName specifies the db table name
func (*Schedule) TableName() string {
	return "jobsd_schedules"
}
