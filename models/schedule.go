package models

import (
	"database/sql"
	"time"
)

// Schedule .
type Schedule struct {
	ID         int64 `gorm:"primaryKey"`
	JobID      int64
	JobName    string
	Unique     sql.NullString `gorm:"unique"`
	Scheduled  bool
	Args       RunArgs
	ScheduleAt time.Time

	RunCount int
	RunLimit int // 0 = Unlimited

	CreatedAt time.Time
	CreatedBy int64
}

// TableName specifies the db table name
func (*Schedule) TableName() string {
	return "jobsd_schedules"
}
