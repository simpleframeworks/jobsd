package models

import (
	"time"
)

// Job .
type Job struct {
	ID             int64  `gorm:"primaryKey"`
	Name           string `gorm:"unique"`
	RunsStarted    int
	RunsFinished   int
	RunsErroredOut int
	RunsTimedOut   int
	CreatedAt      time.Time
	CreatedBy      int64
	UpdatedAt      time.Time
	UpdatedBy      int64
}

// TableName specifies the db table name
func (*Job) TableName() string {
	return "jobsd_jobs"
}
