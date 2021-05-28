package models

import (
	"database/sql"
	"time"
)

// Active .
type Active struct {
	ID       int64 `gorm:"primaryKey"`
	JobID    int64
	JobName  string
	UniqueID sql.NullInt64 `gorm:"unique"`
	Args     RunArgs

	StartedAt time.Time
	TimeoutAt time.Time

	CreatedAt time.Time
	CreatedBy int64
}

// TableName specifies the db table name
func (*Active) TableName() string {
	return "jobsd_active"
}
