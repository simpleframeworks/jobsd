package jobsd

import (
	"time"

	"gorm.io/gorm"
)

// JobRunState is a snapshot of a job runs latest state
type JobRunState struct {
	db                    *gorm.DB
	OriginID              int64
	Name                  *string
	RunCount              uint32
	RunStartedAt          *time.Time
	RunStartedBy          *int64
	RunCompletedAt        *time.Time
	RunCompletedError     *string
	RetriesOnErrorCount   uint32
	RetriesOnTimeoutCount uint32
	Schedule              *string
	ClosedAt              *time.Time
	ClosedBy              *int64
	CreatedAt             time.Time
	CreatedBy             int64
}

// Refresh gets and loads the latest data into JobRunState
func (j *JobRunState) Refresh() error {
	jobRun := JobRun{}
	tx := j.db.Where("id = ? OR origin_id = ?", j.OriginID, j.OriginID).Order("created_at DESC").First(&jobRun)
	if tx.Error != nil {
		return tx.Error
	}

	j.RunCount = jobRun.RunCount
	j.RetriesOnErrorCount = jobRun.RetriesOnErrorCount
	j.RetriesOnTimeoutCount = jobRun.RetriesOnTimeoutCount
	j.CreatedAt = jobRun.CreatedAt
	j.CreatedBy = jobRun.CreatedBy

	j.Name = nil
	if jobRun.Name.Valid {
		j.Name = &jobRun.Name.String
	}
	j.RunStartedAt = nil
	if jobRun.RunStartedAt.Valid {
		j.RunStartedAt = &jobRun.RunStartedAt.Time
	}
	j.RunStartedBy = nil
	if jobRun.RunStartedBy.Valid {
		j.RunStartedBy = &jobRun.RunStartedBy.Int64
	}
	j.RunCompletedAt = nil
	if jobRun.RunCompletedAt.Valid {
		j.RunCompletedAt = &jobRun.RunCompletedAt.Time
	}
	j.RunCompletedError = nil
	if jobRun.RunCompletedError.Valid {
		j.RunCompletedError = &jobRun.RunCompletedError.String
	}
	j.Schedule = nil
	if jobRun.Schedule.Valid {
		j.Schedule = &jobRun.Schedule.String
	}
	j.ClosedAt = nil
	if jobRun.ClosedAt.Valid {
		j.ClosedAt = &jobRun.ClosedAt.Time
	}
	j.ClosedBy = nil
	if jobRun.ClosedBy.Valid {
		j.ClosedBy = &jobRun.ClosedBy.Int64
	}

	return nil
}
