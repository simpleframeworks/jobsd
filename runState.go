package jobsd

import (
	"time"

	"gorm.io/gorm"
)

// RunState is a snapshot of a job runs latest state
type RunState struct {
	db                    *gorm.DB
	OriginID              int64
	Name                  string
	Job                   string
	Schedule              *string
	RunSuccessCount       int
	RunStartedAt          *time.Time
	RunStartedBy          *int64
	RunCompletedAt        *time.Time
	RunCompletedError     *string
	RetriesOnErrorCount   int
	RetriesOnTimeoutCount int
	CreatedAt             time.Time
	CreatedBy             int64
}

// Refresh gets and loads the latest data into RunState
func (j *RunState) Refresh() error {
	jobRun := Run{}
	tx := j.db.Where("id = ? OR origin_id = ?", j.OriginID, j.OriginID).Order("created_at DESC").First(&jobRun)
	if tx.Error != nil {
		return tx.Error
	}

	j.Name = jobRun.Name
	j.RunSuccessCount = jobRun.RunSuccessCount
	j.RetriesOnErrorCount = jobRun.RetriesOnErrorCount
	j.RetriesOnTimeoutCount = jobRun.RetriesOnTimeoutCount
	j.CreatedAt = jobRun.CreatedAt
	j.CreatedBy = jobRun.CreatedBy

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

	return nil
}
