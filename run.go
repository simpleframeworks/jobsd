package jobss

import (
	"database/sql"
	"time"

	"github.com/pkg/errors"
	"github.com/simpleframeworks/logc"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Run is a database representation of a job run
type Run struct {
	ID                    int64 `gorm:"primaryKey"`
	OriginID              int64 `gorm:"index"`
	Name                  string
	NameActive            sql.NullString `gorm:"unique"`
	Job                   sql.NullString
	JobArgs               JobArgs
	Schedule              sql.NullString
	Package               sql.NullString
	Delay                 time.Duration
	RunAt                 time.Time
	RunTotalCount         int
	RunSuccessCount       int
	RunSuccessLimit       sql.NullInt64
	RunStartedAt          sql.NullTime `gorm:"index"`
	RunStartedBy          sql.NullInt64
	RunCompletedAt        sql.NullTime `gorm:"index"`
	RunCompletedError     sql.NullString
	RunTimeout            sql.NullInt64
	RunTimeoutAt          sql.NullTime `gorm:"index"`
	RetriesOnErrorCount   int
	RetriesOnErrorLimit   sql.NullInt64
	RetriesOnTimeoutCount int
	RetriesOnTimeoutLimit sql.NullInt64
	CreatedAt             time.Time `gorm:"index"`
	CreatedBy             int64
}

// TableName specifies the db table name
func (Run) TableName() string {
	return "jobss_runs"
}

// insertGet inserts the Job in the DB. If it already exists it retrieves it.
func (j *Run) insertGet(db *gorm.DB) error {
	if !j.NameActive.Valid {
		return errors.New("the active name, NameActive, is required")
	}
	tx := db.Clauses(clause.OnConflict{DoNothing: true}).Create(j)
	if tx.Error != nil {
		return tx.Error
	}
	if j.ID == 0 {
		db.Where("name_active = ?", j.NameActive).First(j)
	}
	return tx.Error
}

// lock the job to run
func (j *Run) lock(db *gorm.DB, instanceID int64) (bool, error) {
	startedAt := time.Now()
	runTimeoutAt := startedAt.Add(time.Duration(j.RunTimeout.Int64))
	tx := db.Model(j).Where("run_started_at IS NULL").Updates(map[string]interface{}{
		"run_timeout_at": runTimeoutAt,
		"run_started_at": startedAt,
		"run_started_by": instanceID,
	})
	if tx.Error != nil {
		return false, tx.Error
	}
	locked := tx.RowsAffected == 1
	if locked {
		j.RunTimeoutAt = sql.NullTime{Valid: true, Time: runTimeoutAt}
		j.RunStartedAt = sql.NullTime{Valid: true, Time: startedAt}
		j.RunStartedBy = sql.NullInt64{Valid: true, Int64: instanceID}
	}
	return locked, tx.Error
}

// markComplete mark the job completed with error
func (j *Run) markComplete(db *gorm.DB, instanceID int64, jobRunErr error) error {

	j.NameActive = sql.NullString{}

	j.RunTotalCount++

	j.RunCompletedError = sql.NullString{}
	if jobRunErr != nil {
		j.RunCompletedError = sql.NullString{Valid: true, String: jobRunErr.Error()}
	} else {
		j.RunSuccessCount++
	}
	j.RunCompletedAt = sql.NullTime{Valid: true, Time: time.Now()}

	tx := db.Model(j).Where("run_completed_at IS NULL").Updates(map[string]interface{}{
		"name_active":         j.NameActive,
		"run_total_count":     j.RunTotalCount,
		"run_success_count":   j.RunSuccessCount,
		"run_completed_at":    j.RunCompletedAt,
		"run_completed_error": j.RunCompletedError,
	})
	err := tx.Error
	if err == nil && tx.RowsAffected != 1 {
		err = errors.New("could not mark job run as completed")
	}
	return err
}

func (j *Run) hasTimedOut() bool {
	return j.RunTimeoutAt.Valid && j.RunTimeoutAt.Time.After(time.Now())
}

func (j *Run) hasCompleted() bool {
	return j.RunCompletedAt.Valid
}

func (j *Run) hasReachedErrorLimit() bool {
	return j.RetriesOnErrorLimit.Valid && j.RetriesOnErrorCount >= int(j.RetriesOnErrorLimit.Int64)
}

func (j *Run) hasReachedTimeoutLimit() bool {
	return j.RetriesOnTimeoutLimit.Valid && j.RetriesOnTimeoutCount >= int(j.RetriesOnTimeoutLimit.Int64)
}

func (j *Run) needsScheduling() bool {
	if !j.Schedule.Valid {
		return false
	}
	if j.RunSuccessLimit.Valid {
		return j.RunSuccessCount < int(j.RunSuccessLimit.Int64)
	}
	return true
}

func (j *Run) resetErrorRetries() {
	j.RetriesOnErrorCount = 0
}

func (j *Run) resetTimeoutRetries() {
	j.RetriesOnTimeoutCount = 0
}

// cloneReset clones the Run and resets it for the next run
func (j *Run) cloneReset(instanceID int64) Run {
	return Run{
		OriginID:              j.ID,
		Name:                  j.Name,
		NameActive:            sql.NullString{Valid: true, String: j.Name},
		Job:                   j.Job,
		JobArgs:               j.JobArgs,
		RunSuccessCount:       j.RunSuccessCount,
		RunSuccessLimit:       j.RunSuccessLimit,
		RunAt:                 time.Now(),
		RunTimeout:            j.RunTimeout,
		RetriesOnErrorCount:   j.RetriesOnErrorCount,
		RetriesOnErrorLimit:   j.RetriesOnErrorLimit,
		RetriesOnTimeoutCount: j.RetriesOnTimeoutCount,
		RetriesOnTimeoutLimit: j.RetriesOnTimeoutLimit,
		Schedule:              j.Schedule,
		CreatedAt:             time.Now(),
		CreatedBy:             instanceID,
	}
}

func (j *Run) logger(logger logc.Logger) logc.Logger {
	return logger.WithFields(logrus.Fields{
		"Run.ID":   j.ID,
		"Run.Name": j.Name,
		"Run.Job":  j.Job,
	})
}
