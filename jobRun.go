package jobsd

import (
	"database/sql"
	"time"

	"github.com/pkg/errors"
	"github.com/simpleframeworks/logc"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// JobRun .
type JobRun struct {
	ID                    int64 `gorm:"primaryKey"`
	OriginID              int64 `gorm:"index"`
	Name                  sql.NullString
	NameActive            sql.NullString `gorm:"unique"`
	Job                   string
	JobArgs               Args
	Delay                 time.Duration
	RunAt                 time.Time
	RunCount              uint32
	RunLimit              sql.NullInt64
	RunStartedAt          sql.NullTime
	RunStartedBy          sql.NullInt64 `gorm:"index"`
	RunCompletedAt        sql.NullTime
	RunCompletedError     sql.NullString
	RetryTimeout          time.Duration
	RetryTimeoutAt        sql.NullTime `gorm:"index"`
	RetriesOnErrorCount   uint32
	RetriesOnErrorLimit   uint32
	RetriesOnTimeoutCount uint32
	RetriesOnTimeoutLimit uint32
	Schedule              sql.NullString
	ClosedAt              sql.NullTime
	ClosedBy              sql.NullInt64 `gorm:"index"`
	CreatedAt             time.Time     `gorm:"index"`
	CreatedBy             int64
}

// TableName specifies the db table name
func (JobRun) TableName() string {
	return "jobsd_runs"
}

// insertGet inserts the Job in the DB. If it already exists it retrieves it.
func (j *JobRun) insertGet(db *gorm.DB) error {
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

// lockStart lock and start the job to run
func (j *JobRun) lockStart(db *gorm.DB, instanceID int64) (bool, error) {
	startedAt := time.Now()
	retryTimeoutAt := startedAt.Add(j.RetryTimeout)
	tx := db.Model(j).Where("run_started_at IS NULL").Updates(map[string]interface{}{
		"retry_timeout_at": retryTimeoutAt,
		"run_started_at":   startedAt,
		"run_started_by":   instanceID,
	})
	if tx.Error != nil {
		return false, tx.Error
	}
	locked := tx.RowsAffected == 1
	if locked {
		j.RetryTimeoutAt = sql.NullTime{Valid: true, Time: retryTimeoutAt}
		j.RunStartedAt = sql.NullTime{Valid: true, Time: startedAt}
		j.RunStartedBy = sql.NullInt64{Valid: true, Int64: instanceID}
	}
	return locked, tx.Error
}

// complete the job
func (j *JobRun) complete(db *gorm.DB, instanceID int64, jobRunErr error) error {

	completedErr := sql.NullString{}
	if jobRunErr != nil {
		completedErr = sql.NullString{Valid: true, String: jobRunErr.Error()}
	} else {
		j.RunCount++
	}

	tx := db.Model(j).Where("run_completed_at IS NULL").Updates(map[string]interface{}{
		"run_count":           j.RunCount,
		"name_active":         sql.NullString{},
		"run_completed_at":    sql.NullTime{Valid: true, Time: time.Now()},
		"run_completed_error": completedErr,
	})
	if tx.Error != nil {
		return tx.Error
	}

	return db.First(j, j.ID).Error
}

// hasClosed .
func (j *JobRun) hasClosed() bool {
	return j.ClosedAt.Valid && j.ClosedBy.Valid
}

func (j *JobRun) hasCompleted() bool {
	return j.RunCompletedAt.Valid
}

func (j *JobRun) hasCompletedOk() bool {
	return j.hasCompleted() && !j.RunCompletedError.Valid
}

func (j *JobRun) hasCompletedWithError() bool {
	return j.hasCompleted() && j.RunCompletedError.Valid
}

func (j *JobRun) hasReachedErrorLimit() bool {
	return j.RetriesOnErrorCount >= j.RetriesOnErrorLimit
}

func (j *JobRun) hasReachedTimeoutLimit() bool {
	return j.RetriesOnTimeoutCount >= j.RetriesOnTimeoutLimit
}

func (j *JobRun) needsScheduling() bool {
	return j.Schedule.Valid && (!j.RunLimit.Valid || j.RunLimit.Int64 > int64(j.RunCount))
}

func (j *JobRun) resetErrorRetries() {
	j.RetriesOnErrorCount = 0
}

func (j *JobRun) resetTimeoutRetries() {
	j.RetriesOnTimeoutCount = 0
}

// close the job run so no retries or rescheduling can be done
func (j *JobRun) close(db *gorm.DB, instanceID int64) error {
	if j.hasClosed() {
		return nil
	}
	tx := db.Model(j).Where("closed_at IS NULL").Updates(map[string]interface{}{
		"name_active": sql.NullString{},
		"closed_at":   time.Now(),
		"closed_by":   instanceID,
	})
	if tx.RowsAffected != 1 {
		return errors.New("job run already closed")
	}
	return nil
}

// retryOnError does that
func (j *JobRun) retryOnError(db *gorm.DB, instanceID int64) (*JobRun, error) {

	if j.hasClosed() || !j.hasCompletedWithError() || j.hasReachedErrorLimit() {
		return nil, nil
	}

	// retry the job run if it produced an error
	nextJobRun := j.cloneReset(instanceID)
	nextJobRun.RetriesOnErrorCount++

	txErr := db.Transaction(func(tx *gorm.DB) error {
		if err := j.close(tx, instanceID); err != nil {
			return err
		}
		return nextJobRun.insertGet(tx)
	})
	return nextJobRun, txErr
}

// retryOnTimeout does that
func (j *JobRun) retryOnTimeout(db *gorm.DB, instanceID int64) (*JobRun, error) {

	if j.hasClosed() || j.hasCompleted() || j.hasReachedTimeoutLimit() {
		return nil, nil
	}

	// retry the job run because it timed out
	nextJobRun := j.cloneReset(instanceID)
	nextJobRun.RetriesOnTimeoutCount++

	txErr := db.Transaction(func(tx *gorm.DB) error {
		if err := j.close(tx, instanceID); err != nil {
			return err
		}
		return nextJobRun.insertGet(tx)
	})
	return nextJobRun, txErr
}

// reschedule the job run
func (j *JobRun) reschedule(db *gorm.DB, instanceID int64, schedules map[string]ScheduleFunc) (*JobRun, error) {

	if j.hasClosed() || !j.needsScheduling() {
		return nil, nil
	}

	schedule, exists := schedules[j.Schedule.String]
	if !exists {
		return nil, errors.New("cannot reschedule job run, schedule does not exist")
	}

	nextJobRun := j.cloneReset(instanceID)
	nextJobRun.RunAt = schedule(j.RunCompletedAt.Time)
	nextJobRun.resetErrorRetries()
	nextJobRun.resetTimeoutRetries()

	txErr := db.Transaction(func(tx *gorm.DB) error {
		if err := j.close(tx, instanceID); err != nil {
			return err
		}
		return nextJobRun.insertGet(tx)
	})
	return nextJobRun, txErr
}

func (j *JobRun) check(
	jobs map[string]*JobContainer,
	schedules map[string]ScheduleFunc,
) error {

	jobC, exists := jobs[j.Job]
	if !exists {
		return errors.New("cannot run job. job '" + j.Job + "' does not exist")
	}
	if err := jobC.jobFunc.check(j.JobArgs); err != nil {
		return err
	}

	if j.needsScheduling() {
		_, exists := schedules[j.Schedule.String]
		if !exists {
			return errors.New("cannot schedule job. schedule '" + j.Schedule.String + "' missing")
		}
	}

	return nil
}

func (j *JobRun) run(jobsd *JobsD) (int64, error) {
	if err := j.check(jobsd.jobs, jobsd.schedules); err != nil {
		return 0, err
	}

	j.RunAt = time.Now().Add(j.Delay)
	if j.needsScheduling() {
		// schedule it
		j.RunAt = jobsd.schedules[j.Schedule.String](j.RunAt)
	}

	err := j.insertGet(jobsd.db)
	if err != nil {
		return 0, err
	}
	jobsd.log.WithFields(map[string]interface{}{
		"Job.ID":    j.ID,
		"Job.RunAt": j.RunAt,
	}).Trace("created job run")
	jobsd.addJobRun(*j)

	return j.ID, nil
}

// cloneReset clones the JobRun and resets it for the next run
func (j *JobRun) cloneReset(instanceID int64) *JobRun {
	return &JobRun{
		OriginID:              j.ID,
		Name:                  j.Name,
		NameActive:            j.Name,
		Job:                   j.Job,
		JobArgs:               j.JobArgs,
		RunCount:              j.RunCount,
		RunLimit:              j.RunLimit,
		RunAt:                 time.Now(),
		RetriesOnErrorCount:   j.RetriesOnErrorCount,
		RetriesOnErrorLimit:   j.RetriesOnErrorLimit,
		RetriesOnTimeoutCount: j.RetriesOnTimeoutCount,
		RetriesOnTimeoutLimit: j.RetriesOnTimeoutLimit,
		RetryTimeout:          j.RetryTimeout,
		Schedule:              j.Schedule,
		CreatedAt:             time.Now(),
		CreatedBy:             instanceID,
	}
}

func (j *JobRun) logger(logger logc.Logger) logc.Logger {
	return logger.WithFields(logrus.Fields{
		"JobRun.ID":   j.ID,
		"JobRun.Name": j.Name.String,
		"JobRun.Job":  j.Job,
	})
}
