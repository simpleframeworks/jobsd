package jobsd

import (
	"time"

	"github.com/pkg/errors"
	"github.com/simpleframeworks/logc"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// JobRunnable represents a single runnable job run
type JobRunnable struct {
	ID                    int64
	OriginID              int64
	Name                  string
	Job                   string
	JobArgs               JobArgs
	RunAt                 time.Time
	RunTotalCount         int
	RunSuccessCount       int
	RunSuccessLimit       *int
	RunStartedAt          time.Time
	RunStartedBy          int64
	RunTimeout            time.Duration
	RunTimeoutAt          *time.Time
	RetriesOnErrorCount   int
	RetriesOnErrorLimit   *int
	RetriesOnTimeoutCount int
	RetriesOnTimeoutLimit *int
	Schedule              *string
	CreatedAt             time.Time
	CreatedBy             int64
	Stop                  <-chan struct{}
	instanceID            int64
	addJobR               chan<- JobRunnable
	jobRun                *JobRun
	jobSchedule           ScheduleFunc
	jobFunc               JobFunc
	db                    *gorm.DB
	log                   logc.Logger
}

func (j *JobRunnable) schedule() {
	if j.jobRun.needsScheduling() {
		j.jobRun.RunAt = j.jobSchedule(time.Now())
	}
}
func (j *JobRunnable) scheduleNew() {
	j.jobRun.RunAt = time.Now().Add(j.jobRun.Delay)
	if j.jobRun.needsScheduling() {
		j.jobRun.RunAt = j.jobSchedule(j.jobRun.RunAt)
	}
}

// RunRes is the result of trying to run the job
type RunRes int

const (
	// RunResLockLost It could not lock the job to run it
	RunResLockLost RunRes = iota
	// RunResTO It ran and timed out
	RunResTO
	// RunResError It ran and returned an error
	RunResError
	// RunResSuccess It ran successfully
	RunResSuccess
)

// ErrRunTimeout returns if a job times out
var ErrRunTimeout = errors.New("job run timed out")

// ErrRunKill returns if a job was killed
var ErrRunKill = errors.New("job run killed")

func (j *JobRunnable) run() RunRes {
	log := j.logger()

	if !j.lock() {
		return RunResLockLost
	}

	err := j.exec()
	if err == ErrRunTimeout {
		log.Warn("job run timed out")
		j.handleTO()
		return RunResTO
	} else if err != nil {
		log.WithError(err).Warn("job run finished with an error")
		j.handleErr(err)
		return RunResError
	}

	j.handleSuccess()
	return RunResSuccess
}

func (j *JobRunnable) lock() bool {
	log := j.logger()

	log.Trace("locking job run")
	locked, err := j.jobRun.lock(j.db, j.instanceID)
	if err != nil {
		log.WithError(err).Warn("failed to lock job run")
		return false
	}
	if locked {
		j.RunStartedAt = j.jobRun.RunStartedAt.Time
		j.RunStartedBy = j.jobRun.RunStartedBy.Int64
		if j.jobRun.RunTimeoutAt.Valid {
			j.RunTimeoutAt = &j.jobRun.RunTimeoutAt.Time
		}
	}
	return locked
}

func (j *JobRunnable) exec() error {
	execRes := make(chan error)
	go func(execRes chan<- error) {
		//TODO add the JobRunnabled to the first param if needed
		execRes <- j.jobFunc.execute(j.jobRun.JobArgs)
	}(execRes)

	if j.RunTimeoutAt != nil {
		select {
		case <-j.Stop:
			return ErrRunKill
		case <-time.After(j.RunTimeoutAt.Sub(time.Now())):
			return ErrRunTimeout
		case err := <-execRes:
			return err
		}
	}
	return <-execRes
}

func (j *JobRunnable) handleTO() {

	txErr := j.db.Transaction(func(tx *gorm.DB) error {
		if err := j.jobRun.markComplete(j.db, j.instanceID, ErrRunTimeout); err != nil {
			return err
		}
		if !j.jobRun.hasReachedTimeoutLimit() {
			next := j.cloneReset()
			next.jobRun.RetriesOnTimeoutCount++
			next.jobRun.insertGet(tx)
			j.addJobR <- next
		} else {
			return j.reschedule(tx)
		}
		return nil
	})

	if txErr != nil {
		j.logger().WithError(txErr).Error("failed to complete and progress job run after timeout")
	}
}

func (j *JobRunnable) handleErr(err error) {
	txErr := j.db.Transaction(func(tx *gorm.DB) error {
		if err := j.jobRun.markComplete(j.db, j.instanceID, err); err != nil {
			return err
		}
		if !j.jobRun.hasReachedErrorLimit() {
			next := j.cloneReset()
			next.jobRun.RetriesOnErrorCount++
			next.jobRun.insertGet(tx)
			j.addJobR <- next
		} else {
			return j.reschedule(tx)
		}
		return nil
	})

	if txErr != nil {
		j.logger().WithError(txErr).Error("failed to complete and progress job run after erroring out")
	}
}

func (j *JobRunnable) handleSuccess() {
	txErr := j.db.Transaction(func(tx *gorm.DB) error {
		if err := j.jobRun.markComplete(j.db, j.instanceID, nil); err != nil {
			return err
		}
		return j.reschedule(tx)
	})

	if txErr != nil {
		j.logger().WithError(txErr).Error("failed to complete and progress successful job run")
	}
}

func (j *JobRunnable) reschedule(tx *gorm.DB) error {
	if j.jobRun.needsScheduling() {
		next := j.cloneReset()
		next.jobRun.RunAt = j.jobSchedule(j.jobRun.RunCompletedAt.Time)
		next.jobRun.Delay = 0
		next.jobRun.resetErrorRetries()
		next.jobRun.resetTimeoutRetries()
		if err := next.jobRun.insertGet(tx); err != nil {
			return err
		}
		j.addJobR <- next
	}
	return nil
}

func (j *JobRunnable) logger() logc.Logger {
	return j.log.WithFields(logrus.Fields{
		"JobRun.ID":   j.jobRun.ID,
		"JobRun.Name": j.jobRun.Name,
		"JobRun.Job":  j.jobRun.Job,
	})
}

func (j *JobRunnable) cloneReset() JobRunnable {
	nexJobRun := j.jobRun.cloneReset(j.instanceID)
	rtn, _ := newJobRunnable(
		j.db,
		nexJobRun,
		j.jobFunc,
		j.jobSchedule,
		j.instanceID,
	)
	return rtn
}

func newJobRunnable(db *gorm.DB, jobRun JobRun, jobFunc JobFunc, jobSchedule ScheduleFunc, instanceID int64) (JobRunnable, error) {

	rtn := JobRunnable{
		ID:                    jobRun.ID,
		OriginID:              jobRun.OriginID,
		Name:                  jobRun.Name,
		Job:                   jobRun.Job,
		JobArgs:               jobRun.JobArgs,
		RunAt:                 jobRun.RunAt,
		RunTotalCount:         jobRun.RunTotalCount,
		RunSuccessCount:       jobRun.RunSuccessCount,
		RunTimeout:            jobRun.RunTimeout,
		RetriesOnErrorCount:   jobRun.RetriesOnErrorCount,
		RetriesOnTimeoutCount: jobRun.RetriesOnTimeoutCount,
		CreatedAt:             jobRun.CreatedAt,
		CreatedBy:             jobRun.CreatedBy,
		db:                    db,
		jobRun:                &jobRun,
		jobFunc:               jobFunc,
		jobSchedule:           jobSchedule,
	}
	if jobRun.RunSuccessLimit.Valid {
		rsl := int(jobRun.RunSuccessLimit.Int64)
		rtn.RunSuccessLimit = &rsl
	}
	if jobRun.RetriesOnErrorLimit.Valid {
		rel := int(jobRun.RetriesOnErrorLimit.Int64)
		rtn.RetriesOnErrorLimit = &rel
	}
	if jobRun.RetriesOnTimeoutLimit.Valid {
		rtl := int(jobRun.RetriesOnTimeoutLimit.Int64)
		rtn.RetriesOnTimeoutLimit = &rtl
	}
	if jobRun.Schedule.Valid {
		rtn.Schedule = &jobRun.Schedule.String
	}

	err := jobFunc.check(jobRun.JobArgs)

	return rtn, err
}
