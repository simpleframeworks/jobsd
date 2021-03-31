package jobsd

import (
	"time"

	"github.com/pkg/errors"
	"github.com/simpleframeworks/logc"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// Runnable represents a single runnable job run
type Runnable struct {
	stop        chan struct{}
	kill        <-chan struct{}
	instanceID  int64
	addJobR     chan<- Runnable
	jobRun      *Run
	jobFunc     JobFunc
	jobSchedule *ScheduleFunc
	db          *gorm.DB
	log         logc.Logger
}

func (j *Runnable) schedule() {
	j.jobRun.RunAt = time.Now().Add(j.jobRun.Delay)
	if j.jobSchedule != nil && j.jobRun.needsScheduling() {
		j.jobRun.RunAt = (*j.jobSchedule)(j.jobRun.RunAt)
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

func (j *Runnable) run() RunRes {
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

func (j *Runnable) lock() bool {
	log := j.logger()

	log.Trace("locking job run")
	locked, err := j.jobRun.lock(j.db, j.instanceID)
	if err != nil {
		log.WithError(err).Warn("failed to lock job run")
		return false
	}

	return locked
}

func (j *Runnable) exec() error {
	execRes := make(chan error)
	go func(execRes chan<- error) {
		//TODO add the Runnabled to the first param if needed
		execRes <- j.jobFunc.execute(j.jobRun.JobArgs)
	}(execRes)

	j.stop = make(chan struct{})
	if j.jobRun.RunTimeoutAt.Valid {
		select {
		case <-j.kill:
			close(j.stop)
			return ErrRunKill
		case <-time.After(j.jobRun.RunTimeoutAt.Time.Sub(time.Now())):
			close(j.stop)
			return ErrRunTimeout
		case err := <-execRes:
			return err
		}
	}
	return <-execRes
}

func (j *Runnable) handleTO() {

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

func (j *Runnable) handleErr(err error) {
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

func (j *Runnable) handleSuccess() {
	txErr := j.db.Transaction(func(tx *gorm.DB) error {
		if err := j.jobRun.markComplete(tx, j.instanceID, nil); err != nil {
			return err
		}
		return j.reschedule(tx)
	})

	if txErr != nil {
		j.logger().WithError(txErr).Error("failed to complete and progress successful job run")
	}
}

func (j *Runnable) reschedule(tx *gorm.DB) error {
	if j.jobSchedule != nil && j.jobRun.needsScheduling() {
		next := j.cloneReset()
		next.jobRun.resetErrorRetries()
		next.jobRun.resetTimeoutRetries()
		next.jobRun.Delay = 0
		next.schedule()
		if err := next.jobRun.insertGet(tx); err != nil {
			return err
		}
		j.addJobR <- next
	}
	return nil
}

func (j *Runnable) logger() logc.Logger {
	return j.log.WithFields(logrus.Fields{
		"Run.ID":   j.jobRun.ID,
		"Run.Name": j.jobRun.Name,
		"Run.Job":  j.jobRun.Job,
	})
}

func (j *Runnable) cloneReset() Runnable {
	nexRun := j.jobRun.cloneReset(j.instanceID)
	rtn, _ := newRunnable(
		j.db,
		nexRun,
		j.jobFunc,
		j.jobSchedule,
		j.instanceID,
		j.kill,
		j.log,
	)
	return rtn
}

func newRunnable(
	db *gorm.DB,
	jobRun Run,
	jobFunc JobFunc,
	jobSchedule *ScheduleFunc,
	instanceID int64,
	kill <-chan struct{},
	log logc.Logger,
) (Runnable, error) {

	rtn := Runnable{
		instanceID:  instanceID,
		kill:        kill,
		jobRun:      &jobRun,
		jobFunc:     jobFunc,
		jobSchedule: jobSchedule,
		db:          db,
		log:         log,
	}

	err := jobFunc.check(jobRun.JobArgs)

	return rtn, err

}
