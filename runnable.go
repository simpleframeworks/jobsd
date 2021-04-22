package jobsd

import (
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/simpleframeworks/logc"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// Runnable represents a single runnable job run
type Runnable struct {
	instanceID  int64
	jobRun      *Run
	jobFunc     JobFunc
	jobSchedule *ScheduleFunc
	runQAdd     chan<- *Runnable
	db          *gorm.DB
	cancel      chan struct{}
	kill        <-chan struct{}
	log         logc.Logger
	mu          sync.Mutex
}

func (r *Runnable) runAt() time.Time {
	return r.jobRun.RunAt
}

func (r *Runnable) schedule() {
	r.jobRun.RunAt = time.Now().Add(r.jobRun.Delay)
	if r.jobSchedule != nil && r.jobRun.needsScheduling() {
		r.jobRun.RunAt = (*r.jobSchedule)(r.jobRun.RunAt)
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

func (r *Runnable) run() RunRes {

	if !r.lock() {
		r.log.Debug("job run already locked")
		return RunResLockLost
	}
	r.log.Debug("job run lock acquired")

	err := r.exec()
	if err == ErrRunTimeout {
		r.handleTO()
		return RunResTO
	} else if err != nil {
		r.log.WithError(err).Warn("job run finished with an error")
		r.handleErr(err)
		return RunResError
	}

	r.handleSuccess()
	return RunResSuccess
}

func (r *Runnable) lock() bool {

	r.log.Trace("locking job run")
	locked, err := r.jobRun.lock(r.db, r.instanceID)
	if err != nil {
		r.log.WithError(err).Warn("failed to lock job run")
		return false
	}

	return locked
}

func (r *Runnable) exec() (rtn error) {

	r.cancel = make(chan struct{})
	defer close(r.cancel)

	r.log.Debug("run exec")
	runInfo := newRunInfo(*r.jobRun, r.cancel)
	execRes := make(chan error, 1)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				r.log.WithField("error", err).Error("panic occurred on job run - recovered")
				execRes <- fmt.Errorf("panic occurred: %s", fmt.Sprint(err))
			}
		}()
		execRes <- r.jobFunc.execute(runInfo, r.jobRun.JobArgs)
	}()

	if r.jobRun.RunTimeoutAt.Valid {
		timeOut := time.NewTimer(r.jobRun.RunTimeoutAt.Time.Sub(time.Now()))
		cleanTimer := func() {
			if !timeOut.Stop() { // clean up timer
				<-timeOut.C
			}
		}
		select {
		case rtn = <-execRes:
			r.log.Debug("run exec completed")
			cleanTimer()
			close(execRes)
		case <-timeOut.C:
			r.log.Debug("run exec timed out")
			rtn = ErrRunTimeout
		case <-r.kill:
			r.log.Debug("run exec killed")
			cleanTimer()
			rtn = ErrRunKill
		}
		return rtn
	}

	select {
	case rtn = <-execRes:
		r.log.Debug("run exec completed")
	case <-r.kill:
		r.log.Debug("run exec killed")
		rtn = ErrRunKill
	}
	return rtn
}

func (r *Runnable) handleTO() {
	r.log.Debug("handling job run time out")
	var next *Runnable
	txErr := r.db.Transaction(func(tx *gorm.DB) (err error) {
		if err = r.jobRun.markComplete(tx, r.instanceID, ErrRunTimeout); err != nil {
			return err
		}
		if !r.jobRun.hasReachedTimeoutLimit() {
			next = r.cloneReset(false)
			next.jobRun.RetriesOnTimeoutCount++
			next.save(tx)
		} else {
			next, err = r.reschedule(tx)
			return err
		}
		return nil
	})

	if txErr != nil {
		r.log.WithError(txErr).Error("failed to complete and progress job run after time out")
	} else if next != nil {
		r.runQAdd <- next
	}
}

func (r *Runnable) handleErr(err error) {
	r.log.Debug("handling job run error")
	var next *Runnable
	txErr := r.db.Transaction(func(tx *gorm.DB) (err error) {
		if err = r.jobRun.markComplete(tx, r.instanceID, err); err != nil {
			return err
		}
		if !r.jobRun.hasReachedErrorLimit() {
			next = r.cloneReset(false)
			next.jobRun.RetriesOnErrorCount++
			next.save(tx)
		} else {
			next, err = r.reschedule(tx)
			return err
		}
		return nil
	})

	if txErr != nil {
		r.log.WithError(txErr).Error("failed to complete and progress job run after erroring out")
	} else if next != nil {
		r.runQAdd <- next
	}
}

func (r *Runnable) handleSuccess() {
	var next *Runnable
	txErr := r.db.Transaction(func(tx *gorm.DB) (err error) {
		if err = r.jobRun.markComplete(tx, r.instanceID, nil); err != nil {
			return err
		}
		next, err = r.reschedule(tx)
		return err
	})

	if txErr != nil {
		r.log.WithError(txErr).Error("failed to complete and progress successful job run")
	} else if next != nil {
		r.runQAdd <- next
	}
}

func (r *Runnable) reschedule(tx *gorm.DB) (*Runnable, error) {

	if r.jobSchedule == nil || !r.jobRun.needsScheduling() {
		r.log.WithFields(map[string]interface{}{
			"Run.Schedule":        r.jobRun.Schedule,
			"Run.RunSuccessLimit": r.jobRun.RunSuccessLimit,
		}).Debug("rescheduling job not required")
		return nil, nil
	}

	r.log.Debug("rescheduling job")
	next := r.cloneReset(true)
	if err := next.save(tx); err != nil {
		return nil, err
	}
	next.log.WithFields(map[string]interface{}{
		"Run.At": next.jobRun.RunAt,
	}).Debug("rescheduled new job run")

	return next, nil
}

func (r *Runnable) save(tx *gorm.DB) error {
	if err := r.jobRun.insertGet(tx); err != nil {
		return err
	}
	r.log = r.log.WithFields(logrus.Fields{
		"Run.ID":   r.jobRun.ID,
		"Run.Name": r.jobRun.Name,
		"Run.Job":  r.jobRun.Job,
	})
	return nil
}

func (r *Runnable) cloneReset(failCounts bool) *Runnable {
	nextRun := r.jobRun.cloneReset(r.instanceID)
	if failCounts {
		nextRun.resetErrorRetries()
		nextRun.resetTimeoutRetries()
	}
	rtn, _ := newRunnable(
		r.instanceID,
		nextRun,
		r.jobFunc,
		r.jobSchedule,
		r.runQAdd,
		r.db,
		r.kill,
		r.log,
	)
	rtn.schedule()

	return rtn
}

func newRunnable(
	instanceID int64,
	jobRun Run,
	jobFunc JobFunc,
	jobSchedule *ScheduleFunc,
	runQAdd chan<- *Runnable,
	db *gorm.DB,
	kill <-chan struct{},
	log logc.Logger,
) (*Runnable, error) {

	rtn := &Runnable{
		kill:        kill,
		instanceID:  instanceID,
		jobRun:      &jobRun,
		jobFunc:     jobFunc,
		jobSchedule: jobSchedule,
		runQAdd:     runQAdd,
		db:          db,
	}
	rtn.log = log.WithFields(logrus.Fields{
		"Run.ID":   "",
		"Run.Job":  rtn.jobRun.Job,
		"Run.At":   rtn.jobRun.RunAt,
		"Run.Name": rtn.jobRun.Name,
	})
	rtn.schedule()

	err := jobFunc.check(jobRun.JobArgs)

	return rtn, err

}
