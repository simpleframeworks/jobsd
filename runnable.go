package jobsd

import (
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
	stop        chan struct{}
	kill        <-chan struct{}
	log         logc.Logger
	mu          sync.Mutex
}

func (j *Runnable) runAt() time.Time {
	return j.jobRun.RunAt
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
	log := j.log

	if !j.lock() {
		log.Debug("job run already locked")
		return RunResLockLost
	}

	err := j.exec()
	if err == ErrRunTimeout {
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
	log := j.log

	log.Trace("locking job run")
	locked, err := j.jobRun.lock(j.db, j.instanceID)
	if err != nil {
		log.WithError(err).Warn("failed to lock job run")
		return false
	}

	return locked
}

func (j *Runnable) exec() error {
	log := j.log
	j.stop = make(chan struct{})
	defer close(j.stop)

	log.Debug("run exec")
	execRes := make(chan error)
	go func() {
		//TODO add the Runnabled to the first param if needed
		execRes <- j.jobFunc.execute(j.jobRun.JobArgs)
		close(execRes)
	}()

	if j.jobRun.RunTimeoutAt.Valid {
		timeOut := time.NewTimer(j.jobRun.RunTimeoutAt.Time.Sub(time.Now()))
		cleanTimer := func() {
			if !timeOut.Stop() { // clean up timer
				<-timeOut.C
			}
		}
		select {
		case err := <-execRes:
			log.Debug("run exec completed")
			cleanTimer()
			return err
		case <-timeOut.C:
			log.Debug("run exec timed out")
			return ErrRunTimeout
		case <-j.kill:
			log.Debug("run exec killed")
			cleanTimer()
			return ErrRunKill
		}
	}

	err := <-execRes
	log.Debug("run exec completed")
	return err
}

func (j *Runnable) handleTO() {
	log := j.log
	log.Debug("handling job run time out")
	txErr := j.db.Transaction(func(tx *gorm.DB) error {
		if err := j.jobRun.markComplete(tx, j.instanceID, ErrRunTimeout); err != nil {
			return err
		}
		if !j.jobRun.hasReachedTimeoutLimit() {
			next := j.cloneReset(false)
			next.jobRun.RetriesOnTimeoutCount++
			next.save(tx)
			j.runQAdd <- next
		} else {
			return j.reschedule(tx)
		}
		return nil
	})

	if txErr != nil {
		j.log.WithError(txErr).Error("failed to complete and progress job run after time out")
	}
}

func (j *Runnable) handleErr(err error) {
	log := j.log
	log.Debug("handling job run error")
	txErr := j.db.Transaction(func(tx *gorm.DB) error {
		if err := j.jobRun.markComplete(tx, j.instanceID, err); err != nil {
			return err
		}
		if !j.jobRun.hasReachedErrorLimit() {
			next := j.cloneReset(false)
			next.jobRun.RetriesOnErrorCount++
			next.save(tx)
			j.runQAdd <- next
		} else {
			return j.reschedule(tx)
		}
		return nil
	})

	if txErr != nil {
		j.log.WithError(txErr).Error("failed to complete and progress job run after erroring out")
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
		j.log.WithError(txErr).Error("failed to complete and progress successful job run")
	}
}

func (j *Runnable) reschedule(tx *gorm.DB) error {

	if j.jobSchedule != nil && j.jobRun.needsScheduling() {
		next := j.cloneReset(true)
		if err := next.save(tx); err != nil {
			return err
		}
		next.log.WithFields(map[string]interface{}{
			"Run.At": next.jobRun.RunAt,
		}).Debug("reschedule job run")

		j.runQAdd <- next
	}
	return nil
}

func (j *Runnable) save(tx *gorm.DB) error {
	if err := j.jobRun.insertGet(tx); err != nil {
		return err
	}
	j.log = j.log.WithFields(logrus.Fields{
		"Run.ID":   j.jobRun.ID,
		"Run.Name": j.jobRun.Name,
		"Run.Job":  j.jobRun.Job,
	})
	return nil
}

func (j *Runnable) cloneReset(failCounts bool) *Runnable {
	nexRun := j.jobRun.cloneReset(j.instanceID)
	if failCounts {
		nexRun.resetErrorRetries()
		nexRun.resetTimeoutRetries()
	}
	rtn, _ := newRunnable(
		j.instanceID,
		nexRun,
		j.jobFunc,
		j.jobSchedule,
		j.runQAdd,
		j.db,
		j.kill,
		j.log,
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
		"Run.Name": rtn.jobRun.Name,
		"Run.Job":  rtn.jobRun.Job,
	})
	rtn.schedule()

	err := jobFunc.check(jobRun.JobArgs)

	return rtn, err

}
