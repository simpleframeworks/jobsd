package jobsd

import (
	"time"

	"github.com/simpleframeworks/logc"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// JobRunnable represents a single runnable job run
type JobRunnable struct {
	ID                    int64
	InstanceID            int64
	OriginID              int64
	Name                  string
	Job                   string
	Schedule              *string
	RunAt                 time.Time
	RunCount              int
	RunStartedAt          time.Time
	RunStartedBy          int64
	TimeoutAt             *time.Time
	RetriesOnTimeoutCount int
	RetriesOnErrorCount   int
	CreatedAt             time.Time
	Stop                  <-chan struct{}
	jobRun                *JobRun
	jobSchedule           ScheduleFunc
	jobFunc               *JobFunc
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

func (j *JobRunnable) run() error {

	return nil
}
func (j *JobRunnable) lock() bool {
	log := j.logger()

	log.Trace("acquiring job run lock")
	locked, err := j.jobRun.lock(j.db, j.InstanceID)
	if err != nil {
		log.WithError(err).Warn("failed to lock job run")
		return false
	}

	return locked
}
func (j *JobRunnable) exeJobFunc() error {
	return nil
}
func (j *JobRunnable) complete() error {
	return nil
}
func (j *JobRunnable) close() error {
	return nil
}
func (j *JobRunnable) createRetryOnErr() (JobRunnable, error) {
	return JobRunnable{}, nil
}
func (j *JobRunnable) createRetryOnTO() (JobRunnable, error) {
	return JobRunnable{}, nil
}
func (j *JobRunnable) createNext() (JobRunnable, error) {
	return JobRunnable{}, nil
}

func (j *JobRunnable) logger() logc.Logger {
	return j.log.WithFields(logrus.Fields{
		"JobRun.ID":   j.jobRun.ID,
		"JobRun.Name": j.jobRun.Name,
		"JobRun.Job":  j.jobRun.Job,
	})
}

func newJobRunnable(db *gorm.DB, j JobRun, f *JobFunc, s *ScheduleFunc) (JobRunnable, error) {

	return JobRunnable{}, nil
}
