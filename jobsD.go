package jobspec

import (
	"time"

	"github.com/pkg/errors"
	"github.com/simpleframeworks/logc"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// JobsD .
type JobsD struct {
	db                      *gorm.DB
	stop                    chan struct{}
	logger                  logc.Logger
	jobs                    map[string]*Job
	migrate                 bool
	workers                 int
	pullInterval            time.Duration
	pullNum                 int
	defaultTimeout          time.Duration
	defaultRetriesOnTimeout int
	defaultRetriesOnError   int
}

// NewJob creates a configurable job that needs to be registered
func (j *JobsD) NewJob(name string, jobFunc interface{}) *Creator {
	return nil
}

// GetJob gets a job to run
func (j *JobsD) GetJob(name string) *Job {
	return nil
}

// GetDB returns the db
func (j *JobsD) GetDB() *gorm.DB {
	return j.db
}

// SetLogger sets the logger
func (j *JobsD) SetLogger(l logc.Logger) *JobsD {
	j.logger = l.WithFields(logrus.Fields{
		"Service": "JobsD",
	})
	return j
}

// SetWorkers sets the number of workers to process jobs
func (j *JobsD) SetWorkers(workers int) *JobsD {
	j.workers = workers
	return j
}

// SetMigration turns on or off auto-migration
func (j *JobsD) SetMigration(m bool) *JobsD {
	j.migrate = m
	return j
}

// PullInterval sets the time between getting new Runs from the DB and cluster
func (j *JobsD) PullInterval(pullInt time.Duration) *JobsD {
	j.pullInterval = pullInt
	return j
}

// PullNum sets the number of upcoming job runs to retrieve from the DB at a time
func (j *JobsD) PullNum(num int) *JobsD {
	j.pullNum = num
	return j
}

// DefaultTimeout sets the DefaultTimeout
// Setting it to 0 disables timeout
func (j *JobsD) DefaultTimeout(timeout time.Duration) *JobsD {
	j.defaultTimeout = timeout
	return j
}

// DefaultRetriesOnTimout sets how many times a job run can timeout
// Setting it to -1 removes the limit
func (j *JobsD) DefaultRetriesOnTimout(num int) *JobsD {
	j.defaultRetriesOnTimeout = num
	return j
}

// DefaultRetriesOnError sets the DefaultRetriesOnError
// Setting it to -1 removes the limit
func (j *JobsD) DefaultRetriesOnError(num int) *JobsD {
	j.defaultRetriesOnError = num
	return j
}

// JobHistory .
func (j *JobsD) JobHistory(name string, limit int) ([]RunState, error) {
	return []RunState{}, errors.New("not implemented")
}

// Start .
func (j *JobsD) Start() error {
	return errors.New("not implemented")
}

// Stop .
func (j *JobsD) Stop() error {
	close(j.stop)
	return errors.New("not implemented")
}

// New .
func New(db *gorm.DB) *JobsD {

	logger := logc.NewLogrus(logrus.New())
	stop := make(chan struct{})

	tx := db.Session(&gorm.Session{
		AllowGlobalUpdate:      true,
		SkipDefaultTransaction: true,
	})

	return &JobsD{
		db:                      tx,
		stop:                    stop,
		logger:                  logger,
		jobs:                    map[string]*Job{},
		migrate:                 true,
		workers:                 10,
		pullInterval:            time.Second * 10,
		pullNum:                 1000,
		defaultTimeout:          time.Minute * 30,
		defaultRetriesOnTimeout: 3,
		defaultRetriesOnError:   3,
	}
}

// ScheduleFunc .
type ScheduleFunc func(now time.Time) time.Time
