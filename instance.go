package jobspec

import (
	"time"

	"github.com/pkg/errors"
	"github.com/simpleframeworks/logc"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// Instance .
type Instance struct {
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
func (j *Instance) NewJob(name string, jobFunc interface{}) *Creator {
	return nil
}

// GetJob gets a job to run
func (j *Instance) GetJob(name string) *Job {
	return nil
}

// GetDB returns the db
func (j *Instance) GetDB() *gorm.DB {
	return j.db
}

// SetLogger sets the logger
func (j *Instance) SetLogger(l logc.Logger) *Instance {
	j.logger = l.WithFields(logrus.Fields{
		"Service": "Instance",
	})
	return j
}

// SetWorkers sets the number of workers to process jobs
func (j *Instance) SetWorkers(workers int) *Instance {
	j.workers = workers
	return j
}

// SetMigration turns on or off auto-migration
func (j *Instance) SetMigration(m bool) *Instance {
	j.migrate = m
	return j
}

// PullInterval sets the time between getting new Runs from the DB and cluster
func (j *Instance) PullInterval(pullInt time.Duration) *Instance {
	j.pullInterval = pullInt
	return j
}

// PullNum sets the number of upcoming job runs to retrieve from the DB at a time
func (j *Instance) PullNum(num int) *Instance {
	j.pullNum = num
	return j
}

// DefaultTimeout sets the DefaultTimeout
// Setting it to 0 disables timeout
func (j *Instance) DefaultTimeout(timeout time.Duration) *Instance {
	j.defaultTimeout = timeout
	return j
}

// DefaultRetriesOnTimout sets how many times a job run can timeout
// Setting it to -1 removes the limit
func (j *Instance) DefaultRetriesOnTimout(num int) *Instance {
	j.defaultRetriesOnTimeout = num
	return j
}

// DefaultRetriesOnError sets the DefaultRetriesOnError
// Setting it to -1 removes the limit
func (j *Instance) DefaultRetriesOnError(num int) *Instance {
	j.defaultRetriesOnError = num
	return j
}

// JobHistory .
func (j *Instance) JobHistory(name string, limit int) ([]RunState, error) {
	return []RunState{}, errors.New("not implemented")
}

// Start .
func (j *Instance) Start() error {
	return errors.New("not implemented")
}

// Stop .
func (j *Instance) Stop() error {
	close(j.stop)
	return errors.New("not implemented")
}

// New .
func New(db *gorm.DB) *Instance {

	logger := logc.NewLogrus(logrus.New())
	stop := make(chan struct{})

	tx := db.Session(&gorm.Session{
		AllowGlobalUpdate:      true,
		SkipDefaultTransaction: true,
	})

	return &Instance{
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
