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
	db           *gorm.DB
	logger       logc.Logger
	jobs         map[string]Job
	migrate      bool
	workers      int
	pullInterval time.Duration
	pullNum      int

	defaultTimeout          time.Duration
	defaultRetriesOnTimeout int
	defaultRetriesOnError   int

	stopped bool
	stop    chan struct{}
}

// NewJob creates a configurable job that needs to be registered
func (i *Instance) NewJob(name string, jobFunc interface{}) *SpecMaker {
	return &SpecMaker{
		spec: spec{
			timeout:          i.defaultTimeout,
			retriesOnTimeout: i.defaultRetriesOnTimeout,
			retriesOnError:   i.defaultRetriesOnError,
		},
		makeJob: i.makeJob,
	}
}

func (i *Instance) makeJob(s spec) (Job, error) {
	return Job{}, errors.New("not implemented")
}

func (i *Instance) makeRun(s spec) (RunState, error) {
	return RunState{}, errors.New("not implemented")
}

// GetJob gets a job to run
func (i *Instance) GetJob(name string) (Job, error) {
	job, exists := i.jobs[name]
	if exists {
		return job, nil
	}
	return job, errors.New("job has not be registered")
}

// GetDB returns the db
func (i *Instance) GetDB() *gorm.DB {
	return i.db
}

// SetLogger sets the logger
func (i *Instance) SetLogger(l logc.Logger) *Instance {
	i.logger = l.WithFields(logrus.Fields{
		"Service": "Instance",
	})
	return i
}

// SetWorkers sets the number of workers to process jobs
// Must be called before start to have an effect
func (i *Instance) SetWorkers(workers int) *Instance {
	i.workers = workers
	return i
}

// SetMigration turns on or off auto-migration
// Must be called before start to have an effect
func (i *Instance) SetMigration(m bool) *Instance {
	i.migrate = m
	return i
}

// PullInterval sets the time between getting new Runs from the DB and cluster
// Must be called before start to have an effect
func (i *Instance) PullInterval(pullInt time.Duration) *Instance {
	i.pullInterval = pullInt
	return i
}

// PullNum sets the number of upcoming job runs to retrieve from the DB at a time
// Must be called before start to have an effect
func (i *Instance) PullNum(num int) *Instance {
	i.pullNum = num
	return i
}

// DefaultTimeout sets the DefaultTimeout
// Setting it to 0 disables timeout
// Must be called before start to have an effect
func (i *Instance) DefaultTimeout(timeout time.Duration) *Instance {
	i.defaultTimeout = timeout
	return i
}

// DefaultRetriesOnTimout sets how many times a job run can timeout
// Setting it to -1 removes the limit
// Must be called before start to have an effect
func (i *Instance) DefaultRetriesOnTimout(num int) *Instance {
	i.defaultRetriesOnTimeout = num
	return i
}

// DefaultRetriesOnError sets the DefaultRetriesOnError
// Setting it to -1 removes the limit
// Must be called before start to have an effect
func (i *Instance) DefaultRetriesOnError(num int) *Instance {
	i.defaultRetriesOnError = num
	return i
}

// JobHistory .
func (i *Instance) JobHistory(name string, limit int) ([]RunState, error) {
	return []RunState{}, errors.New("not implemented")
}

// Start .
func (i *Instance) Start() error {
	return errors.New("not implemented")
}

// Stop .
func (i *Instance) Stop() error {
	close(i.stop)
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
		jobs:                    map[string]Job{},
		migrate:                 true,
		workers:                 10,
		pullInterval:            time.Second * 10,
		pullNum:                 1000,
		defaultTimeout:          time.Minute * 30,
		defaultRetriesOnTimeout: 3,
		defaultRetriesOnError:   3,
	}
}

// ScheduleFunc is used to schedule when a job will run
type ScheduleFunc func(now time.Time) time.Time

// JobFunc is used to define the work a job needs to do
type JobFunc func(helper RunHelper) error
