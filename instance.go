package jobspec

import (
	"database/sql"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/simpleframeworks/jobspec/models"
	"github.com/simpleframeworks/logc"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Instance .
type Instance struct {
	db           *gorm.DB
	logger       logc.Logger
	jobs         map[string]Job
	jobsMu       sync.Mutex
	migrate      bool
	workers      int
	pullInterval time.Duration
	pullNum      int

	defaultTimeout          time.Duration
	defaultRetriesOnTimeout int
	defaultRetriesOnError   int

	instance *models.Instance
	runQueue *runQueue
	stopped  bool
	stop     chan struct{}
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

func (i *Instance) makeJob(s spec) (job Job, err error) {

	i.jobsMu.Lock()
	defer i.jobsMu.Unlock()

	_, exists := i.jobs[s.jobName]
	if exists {
		return job, errors.New("job already exists")
	}

	job.job = &models.Job{
		Name: s.jobName,
	}
	tx := i.db.Clauses(clause.OnConflict{DoNothing: true}).Create(job.job)
	if tx.Error != nil {
		return job, tx.Error
	}
	if job.job.ID == 0 {
		tx = i.db.Where("name = ?", s.jobName).First(job.job)
		if tx.Error != nil {
			return job, tx.Error
		}
	}

	s.jobID = job.job.ID

	job.spec = s
	job.makeRun = i.makeRun

	i.jobs[s.jobName] = job

	return job, nil
}

func (i *Instance) specToRun(s spec, args []interface{}) *models.Run {
	uniqueRun := sql.NullString{}
	uniqueSchedule := sql.NullString{}
	if s.unique {
		if s.schedule {
			uniqueSchedule.Valid = true
			uniqueSchedule.String = s.jobName
		} else {
			uniqueRun.Valid = true
			uniqueRun.String = s.jobName
		}
	}
	now := time.Now()
	runAt := now
	if s.schedule {
		runAt = s.scheduleFunc(runAt)
	}

	rtn := &models.Run{
		JobID:          s.jobID,
		JobName:        s.jobName,
		UniqueRun:      uniqueRun,
		UniqueSchedule: uniqueSchedule,
		Scheduled:      s.schedule,
		Args:           args,
		RunAt:          runAt,
		CreatedAt:      now,
		CreatedBy:      i.instance.ID,
	}

	return rtn
}

func (i *Instance) makeRun(jobID int64, s spec, args []interface{}) (rtn RunState, err error) {

	theModel := i.specToRun(s, args)
	tx := i.db.Save(theModel)
	if tx.Error != nil {
		return rtn, tx.Error
	}

	theRun := &run{
		db:   i.db,
		run:  theModel,
		spec: &s,
		stop: i.stop,
	}
	if i.runQueue.push(theRun) {
		rtn = theRun.runState()
	} else {
		err = errors.New("job run already queued")
	}

	return rtn, err
}

// GetJob gets a job to run
func (i *Instance) GetJob(name string) (Job, error) {
	i.jobsMu.Lock()
	defer i.jobsMu.Unlock()

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

	tx := db.Session(&gorm.Session{
		AllowGlobalUpdate:      true,
		SkipDefaultTransaction: true,
	})

	return &Instance{
		db:           tx,
		logger:       logger,
		jobs:         map[string]Job{},
		migrate:      true,
		workers:      10,
		pullInterval: time.Second * 10,
		pullNum:      1000,

		defaultTimeout:          time.Minute * 30,
		defaultRetriesOnTimeout: 3,
		defaultRetriesOnError:   3,

		runQueue: newRunQueue(),
		stopped:  false,
		stop:     make(chan struct{}),
	}
}

// ScheduleFunc is used to schedule when a job will run
type ScheduleFunc func(now time.Time) time.Time

// JobFunc is used to define the work a job needs to do
type JobFunc func(helper RunHelper) error
