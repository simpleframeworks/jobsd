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
	db     *gorm.DB
	logger logc.Logger
	jobs   map[string]Job
	jobsMu sync.Mutex

	defaultTimeout          time.Duration
	defaultRetriesOnTimeout int
	defaultRetriesOnError   int

	model    *models.Instance
	runQueue *runQueue
	runNow   chan *run
	stopped  bool
	stop     chan struct{}
}

// NewJob creates a configurable job that needs to be registered
func (i *Instance) NewJob(name string, jobFunc JobFunc) *SpecMaker {
	return &SpecMaker{
		spec: spec{
			jobName:          name,
			jobFunc:          jobFunc,
			timeout:          i.defaultTimeout,
			retriesOnTimeout: i.defaultRetriesOnTimeout,
			retriesOnError:   i.defaultRetriesOnError,
		},
		registerJob: i.registerJob,
	}
}

func (i *Instance) registerJob(s spec) (job Job, err error) {
	i.jobsMu.Lock()
	defer i.jobsMu.Unlock()

	log := i.logger.WithFields(map[string]interface{}{
		"job.name": s.jobName,
	})
	log.Debug("registering job - start")

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

	log.Debug("registering job - end")
	return job, nil
}

func (i *Instance) specToRun(s spec, args []interface{}) *models.Run {
	unique := sql.NullString{}
	if s.unique {
		unique.Valid = true
		unique.String = s.jobName
	}
	now := time.Now()
	runAt := now
	if s.schedule {
		runAt = s.scheduleFunc(runAt)
	}

	rtn := &models.Run{
		JobID:     s.jobID,
		JobName:   s.jobName,
		Unique:    unique,
		Scheduled: s.schedule,
		Args:      args,
		RunAt:     runAt,
		CreatedAt: now,
		CreatedBy: i.model.ID,
	}

	return rtn
}

func (i *Instance) makeRun(s spec, args []interface{}) (rtn RunState, err error) {

	model := i.specToRun(s, args)
	tx := i.db.Save(model)
	if tx.Error != nil {
		return rtn, tx.Error
	}
	logger := i.logger.WithFields(map[string]interface{}{
		"run.id":       model.ID,
		"run.job_id":   model.JobID,
		"run.job_name": model.JobName,
	})
	theRun := &run{
		db:     i.db,
		logger: logger,
		model:  model,
		spec:   &s,
		stop:   i.stop,
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
		"service": "JobsD",
	})
	return i
}

// SetWorkers sets the number of workers to process jobs
// Must be called before start to have an effect
func (i *Instance) SetWorkers(workers int) *Instance {
	i.model.Workers = workers
	return i
}

// SetMigration turns on or off auto-migration
// Must be called before start to have an effect
func (i *Instance) SetMigration(m bool) *Instance {
	i.model.Migrate = m
	return i
}

// PullInterval sets the time between getting new Runs from the DB and cluster
// Must be called before start to have an effect
func (i *Instance) PullInterval(pullInt time.Duration) *Instance {
	i.model.PullInterval = pullInt
	return i
}

// PullLimit sets the number of upcoming job runs to retrieve from the DB at a time
// Must be called before start to have an effect
func (i *Instance) PullLimit(num int) *Instance {
	i.model.PullLimit = num
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
	if !i.stopped {
		return errors.New("instance already started")
	}

	i.logger.Debug("starting up instance - start")
	if i.model.Migrate {
		txErr := i.db.AutoMigrate(&models.Instance{}, &models.Job{}, &models.Run{})
		if txErr != nil {
			return txErr
		}
	}

	now := time.Now()
	i.model.LastSeenAt = now
	i.model.StartedAt = now

	if err := i.db.Save(i.model).Error; err != nil {
		return err
	}

	i.stopped = false
	i.logger.Debug("starting workers")
	i.startWorkers()
	i.logger.Debug("starting producer")
	i.startProducers()

	i.logger.Debug("starting up instance - end")
	return nil
}

func (i *Instance) startWorkers() {

	// This is the work that workers consume
	// It needs to be buffered to avoid a deadlock
	// because work can be generated from a worker
	i.runNow = make(chan *run, i.model.Workers)

	for j := 0; j < i.model.Workers; j++ {
		go i.worker()
	}
}

func (i *Instance) worker() {
	i.logger.Trace("starting worker")
	defer i.logger.Trace("worker stopped")
	for {
		select {
		case <-i.stop:
			return
		case run := <-i.runNow:
			run.logger.Trace("worker received run. executing.")
			run.exec()
		}
	}
}

func (i *Instance) startProducers() {
	go i.runDelegator()
}

func (i *Instance) runDelegator() {
	for {
		waitTime := time.Second * 5
		now := time.Now()

		if len(i.runNow) >= i.model.Workers {
			i.logger.Trace("all workers are busy and runs are waiting")
			waitTime = time.Millisecond * 100
		} else if i.runQueue.len() > 0 {
			nextRunAt := i.runQueue.peek().model.RunAt
			if now.Equal(nextRunAt) || now.After(nextRunAt) {
				run := i.runQueue.pop()
				run.logger.Debug("run ready. delegating to a worker")
				i.runNow <- run
			} else {
				waitTime = nextRunAt.Sub(now)
			}
		}

		i.logger.WithField("wait_time", waitTime).Debug("waiting for run")
		timer := time.NewTimer(waitTime)

		select {
		case <-i.stop:
			i.logger.Debug("stopping run delegator")
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-i.runQueue.pushed():
			i.logger.Trace("run pushed to queue. resetting and checking for run.")
			break
		case <-timer.C:
			break
		}
	}
}

// Stop .
func (i *Instance) Stop() error {
	i.logger.Debug("stopping instance")
	close(i.stop)
	i.stopped = true
	return nil
}

// New .
func New(db *gorm.DB) *Instance {

	logger := logc.NewLogrus(logrus.New())

	tx := db.Session(&gorm.Session{
		AllowGlobalUpdate:      true,
		SkipDefaultTransaction: true,
	})

	model := &models.Instance{
		Workers:      10,
		Migrate:      true,
		PullInterval: time.Second * 10,
		PullLimit:    1000,
	}

	return &Instance{
		db:     tx,
		logger: logger,
		jobs:   map[string]Job{},

		defaultTimeout:          time.Minute * 30,
		defaultRetriesOnTimeout: 3,
		defaultRetriesOnError:   3,

		model:    model,
		runQueue: newRunQueue(),
		stopped:  true,
		stop:     make(chan struct{}),
	}
}

// ScheduleFunc is used to schedule when a job will run
type ScheduleFunc func(now time.Time) time.Time

// JobFunc is used to define the work a job needs to do
type JobFunc func(helper RunHelper) error
