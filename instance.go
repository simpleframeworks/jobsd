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
	defaultTimeout          time.Duration
	defaultRetriesOnTimeout int
	defaultRetriesOnError   int

	db     *gorm.DB
	logger logc.Logger
	jobs   map[string]Job
	jobsMu sync.Mutex

	model *models.Instance

	runQ      *TimeQ
	scheduleQ *TimeQ
	timeoutQ  *TimeQ

	started bool
	stop    chan struct{}
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
	job.queueRun = i.queueRun
	job.queueSchedule = i.queueSchedule

	i.jobs[s.jobName] = job

	log.Debug("registering job - end")
	return job, nil
}

func (i *Instance) specToRun(runAt time.Time, s spec, args []interface{}) *models.Run {
	return &models.Run{
		JobID:       s.jobID,
		JobName:     s.jobName,
		Deduplicate: s.deduplicate,
		Scheduled:   s.schedule,
		Args:        args,
		RunAt:       runAt,
		CreatedAt:   time.Now(),
		CreatedBy:   i.model.ID,
	}
}

func (i *Instance) queueRun(runAt time.Time, s spec, args []interface{}, model *models.Run, tx *gorm.DB) (rtn RunState, err error) {
	if tx == nil {
		tx = i.db
	}
	if model == nil {
		model = i.specToRun(runAt, s, args)
		res := tx.Save(model)
		if res.Error != nil {
			return rtn, tx.Error
		}
	}
	logger := i.logger.WithFields(map[string]interface{}{
		"run.id":       model.ID,
		"run.job.id":   model.JobID,
		"run.job.name": model.JobName,
	})
	theRunner := &runner{
		db:       i.db,
		logger:   logger,
		modelRun: model,
		spec:     &s,
		stop:     i.stop,
	}
	logger.Trace("queuing job run")
	if i.runQ.Push(theRunner) {
		rtn = theRunner.runState()
	} else {
		logger.Debug("job run already queued")
	}
	return rtn, err
}

func (i *Instance) specToSchedule(scheduleAt time.Time, nextRunAt time.Time, s spec, args []interface{}) *models.Schedule {
	return &models.Schedule{
		JobID: s.jobID,
		Args:  args,

		ScheduleAt: scheduleAt,
		NextRunAt:  nextRunAt,

		ScheduleCount: 0,
		ScheduleLimit: s.limit,

		LastScheduledAt: sql.NullTime{},
		LastScheduledBy: sql.NullInt64{},

		CreatedAt: time.Now(),
		CreatedBy: i.model.ID,
	}
}

func (i *Instance) queueSchedule(s spec, args []interface{}, model *models.Schedule, tx *gorm.DB) (rtn ScheduleState, err error) {
	if tx == nil {
		tx = i.db
	}
	now := time.Now()
	nextRunAt := s.runAt(now)
	scheduleAt := nextRunAt.Add(-ScheduleTimingOffset)
	if model == nil {
		model = i.specToSchedule(nextRunAt, scheduleAt, s, args)
		res := tx.Save(model)
		if res.Error != nil {
			return rtn, tx.Error
		}
	}
	logger := i.logger.WithFields(map[string]interface{}{
		"schedule.id":     model.ID,
		"schedule.job.id": model.JobID,
	})
	theScheduler := &scheduler{
		db:     i.db,
		logger: logger,
		model:  model,
		spec:   &s,
		stop:   i.stop,
	}

	logger.Trace("queuing job schedule")
	if i.scheduleQ.Push(theScheduler) {
		rtn = theScheduler.scheduleState()
	} else {
		logger.Debug("schedule run already queued")
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

// DefaultRetriesOnTimeout sets how many times a job run can timeout
// Setting it to -1 removes the limit
// Must be called before start to have an effect
func (i *Instance) DefaultRetriesOnTimeout(num int) *Instance {
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
	if i.started {
		return errors.New("instance already started")
	}

	i.logger.Debug("starting up instance - start")
	if i.model.Migrate {
		txErr := i.db.AutoMigrate(
			&models.Instance{}, &models.Job{}, &models.Run{},
			&models.Schedule{}, &models.Active{},
		)
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

	i.started = true
	i.startWorkers()

	i.logger.Debug("starting up instance - end")
	return nil
}

func (i *Instance) startWorkers() {
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
		case ti := <-i.runQ.Stream():
			run := ti.(*runner)
			run.logger.Trace("worker received run. executing.")
			run.exec(i.model.ID)
		}
	}
}

// Stop .
func (i *Instance) Stop() error {
	i.logger.Debug("stopping instance")

	i.runQ.StopStream()
	i.scheduleQ.StopStream()
	i.timeoutQ.StopStream()

	close(i.stop)

	i.started = false
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

		runQ:      NewTimeQ(),
		scheduleQ: NewTimeQ(),
		timeoutQ:  NewTimeQ(),

		model:   model,
		started: false,
		stop:    make(chan struct{}),
	}
}

// ScheduleFunc is used to schedule when a job will run
type ScheduleFunc func(now time.Time) time.Time

// JobFunc is used to define the work a job needs to do
type JobFunc func(helper RunHelper) error
