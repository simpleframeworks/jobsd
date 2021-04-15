package jobsd

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/simpleframeworks/logc"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// ScheduleFunc .
type ScheduleFunc func(now time.Time) time.Time

// Instance .
type Instance struct {
	ID                    int64 `gorm:"primaryKey"`
	Workers               int
	AutoMigrate           bool
	SupportedJobs         string
	SupportedSchedules    string
	PollInterval          time.Duration // When to poll the DB for Runs
	PollLimit             int           // How many Runs to get during polling
	TimeoutCheck          time.Duration // Time between checking job runs for timeout or error
	RunTimeout            sql.NullInt64 // Default job retry timeout
	RetriesOnTimeoutLimit sql.NullInt64 // Default number of retries for a job after timeout
	RetriesOnErrorLimit   sql.NullInt64 // Default number of retries for a job after error
	RunsStarted           int           // Job runs started
	RunsRescheduled       int           // Job runs rescheduled after finishing
	RunsTimedOut          int           // Job runs timed out
	RunsTimedOutRes       int           // Job runs resurrected after time out
	RunsErrors            int           // Job runs that have returned an error
	LastSeenAt            sql.NullTime  // Last time instance was alive
	ShutdownAt            sql.NullTime
	CreatedAt             time.Time
}

// TableName specifies the db table name
func (Instance) TableName() string {
	return "jobsd_instances"
}

// JobsD .
type JobsD struct {
	log                   logc.Logger
	instance              Instance
	instanceMu            sync.Mutex
	started               bool
	jobs                  map[string]*JobContainer
	schedules             map[string]ScheduleFunc
	runQ                  *RunnableQueue
	runQReset             chan struct{}
	runQAdd               chan *Runnable
	runNow                chan *Runnable
	busyWorkers           int32
	producerCtx           context.Context
	producerCtxCancelFunc context.CancelFunc
	producerCancelWait    sync.WaitGroup
	workerCtx             context.Context
	workerCtxCancelFunc   context.CancelFunc
	workertCxCancelWait   sync.WaitGroup
	adderCtx              context.Context
	adderCtxCancelFunc    context.CancelFunc
	addertCxCancelWait    sync.WaitGroup
	db                    *gorm.DB
}

// RegisterJob registers a job to be run when required.
// name parameter should not contain a comma.
// jobFunc parameter should be any func that return an error. All jobFunc params must be gob serializable.
func (j *JobsD) RegisterJob(name string, jobFunc interface{}) *JobContainer {
	jobC := &JobContainer{
		jobFunc:             NewJobFunc(jobFunc),
		runTimeout:          j.instance.RunTimeout,
		retriesTimeoutLimit: j.instance.RetriesOnTimeoutLimit,
		retriesErrorLimit:   j.instance.RetriesOnErrorLimit,
	}

	name = strings.ReplaceAll(name, ",", "")
	j.jobs[name] = jobC

	names := make([]string, len(j.jobs))
	i := 0
	for n := range j.jobs {
		names[i] = n
		i++
	}
	j.instance.SupportedJobs = strings.Join(names, ",")

	return jobC
}

// RegisterSchedule adds a schedule
// name parameter should not contain a comma.
func (j *JobsD) RegisterSchedule(name string, scheduleFunc ScheduleFunc) {

	name = strings.ReplaceAll(name, ",", "")
	j.schedules[name] = scheduleFunc

	names := make([]string, len(j.schedules))
	i := 0
	for n := range j.schedules {
		names[i] = n
		i++
	}
	j.instance.SupportedSchedules = strings.Join(names, ",")

}

// Up starts up the JobsD service instance
func (j *JobsD) Up() error {
	if j.started {
		j.log.Warn("the service is already up")
		return nil
	}

	j.log.Debug("bringing up the service - started")

	if j.instance.AutoMigrate {
		txErr := j.db.AutoMigrate(&Run{}, &Instance{})
		if txErr != nil {
			return txErr
		}
	}

	if err := j.db.Save(&j.instance).Error; err != nil {
		return err
	}

	j.log = j.log.WithField("Instance.ID", j.instance.ID)

	// The maximum in flight jobs possible (handled by workers, producers and the adder)
	// Need this in order to shutdown without a circular deadlock
	maxInflight := j.instance.Workers + (j.instance.PollLimit * 2)

	j.started = true
	j.runNow = make(chan *Runnable)
	j.runQAdd = make(chan *Runnable, maxInflight)
	j.runQReset = make(chan struct{}, maxInflight)

	j.createWorkers()
	j.createProducers()
	j.createAdder()

	j.log.Debug("bringing up the service - completed")
	return nil
}

func (j *JobsD) createWorkers() {
	j.workerCtx, j.workerCtxCancelFunc = context.WithCancel(context.Background())
	j.workertCxCancelWait = sync.WaitGroup{}

	j.log.WithField("workers", j.instance.Workers).Debug("creating workers")
	for i := 0; i < j.instance.Workers; i++ {
		j.workertCxCancelWait.Add(1)
		go j.runner(j.workerCtx.Done())
	}
}

func (j *JobsD) createProducers() {
	j.producerCtx, j.producerCtxCancelFunc = context.WithCancel(context.Background())
	j.producerCancelWait = sync.WaitGroup{}

	j.producerCancelWait.Add(3)
	go j.runnableLoader(j.producerCtx.Done())
	go j.runnableDelegator(j.producerCtx.Done())
	go j.runnableResurrector(j.producerCtx.Done())
}

func (j *JobsD) createAdder() {
	j.adderCtx, j.adderCtxCancelFunc = context.WithCancel(context.Background())
	j.addertCxCancelWait = sync.WaitGroup{}

	j.addertCxCancelWait.Add(1)
	go j.runnableAdder(j.adderCtx.Done())
}

func (j *JobsD) runnableAdder(done <-chan struct{}) {
	for {
		select {
		case <-done:
			j.log.Trace("shutdown runnableAdder")
			j.addertCxCancelWait.Done()
			return
		case jr := <-j.runQAdd:
			jr.log.Debug("adding job runnable to run queue")
			if j.runQ.Push(jr) {
				j.runQReset <- struct{}{}
			}
		}
	}
}

func (j *JobsD) runnableLoader(done <-chan struct{}) {
	for {
		select {
		case <-done:
			j.log.Trace("shutdown runnableLoader")
			j.producerCancelWait.Done()
			return
		case <-time.After(j.instance.PollInterval):
			break
		}

		j.log.Trace("loading job runs from the DB - started")

		jobRuns := []Run{}
		tx := j.db.Where("run_started_at IS NULL AND created_by <> ?", j.instance.ID).Order("run_at ASC").
			Limit(j.instance.PollLimit).Find(&jobRuns)
		if tx.Error != nil {
			j.log.WithError(tx.Error).Warn("failed to load job runs from DB")
		}

		for _, jobRun := range jobRuns {
			runlog := jobRun.logger(j.log)
			jr, err := j.buildRunnable(jobRun)
			if err != nil {
				runlog.WithError(err).Warn("failed to load job")
				continue
			}
			j.runQAdd <- jr
			runlog.Trace("added job run from DB")
		}

		err := j.updateInstance()
		if err != nil {
			j.log.WithError(err).Warn("failed to update instance status")
		}

		j.log.Trace("loading job runs from the DB - completed")
	}
}

func (j *JobsD) runnableDelegator(done <-chan struct{}) {
	var waitTime time.Duration
	for {
		waitTime = time.Second * 5
		now := time.Now()

		if j.runQ.Len() > 0 {
			nextRunAt := j.runQ.Peek().runAt()
			if atomic.LoadInt32(&j.busyWorkers) >= int32(j.instance.Workers) {
				// introduce a pause because all workers are busy
				waitTime = time.Millisecond * 20
			} else if now.Equal(nextRunAt) || now.After(nextRunAt) {
				runnable := j.runQ.Pop()
				runnable.log.Debug("delegating run to worker")
				j.runNow <- runnable
				continue
			} else {
				waitTime = nextRunAt.Sub(now)
			}
		}
		j.log.WithField("WaitTime", waitTime).Debug("waiting for run")
		timer := time.NewTimer(waitTime)

		select {
		case <-done:
			j.log.Trace("shutdown runnableDelegator")
			if !timer.Stop() {
				<-timer.C
			}
			j.producerCancelWait.Done()
			return
		case <-j.runQReset:
			if !timer.Stop() {
				<-timer.C
			}
			// Flush j.runQReset so we don't constantly re-start the timer
			for i := 0; i < len(j.runQReset); i++ {
				<-j.runQReset
			}
			break
		case <-timer.C:
			break
		}
	}
}

func (j *JobsD) runner(done <-chan struct{}) {
	for {
		select {
		case <-done:
			j.log.Trace("shutdown runner")
			j.workertCxCancelWait.Done()
			return
		case jr := <-j.runNow:

			atomic.AddInt32(&j.busyWorkers, 1)
			jr.log.Debug("running job - started")

			j.incRunsStarted()

			res := jr.run()
			if res == RunResError {
				j.incRunsErrors()
				jr.log.Debug("running job - error out")
			} else if res == RunResTO {
				j.incRunsTimedOut()
				jr.log.Debug("running job - timed out")
			} else if res == RunResLockLost {
				jr.log.Debug("running job - skipped")
			} else {
				jr.log.Debug("running job - completed")
			}

			atomic.AddInt32(&j.busyWorkers, -1)
		}
	}
}

func (j *JobsD) runnableResurrector(done <-chan struct{}) {
	for {
		select {
		case <-done:
			j.log.Trace("shutdown runnableResurrector")
			j.producerCancelWait.Done()
			return
		case <-time.After(j.instance.TimeoutCheck):
			break
		}
		j.log.Trace("finding job runs to resurrect - started")

		jobRuns := []Run{}
		j.db.Where(
			"run_started_at IS NOT NULL AND run_completed_at IS NULL AND run_timeout_at <= ?",
			time.Now(),
		).Limit(j.instance.PollLimit).Find(&jobRuns)

		if len(jobRuns) > 0 {
			j.log.WithField("count", len(jobRuns)).Debug("job runs for resurrection found")

			for _, jobRun := range jobRuns {

				if jobRun.hasTimedOut() {
					jr, err := j.buildRunnable(jobRun)
					if err != nil {
						jr.log.Warn("could not build job runnable from resurrected job run")
						continue
					}
					jr.handleTO()
					j.incRunsTimedOut()
				}
			}

			if err := j.updateInstance(); err != nil {
				j.log.WithError(err).Warn("failed to update instance status")
			}

			j.log.Trace("finding job runs to resurrect - completed")
		}
	}
}

func (j *JobsD) incRunsStarted() {
	j.instanceMu.Lock()
	defer j.instanceMu.Unlock()
	j.instance.RunsStarted++
}

func (j *JobsD) incRunsErrors() {
	j.instanceMu.Lock()
	defer j.instanceMu.Unlock()
	j.instance.RunsErrors++
}

func (j *JobsD) incRunsTimedOut() {
	j.instanceMu.Lock()
	defer j.instanceMu.Unlock()
	j.instance.RunsTimedOut++
}

func (j *JobsD) incRunsTimedOutRes() {
	j.instanceMu.Lock()
	defer j.instanceMu.Unlock()
	j.instance.RunsTimedOutRes++
}

func (j *JobsD) incRunsRescheduled() {
	j.instanceMu.Lock()
	defer j.instanceMu.Unlock()
	j.instance.RunsRescheduled++
}

func (j *JobsD) updateInstance() error {
	j.instanceMu.Lock()
	defer j.instanceMu.Unlock()

	j.instance.LastSeenAt = sql.NullTime{Valid: true, Time: time.Now()}

	// To avoid a race we clone it to update
	toSave := j.instance
	err := j.db.Save(&toSave).Error
	return err
}

// Down shutsdown the JobsD service instance
func (j *JobsD) Down() error {

	j.log.Debug("shuting down the JobsD service - started")

	j.producerCtxCancelFunc()
	j.producerCancelWait.Wait()

	j.workerCtxCancelFunc()
	j.workertCxCancelWait.Wait()

	j.adderCtxCancelFunc()
	j.addertCxCancelWait.Wait()

	close(j.runNow)
	close(j.runQAdd)
	close(j.runQReset)
	j.started = false

	j.instance.ShutdownAt = sql.NullTime{Valid: true, Time: time.Now()}
	err := j.updateInstance()

	j.log.Debug("shuting down the JobsD service - completed")

	return err
}

func (j *JobsD) buildRunnable(jr Run) (rtn *Runnable, err error) {

	jobC, exists := j.jobs[jr.Job]
	if !exists {
		return rtn, errors.New("cannot run job. job '" + jr.Job + "' does not exist")
	}
	if err := jobC.jobFunc.check(jr.JobArgs); err != nil {
		return rtn, err
	}

	var scheduleFunc *ScheduleFunc
	if jr.needsScheduling() {
		s, exists := j.schedules[jr.Schedule.String]
		if !exists {
			return rtn, errors.New("cannot schedule job. schedule '" + jr.Schedule.String + "' missing")
		}
		scheduleFunc = &s
	}

	return newRunnable(
		j.instance.ID,
		jr,
		jobC.jobFunc,
		scheduleFunc,
		j.runQAdd,
		j.db,
		j.workerCtx.Done(),
		j.log,
	)
}

func (j *JobsD) createRunnable(jr Run) (rtn *Runnable, err error) {

	rtn, err = j.buildRunnable(jr)
	if err != nil {
		return rtn, err
	}

	err = rtn.save(j.db)
	if err != nil {
		return rtn, err
	}

	j.log.WithFields(map[string]interface{}{
		"Run.ID":    rtn.jobRun.ID,
		"Run.RunAt": rtn.jobRun.RunAt,
	}).Trace("created runnable job")
	j.runQAdd <- rtn

	return rtn, err
}

// CreateRun . Create a job run.
func (j *JobsD) CreateRun(job string, jobParams ...interface{}) *RunOnceCreator {
	name := uuid.Must(uuid.NewUUID()).String() // We die here if time fails.
	now := time.Now()
	jr := Run{
		Name:            name,
		NameActive:      sql.NullString{Valid: true, String: name},
		Job:             job,
		JobArgs:         jobParams,
		RunAt:           now,
		RunSuccessLimit: sql.NullInt64{Valid: true, Int64: 1},
		CreatedAt:       now,
		CreatedBy:       j.instance.ID,
	}
	if jobC, ok := j.jobs[job]; ok {
		jr.RunTimeout = jobC.runTimeout
		jr.RetriesOnTimeoutLimit = jobC.retriesTimeoutLimit
		jr.RetriesOnErrorLimit = jobC.retriesErrorLimit
	}
	return &RunOnceCreator{
		jobsd:  j,
		jobRun: jr,
	}
}

// GetRunState retrieves the current state of the job run
func (j *JobsD) GetRunState(id int64) *RunState {
	rtn := &RunState{
		db:       j.db,
		OriginID: id,
	}
	rtn.Refresh()
	return rtn
}

// WorkerNum sets the number of workers to process jobs
func (j *JobsD) WorkerNum(workers int) *JobsD {
	if !j.started {
		j.instance.Workers = workers
	}
	return j
}

// AutoMigration turns on or off auto-migration
func (j *JobsD) AutoMigration(run bool) *JobsD {
	if !j.started {
		j.instance.AutoMigrate = run
	}
	return j
}

// PollInterval sets the time between getting Runs from the DB
func (j *JobsD) PollInterval(pollInt time.Duration) *JobsD {
	if !j.started {
		j.instance.PollInterval = pollInt
	}
	return j
}

// PollLimit sets the number of upcoming Runs to retrieve from the DB at a time
func (j *JobsD) PollLimit(limit int) *JobsD {
	if !j.started {
		j.instance.PollLimit = limit
	}
	return j
}

// RunTimeout sets the RunTimeout
// Setting it to 0 disables timeout
func (j *JobsD) RunTimeout(timeout time.Duration) *JobsD {
	if j.started {
		return j
	}
	if timeout <= 0 {
		j.instance.RunTimeout = sql.NullInt64{}
	} else {
		j.instance.RunTimeout = sql.NullInt64{Valid: true, Int64: int64(timeout)}
	}
	return j
}

// RetriesTimeoutLimit sets how many times a job run can timeout
// Setting it to -1 removes the limit
func (j *JobsD) RetriesTimeoutLimit(limit int) *JobsD {
	if j.started {
		return j
	}
	if limit < 0 {
		j.instance.RetriesOnTimeoutLimit = sql.NullInt64{}
	} else {
		j.instance.RetriesOnTimeoutLimit = sql.NullInt64{Valid: true, Int64: int64(limit)}
	}
	return j
}

// RetryErrorLimit sets the RetryErrorLimit
// Setting it to -1 removes the limit
func (j *JobsD) RetryErrorLimit(limit int) *JobsD {
	if j.started {
		return j
	}
	if limit < 0 {
		j.instance.RetriesOnErrorLimit = sql.NullInt64{}
	} else {
		j.instance.RetriesOnErrorLimit = sql.NullInt64{Valid: true, Int64: int64(limit)}
	}
	return j
}

// TimeoutCheck sets the time between retry timeout checks
func (j *JobsD) TimeoutCheck(interval time.Duration) *JobsD {
	if !j.started {
		j.instance.TimeoutCheck = interval
	}
	return j
}

// Logger sets logrus logger
func (j *JobsD) Logger(logger logc.Logger) *JobsD {
	if !j.started {
		j.log = logger.WithFields(logrus.Fields{
			"Service": "JobsD",
		})
	}
	return j
}

// New .
func New(db *gorm.DB) *JobsD {

	rtn := &JobsD{
		instance: Instance{
			Workers:               10,
			AutoMigrate:           true,
			PollInterval:          time.Duration(time.Second * 5),
			PollLimit:             1000,
			TimeoutCheck:          time.Duration(time.Second * 30),
			RunTimeout:            sql.NullInt64{Valid: true, Int64: int64(time.Duration(time.Minute * 30))},
			RetriesOnTimeoutLimit: sql.NullInt64{Valid: true, Int64: 3},
			RetriesOnErrorLimit:   sql.NullInt64{Valid: true, Int64: 3},
		},
		jobs:      map[string]*JobContainer{},
		schedules: map[string]ScheduleFunc{},
		runQ:      NewRunnableQueue(),
		db:        db,
	}

	rtn.Logger(logc.NewLogrus(logrus.New()))

	return rtn
}
