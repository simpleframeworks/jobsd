package jobsd

import (
	"context"
	"database/sql"
	"strings"
	"sync"
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
	ID                   int64 `gorm:"primaryKey"`
	Workers              int
	AutoMigrate          bool
	SupportedJobs        string
	SupportedSchedules   string
	JobPollInterval      time.Duration // When to poll the DB for JobRuns
	JobPollLimit         int           // How many JobRuns to get during polling
	JobRetryCheck        time.Duration // Time between checking job runs for timeout or error
	JobRetryTimeout      time.Duration // Default job retry timeout
	JobRetryTimeoutLimit int           // Default number of retries for a job after timeout
	JobRetryErrorLimit   int           // Default number of retries for a job after error
	JobRuns              int           // Job runs started
	JobRunsRescheduled   int           // Job runs rescheduled after finishing
	JobRunsTimedOut      int           // Job runs timed out
	JobRunsTimedOutRes   int           // Job runs resurrected after time out
	JobRunsErrors        int           // Job runs that have returned an error
	LastSeenAt           sql.NullTime  // Last time instance was alive
	ShutdownAt           sql.NullTime
	CreatedAt            time.Time
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
	runQ                  *JobRunnableQueue
	runQNew               chan struct{}
	runNow                chan JobRunnable
	producerCtx           context.Context
	producerCtxCancelFunc context.CancelFunc
	producerCancelWait    sync.WaitGroup
	workerCtx             context.Context
	workerCtxCancelFunc   context.CancelFunc
	workertCxCancelWait   sync.WaitGroup
	db                    *gorm.DB
}

// RegisterJob registers a job to be run when required.
// name parameter should not contain a comma.
// jobFunc parameter should be any func that return an error. All jobFunc params must be gob serializable.
func (j *JobsD) RegisterJob(name string, jobFunc interface{}) *JobContainer {
	jobC := &JobContainer{
		jobFunc:             NewJobFunc(jobFunc),
		retryTimeout:        j.instance.JobRetryTimeout,
		retryOnTimeoutLimit: j.instance.JobRetryTimeoutLimit,
		retryOnErrorLimit:   j.instance.JobRetryErrorLimit,
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
		txErr := j.db.AutoMigrate(&JobRun{}, &Instance{})
		if txErr != nil {
			return txErr
		}
	}

	if err := j.db.Save(&j.instance).Error; err != nil {
		return err
	}

	j.log = j.log.WithField("Instance.ID", j.instance.ID)

	j.started = true
	j.runNow = make(chan JobRunnable)
	j.runQNew = make(chan struct{})

	j.producerCtx, j.producerCtxCancelFunc = context.WithCancel(context.Background())
	j.producerCancelWait = sync.WaitGroup{}

	j.workerCtx, j.workerCtxCancelFunc = context.WithCancel(context.Background())
	j.workertCxCancelWait = sync.WaitGroup{}

	j.createWorkers()
	j.createProducers()

	j.log.Debug("bringing up the service - completed")
	return nil
}

func (j *JobsD) createWorkers() {
	wksDone := make([]chan struct{}, j.instance.Workers)
	for i := 0; i < j.instance.Workers; i++ {
		j.workertCxCancelWait.Add(1)
		done := make(chan struct{})
		wksDone = append(wksDone, done)
		go j.jobRunner(done)
	}

	// Tell the workers to finish up
	go func() {
		<-j.workerCtx.Done()
		for _, ch := range wksDone {
			go func(c chan struct{}) {
				c <- struct{}{}
				close(c)
			}(ch)
		}
	}()
}

func (j *JobsD) createProducers() {

	j.producerCancelWait.Add(3)
	go j.jobLoader(j.producerCtx.Done())
	go j.jobDelegator(j.producerCtx.Done())
	go j.jobResurrector(j.producerCtx.Done())
}

func (j *JobsD) jobLoader(done <-chan struct{}) {
	for {
		select {
		case <-done:
			j.log.Trace("shutdown jobLoader")
			j.producerCancelWait.Done()
			return
		case <-time.After(j.instance.JobPollInterval):
			break
		}

		j.log.Trace("loading jobs from the DB - started")

		jobRuns := []JobRun{}
		tx := j.db.Where("run_started_at IS NULL").Order("run_at ASC").
			Limit(int(j.instance.JobPollLimit)).Find(&jobRuns)
		if tx.Error != nil {
			j.log.WithError(tx.Error).Warn("failed to load job runs from DB")
		}

		for _, jobRun := range jobRuns {
			runlog := jobRun.logger(j.log)
			jr, err := j.buildJobRunnable(jobRun)
			if err != nil {
				runlog.WithError(err).Warn("failed to load job")
			}
			j.runQ.Push(jr)
			runlog.Trace("added job run from DB")
		}

		err := j.updateInstance()
		if err != nil {
			j.log.WithError(err).Warn("failed to update instance status")
		}

		if len(jobRuns) > 0 {
			j.runQNew <- struct{}{}
		}

		j.log.Trace("loading jobs from the DB - completed")
	}
}

func (j *JobsD) jobDelegator(done <-chan struct{}) {
	for {
		waitTime := time.Second * 10
		now := time.Now()

		if j.runQ.Len() > 0 {
			nextRunAt := j.runQ.Peek().RunAt
			if now.Equal(nextRunAt) || now.After(nextRunAt) {
				j.log.WithField("Job.ID", j.runQ.Peek().ID).Trace("delegating job")
				j.runNow <- j.runQ.Pop()
				continue
			} else {
				waitTime = nextRunAt.Sub(now)
			}
		}
		j.log.Trace("waiting for job")

		select {
		case <-done:
			j.log.Trace("shutdown jobDelegator")
			j.producerCancelWait.Done()
			return
		case <-j.runQNew:
			break
		case <-time.After(waitTime):
			break
		}
	}
}

func (j *JobsD) jobRunner(done <-chan struct{}) {
	for {
		select {
		case <-done:
			j.log.Trace("shutdown jobRunner")
			j.workertCxCancelWait.Done()
			return
		case jobRunnable := <-j.runNow:
			log := jobRunnable.logger()

			log.Trace("running job - started")

			// run the job
			j.incJobRuns()

			res := jobRunnable.run()
			if res == RunResError {
				j.incJobRunsErrors()
			} else if res == RunResTO {
				j.incJobRunsTimedOut()
			}

			log.Trace("running job - completed")
		}

	}
}

func (j *JobsD) jobResurrector(done <-chan struct{}) {
	for {
		select {
		case <-done:
			j.producerCancelWait.Done()
			return
		case <-time.After(j.instance.JobRetryCheck):
			break
		}
		j.log.Trace("finding jobs to resurrect - started")

		jobRuns := []JobRun{}
		j.db.Where(
			"run_started_at IS NOT NULL AND completed_at IS NULL AND job_timeout_at <= ?",
			time.Now(),
		).Limit(int(j.instance.JobPollLimit)).Find(&jobRuns)

		if len(jobRuns) > 0 {
			j.log.WithField("count", len(jobRuns)).Debug("job runs for resurrection found")

			for _, jobRun := range jobRuns {

				if jobRun.hasTimedOut() {
					jobRunnable, err := j.buildJobRunnable(jobRun)
					if err != nil {
						jobRunnable.logger().Warn("could not build job runnable from resurrected job run")
						continue
					}
					jobRunnable.handleTO()
					j.incJobRunsTimedOutRes()
				}
			}

			if err := j.updateInstance(); err != nil {
				j.log.WithError(err).Warn("failed to update instance status")
			}

			j.log.Trace("finding jobs to resurrect - completed")
		}
	}
}

func (j *JobsD) addJobRun(jr JobRunnable) {
	j.runQ.Push(jr)

	// Notify the delegator of the new item to run
	// This needs to be async to prevent a job run from deadlocking trying to reschedule
	go func() { j.runQNew <- struct{}{} }()
}

func (j *JobsD) incJobRuns() {
	j.instanceMu.Lock()
	defer j.instanceMu.Unlock()
	j.instance.JobRuns++
}

func (j *JobsD) incJobRunsErrors() {
	j.instanceMu.Lock()
	defer j.instanceMu.Unlock()
	j.instance.JobRunsErrors++
}

func (j *JobsD) incJobRunsTimedOut() {
	j.instanceMu.Lock()
	defer j.instanceMu.Unlock()
	j.instance.JobRunsTimedOut++
}

func (j *JobsD) incJobRunsTimedOutRes() {
	j.instanceMu.Lock()
	defer j.instanceMu.Unlock()
	j.instance.JobRunsTimedOutRes++
}

func (j *JobsD) incJobRunsRescheduled() {
	j.instanceMu.Lock()
	defer j.instanceMu.Unlock()
	j.instance.JobRunsRescheduled++
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

	close(j.runNow)
	close(j.runQNew)
	j.started = false

	j.instance.ShutdownAt = sql.NullTime{Valid: true, Time: time.Now()}
	err := j.updateInstance()

	j.log.Debug("shuting down the JobsD service - completed")

	return err
}

func (j *JobsD) buildJobRunnable(jr JobRun) (rtn JobRunnable, err error) {

	jobC, exists := j.jobs[jr.Job]
	if !exists {
		return rtn, errors.New("cannot run job. job '" + jr.Job + "' does not exist")
	}
	if err := jobC.jobFunc.check(jr.JobArgs); err != nil {
		return rtn, err
	}

	var schedule *ScheduleFunc
	if jr.needsScheduling() {
		schedule, exists := j.schedules[jr.Schedule.String]
		if !exists {
			return rtn, errors.New("cannot schedule job. schedule '" + jr.Schedule.String + "' missing")
		}
	}

	return newJobRunnable(j.db, jr, jobC.jobFunc, schedule, j.log, j.instance.ID)
}

func (j *JobsD) createJobRunnable(jr JobRun) (rtn JobRunnable, err error) {

	rtn, err = j.buildJobRunnable(jr)
	if err != nil {
		return rtn, err
	}
	rtn.schedule()

	err = rtn.jobRun.insertGet(j.db)
	if err != nil {
		return rtn, err
	}

	j.log.WithFields(map[string]interface{}{
		"Job.ID":    rtn.jobRun.ID,
		"Job.RunAt": rtn.jobRun.RunAt,
	}).Trace("created runnable job")

	j.addJobRun(rtn)

	return rtn, err
}

// CreateRun .
func (j *JobsD) CreateRun(job string, jobParams ...interface{}) *RunOnceCreator {
	name := uuid.Must(uuid.NewUUID()).String() // We die here if time fails.
	rtn := &RunOnceCreator{
		jobsd: j,
		jobRun: JobRun{
			Name:            name,
			NameActive:      sql.NullString{Valid: true, String: name},
			Job:             job,
			JobArgs:         jobParams,
			RunAt:           time.Now(),
			RunSuccessLimit: sql.NullInt64{Valid: true, Int64: 1},
			RunTimeout:      sql.NullInt64{Valid: true, Int64: int64(j.instance.JobRetryTimeout)}, //TODO make optional
			CreatedAt:       time.Now(),
			CreatedBy:       j.instance.ID,
		},
	}
	if jobC, ok := j.jobs[job]; ok {
		rtn.jobRun.RetryTimeout = jobC.retryTimeout
		rtn.jobRun.RetriesOnTimeoutLimit = jobC.retryOnTimeoutLimit
		rtn.jobRun.RetriesOnErrorLimit = jobC.retryOnErrorLimit
	}

	return rtn
}

// GetJobRunState retrieves the current state of the job run
func (j *JobsD) GetJobRunState(id int64) *JobRunState {
	rtn := &JobRunState{
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

// JobPollInterval sets the time between getting JobRuns from the DB
func (j *JobsD) JobPollInterval(pollInt time.Duration) *JobsD {
	if !j.started {
		j.instance.JobPollInterval = pollInt
	}
	return j
}

// JobPollLimit sets the number of upcoming JobRuns to retrieve from the DB at a time
func (j *JobsD) JobPollLimit(limit int) *JobsD {
	if !j.started {
		j.instance.JobPollLimit = limit
	}
	return j
}

// JobRetryTimeout sets default job retry timeout
func (j *JobsD) JobRetryTimeout(timeout time.Duration) *JobsD {
	if !j.started {
		j.instance.JobRetryTimeout = timeout
	}
	return j
}

// JobRetryTimeoutLimit default job retry on timeout limit
func (j *JobsD) JobRetryTimeoutLimit(limit int) *JobsD {
	if !j.started {
		j.instance.JobRetryTimeoutLimit = limit
	}
	return j
}

// JobRetryTimeoutCheck sets the time between retry timeout checks
func (j *JobsD) JobRetryTimeoutCheck(interval time.Duration) *JobsD {
	if !j.started {
		j.instance.JobRetryCheck = interval
	}
	return j
}

// JobRetryErrorLimit default job retry on error limit
func (j *JobsD) JobRetryErrorLimit(limit int) *JobsD {
	if !j.started {
		j.instance.JobRetryErrorLimit = limit
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
			Workers:              10,
			AutoMigrate:          true,
			JobPollInterval:      time.Duration(time.Second * 5),
			JobPollLimit:         1000,
			JobRetryTimeout:      time.Duration(time.Minute * 30),
			JobRetryCheck:        time.Duration(time.Second * 30),
			JobRetryTimeoutLimit: 3,
			JobRetryErrorLimit:   3,
		},
		jobs:      map[string]*JobContainer{},
		schedules: map[string]ScheduleFunc{},
		runQ:      NewJobRunnableQueue(),
		db:        db,
	}

	rtn.Logger(logc.NewLogrus(logrus.New()))

	return rtn
}
