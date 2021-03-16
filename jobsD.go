package jobsd

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/simpleframeworks/logc"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// ScheduleFunc .
type ScheduleFunc func(now time.Time) time.Time

// Instance .
type Instance struct {
	ID                     int64 `gorm:"primaryKey"`
	Workers                int
	SupportedJobs          string
	SupportedSchedules     string
	JobPollInterval        time.Duration // When to poll the DB for JobRuns
	JobPollLimit           int           // How many JobRuns to get during polling
	JobRetryCheck          time.Duration // Time between checking job runs for timeout or error
	JobRetryTimeout        time.Duration // Default job retry timeout
	JobRetryTimeoutLimit   int           // Default number of retries for a job after timeout
	JobRetryErrorLimit     int           // Default number of retries for a job after error
	JobRuns                int           // Job runs started
	JobRunsError           int           // Job runs that returned error
	JobRunsRescheduled     int           // Job runs rescheduled after finishing
	JobRunsRequeuedError   int           // Job runs requeued after error
	JobRunsRequeuedTimeout int           // Job runs requeued after timeout
	LastSeenAt             sql.NullTime  // Last time instance was alive
	ShutdownAt             sql.NullTime
	CreatedAt              time.Time
}

// JobsD .
type JobsD struct {
	log                   logc.Logger
	Instance              *Instance
	started               bool
	jobs                  map[string]*JobContainer
	schedules             map[string]ScheduleFunc
	runQ                  *JobRunQueue
	runQNew               chan struct{}
	runNow                chan JobRun
	producerCtx           context.Context
	producerCtxCancelFunc context.CancelFunc
	producerCancelWait    sync.WaitGroup
	workerCtx             context.Context
	workerCtxCancelFunc   context.CancelFunc
	workertCxCancelWait   sync.WaitGroup
	dbMigrate             bool
	db                    *gorm.DB
}

// RegisterJob registers a job to be run when required.
// name parameter should not contain a comma.
// jobFunc parameter should be any func that return an error. All jobFunc params must be gob serializable.
func (j *JobsD) RegisterJob(name string, jobFunc interface{}) *JobContainer {
	jobC := &JobContainer{
		jobFunc:             NewJobFunc(jobFunc),
		retryTimeout:        j.Instance.JobRetryTimeout,
		retryOnTimeoutLimit: j.Instance.JobRetryTimeoutLimit,
		retryOnErrorLimit:   j.Instance.JobRetryErrorLimit,
	}

	name = strings.ReplaceAll(name, ",", "")
	j.jobs[name] = jobC

	names := make([]string, len(j.jobs))
	for n := range j.jobs {
		names = append(names, n)
	}
	j.Instance.SupportedJobs = strings.Join(names, ",")

	return jobC
}

// RegisterSchedule adds a schedule
// name parameter should not contain a comma.
func (j *JobsD) RegisterSchedule(name string, scheduleFunc ScheduleFunc) {

	name = strings.ReplaceAll(name, ",", "")
	j.schedules[name] = scheduleFunc

	names := make([]string, len(j.jobs))
	for n := range j.jobs {
		names = append(names, n)
	}
	j.Instance.SupportedJobs = strings.Join(names, ",")

}

// Up starts up the JobsD service instance
func (j *JobsD) Up() error {
	if j.started {
		j.log.Warn("the service is already up")
		return nil
	}

	j.log.Debug("bringing up the service - started")

	if j.dbMigrate {
		txErr := j.db.AutoMigrate(&JobRun{}, &Instance{})
		if txErr != nil {
			return txErr
		}
	}

	tx := j.db.Save(j.Instance)
	if tx.Error != nil {
		return tx.Error
	}

	j.log = j.log.WithField("Instance.ID", j.Instance.ID)

	j.started = true
	j.runNow = make(chan JobRun)
	j.runQNew = make(chan struct{})

	j.producerCtx, j.producerCtxCancelFunc = context.WithCancel(context.Background())
	j.producerCancelWait = sync.WaitGroup{}

	j.workerCtx, j.workerCtxCancelFunc = context.WithCancel(context.Background())
	j.workertCxCancelWait = sync.WaitGroup{}

	j.producerCancelWait.Add(3)
	go j.jobLoader()
	go j.jobDelegator()
	go j.jobResurrector()

	for i := 0; i < j.Instance.Workers; i++ {
		j.workertCxCancelWait.Add(1)
		go j.jobRunner()
	}

	j.log.Debug("bringing up the service - completed")
	return nil
}

func (j *JobsD) jobLoader() {
	for {
		select {
		case <-j.producerCtx.Done():
			j.log.Trace("shutdown jobLoader")
			j.producerCancelWait.Done()
			return
		case <-time.After(j.Instance.JobPollInterval):
			break
		}

		j.log.Trace("loading jobs from the DB - started")

		jobRuns := []JobRun{}
		tx := j.db.Where("run_started_by IS NULL").Order("run_at ASC").
			Limit(j.Instance.JobPollLimit).Find(&jobRuns)
		if tx.Error != nil {
			j.log.WithError(tx.Error).Warn("failed to load job runs from DB")
		}

		for _, jobRun := range jobRuns {
			// Make sure the job is compatible
			if err := jobRun.check(j.jobs, j.schedules); err == nil {
				j.runQ.Push(jobRun)
				j.log.WithField("Job.ID", jobRun.ID).Trace("added job run from DB")
			}
		}

		j.Instance.LastSeenAt = sql.NullTime{Valid: true, Time: time.Now()}
		tx = j.db.Save(j.Instance)
		if tx.Error != nil {
			j.log.WithError(tx.Error).Warn("failed to update instance status")
		}

		if len(jobRuns) > 0 {
			j.runQNew <- struct{}{}
		}

		j.log.Trace("loading jobs from the DB - completed")
	}
}

func (j *JobsD) jobDelegator() {
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
		case <-j.producerCtx.Done():
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

func (j *JobsD) jobRunner() {
	for {
		select {
		case <-j.workerCtx.Done():
			j.log.Trace("shutdown jobRunner")
			j.workertCxCancelWait.Done()
			return
		case jobRun := <-j.runNow:
			log := jobRun.logger(j.log)

			log.Trace("acquiring job run lock")

			// lock the job
			locked, err := jobRun.lockStart(j.db, j.Instance.ID)
			if err != nil {
				log.WithError(err).Error("failed to lock job run")
				break
			}
			if !locked {
				break // another instance is running it
			}

			log.Trace("running job - started")

			// run the job
			j.Instance.JobRuns++
			jobContainer, ok := j.jobs[jobRun.Job]
			if !ok {
				log.WithField("JobName", jobRun.Job).Error("can not run job. job does not exists")
			}
			jobErr := jobContainer.jobFunc.execute(jobRun.JobArgs)
			if jobErr != nil {
				j.Instance.JobRunsError++
				log.WithError(jobErr).Warn("job run finished with an error")
			}

			completeErr := jobRun.complete(j.db, j.Instance.ID, jobErr)
			if completeErr != nil {
				log.WithError(completeErr).Error("error ocurred while finishing job")
				return
			}
			j.jobFinish(jobRun)

			log.Trace("running job - completed")
		}

	}
}

func (j *JobsD) jobFinish(jobRun JobRun) {
	log := jobRun.logger(j.log)

	retryJobRun, retryErr := jobRun.retryOnError(j.db, j.Instance.ID)
	if retryErr != nil {
		log.WithError(retryErr).Error("could not create job retry")
		return
	}
	if retryJobRun != nil {
		log.WithFields(logrus.Fields{
			"JobRunRetriesOnErrorCount": retryJobRun.RetriesOnErrorCount,
			"JobRunRetriesOnErrorLimit": retryJobRun.RetriesOnErrorLimit,
		}).Info("job run error retry has been queued")

		j.Instance.JobRunsRequeuedError++
		j.addJobRun(*retryJobRun)
		return
	}

	rescheduleJobRun, rescheduleErr := jobRun.reschedule(j.db, j.Instance.ID, j.schedules)
	if rescheduleErr != nil {
		log.WithError(rescheduleErr).Error("could not reschedule job")
		return
	}
	if rescheduleJobRun != nil {
		j.Instance.JobRunsRescheduled++
		j.addJobRun(*rescheduleJobRun)
		return
	}

	if err := jobRun.close(j.db, j.Instance.ID); err != nil {
		log.WithError(err).Error("could not close job run")
	}
}

func (j *JobsD) jobResurrector() {
	for {
		select {
		case <-j.producerCtx.Done():
			j.producerCancelWait.Done()
			return
		case <-time.After(j.Instance.JobRetryCheck):
			break
		}
		j.log.Trace("finding jobs to resurrect - started")

		jobRuns := []JobRun{}
		j.db.Where(
			"run_started_by IS NOT NULL AND closed_by IS NULL AND retry_timeout_at <= ?",
			time.Now(),
		).Limit(j.Instance.JobPollLimit).Find(&jobRuns)

		if len(jobRuns) > 0 {
			j.log.WithField("count", len(jobRuns)).Debug("job runs for resurrection found")
		}

		for _, jobRun := range jobRuns {
			log := jobRun.logger(j.log)

			retryJobRun, retryErr := jobRun.retryOnTimeout(j.db, j.Instance.ID)
			if retryErr != nil {
				log.WithError(retryErr).Error("could not create job resurrection timeout retry")
				continue
			}
			if retryJobRun != nil {
				log.WithFields(logrus.Fields{
					"JobRun.RetriesOnTimeoutCount": retryJobRun.RetriesOnTimeoutCount,
					"JobRun.RetriesOnTimeoutLimit": retryJobRun.RetriesOnTimeoutLimit,
				}).Info("job run timeout retry has been queued")

				j.Instance.JobRunsRequeuedTimeout++
				j.addJobRun(*retryJobRun)
				continue
			}

			j.jobFinish(jobRun)
		}

		j.Instance.LastSeenAt = sql.NullTime{Valid: true, Time: time.Now()}
		j.db.Save(j.Instance)

		j.log.Trace("finding jobs to resurrect - completed")
	}
}

func (j *JobsD) addJobRun(jobRun JobRun) {
	j.runQ.Push(jobRun)

	// Notify the delegator of the new item to run
	// This needs to be async to prevent a job run from deadlocking trying to reschedule
	go func() { j.runQNew <- struct{}{} }()
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

	j.Instance.ShutdownAt = sql.NullTime{Valid: true, Time: time.Now()}
	j.Instance.LastSeenAt = j.Instance.ShutdownAt
	tx := j.db.Save(j.Instance)

	j.log.Debug("shuting down the JobsD service - completed")

	return tx.Error
}

// CreateRun .
func (j *JobsD) CreateRun(job string, jobParams ...interface{}) *RunOnceCreator {
	theUUID, err := uuid.NewUUID()
	if err != nil {
		// This only happens when the uuid lib cannot get the time.
		// Since time is critical and this error is extremely rate we die here.
		panic(err)
	}

	name := sql.NullString{Valid: true, String: theUUID.String()}
	rtn := &RunOnceCreator{
		queued: j,
		jobRun: &JobRun{
			Name:         name,
			NameActive:   name,
			Job:          job,
			JobArgs:      jobParams,
			RunAt:        time.Now(),
			RunLimit:     sql.NullInt64{Valid: true, Int64: 1},
			RetryTimeout: j.Instance.JobRetryTimeout,
			CreatedAt:    time.Now(),
			CreatedBy:    j.Instance.ID,
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
		j.Instance.Workers = workers
	}
	return j
}

// JobPollInterval sets the time between getting JobRuns from the DB
func (j *JobsD) JobPollInterval(pollInt time.Duration) *JobsD {
	if !j.started {
		j.Instance.JobPollInterval = pollInt
	}
	return j
}

// JobPollLimit sets the number of upcoming JobRuns to retreive from the DB at a time
func (j *JobsD) JobPollLimit(limit int) *JobsD {
	if !j.started {
		j.Instance.JobPollLimit = limit
	}
	return j
}

// JobRetryTimeout sets default job retry timeout
func (j *JobsD) JobRetryTimeout(timeout time.Duration) *JobsD {
	if !j.started {
		j.Instance.JobRetryTimeout = timeout
	}
	return j
}

// JobRetryTimeoutLimit default job retry on timeout limit
func (j *JobsD) JobRetryTimeoutLimit(limit int) *JobsD {
	if !j.started {
		j.Instance.JobRetryTimeoutLimit = limit
	}
	return j
}

// JobRetryTimeoutCheck sets the time between retry timeout checks
func (j *JobsD) JobRetryTimeoutCheck(interval time.Duration) *JobsD {
	if !j.started {
		j.Instance.JobRetryCheck = interval
	}
	return j
}

// JobRetryErrorLimit default job retry on error limit
func (j *JobsD) JobRetryErrorLimit(limit int) *JobsD {
	if !j.started {
		j.Instance.JobRetryErrorLimit = limit
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
		Instance: &Instance{
			Workers:              10,
			JobPollInterval:      time.Duration(time.Second * 5),
			JobPollLimit:         1000,
			JobRetryTimeout:      time.Duration(time.Minute * 30),
			JobRetryCheck:        time.Duration(time.Second * 30),
			JobRetryTimeoutLimit: 3,
			JobRetryErrorLimit:   3,
		},
		jobs:      map[string]*JobContainer{},
		schedules: map[string]ScheduleFunc{},
		runQ:      NewJobRunQueue(),
		dbMigrate: true,
		db:        db,
	}

	rtn.Logger(logc.NewLogrus(logrus.New()))

	return rtn
}
