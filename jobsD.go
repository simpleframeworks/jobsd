package jobspec

import (
	"time"

	"github.com/pkg/errors"
	"github.com/simpleframeworks/logc"
	"gorm.io/gorm"
)

// JobsD .
type JobsD struct{}

// NewJob .
func (j *JobsD) NewJob(name string, jobFunc interface{}) *JobCreator {
	return nil
}

// GetJob .
func (j *JobsD) GetJob(name string) *Job {
	return nil
}

// GetDB .
func (j *JobsD) GetDB() *gorm.DB {
	return nil
}

// SetLogger .
func (j *JobsD) SetLogger(l logc.Logger) *JobsD {
	return nil
}

// SetMigration .
func (j *JobsD) SetMigration(m bool) *JobsD {
	return nil
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
	return errors.New("not implemented")
}

// New .
func New(db *gorm.DB) *JobsD {
	return &JobsD{}
}

// Job .
type Job struct{}

// ID .
func (j *Job) ID() int64 {
	return 0
}

// Run .
func (j *Job) Run() (*RunState, error) {
	return &RunState{}, errors.New("not implemented")
}

// History .
func (j *Job) History(limit int) ([]*RunState, error) {
	return []*RunState{}, errors.New("not implemented")
}

// JobSpec defines a complete implementation of a job that can run and optionally be scheduled
type JobSpec interface {
	// Name of the job
	Name() string

	// Unique if true ensures only one package Job func is running at a time across the cluster.
	// The Name is used to deduplicate running Job funcs
	Unique() bool

	// Job is any work that needs to be done
	Job(info RunState) error

	// Timeout determines how long to wait till we cleanup a running Job and then send a RunState.Cancel message
	// A timeout of 0 will disable this and let a job run indefinitely
	Timeout() time.Duration

	// RetriesOnError determines how many times to retry a Job if it returns an error
	//  0 will disable retries
	// -1 will retry a Job on error indefinitely
	RetriesOnError() int

	// RetriesOnTimeout determines how many times to retry a Job if it times out
	//  0 will disable retries
	// -1 will retry a Job on timeout indefinitely
	RetriesOnTimeout() int

	// Schedule if true schedules the Job
	// if false the job is run immediately and only runs once
	Schedule() bool

	// Scheduler given a time return the next time the job should run
	Scheduler(now time.Time) time.Time

	// Limit sets the number of times a Job will run (ignoring errors and timeouts)
	// 0 or -1 will ensure the Job is scheduled to run indefinitely
	Limit() int

	// Delay adds a delay before scheduling or running the job the very first time
	Delay() time.Duration
}

// JobCreator .
type JobCreator struct{}

// Register .
func (c *JobCreator) Register() (*Job, error) { return nil, errors.New("not implemented") }

// Name .
func (c *JobCreator) Name(name string) *JobCreator { return c }

// Schedule .
func (c *JobCreator) Schedule(schedule ScheduleFunc) *JobCreator { return c }

// Unique .
func (c *JobCreator) Unique(isUnique bool) *JobCreator { return c }

// Delay .
func (c *JobCreator) Delay(delay time.Duration) *JobCreator { return c }

// Timeout .
func (c *JobCreator) Timeout(timeout time.Duration) *JobCreator { return c }

// TimeoutLimit .
func (c *JobCreator) TimeoutLimit(limit int) *JobCreator { return c }

// ErrorLimit .
func (c *JobCreator) ErrorLimit(limit int) *JobCreator { return c }

// Limit .
func (c *JobCreator) Limit(limit int) *JobCreator { return c }

// RunState .
type RunState struct{}

// Refresh the run state
func (j *RunState) Refresh() error {
	return errors.New("not implemented")
}

// Completed returns true if the job run is complete
func (j *RunState) Completed() bool {
	return false
}

// ScheduleFunc .
type ScheduleFunc func(now time.Time) time.Time

// RunHelper .
type RunHelper struct{}
