package jobspec

import (
	"time"

	"github.com/pkg/errors"
)

// JobS .
type JobS struct{}

// NewJob .
func (j *JobS) NewJob(name string, jobFunc interface{}) JobSpecRunCreator {
	return nil
}

// GetJob .
func (j *JobS) GetJob(name string) *Job {
	return nil
}

// JobHistory .
func (j *JobS) JobHistory(name int64, limit int) ([]RunInfo, error) {
	return []RunInfo{}, errors.New("not implemented")
}

// Job .
type Job struct{}

// ID .
func (j *Job) ID() int64 {
	return 0
}

// Run .
func (j *Job) Run() error {
	return errors.New("not implemented")
}

// History .
func (j *Job) History(limit int) ([]RunInfo, error) {
	return []RunInfo{}, errors.New("not implemented")
}

// JobSpec defines a complete implementation of a job that can run and optionally be scheduled
type JobSpec interface {
	// Name the package name
	Name() string

	// Unique if true ensures only one package Job func is running at a time across the cluster.
	// The Name is used to deduplicate running Job funcs
	Unique() bool

	// Job is any work that needs to be done
	Job(info RunInfo) error

	// Timeout determines how long to wait till we cleanup a running Job and then send a RunInfo.Cancel message
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

// JobSpecRunCreator .
type JobSpecRunCreator interface {
	Register() (*Job, error)
	Name(name string) JobSpecRunCreator
	Schedule(schedule ScheduleFunc) JobSpecScheduleCreator
	Unique(isUnique bool) JobSpecRunCreator
	Delay(delay time.Duration) JobSpecRunCreator
	Timeout(timeout time.Duration) JobSpecRunCreator
	TimeoutLimit(limit int) JobSpecRunCreator
	ErrorLimit(limit int) JobSpecRunCreator
}

// JobSpecScheduleCreator .
type JobSpecScheduleCreator interface {
	Register() (*Job, error)
	Name(name string) JobSpecScheduleCreator
	Schedule(schedule ScheduleFunc) JobSpecScheduleCreator
	Unique(isUnique bool) JobSpecScheduleCreator
	Delay(delay time.Duration) JobSpecScheduleCreator
	Timeout(timeout time.Duration) JobSpecScheduleCreator
	TimeoutLimit(limit int) JobSpecScheduleCreator
	ErrorLimit(limit int) JobSpecScheduleCreator
	Limit(limit int) JobSpecScheduleCreator
}

// jobSpecCreator .
type jobSpecCreator struct{}

// RunInfo .
type RunInfo struct{}

// ScheduleFunc .
type ScheduleFunc func(now time.Time) time.Time
