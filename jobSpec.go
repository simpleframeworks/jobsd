package jobspec

import (
	"time"

	"github.com/pkg/errors"
	"github.com/simpleframeworks/logc"
	"gorm.io/gorm"
)

// JobSpec .
type JobSpec struct{}

// NewJob .
func (j *JobSpec) NewJob(name string, jobFunc interface{}) CreatorRunable {
	return nil
}

// GetJob .
func (j *JobSpec) GetJob(name string) *Job {
	return nil
}

// GetDB .
func (j *JobSpec) GetDB() *gorm.DB {
	return nil
}

// SetLogger .
func (j *JobSpec) SetLogger(l logc.Logger) *JobSpec {
	return nil
}

// SetMigration .
func (j *JobSpec) SetMigration(m bool) *JobSpec {
	return nil
}

// JobHistory .
func (j *JobSpec) JobHistory(name int64, limit int) ([]RunInfo, error) {
	return []RunInfo{}, errors.New("not implemented")
}

// Start .
func (j *JobSpec) Start() error {
	return errors.New("not implemented")
}

// Stop .
func (j *JobSpec) Stop() error {
	return errors.New("not implemented")
}

// New .
func New(db *gorm.DB) *JobSpec {
	return &JobSpec{}
}

// Job .
type Job struct{}

// ID .
func (j *Job) ID() int64 {
	return 0
}

// Run .
func (j *Job) Run() (RunInfo, error) {
	return RunInfo{}, errors.New("not implemented")
}

// History .
func (j *Job) History(limit int) ([]RunInfo, error) {
	return []RunInfo{}, errors.New("not implemented")
}

// Spec defines a complete implementation of a job that can run and optionally be scheduled
type Spec interface {
	// Name of the job
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

// CreatorRunable .
type CreatorRunable interface {
	Register() (*Job, error)
	Name(name string) CreatorRunable
	Schedule(schedule ScheduleFunc) CreatorScheduleable
	Unique(isUnique bool) CreatorRunable
	Delay(delay time.Duration) CreatorRunable
	Timeout(timeout time.Duration) CreatorRunable
	TimeoutLimit(limit int) CreatorRunable
	ErrorLimit(limit int) CreatorRunable
}

// CreatorScheduleable .
type CreatorScheduleable interface {
	Register() (*Job, error)
	Name(name string) CreatorScheduleable
	Schedule(schedule ScheduleFunc) CreatorScheduleable
	Unique(isUnique bool) CreatorScheduleable
	Delay(delay time.Duration) CreatorScheduleable
	Timeout(timeout time.Duration) CreatorScheduleable
	TimeoutLimit(limit int) CreatorScheduleable
	ErrorLimit(limit int) CreatorScheduleable
	Limit(limit int) CreatorScheduleable
}

// creator .
type creator struct{}

// RunInfo .
type RunInfo struct{}

// ScheduleFunc .
type ScheduleFunc func(now time.Time) time.Time
