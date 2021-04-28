package jobspec

import (
	"time"

	"github.com/pkg/errors"
)

// Spec defines a complete implementation of a job that can run and optionally be scheduled
type Spec interface {
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

type specGeneric struct{}

// Creator .
type Creator struct{}

// Register .
func (c *Creator) Register() (*Job, error) { return nil, errors.New("not implemented") }

// Name .
func (c *Creator) Name(name string) *Creator { return c }

// Schedule .
func (c *Creator) Schedule(schedule ScheduleFunc) *Creator { return c }

// Unique .
func (c *Creator) Unique(isUnique bool) *Creator { return c }

// Delay .
func (c *Creator) Delay(delay time.Duration) *Creator { return c }

// Timeout .
func (c *Creator) Timeout(timeout time.Duration) *Creator { return c }

// TimeoutLimit .
func (c *Creator) TimeoutLimit(limit int) *Creator { return c }

// ErrorLimit .
func (c *Creator) ErrorLimit(limit int) *Creator { return c }

// Limit .
func (c *Creator) Limit(limit int) *Creator { return c }
