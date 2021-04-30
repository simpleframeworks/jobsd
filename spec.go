package jobspec

import (
	"time"
)

// spec defines a complete implementation of a job that can run and optionally be scheduled
type spec struct {
	name             string
	jobFunc          JobFunc
	unique           bool
	timeout          time.Duration
	retriesOnError   int
	retriesOnTimeout int
	schedule         bool
	scheduleFunc     ScheduleFunc
	limit            int
}

func (s *spec) run(helper RunHelper) error {
	return s.jobFunc(helper)
}

// SpecMaker .
type SpecMaker struct {
	spec    spec
	makeJob func(spec) (Job, error)
}

// Register , registers the job against the JobsD instance. This needs to be done to return a job that can be run.
func (c *SpecMaker) Register() (Job, error) {
	return c.makeJob(c.spec)
}

// Name of the job
func (c *SpecMaker) Name(name string) *SpecMaker {
	c.spec.name = name
	return c
}

// Job , sets the job func. It should already be set
func (c *SpecMaker) Job(jobFunc JobFunc) *SpecMaker {
	c.spec.jobFunc = jobFunc
	return c
}

// Schedule set the scheduler that given a time returns the next time the job should run
func (c *SpecMaker) Schedule(schedule ScheduleFunc) *SpecMaker {
	c.spec.schedule = true
	c.spec.scheduleFunc = schedule
	return c
}

// Unique if true ensures only one Job func or Scheduled Job func is running at a time across the cluster.
// The Name is used to deduplicate running Job funcs
func (c *SpecMaker) Unique(isUnique bool) *SpecMaker {
	c.spec.unique = isUnique
	return c
}

// Timeout determines how long to wait till we cleanup a running Job and then send a RunHelper.Cancel message
// A timeout of 0 will disable this and let a job run indefinitely
func (c *SpecMaker) Timeout(timeout time.Duration) *SpecMaker {
	c.spec.timeout = timeout
	return c
}

// TimeoutRetries determines how many times to retry a Job if it times out
//  0 will disable retries
// -1 will retry a Job on timeout indefinitely
func (c *SpecMaker) TimeoutRetries(retries int) *SpecMaker {
	c.spec.retriesOnTimeout = retries
	return c
}

// ErrorRetries determines how many times to retry a Job if it returns an error
//  0 will disable retries
// -1 will retry a Job on error indefinitely
func (c *SpecMaker) ErrorRetries(retries int) *SpecMaker {
	c.spec.retriesOnError = retries
	return c
}

// Limit sets the number of times a Job will run (ignoring errors and timeouts)
// 0 or -1 will ensure the Job is scheduled to run indefinitely
func (c *SpecMaker) Limit(limit int) *SpecMaker {
	c.spec.limit = limit
	return c
}
