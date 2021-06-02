package jobspec

import (
	"time"
)

// spec defines a complete implementation of a job that can run and optionally be scheduled
type spec struct {
	jobID            int64
	jobName          string
	jobFunc          JobFunc
	deduplicate      bool
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

func (s *spec) runAt(now time.Time) time.Time {
	return s.scheduleFunc(time.Now())
}

// ScheduleTimingOffset is the duration before running a job to schedule it
const ScheduleTimingOffset = time.Second * 10

// SpecMaker .
type SpecMaker struct {
	spec        spec
	registerJob func(spec) (Job, error)
}

// Register , registers the job against the JobsD instance. This needs to be done to return a job that can be run.
func (c *SpecMaker) Register() (Job, error) {
	return c.registerJob(c.spec)
}

// SetName of the job
func (c *SpecMaker) SetName(jobName string) *SpecMaker {
	c.spec.jobName = jobName
	return c
}

// SetJob , sets the job func. It should already be set
func (c *SpecMaker) SetJob(jobFunc JobFunc) *SpecMaker {
	c.spec.jobFunc = jobFunc
	return c
}

// SetSchedule sets the scheduler that given a time returns the next time the job should run
func (c *SpecMaker) SetSchedule(schedule ScheduleFunc) *SpecMaker {
	c.spec.schedule = true
	c.spec.scheduleFunc = schedule
	return c
}

// SetTimeout determines how long to wait till we cleanup a running Job and then send a RunHelper.Cancel message
// A timeout of 0 will disable this and let a job run indefinitely
func (c *SpecMaker) SetTimeout(timeout time.Duration) *SpecMaker {
	c.spec.timeout = timeout
	return c
}

// SetTimeoutRetries determines how many times to retry a Job if it times out
//  0 will disable retries
// -1 will retry a Job on timeout indefinitely
func (c *SpecMaker) SetTimeoutRetries(retries int) *SpecMaker {
	c.spec.retriesOnTimeout = retries
	return c
}

// SetErrorRetries determines how many times to retry a Job if it returns an error
//  0 will disable retries
// -1 will retry a Job on error indefinitely
func (c *SpecMaker) SetErrorRetries(retries int) *SpecMaker {
	c.spec.retriesOnError = retries
	return c
}

// SetLimit sets the number of times a Job will run (ignoring errors and timeouts)
// 0 or -1 will ensure the Job is scheduled to run indefinitely
func (c *SpecMaker) SetLimit(limit int) *SpecMaker {
	c.spec.limit = limit
	return c
}

// SetDeduplicate .
func (c *SpecMaker) SetDeduplicate(deduplicate bool) *SpecMaker {
	c.spec.deduplicate = deduplicate
	return c
}
