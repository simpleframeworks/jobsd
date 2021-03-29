package jobsd

import (
	"database/sql"
	"time"

	"github.com/pkg/errors"
)

// RunOnceCreator creates a job run that runs only once
type RunOnceCreator struct {
	done   bool
	jobsd  *JobsD
	jobRun JobRun
}

// Unique gives the run a unique name across the cluster.
// i.e only one job with a unique name can be running or jobsd at the same time.
func (r *RunOnceCreator) Unique(name string) *RunOnceCreator {
	if r.done {
		return r
	}
	r.jobRun.Name = name
	r.jobRun.NameActive = sql.NullString{Valid: true, String: name}
	return r
}

// RunTimeout sets the RunTimeout
// Setting it to 0 disables timeout
func (r *RunOnceCreator) RunTimeout(timeout time.Duration) *RunOnceCreator {
	if r.done {
		return r
	}
	if timeout <= 0 {
		r.jobRun.RunTimeout = sql.NullInt64{}
	} else {
		r.jobRun.RunTimeout = sql.NullInt64{Valid: true, Int64: int64(timeout)}
	}
	return r
}

// RetriesTimeoutLimit sets how many times a job run can timeout
// Setting it to -1 removes the limit
func (r *RunOnceCreator) RetriesTimeoutLimit(limit int) *RunOnceCreator {
	if r.done {
		return r
	}
	if limit < 0 {
		r.jobRun.RetriesOnTimeoutLimit = sql.NullInt64{}
	} else {
		r.jobRun.RetriesOnTimeoutLimit = sql.NullInt64{Valid: true, Int64: int64(limit)}
	}
	return r
}

// RetryErrorLimit sets the RetryErrorLimit
// Setting it to -1 removes the limit
func (r *RunOnceCreator) RetryErrorLimit(limit int) *RunOnceCreator {
	if r.done {
		return r
	}
	if limit < 0 {
		r.jobRun.RetriesOnErrorLimit = sql.NullInt64{}
	} else {
		r.jobRun.RetriesOnErrorLimit = sql.NullInt64{Valid: true, Int64: int64(limit)}
	}
	return r
}

// Run the job
// returns the Job Run ID and Error
func (r *RunOnceCreator) Run() (int64, error) {
	if r.done {
		return 0, errors.New("run can only be called once")
	}
	r.done = true
	jr, err := r.jobsd.createJobRunnable(r.jobRun)
	jr.schedule()
	return jr.jobRun.ID, err
}

// RunAfter the job
// returns the Job Run ID and Error
func (r *RunOnceCreator) RunAfter(delay time.Duration) (int64, error) {
	if r.done {
		return 0, errors.New("run delayed can only be called once")
	}
	r.jobRun.Delay = delay
	r.done = true
	return r.jobRun.run(r.jobsd)
}

// Schedule the job
func (r *RunOnceCreator) Schedule(schedule string) *RunScheduleCreator {
	rtn := &RunScheduleCreator{
		jobsd:  r.jobsd,
		jobRun: r.jobRun,
	}
	rtn.jobRun.Schedule = sql.NullString{Valid: true, String: schedule}
	return rtn
}

// RunScheduleCreator create a job run that runs according to a schedule
type RunScheduleCreator struct {
	done   bool
	jobsd  *JobsD
	jobRun JobRun
}

// Unique gives the run a unique name across the cluster.
// i.e only one job with a unique name can be running or jobsd at the same time.
func (r *RunScheduleCreator) Unique(name string) *RunScheduleCreator {
	if r.done {
		return r
	}
	r.jobRun.Name = name
	r.jobRun.NameActive = sql.NullString{Valid: true, String: name}
	return r
}

// RetryTimeout sets the RetryTimeout
// Setting it to 0 disables timeout
func (r *RunScheduleCreator) RetryTimeout(timeout time.Duration) *RunScheduleCreator {
	if r.done {
		return r
	}
	r.jobRun.RetryTimeout = timeout
	return r
}

// RetryTimeoutLimit sets the RetryTimeoutLimit
// Setting it to -1 removes the limit
func (r *RunScheduleCreator) RetryTimeoutLimit(limit int) *RunScheduleCreator {
	if r.done {
		return r
	}
	r.jobRun.RetriesOnTimeoutLimit = limit
	return r
}

// RetryErrorLimit sets the RetryErrorLimit
// Setting it to -1 removes the limit
func (r *RunScheduleCreator) RetryErrorLimit(limit int) *RunScheduleCreator {
	if r.done {
		return r
	}
	r.jobRun.RetriesOnErrorLimit = limit
	return r
}

// Limit sets how many times the job can successfully run
func (r *RunScheduleCreator) Limit(limit int) *RunScheduleCreator {
	if r.done {
		return r
	}
	r.jobRun.RunLimit = sql.NullInt64{Valid: true, Int64: int64(limit)}
	return r
}

// Run the job according to the schedule
// returns the Job Run ID and Error
func (r *RunScheduleCreator) Run() (int64, error) {
	if r.done {
		return 0, errors.New("run already called")
	}
	r.done = true
	jr, err := r.jobsd.createJobRunnable(r.jobRun)
	jr.schedule()
	return jr.jobRun.ID, err
}

// RunAfter the specified duration
// returns the Job Run ID and Error
func (r *RunScheduleCreator) RunAfter(delay time.Duration) (int64, error) {
	if r.done {
		return 0, errors.New("run delayed already called")
	}
	r.jobRun.Delay = delay
	r.done = true
	return r.jobRun.run(r.jobsd)
}
