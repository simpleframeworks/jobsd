package jobsd

import (
	"database/sql"
	"time"

	"github.com/pkg/errors"
)

// RunOnceCreator .
type RunOnceCreator struct {
	done   bool
	jobsd  *JobsD
	jobRun *JobRun
}

// Unique gives the run a unique name across the cluster.
// i.e only one job with a unique name can be running or jobsd at the same time.
func (r *RunOnceCreator) Unique(name string) *RunOnceCreator {
	if r.done {
		return r
	}
	r.jobRun.Name = sql.NullString{Valid: true, String: name}
	r.jobRun.NameActive = sql.NullString{Valid: true, String: name}
	return r
}

// Run the job
// returns the Job Run ID and Error
func (r *RunOnceCreator) Run() (int64, error) {
	if r.done {
		return 0, errors.New("run can only be called once")
	}
	r.done = true
	return r.jobRun.run(r.jobsd)
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
	rtn.jobRun.RunLimit = sql.NullInt64{}
	return rtn
}

// RunScheduleCreator .
type RunScheduleCreator struct {
	done   bool
	jobsd  *JobsD
	jobRun *JobRun
}

// Unique gives the run a unique name across the cluster.
// i.e only one job with a unique name can be running or jobsd at the same time.
func (r *RunScheduleCreator) Unique(name string) *RunScheduleCreator {
	if r.done {
		return r
	}
	r.jobRun.Name = sql.NullString{Valid: true, String: name}
	r.jobRun.NameActive = sql.NullString{Valid: true, String: name}
	return r
}

// Limit .
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
	return r.jobRun.run(r.jobsd)
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
