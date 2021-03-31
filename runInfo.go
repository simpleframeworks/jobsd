package jobsd

import "time"

// RunInfo exposes information and functions to a running job
type RunInfo struct {
	ID                    int64
	OriginID              int64
	Name                  string
	Job                   string
	JobArgs               JobArgs
	RunAt                 time.Time
	RunTotalCount         int
	RunSuccessCount       int
	RunSuccessLimit       *int
	RunStartedAt          time.Time
	RunStartedBy          int64
	RunTimeout            time.Duration
	RunTimeoutAt          *time.Time
	RetriesOnErrorCount   int
	RetriesOnErrorLimit   *int
	RetriesOnTimeoutCount int
	RetriesOnTimeoutLimit *int
	Schedule              *string
	CreatedAt             time.Time
	CreatedBy             int64
	Stop                  <-chan struct{}
}

func newRunInfo(r Runnable) RunInfo {

	rtn := RunInfo{
		ID:                    r.jobRun.ID,
		OriginID:              r.jobRun.OriginID,
		Name:                  r.jobRun.Name,
		Job:                   r.jobRun.Job,
		JobArgs:               r.jobRun.JobArgs,
		RunAt:                 r.jobRun.RunAt,
		RunTotalCount:         r.jobRun.RunTotalCount,
		RunSuccessCount:       r.jobRun.RunSuccessCount,
		RetriesOnErrorCount:   r.jobRun.RetriesOnErrorCount,
		RetriesOnTimeoutCount: r.jobRun.RetriesOnTimeoutCount,
		CreatedAt:             r.jobRun.CreatedAt,
		CreatedBy:             r.jobRun.CreatedBy,
		Stop:                  r.stop,
	}

	if r.jobRun.RunSuccessLimit.Valid {
		rsl := int(r.jobRun.RunSuccessLimit.Int64)
		rtn.RunSuccessLimit = &rsl
	}
	if r.jobRun.RetriesOnErrorLimit.Valid {
		rel := int(r.jobRun.RetriesOnErrorLimit.Int64)
		rtn.RetriesOnErrorLimit = &rel
	}
	if r.jobRun.RunTimeout.Valid {
		rtn.RunTimeout = time.Duration(r.jobRun.RunTimeout.Int64)
	}
	if r.jobRun.RetriesOnTimeoutLimit.Valid {
		rtl := int(r.jobRun.RetriesOnTimeoutLimit.Int64)
		rtn.RetriesOnTimeoutLimit = &rtl
	}
	if r.jobRun.Schedule.Valid {
		rtn.Schedule = &r.jobRun.Schedule.String
	}

	return rtn
}
