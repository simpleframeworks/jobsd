package jobss

import "time"

// RunInfo exposes information and functions to a running job
type RunInfo struct {
	ID                    int64
	OriginID              int64
	Name                  string
	Job                   *string
	JobArgs               JobArgs
	Schedule              *string
	Package               *string
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
	CreatedAt             time.Time
	CreatedBy             int64
	Cancel                <-chan struct{}
}

func newRunInfo(r Run, cancel <-chan struct{}) RunInfo {

	rtn := RunInfo{
		ID:                    r.ID,
		OriginID:              r.OriginID,
		Name:                  r.Name,
		JobArgs:               r.JobArgs,
		RunAt:                 r.RunAt,
		RunTotalCount:         r.RunTotalCount,
		RunSuccessCount:       r.RunSuccessCount,
		RetriesOnErrorCount:   r.RetriesOnErrorCount,
		RetriesOnTimeoutCount: r.RetriesOnTimeoutCount,
		CreatedAt:             r.CreatedAt,
		CreatedBy:             r.CreatedBy,
		Cancel:                cancel,
	}

	if r.Job.Valid {
		rtn.Job = &r.Job.String
	}
	if r.Schedule.Valid {
		rtn.Schedule = &r.Schedule.String
	}
	if r.Package.Valid {
		rtn.Package = &r.Package.String
	}

	if r.RunSuccessLimit.Valid {
		rsl := int(r.RunSuccessLimit.Int64)
		rtn.RunSuccessLimit = &rsl
	}
	if r.RetriesOnErrorLimit.Valid {
		rel := int(r.RetriesOnErrorLimit.Int64)
		rtn.RetriesOnErrorLimit = &rel
	}
	if r.RunTimeout.Valid {
		rtn.RunTimeout = time.Duration(r.RunTimeout.Int64)
	}
	if r.RetriesOnTimeoutLimit.Valid {
		rtl := int(r.RetriesOnTimeoutLimit.Int64)
		rtn.RetriesOnTimeoutLimit = &rtl
	}

	return rtn
}
