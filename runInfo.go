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

func newRunInfo(r Run, stop <-chan struct{}) RunInfo {

	rtn := RunInfo{
		ID:                    r.ID,
		OriginID:              r.OriginID,
		Name:                  r.Name,
		Job:                   r.Job,
		JobArgs:               r.JobArgs,
		RunAt:                 r.RunAt,
		RunTotalCount:         r.RunTotalCount,
		RunSuccessCount:       r.RunSuccessCount,
		RetriesOnErrorCount:   r.RetriesOnErrorCount,
		RetriesOnTimeoutCount: r.RetriesOnTimeoutCount,
		CreatedAt:             r.CreatedAt,
		CreatedBy:             r.CreatedBy,
		Stop:                  stop,
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
	if r.Schedule.Valid {
		rtn.Schedule = &r.Schedule.String
	}

	return rtn
}
