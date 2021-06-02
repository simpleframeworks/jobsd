package jobspec

import (
	"time"

	"github.com/pkg/errors"
	"github.com/simpleframeworks/jobspec/models"
	"gorm.io/gorm"
)

// Job .
type Job struct {
	job  *models.Job
	spec spec

	queueRun      func(runAt time.Time, s spec, args []interface{}, model *models.Run, tx *gorm.DB) (RunState, error)
	queueSchedule func(s spec, args []interface{}, model *models.Schedule, tx *gorm.DB) (ScheduleState, error)
}

// Name .
func (j *Job) Name() string {
	return j.job.Name
}

// RunIt .
func (j *Job) RunIt(args ...interface{}) (*RunState, error) {
	spec := j.spec
	runState, err := j.queueRun(time.Now(), spec, args, nil, nil)
	return &runState, err
}

// RunAfter .
func (j *Job) RunAfter(delay time.Duration, args ...interface{}) (*RunState, error) {
	spec := j.spec
	runState, err := j.queueRun(time.Now().Add(delay), spec, args, nil, nil)
	return &runState, err
}

// ScheduleIt .
func (j *Job) ScheduleIt(args ...interface{}) (*ScheduleState, error) {
	if j.spec.schedule {
		spec := j.spec
		runState, err := j.queueSchedule(spec, args, nil, nil)
		return &runState, err
	}
	return nil, errors.New("no schedule associated with this job")
}

// GetRunStates .
func (j *Job) GetRunStates(limit int) ([]*RunState, error) {
	return []*RunState{}, errors.New("not implemented")
}

// GetScheduleStates .
func (j *Job) GetScheduleStates(limit int) ([]*RunState, error) {
	return []*RunState{}, errors.New("not implemented")
}
