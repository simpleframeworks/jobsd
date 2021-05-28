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

	queueRun      func(s spec, args []interface{}, model *models.Run, tx *gorm.DB) (RunState, error)
	queueSchedule func(s spec, args []interface{}, model *models.Schedule, tx *gorm.DB) (RunState, error)
}

// Name .
func (j *Job) Name() string {
	return j.job.Name
}

// RunIt .
func (j *Job) RunIt(args ...interface{}) (*RunState, error) {
	spec := j.spec
	runState, err := j.queueRun(spec, args, nil, nil)
	return &runState, err
}

// RunAfter .
func (j *Job) RunAfter(delay time.Duration, args ...interface{}) (*RunState, error) {
	return nil, errors.New("not implemented")
}

// ScheduleIt .
func (j *Job) ScheduleIt(args ...interface{}) (*RunState, error) {
	if j.spec.schedule {
		return nil, errors.New("not implemented")
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
