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

	queueRun func(s spec, args []interface{}, tx *gorm.DB) (RunState, error)
}

// Name .
func (j *Job) Name() string {
	return j.job.Name
}

// Run .
func (j *Job) Run(args ...interface{}) (*RunState, error) {
	spec := j.spec
	spec.schedule = false
	runState, err := j.queueRun(spec, args, nil)
	return &runState, err
}

// RunDistinct .
func (j *Job) RunDistinct(args ...interface{}) (*RunState, error) {
	spec := j.spec
	spec.schedule = false
	spec.unique = true
	runState, err := j.queueRun(spec, args, nil)
	return &runState, err
}

// RunAfter .
func (j *Job) RunAfter(delay time.Duration, args ...interface{}) (*RunState, error) {
	return nil, errors.New("not implemented")
}

// RunDistinctAfter .
func (j *Job) RunDistinctAfter(delay time.Duration, args ...interface{}) (*RunState, error) {
	return nil, errors.New("not implemented")
}

// Schedule .
func (j *Job) Schedule(args ...interface{}) (*RunState, error) {
	if j.spec.schedule {
		return nil, errors.New("not implemented")
	}
	return nil, errors.New("no schedule associated with this job")
}

// ScheduleDistinct .
func (j *Job) ScheduleDistinct(args ...interface{}) (*RunState, error) {
	if j.spec.schedule {
		return nil, errors.New("not implemented")
	}
	return nil, errors.New("no schedule associated with this job")
}

// History .
func (j *Job) History(limit int) ([]*RunState, error) {
	return []*RunState{}, errors.New("not implemented")
}
