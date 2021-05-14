package jobspec

import (
	"github.com/pkg/errors"
	"github.com/simpleframeworks/jobspec/models"
)

// Job .
type Job struct {
	job  *models.Job
	spec spec

	makeRun func(s spec, args []interface{}) (RunState, error)
}

// Name .
func (j *Job) Name() string {
	return j.job.Name
}

// Run .
func (j *Job) Run(args ...interface{}) (*RunState, error) {
	spec := j.spec
	spec.schedule = false
	runState, err := j.makeRun(spec, args)
	return &runState, err
}

// RunDistinct .
func (j *Job) RunDistinct(args ...interface{}) (*RunState, error) {
	spec := j.spec
	spec.schedule = false
	spec.unique = true
	runState, err := j.makeRun(spec, args)
	return &runState, err
}

// Schedule .
func (j *Job) Schedule(args ...interface{}) (*RunState, error) {
	if j.spec.schedule {
		spec := j.spec
		runState, err := j.makeRun(spec, args)
		return &runState, err
	}
	return nil, errors.New("no schedule associated with this job")
}

// ScheduleDistinct .
func (j *Job) ScheduleDistinct(args ...interface{}) (*RunState, error) {
	if j.spec.schedule {
		spec := j.spec
		spec.unique = true
		runState, err := j.makeRun(spec, args)
		return &runState, err
	}
	return nil, errors.New("no schedule associated with this job")
}

// History .
func (j *Job) History(limit int) ([]*RunState, error) {
	return []*RunState{}, errors.New("not implemented")
}
