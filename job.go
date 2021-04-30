package jobspec

import (
	"github.com/pkg/errors"
	"github.com/simpleframeworks/jobspec/models"
	"gorm.io/gorm"
)

// Job .
type Job struct {
	db   *gorm.DB
	job  *models.Job
	spec spec

	makeRun func(s spec, args ...interface{}) (RunState, error)
}

// Name .
func (j *Job) Name() string {
	return j.job.Name
}

// Run .
func (j *Job) Run(args ...interface{}) (*RunState, error) {
	return &RunState{}, errors.New("not implemented")
}

// Schedule .
func (j *Job) Schedule() (*RunState, error) {
	return &RunState{}, errors.New("not implemented")
}

// History .
func (j *Job) History(limit int) ([]*RunState, error) {
	return []*RunState{}, errors.New("not implemented")
}
