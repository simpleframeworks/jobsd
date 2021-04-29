package jobspec

import (
	"github.com/pkg/errors"
	"github.com/simpleframeworks/jobspec/internal"
)

// Job .
type Job struct {
	model internal.Job
	spec  Spec
}

// Run .
func (j *Job) Run() (*RunState, error) {
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
