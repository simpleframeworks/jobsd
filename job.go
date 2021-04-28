package jobspec

import "github.com/pkg/errors"

// Job .
type Job struct{}

// ID .
func (j *Job) ID() int64 {
	return 0
}

// Run .
func (j *Job) Run() (*RunState, error) {
	return &RunState{}, errors.New("not implemented")
}

// History .
func (j *Job) History(limit int) ([]*RunState, error) {
	return []*RunState{}, errors.New("not implemented")
}
