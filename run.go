package jobspec

import "github.com/pkg/errors"

// RunHelper .
type RunHelper struct{}

// RunState .
type RunState struct{}

// Refresh the run state
func (j *RunState) Refresh() error {
	return errors.New("not implemented")
}

// Completed returns true if the job run is complete
func (j *RunState) Completed() bool {
	return false
}
