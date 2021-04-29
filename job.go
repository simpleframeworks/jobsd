package jobspec

import (
	"time"

	"github.com/pkg/errors"
)

// Job .
type Job struct {
	model JobModel
	spec  Spec
}

// JobModel .
type JobModel struct {
	ID        int64 `gorm:"primaryKey"`
	Name      string
	CreatedAt time.Time
	CreatedBy int64
}

// Run .
func (j *Job) Run() (*RunState, error) {
	return &RunState{}, errors.New("not implemented")
}

// History .
func (j *Job) History(limit int) ([]*RunState, error) {
	return []*RunState{}, errors.New("not implemented")
}
