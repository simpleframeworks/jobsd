package jobspec

import (
	"time"

	"github.com/pkg/errors"
	"github.com/simpleframeworks/logc"
	"gorm.io/gorm"
)

// JobsD .
type JobsD struct{}

// NewJob .
func (j *JobsD) NewJob(name string, jobFunc interface{}) *Creator {
	return nil
}

// GetJob .
func (j *JobsD) GetJob(name string) *Job {
	return nil
}

// GetDB .
func (j *JobsD) GetDB() *gorm.DB {
	return nil
}

// SetLogger .
func (j *JobsD) SetLogger(l logc.Logger) *JobsD {
	return nil
}

// SetMigration .
func (j *JobsD) SetMigration(m bool) *JobsD {
	return nil
}

// JobHistory .
func (j *JobsD) JobHistory(name string, limit int) ([]RunState, error) {
	return []RunState{}, errors.New("not implemented")
}

// Start .
func (j *JobsD) Start() error {
	return errors.New("not implemented")
}

// Stop .
func (j *JobsD) Stop() error {
	return errors.New("not implemented")
}

// New .
func New(db *gorm.DB) *JobsD {
	return &JobsD{}
}

// ScheduleFunc .
type ScheduleFunc func(now time.Time) time.Time
