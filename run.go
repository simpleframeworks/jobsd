package jobspec

import (
	"github.com/pkg/errors"
	"github.com/simpleframeworks/jobspec/models"
	"gorm.io/gorm"
)

type run struct {
	db   *gorm.DB
	run  *models.Run
	spec *spec
	stop chan struct{}
}

func (r *run) lock() bool {
	return false
}

func (r *run) exec() {
	helper := RunHelper{}
	if err := r.spec.run(helper); err != nil {
		r.errorOut(err)
	} else {
		r.finish()
	}

}
func (r *run) finish()            {}
func (r *run) reschedule()        {}
func (r *run) timeOut()           {}
func (r *run) errorOut(err error) {}

func (r *run) runState() RunState {
	return RunState{}
}

// RunHelper .
type RunHelper struct {
	args   models.RunArgs
	cancel chan struct{}
}

// Args is the run args for the job
func (r *RunHelper) Args() models.RunArgs { return r.args }

// Cancel notifies when the job func needs to stop due to timeout or the instance stopping
func (r *RunHelper) Cancel() chan struct{} { return r.cancel }

// RunState .
type RunState struct {
}

// Refresh the run state
func (j *RunState) Refresh() error {
	return errors.New("not implemented")
}

// Completed returns true if the job run is complete
func (j *RunState) Completed() bool {
	return false
}
