package jobspec

import (
	"database/sql"
	"time"

	"github.com/simpleframeworks/jobspec/models"
	"github.com/simpleframeworks/logc"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type runner struct {
	db          *gorm.DB
	logger      logc.Logger
	modelRun    *models.Run
	modelActive *models.Active

	spec *spec
	stop chan struct{}
}

func (r *runner) TimeQID() int64 {
	return r.modelRun.ID
}

func (r *runner) TimeQTime() time.Time {
	return r.modelRun.RunAt
}

func (r *runner) exec(instanceID int64) {
	activated, err := r.activate(instanceID)
	if err != nil {
		r.logger.WithError(err).Error("cannot lock to run")
		return
	} else if !activated {
		r.logger.Debug("already activated, skipping run")
	} else {
		// TODO timeout here
		helper := RunHelper{
			args:   r.modelRun.Args,
			cancel: r.stop,
		}
		if err := r.spec.run(helper); err != nil {
			r.errorOut(err)
		} else {
			r.complete(nil)
		}
	}
}

func (r *runner) activate(instanceID int64) (bool, error) {
	startedAt := time.Now()
	timeoutAt := sql.NullTime{}
	if r.modelRun.RunTimeout > 0 {
		timeoutAt.Valid = true
		timeoutAt.Time = startedAt.Add(time.Duration(r.modelRun.RunTimeout))
	}
	uniqueID := sql.NullInt64{}
	if r.modelRun.Deduplicate {
		uniqueID.Valid = true
		uniqueID.Int64 = r.modelRun.JobID
	}
	r.modelActive = &models.Active{
		RunID:     r.modelRun.ID,
		JobID:     r.modelRun.JobID,
		JobName:   r.modelRun.JobName,
		UniqueID:  uniqueID,
		Args:      r.modelRun.Args,
		StartedAt: startedAt,
		TimeoutAt: timeoutAt,
		CreatedBy: instanceID,
	}
	res := r.db.Clauses(clause.OnConflict{DoNothing: true}).Create(r.modelActive)
	return res.RowsAffected == 1, res.Error
}

func (r *runner) timeOut() {
	//TODO
}

func (r *runner) errorOut(err error) {
	//TODO
}

func (r *runner) complete(runErr error) {
	r.modelRun.RunCompletedAt = sql.NullTime{Valid: true, Time: time.Now()}
	if runErr != nil {
		r.modelRun.RunCompletedError = runErr.Error()
	}

	res := r.db.Model(r.modelRun).Where("run_completed_at IS NULL").Updates(map[string]interface{}{
		"run_completed_at":    r.modelRun.RunCompletedAt,
		"run_completed_error": r.modelRun.RunCompletedError,
	})
	err := res.Error
	if err != nil {
		r.logger.WithError(err).Error("db error. could not mark job run as completed.")
		return
	} else if res.RowsAffected != 1 {
		r.logger.Error("could not mark job run as completed. already marked.")
		return
	}
}

func (r *runner) runState() RunState {
	return modelRunToRunState(*r.modelRun, r.db)
}

// RunHelper .
type RunHelper struct {
	args   models.RunArgs
	cancel chan struct{}
}

// Args is the run args for the job
func (r *RunHelper) Args() models.RunArgs { return r.args }

// Cancel return a channel that notifies when the job func needs to stop due to timeout or the instance stopping
func (r *RunHelper) Cancel() chan struct{} { return r.cancel }

// RunState .
type RunState struct {
	db          *gorm.DB
	model       *models.Run
	ID          int64
	JobID       int64
	JobName     string
	Deduplicate bool
	Scheduled   bool
	Args        models.RunArgs
	RunAt       time.Time

	RunStartedBy      *int64
	RunStartedAt      *time.Time
	RunCompletedAt    *time.Time
	RunCompletedError string
	RunTimeout        time.Duration
	RunTimeoutAt      *time.Time

	RetryCount int

	CreatedAt time.Time
	CreatedBy int64
}

// Refresh the run state
func (r *RunState) Refresh() error {

	model := &models.Run{}
	res := r.db.First(model, r.model.ID)
	if res.Error != nil {
		return res.Error
	}

	r.model = model

	r.ID = r.model.ID
	r.JobID = r.model.JobID
	r.JobName = r.model.JobName
	r.Deduplicate = r.model.Deduplicate
	r.Scheduled = r.model.Scheduled
	r.Args = r.model.Args
	r.RunAt = r.model.RunAt
	r.RunStartedBy = models.NullToNilInt64(r.model.RunStartedBy)
	r.RunStartedAt = models.NullToNilTime(r.model.RunStartedAt)
	r.RunCompletedAt = models.NullToNilTime(r.model.RunCompletedAt)
	r.RunCompletedError = r.model.RunCompletedError
	r.RunTimeout = r.model.RunTimeout
	r.RunTimeoutAt = models.NullToNilTime(r.model.RunTimeoutAt)
	r.RetryCount = r.model.RetryCount

	r.CreatedAt = r.model.CreatedAt
	r.CreatedBy = r.model.CreatedBy

	return nil
}

// Started returns true if the job run has started
func (r *RunState) Started() bool {
	return r.RunStartedAt != nil
}

// Completed returns true if the job run is complete
func (r *RunState) Completed() bool {
	return r.RunCompletedAt != nil
}

func modelRunToRunState(model models.Run, db *gorm.DB) RunState {
	return RunState{
		db:                db,
		model:             &model,
		ID:                model.ID,
		JobID:             model.JobID,
		JobName:           model.JobName,
		Deduplicate:       model.Deduplicate,
		Scheduled:         model.Scheduled,
		Args:              model.Args,
		RunAt:             model.RunAt,
		RunStartedBy:      models.NullToNilInt64(model.RunStartedBy),
		RunStartedAt:      models.NullToNilTime(model.RunStartedAt),
		RunCompletedAt:    models.NullToNilTime(model.RunCompletedAt),
		RunCompletedError: model.RunCompletedError,
		RunTimeout:        model.RunTimeout,
		RunTimeoutAt:      models.NullToNilTime(model.RunTimeoutAt),
		RetryCount:        model.RetryCount,
		CreatedAt:         model.CreatedAt,
		CreatedBy:         model.CreatedBy,
	}
}
