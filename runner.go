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
		return
	}
	// TODO timeout here
	r.logger.Trace("run activated")
	helper := RunHelper{
		args:   r.modelRun.Args,
		cancel: r.stop,
	}
	if err := r.spec.run(helper); err != nil {
		r.errorOut(err)
	} else {
		r.complete(nil)
	}
	r.logger.Trace("run completed")
}

func (r *runner) buildActive(instanceID int64, startedAt time.Time) *models.Active {
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
	return &models.Active{
		RunID:     r.modelRun.ID,
		JobID:     r.modelRun.JobID,
		JobName:   r.modelRun.JobName,
		UniqueID:  uniqueID,
		Args:      r.modelRun.Args,
		StartedAt: startedAt,
		TimeoutAt: timeoutAt,
		CreatedBy: instanceID,
	}
}

func (r *runner) activate(instanceID int64) (bool, error) {
	startedAt := time.Now()
	r.modelActive = r.buildActive(instanceID, startedAt)
	rtn := false
	err := r.db.Transaction(func(tx *gorm.DB) error {
		res0 := tx.Model(r.modelRun).Where("run_started_by IS NULL").Updates(map[string]interface{}{
			"run_started_by": instanceID,
			"run_started_at": startedAt,
		})
		if res0.Error != nil {
			return res0.Error
		} else if res0.RowsAffected != 1 {
			return nil
		}
		res1 := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(r.modelActive)
		if res1.Error != nil {
			return res1.Error
		} else if res1.RowsAffected == 1 {
			rtn = true
		} else {
			r.logger.Trace("run deduplicated")
			res2 := tx.Model(r.modelRun).Updates(map[string]interface{}{
				"run_deduplicated": true,
				"run_completed_at": startedAt,
			})
			if res2.Error != nil {
				return res2.Error
			}
		}
		return nil
	})
	return rtn, err
}

func (r *runner) timeOut() {
	//TODO
}

func (r *runner) errorOut(err error) {
	//TODO
}

func (r *runner) complete(runErr error) (err error) {
	for i := 0; i < 3; i++ {
		err = r.completeAttempt(runErr)
		if err != nil {
			r.logger.WithError(err).WithField("attempt", i).Error("cannot complete job run")
		} else {
			return
		}
	}
	return
}

func (r *runner) completeAttempt(runErr error) error {
	r.modelRun.RunCompletedAt = sql.NullTime{Valid: true, Time: time.Now()}
	if runErr != nil {
		r.modelRun.RunCompletedError = runErr.Error()
	}
	err := r.db.Transaction(func(tx *gorm.DB) error {
		res0 := tx.Model(r.modelRun).Where("run_completed_at IS NULL").Updates(map[string]interface{}{
			"run_completed_at":    r.modelRun.RunCompletedAt,
			"run_completed_error": r.modelRun.RunCompletedError,
		})
		if res0.Error != nil {
			return res0.Error
		} else if res0.RowsAffected != 1 {
			r.logger.Warn("job run already marked as completed")
		}
		res1 := tx.Delete(r.modelActive)
		if res1.Error != nil {
			return res1.Error
		}
		return nil
	})
	return err
}

func (r *runner) runState() RunState {
	return modelToRunState(*r.modelRun, r.db)
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

	r.ID = model.ID
	r.JobID = model.JobID
	r.JobName = model.JobName
	r.Deduplicate = model.Deduplicate
	r.Scheduled = model.Scheduled
	r.Args = model.Args
	r.RunAt = model.RunAt
	r.RunStartedBy = models.NullToNilInt64(r.model.RunStartedBy)
	r.RunStartedAt = models.NullToNilTime(r.model.RunStartedAt)
	r.RunCompletedAt = models.NullToNilTime(r.model.RunCompletedAt)
	r.RunCompletedError = model.RunCompletedError
	r.RunTimeout = model.RunTimeout
	r.RunTimeoutAt = models.NullToNilTime(r.model.RunTimeoutAt)

	r.CreatedAt = model.CreatedAt
	r.CreatedBy = model.CreatedBy

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

func modelToRunState(model models.Run, db *gorm.DB) RunState {
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
		CreatedAt:         model.CreatedAt,
		CreatedBy:         model.CreatedBy,
	}
}

func getRunStates(db *gorm.DB, jobID int64, limit int) (rtn []*RunState, err error) {
	runs := []models.Run{}
	res := db.Where("job_id = ?", jobID).Order("created_at DESC").Limit(limit).Find(&runs)
	if res.Error != nil {
		return rtn, res.Error
	}
	for _, run := range runs {
		runState := modelToRunState(run, db)
		rtn = append(rtn, &runState)
	}
	return rtn, nil
}
