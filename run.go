package jobspec

import (
	"database/sql"
	"time"

	"github.com/pkg/errors"
	"github.com/simpleframeworks/jobspec/models"
	"github.com/simpleframeworks/logc"
	"gorm.io/gorm"
)

type run struct {
	db     *gorm.DB
	logger logc.Logger
	model  *models.Run
	spec   *spec
	stop   chan struct{}
}

func (r *run) lock(instanceID int64) (bool, error) {
	startedAt := time.Now()
	runTimeoutAt := sql.NullTime{}
	if r.model.RunTimeout > 0 {
		runTimeoutAt.Valid = true
		runTimeoutAt.Time = startedAt.Add(time.Duration(r.model.RunTimeout))
	}
	tx := r.db.Model(r.model).Where("run_started_at IS NULL").Updates(map[string]interface{}{
		"run_timeout_at": runTimeoutAt,
		"run_started_at": startedAt,
		"run_started_by": instanceID,
	})
	if tx.Error != nil {
		return false, tx.Error
	}
	locked := tx.RowsAffected == 1
	if locked {
		r.model.RunTimeoutAt = runTimeoutAt
		r.model.RunStartedAt = sql.NullTime{Valid: true, Time: startedAt}
		r.model.RunBy = sql.NullInt64{Valid: true, Int64: instanceID}
	}
	return locked, tx.Error
}

func (r *run) exec() {
	// TODO timeout here
	helper := RunHelper{
		args:   r.model.Args,
		cancel: r.stop,
	}
	if err := r.spec.run(helper); err != nil {
		r.errorOut(err)
	} else {
		r.complete(nil)
	}
}

func (r *run) complete(runErr error) {
	r.model.UniqueRun = sql.NullString{}
	r.model.UniqueSchedule = sql.NullString{}
	r.model.RunCompletedAt = sql.NullTime{Valid: true, Time: time.Now()}
	if runErr != nil {
		r.model.RunCompletedError = runErr.Error()
	}

	tx := r.db.Model(r.model).Where("run_completed_at IS NULL").Updates(map[string]interface{}{
		"unique_run":          r.model.UniqueRun,
		"unique_schedule":     r.model.UniqueSchedule,
		"run_completed_at":    r.model.RunCompletedAt,
		"run_completed_error": r.model.RunCompletedError,
	})
	err := tx.Error
	if err != nil {
		r.logger.WithError(err).Error("db error. could not mark job run as completed.")
		return
	} else if tx.RowsAffected != 1 {
		r.logger.Error("could not mark job run as completed. already marked.")
		return
	}

	if r.model.Scheduled && r.model.CountReschedule < r.spec.limit {
		r.reschedule()
	}
}

func (r *run) reschedule() {
	//TODO
}
func (r *run) timeOut() {
	//TODO
}

func (r *run) errorOut(err error) {
	//TODO
}

func (r *run) runState() RunState {
	return modelRunToRunState(*r.model, r.db)
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
	db                *gorm.DB
	ID                int64
	JobID             int64
	JobName           string
	UniqueRun         *string
	UniqueSchedule    *string
	Scheduled         bool
	Args              models.RunArgs
	RunAt             time.Time
	RunBy             *int64
	RunStartedAt      *time.Time
	RunCompletedAt    *time.Time
	RunCompletedError string
	RunTimeout        time.Duration
	RunTimeoutAt      *time.Time
	CountRetry        int
	CountReschedule   int
	CreatedAt         time.Time
	CreatedBy         int64
}

// Refresh the run state
func (r *RunState) Refresh() error {
	// TODO
	return errors.New("not implemented")
}

// Started returns true if the job run has started
func (r *RunState) Started() bool {
	return r.RunStartedAt != nil
}

// Completed returns true if the job run is complete
func (r *RunState) Completed() bool {
	return r.RunCompletedAt != nil
}

func modelRunToRunState(run models.Run, db *gorm.DB) RunState {
	return RunState{
		db:                db,
		ID:                run.ID,
		JobID:             run.JobID,
		JobName:           run.JobName,
		UniqueRun:         models.NullToNilString(run.UniqueRun),
		UniqueSchedule:    models.NullToNilString(run.UniqueSchedule),
		Scheduled:         run.Scheduled,
		Args:              run.Args,
		RunAt:             run.RunAt,
		RunBy:             models.NullToNilInt64(run.RunBy),
		RunStartedAt:      models.NullToNilTime(run.RunStartedAt),
		RunCompletedAt:    models.NullToNilTime(run.RunCompletedAt),
		RunCompletedError: run.RunCompletedError,
		RunTimeout:        run.RunTimeout,
		RunTimeoutAt:      models.NullToNilTime(run.RunTimeoutAt),
		CountRetry:        run.CountRetry,
		CountReschedule:   run.CountReschedule,
		CreatedAt:         run.CreatedAt,
		CreatedBy:         run.CreatedBy,
	}
}
