package jobspec

import (
	"time"

	"github.com/simpleframeworks/jobspec/models"
	"github.com/simpleframeworks/logc"
	"gorm.io/gorm"
)

type scheduler struct {
	db     *gorm.DB
	logger logc.Logger
	model  *models.Schedule
	spec   *spec
	stop   chan struct{}

	queueRun func(runAt time.Time, s spec, args []interface{}, id *int64, tx *gorm.DB) (ScheduleState, error)
}

func (s *scheduler) TimeQID() int64 {
	return s.model.ID
}

func (s *scheduler) TimeQTime() time.Time {
	return s.model.ScheduleAt
}

func (s *scheduler) exec(instanceID int64) {
	s.db.Transaction(func(tx *gorm.DB) error {
		scheduled, err0 := s.schedule(tx, instanceID)
		if err0 != nil {
			s.logger.WithError(err0).Error("cannot schedule")
			return err0
		} else if !scheduled {
			s.logger.Debug("scheduled already, skipping")
		} else {
			_, err1 := s.queueRun(s.model.NextRunAt, *s.spec, s.model.Args, nil, tx)
			if err1 != nil {
				s.logger.WithError(err1).Error("failed to schedule job")
			}
		}
		return nil
	})
	s.db.First(s.model, s.model.ID)
}

func (s *scheduler) schedule(tx *gorm.DB, instanceID int64) (bool, error) {
	now := time.Now()
	s.model.NextRunAt = s.spec.runAt(now)
	s.model.ScheduleAt = s.model.NextRunAt.Add(-ScheduleTimingOffset)
	res := tx.Model(s.model).Where("schedule_count = ?", s.model.ScheduleCount).
		Updates(map[string]interface{}{
			"schedule_at":       s.model.ScheduleAt,
			"next_run_at":       s.model.NextRunAt,
			"schedule_count":    s.model.ScheduleCount + 1,
			"last_scheduled_at": now,
			"last_scheduled_by": instanceID,
		})
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected == 1, res.Error
}

func (s *scheduler) scheduleState() ScheduleState {
	return modelToScheduleState(*s.model, s.db)
}

// ScheduleState .
type ScheduleState struct {
	db    *gorm.DB
	model *models.Schedule

	ID         int64
	JobID      int64
	Args       models.RunArgs
	ScheduleAt time.Time
	NextRunAt  time.Time

	ScheduleCount int
	ScheduleLimit int

	LastScheduledAt *time.Time
	LastScheduledBy *int64

	CreatedAt time.Time
	CreatedBy int64
}

// Refresh the schedule state
func (s *ScheduleState) Refresh() error {

	model := &models.Schedule{}
	res := s.db.First(model, s.model.ID)
	if res.Error != nil {
		return res.Error
	}

	s.model = model

	s.ID = model.ID
	s.JobID = model.JobID
	s.Args = model.Args
	s.ScheduleAt = model.ScheduleAt
	s.NextRunAt = model.NextRunAt

	s.ScheduleCount = model.ScheduleCount
	s.ScheduleLimit = model.ScheduleLimit

	s.LastScheduledAt = models.NullToNilTime(model.LastScheduledAt)
	s.LastScheduledBy = models.NullToNilInt64(model.LastScheduledBy)

	s.CreatedAt = model.CreatedAt
	s.CreatedBy = model.CreatedBy

	return nil
}

// LatestRun gets the latest scheduled job run state
func (s *ScheduleState) LatestRun() (*RunState, error) {
	runStates, err := getRunStates(s.db, s.JobID, 1)
	if err != nil {
		return nil, err
	}
	if len(runStates) > 0 {
		return runStates[0], nil
	}
	return nil, nil
}

// LatestRuns gets the n latest scheduled job run states
func (s *ScheduleState) LatestRuns(n int) ([]*RunState, error) {
	runStates, err := getRunStates(s.db, s.JobID, n)
	if err != nil {
		return nil, err
	}
	return runStates, nil
}

func modelToScheduleState(model models.Schedule, db *gorm.DB) ScheduleState {
	return ScheduleState{
		db:    db,
		model: &model,

		ID:         model.ID,
		JobID:      model.JobID,
		Args:       model.Args,
		ScheduleAt: model.ScheduleAt,
		NextRunAt:  model.NextRunAt,

		ScheduleCount: model.ScheduleCount,
		ScheduleLimit: model.ScheduleLimit,

		LastScheduledAt: models.NullToNilTime(model.LastScheduledAt),
		LastScheduledBy: models.NullToNilInt64(model.LastScheduledBy),

		CreatedAt: model.CreatedAt,
		CreatedBy: model.CreatedBy,
	}
}
