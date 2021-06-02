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

	queueRun func(runAt time.Time, s spec, args []interface{}, id *int64, tx *gorm.DB) (RunState, error)
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

func (s *scheduler) setTimings(now time.Time) {
	s.model.NextRunAt = s.spec.scheduleFunc(time.Now())
	s.model.ScheduleAt = s.model.NextRunAt.Add(time.Second * -10)
}

func (s *scheduler) schedule(tx *gorm.DB, instanceID int64) (bool, error) {
	now := time.Now()
	s.setTimings(now)
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
