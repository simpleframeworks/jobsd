package jobspec

import (
	"time"

	"github.com/simpleframeworks/jobspec/models"
	"github.com/simpleframeworks/logc"
	"gorm.io/gorm"
)

type schedule struct {
	db     *gorm.DB
	logger logc.Logger
	model  *models.Schedule
	spec   *spec
	stop   chan struct{}

	queueRun func(s spec, args []interface{}, id *int64, tx *gorm.DB) (RunState, error)
}

func (s *schedule) QueueID() int64 {
	return s.model.ID
}

func (s *schedule) QueueTime() time.Time {
	return s.model.ScheduleAt.Add(-10 * time.Second)
}

func (s *schedule) exec(instanceID int64) {
	s.db.Transaction(func(tx *gorm.DB) error {
		advanced, err0 := s.advance(tx, instanceID)
		if err0 != nil {
			s.logger.WithError(err0).Error("cannot lock to schedule")
			return err0
		} else if !advanced {
			s.logger.Debug("schedule already run skipping")
		} else {
			_, err1 := s.queueRun(*s.spec, s.model.Args, nil, tx)
			if err1 != nil {
				s.logger.WithError(err1).Error("failed to schedule run")
			}
		}
		return nil
	})
	s.db.First(s.model, s.model.ID)
}

func (s *schedule) advance(tx *gorm.DB, instanceID int64) (bool, error) {
	now := time.Now()
	scheduleNext := s.spec.scheduleFunc(now)
	res := tx.Model(s.model).Where("schedule_count = ?", s.model.ScheduleCount).
		Updates(map[string]interface{}{
			"schedule_at":       scheduleNext,
			"schedule_count":    s.model.ScheduleCount + 1,
			"last_scheduled_at": now,
			"last_scheduled_by": instanceID,
		})
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected == 1, res.Error
}
