package logc

import (
	"context"
	"errors"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/utils"
)

// GormLogger .
type GormLogger struct {
	log                   Logger
	slowThreshold         time.Duration
	sourceField           string
	skipErrRecordNotFound bool
	silent                bool
}

// LogMode .
func (l *GormLogger) LogMode(logger.LogLevel) logger.Interface {
	return l
}

// Info .
func (l *GormLogger) Info(ctx context.Context, s string, args ...interface{}) {
	newArgs := append([]interface{}{}, s)
	newArgs = append(newArgs, args...)
	l.log.Info(newArgs...)
}

// Warn .
func (l *GormLogger) Warn(ctx context.Context, s string, args ...interface{}) {
	newArgs := append([]interface{}{}, s)
	newArgs = append(newArgs, args...)
	l.log.Warn(newArgs...)
}

// Error .
func (l *GormLogger) Error(ctx context.Context, s string, args ...interface{}) {
	newArgs := append([]interface{}{}, s)
	newArgs = append(newArgs, args...)
	l.log.Error(newArgs...)
}

// Trace .
func (l *GormLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if !l.silent {
		elapsed := time.Since(begin)
		sql, _ := fc()
		fields := logrus.Fields{}
		if l.sourceField != "" {
			fields[l.sourceField] = utils.FileWithLineNum()
		}
		if err != nil && !(errors.Is(err, gorm.ErrRecordNotFound) && l.skipErrRecordNotFound) {
			fields[logrus.ErrorKey] = err
			l.log.WithFields(fields).Error("%s [%s]", sql, elapsed)
			return
		}

		if l.slowThreshold != 0 && elapsed > l.slowThreshold {
			l.log.WithFields(fields).Warn("%s [%s]", sql, elapsed)
			return
		}

		l.log.WithFields(fields).Debug("%s [%s]", sql, elapsed)
	}
}

// NewGormLogger .
func NewGormLogger(logger Logger) *GormLogger {
	return &GormLogger{
		log:           logger,
		slowThreshold: time.Second * time.Duration(1),
		sourceField:   "src",
		silent:        false,
	}
}
