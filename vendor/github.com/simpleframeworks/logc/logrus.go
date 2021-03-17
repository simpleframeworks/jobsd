package logc

import "github.com/sirupsen/logrus"

// Logrus compatible wrapper for the logger interface
type Logrus struct {
	entry *logrus.Entry
}

// Trace .
func (l *Logrus) Trace(args ...interface{}) {
	l.entry.Trace(args...)
}

// Debug .
func (l *Logrus) Debug(args ...interface{}) {
	l.entry.Debug(args...)
}

// Info .
func (l *Logrus) Info(args ...interface{}) {
	l.entry.Info(args...)
}

// Warn .
func (l *Logrus) Warn(args ...interface{}) {
	l.entry.Warn(args...)
}

// Error .
func (l *Logrus) Error(args ...interface{}) {
	l.entry.Error(args...)
}

// WithField .
func (l *Logrus) WithField(key string, value interface{}) Logger {
	return NewLogrusEntry(l.entry.WithField(key, value))
}

// WithFields .
func (l *Logrus) WithFields(fields map[string]interface{}) Logger {
	return NewLogrusEntry(l.entry.WithFields(logrus.Fields(fields)))
}

// WithError .
func (l *Logrus) WithError(err error) Logger {
	return NewLogrusEntry(l.entry.WithError(err))
}

// NewLogrus compatible logger
func NewLogrus(logger *logrus.Logger) Logger {
	return &Logrus{logrus.NewEntry(logger)}
}

// NewLogrusEntry compatible logger
func NewLogrusEntry(entry *logrus.Entry) Logger {
	return &Logrus{entry}
}
