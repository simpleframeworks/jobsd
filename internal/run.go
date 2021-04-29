package internal

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/gob"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm/schema"
)

// Run .
type Run struct {
	ID                int64 `gorm:"primaryKey"`
	Job               Job
	JobID             string
	JobName           string
	UniqueRun         sql.NullString `gorm:"unique"`
	UniqueSchedule    sql.NullString `gorm:"unique"`
	RunType           string
	Args              RunArgs
	RunAt             time.Time
	RunBy             sql.NullInt64
	RunStartedAt      sql.NullTime `gorm:"index"`
	RunCompletedAt    sql.NullTime `gorm:"index"`
	RunCompletedError sql.NullString
	RunTimeout        sql.NullInt64
	RunTimeoutAt      sql.NullTime `gorm:"index"`
	CreatedAt         time.Time    `gorm:"index"`
	CreatedBy         int64
}

// TableName specifies the db table name
func (Run) TableName() string {
	return "jobsd_runs"
}

// RunArgs holds job func parameters used to run a job. It can be serialized for DB storage
type RunArgs []interface{}

// GormDataType .
func (p RunArgs) GormDataType() string {
	return string(schema.Bytes)
}

// Scan scan value into []
func (p *RunArgs) Scan(value interface{}) error {
	data, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("failed to unmarshal params value:", value))
	}
	r := bytes.NewReader(data)
	dec := gob.NewDecoder(r)
	return dec.Decode(p)
}

// Value return params value, implement driver.Valuer interface
func (p RunArgs) Value() (driver.Value, error) {
	if len(p) == 0 {
		return nil, nil
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(p)
	return buf.Bytes(), nil
}
