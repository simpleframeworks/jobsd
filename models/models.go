package models

import (
	"database/sql"
	"time"
)

// NullToNilString .
func NullToNilString(str sql.NullString) *string {
	if str.Valid {
		tmp := str.String
		return &tmp
	}
	return nil
}

// NullToNilInt64 .
func NullToNilInt64(str sql.NullInt64) *int64 {
	if str.Valid {
		tmp := str.Int64
		return &tmp
	}
	return nil
}

// NullToNilTime .
func NullToNilTime(str sql.NullTime) *time.Time {
	if str.Valid {
		tmp := str.Time
		return &tmp
	}
	return nil
}
