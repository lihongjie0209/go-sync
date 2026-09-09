package config

import (
	"errors"
	"regexp"
	"time"
)

// MySQL controls MySQL 5.6/5.7 snapshot and row-binlog capture.
type MySQL struct {
	ServerID          uint32 `json:"server_id"`
	AllowSnapshotLock bool   `json:"allow_snapshot_lock"`
	ConnectTimeout    string `json:"connect_timeout"`
	SnapshotTimeout   string `json:"snapshot_timeout"`
}

var mysqlIdentifier = regexp.MustCompile(`^[A-Za-z0-9_$]{1,64}$`)

func (c MySQL) validate(tables []Table) error {
	if c.ServerID == 0 {
		return errors.New("mysql.server_id must be nonzero and unique among replication clients")
	}
	for _, value := range []string{c.ConnectTimeout, c.SnapshotTimeout} {
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			return errors.New("mysql durations must be positive")
		}
	}
	for _, table := range tables {
		if !mysqlIdentifier.MatchString(table.Schema) || !mysqlIdentifier.MatchString(table.Name) {
			return errors.New("mysql table names must contain 1-64 letters, digits, underscores or dollar signs")
		}
	}
	return nil
}
