package config

import (
	"errors"
	"regexp"
	"time"
)

// SQLServerLegacy controls the trigger-backed SQL Server 2000 collector.
type SQLServerLegacy struct {
	AutoInstall        bool   `json:"auto_install"`
	AllowSnapshotLocks bool   `json:"allow_snapshot_locks"`
	Owner              string `json:"owner"`
	Prefix             string `json:"prefix"`
	PollInterval       string `json:"poll_interval"`
	QueryTimeout       string `json:"query_timeout"`
	SnapshotTimeout    string `json:"snapshot_timeout"`
}

var legacyIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

func (c SQLServerLegacy) validate(tables []Table) error {
	if !legacyIdentifier.MatchString(c.Owner) || !legacyIdentifier.MatchString(c.Prefix) {
		return errors.New("sqlserver_legacy owner and prefix must be simple identifiers of at most 64 characters")
	}
	for _, value := range []string{c.PollInterval, c.QueryTimeout, c.SnapshotTimeout} {
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			return errors.New("sqlserver_legacy durations must be positive")
		}
	}
	for _, table := range tables {
		if !legacyIdentifier.MatchString(table.Schema) || !legacyIdentifier.MatchString(table.Name) {
			return errors.New("sqlserver_legacy table names must be simple identifiers of at most 64 characters")
		}
	}
	return nil
}
