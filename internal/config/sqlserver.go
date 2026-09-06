package config

import (
	"errors"
	"strings"
	"time"
	"unicode/utf16"
)

// SQLServer controls CDC polling and the explicitly opted-in locking snapshot.
// FenceTable must be a dedicated, DBA-provisioned CDC table, outside Tables.
type SQLServer struct {
	AllowSnapshotLocks bool   `json:"allow_snapshot_locks"`
	FenceTable         Table  `json:"fence_table"`
	PollInterval       string `json:"poll_interval"`
	QueryTimeout       string `json:"query_timeout"`
	SnapshotTimeout    string `json:"snapshot_timeout"`
	FenceTimeout       string `json:"fence_timeout"`
}

func (c SQLServer) validate(tables []Table) error {
	for _, value := range []string{c.PollInterval, c.QueryTimeout, c.SnapshotTimeout, c.FenceTimeout} {
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			return errors.New("sqlserver durations must be positive")
		}
	}
	for _, table := range append(append([]Table{}, tables...), c.FenceTable) {
		for _, name := range []string{table.Schema, table.Name} {
			if name == "" || strings.ContainsRune(name, 0) || len(utf16.Encode([]rune(name))) > 128 {
				return errors.New("sqlserver table and fence names must contain 1–128 utf16 code units and no nul")
			}
		}
	}
	for _, table := range tables {
		if table == c.FenceTable {
			return errors.New("sqlserver fence_table must not be a captured business table")
		}
	}
	return nil
}
