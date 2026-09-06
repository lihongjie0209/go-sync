package config

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestSourceFingerprintCompatibility(t *testing.T) {
	t.Parallel()
	c := Defaults()
	c.SourceID, c.Slot = "test", "slot"
	c.Tables = []Table{{Schema: "public", Name: "items"}}
	old := sha256.Sum256([]byte(`{"Source":"test","Slot":"slot","Tables":[{"schema":"public","name":"items"}]}`))
	want := hex.EncodeToString(old[:])
	if c.Fingerprint() != want {
		t.Fatal("legacy PostgreSQL fingerprint changed")
	}
	c.SourceType = "postgres"
	if c.Fingerprint() != want {
		t.Fatal("explicit PostgreSQL changed scope")
	}
	c.SourceType = "sqlserver"
	if c.Fingerprint() == want {
		t.Fatal("source engines share state")
	}
	want = c.Fingerprint()
	c.SQLServer.AllowSnapshotLocks = true
	c.SQLServer.PollInterval = "2s"
	if c.Fingerprint() != want {
		t.Fatal("operational settings changed capture scope")
	}
}

func TestSQLServerConfiguration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "unknown source", mutate: func(c *Config) { c.SourceType = "mssql" }},
		{name: "no fence", mutate: func(c *Config) { c.SQLServer.FenceTable = Table{} }},
		{name: "fence in scope", mutate: func(c *Config) { c.SQLServer.FenceTable = c.Tables[0] }},
		{name: "zero interval", mutate: func(c *Config) { c.SQLServer.PollInterval = "0s" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := Defaults()
			c.SourceType, c.SourceID, c.DSN, c.URL = "sqlserver", "test", "sqlserver://localhost?database=test", "http://localhost/cdc"
			c.Tables = []Table{{Schema: "dbo", Name: "items"}}
			c.SQLServer.FenceTable = Table{Schema: "dbo", Name: "fence"}
			if err := c.Validate(); err != nil {
				t.Fatal(err)
			}
			if c.SQLServer.AllowSnapshotLocks {
				t.Fatal("locking snapshots enabled by default")
			}
			tc.mutate(&c)
			if err := c.Validate(); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}
