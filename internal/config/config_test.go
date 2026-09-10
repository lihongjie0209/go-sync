package config

import (
	"testing"
)

func TestConfigurationValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
	}{
		{"unsafe slot", func(c *Config) { c.Slot = "x'; DROP TABLE t" }},
		{"missing source", func(c *Config) { c.SourceID = "" }},
		{"no tables", func(c *Config) { c.Tables = nil }},
		{"invalid url", func(c *Config) { c.URL = "file:///tmp/data" }},
		{"duplicate table", func(c *Config) { c.Tables = append(c.Tables, c.Tables[0]) }},
		{"invalid limits", func(c *Config) { c.QueueBytes = 1 }},
		{"invalid timeout", func(c *Config) { c.HTTPTimeout = "0s" }},
		{"header injection", func(c *Config) { c.Headers = map[string]string{"X-Test": "value\r\nOther: value"} }},
		{"metrics missing port", func(c *Config) { c.MetricsAddr = "localhost" }},
		{"metrics invalid port", func(c *Config) { c.MetricsAddr = "localhost:65536" }},
		{"metrics named port", func(c *Config) { c.MetricsAddr = "localhost:http" }},
		{"metrics ephemeral port", func(c *Config) { c.MetricsAddr = "localhost:0" }},
		{"plugin path nul", func(c *Config) { c.PostgresBinDir = "invalid\x00path" }},
		{"log path nul", func(c *Config) { c.Log.File = "invalid\x00path" }},
		{"log level", func(c *Config) { c.Log.Level = "verbose" }},
		{"log size", func(c *Config) { c.Log.MaxSizeMB = 0 }},
		{"log backups", func(c *Config) { c.Log.MaxBackups = -1 }},
		{"log age", func(c *Config) { c.Log.MaxAgeDays = -1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := Defaults()
			c.SourceID = "test"
			c.DSN = "postgres://localhost/postgres"
			c.Slot = "test"
			c.URL = "https://example.invalid/cdc"
			c.Tables = []Table{{Schema: "public", Name: "items"}}
			if e := c.Validate(); e != nil {
				t.Fatal(e)
			}
			tc.mutate(&c)
			if e := c.Validate(); e == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}

func TestLogSettingsDoNotChangeCaptureScope(t *testing.T) {
	t.Parallel()
	c := Defaults()
	fingerprint := c.Fingerprint()
	c.Log = Log{File: `D:\logs\go-sync.log`, Level: "debug", MaxSizeMB: 10, MaxBackups: 2, MaxAgeDays: 1}
	if c.Fingerprint() != fingerprint {
		t.Fatal("log settings changed capture scope")
	}
}

func TestPluginSettingsDoNotChangeCaptureScope(t *testing.T) {
	t.Parallel()
	c := Defaults()
	if !c.Wal2JSONAutoInstall || c.PostgresBinDir != "" {
		t.Fatal("unexpected plugin defaults")
	}
	fingerprint := c.Fingerprint()
	c.Wal2JSONAutoInstall = false
	c.PostgresBinDir = `D:\pms\PostgreSQL\bin`
	if c.Fingerprint() != fingerprint {
		t.Fatal("plugin settings changed capture scope")
	}
}

func TestMetricsAddressDoesNotChangeCaptureScope(t *testing.T) {
	for _, addr := range []string{"", "127.0.0.1:9090", "[::1]:9090", ":9090"} {
		t.Run(addr, func(t *testing.T) {
			c := Defaults()
			c.SourceID, c.Slot, c.DSN, c.URL = "test", "test", "postgres://localhost/postgres", "http://localhost/cdc"
			c.Tables = []Table{{Schema: "public", Name: "items"}}
			fingerprint := c.Fingerprint()
			c.MetricsAddr = addr
			if err := c.Validate(); err != nil {
				t.Fatal(err)
			}
			if c.Fingerprint() != fingerprint {
				t.Fatal("metrics changed capture scope")
			}
		})
	}
}
