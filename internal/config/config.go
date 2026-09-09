package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

type Table struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
}

// FileColumn turns a path stored in one selected column into an embedded file.
type FileColumn struct {
	Schema   string `json:"schema"`
	Table    string `json:"table"`
	Column   string `json:"column"`
	RootDir  string `json:"root_dir"`
	MaxBytes int64  `json:"max_bytes"`
}

func (t Table) String() string { return t.Schema + "." + t.Name }

type Config struct {
	SourceType          string            `json:"source_type,omitempty"`
	SQLServer           SQLServer         `json:"sqlserver,omitempty"`
	SQLServerLegacy     SQLServerLegacy   `json:"sqlserver_legacy,omitempty"`
	MySQL               MySQL             `json:"mysql,omitempty"`
	SourceID            string            `json:"source_id"`
	DSN                 string            `json:"dsn"`
	Slot                string            `json:"slot"`
	Tables              []Table           `json:"tables"`
	FileColumns         []FileColumn      `json:"file_columns,omitempty"`
	DataDir             string            `json:"data_dir"`
	URL                 string            `json:"http_url"`
	Headers             map[string]string `json:"http_headers"`
	QueueBytes          int64             `json:"queue_bytes"`
	ReserveBytes        uint64            `json:"reserve_bytes"`
	BatchRows           int               `json:"batch_rows"`
	BatchBytes          int               `json:"batch_bytes"`
	MaxRowBytes         int               `json:"max_row_bytes"`
	HTTPTimeout         string            `json:"http_timeout"`
	RetryMin            string            `json:"retry_min"`
	RetryMax            string            `json:"retry_max"`
	MetricsAddr         string            `json:"metrics_addr"`
	Wal2JSONAutoInstall bool              `json:"wal2json_auto_install"`
	PostgresBinDir      string            `json:"postgres_bin_dir"`
}

func Defaults() Config {
	return Config{DataDir: "data", Wal2JSONAutoInstall: true, QueueBytes: 10 << 30, ReserveBytes: 256 << 20,
		SQLServer:       SQLServer{PollInterval: "1s", QueryTimeout: "5m", SnapshotTimeout: "1h", FenceTimeout: "2m"},
		SQLServerLegacy: SQLServerLegacy{AutoInstall: true, Owner: "dbo", Prefix: "go_sync_legacy", PollInterval: "1s", QueryTimeout: "5m", SnapshotTimeout: "1h"},
		MySQL:           MySQL{ServerID: 100001, ConnectTimeout: "30s", SnapshotTimeout: "1h"},
		BatchRows:       500, BatchBytes: 1 << 20, MaxRowBytes: 16 << 20,
		HTTPTimeout: "30s", RetryMin: "1s", RetryMax: "60s"}
}

func Load(path string) (Config, error) {
	c := Defaults()
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, fmt.Errorf("config: %w", err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return c, errors.New("config: trailing json")
	}
	// Expand only secrets, not arbitrary configuration or paths.
	c.DSN = os.ExpandEnv(c.DSN)
	for k, v := range c.Headers {
		c.Headers[k] = os.ExpandEnv(v)
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	if c.Engine() != "postgres" && c.Engine() != "sqlserver" && c.Engine() != "sqlserver_legacy" && c.Engine() != "mysql" {
		return errors.New("source_type must be postgres, sqlserver, sqlserver_legacy or mysql")
	}
	if c.Engine() == "sqlserver" {
		if err := c.SQLServer.validate(c.Tables); err != nil {
			return err
		}
	}
	if c.Engine() == "sqlserver_legacy" {
		if err := c.SQLServerLegacy.validate(c.Tables); err != nil {
			return err
		}
	}
	if c.Engine() == "mysql" {
		if err := c.MySQL.validate(c.Tables); err != nil {
			return err
		}
	}
	if strings.ContainsRune(c.PostgresBinDir, 0) {
		return errors.New("postgres_bin_dir must not contain nul")
	}
	if c.MetricsAddr != "" {
		_, port, err := net.SplitHostPort(c.MetricsAddr)
		n, parseErr := strconv.Atoi(port)
		if err != nil || parseErr != nil || n < 1 || n > 65535 {
			return errors.New("metrics_addr must be host:port with a numeric port between 1 and 65535")
		}
	}
	if c.SourceID == "" || c.DSN == "" || c.DataDir == "" {
		return errors.New("source_id, dsn and data_dir are required")
	}
	if !regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`).MatchString(c.SourceID) {
		return errors.New("source_id must contain 1–128 ascii letters, digits, dots, underscores or hyphens")
	}
	if c.Engine() == "postgres" && !regexp.MustCompile(`^[a-z0-9_]{1,63}$`).MatchString(c.Slot) {
		return errors.New("slot must contain 1–63 lowercase letters, digits or underscores")
	}
	u, err := url.Parse(c.URL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return errors.New("http_url must be an http(s) url without credentials")
	}
	if len(c.Tables) == 0 {
		return errors.New("tables must not be empty")
	}
	seen := map[Table]bool{}
	for _, t := range c.Tables {
		if t.Schema == "" || t.Name == "" || strings.ContainsRune(t.Schema+t.Name, 0) || seen[t] {
			return errors.New("table names must be nonempty, unique and contain no nul")
		}
		seen[t] = true
	}
	if c.BatchRows < 1 || c.BatchBytes < 1024 || c.MaxRowBytes < c.BatchBytes || c.QueueBytes < int64(c.MaxRowBytes)*2 {
		return errors.New("invalid batch or queue limits")
	}
	fileRules := map[[3]string]bool{}
	for _, rule := range c.FileColumns {
		key := [3]string{rule.Schema, rule.Table, rule.Column}
		if rule.Schema == "" || rule.Table == "" || rule.Column == "" || rule.RootDir == "" || strings.ContainsRune(rule.Schema+rule.Table+rule.Column+rule.RootDir, 0) {
			return errors.New("file_columns require schema, table, column and root_dir without nul")
		}
		if !filepath.IsAbs(rule.RootDir) {
			return errors.New("file_columns root_dir must be an absolute path")
		}
		if rule.MaxBytes < 1 || rule.MaxBytes > int64(c.MaxRowBytes) {
			return errors.New("file_columns max_bytes must be positive and no larger than max_row_bytes")
		}
		if !seen[Table{Schema: rule.Schema, Name: rule.Table}] {
			return errors.New("file_columns must reference a selected table")
		}
		if fileRules[key] {
			return errors.New("file_columns must be unique")
		}
		fileRules[key] = true
	}
	for _, v := range []string{c.HTTPTimeout, c.RetryMin, c.RetryMax} {
		d, e := time.ParseDuration(v)
		if e != nil || d <= 0 {
			return errors.New("timeouts must be positive durations")
		}
	}
	lo, _ := time.ParseDuration(c.RetryMin)
	hi, _ := time.ParseDuration(c.RetryMax)
	if lo > hi {
		return errors.New("retry_min exceeds retry_max")
	}
	for k, v := range c.Headers {
		if strings.ContainsAny(k+v, "\r\n") || k == "" {
			return errors.New("invalid http header")
		}
	}
	return nil
}

// Fingerprint prevents replaying persisted state with a different capture scope.
func (c Config) Fingerprint() string {
	tables := slices.Clone(c.Tables)
	slices.SortFunc(tables, func(a, b Table) int { return strings.Compare(a.String(), b.String()) })
	b, _ := json.Marshal(struct {
		Source, Slot string
		Tables       []Table
	}{c.SourceID, c.Slot, tables})
	// Keep the historical PostgreSQL fingerprint byte-for-byte compatible.
	if c.Engine() == "sqlserver" || c.Engine() == "sqlserver_legacy" || c.Engine() == "mysql" {
		b, _ = json.Marshal(struct {
			Engine string
			Source string
			Tables []Table
		}{Engine: c.Engine(), Source: c.SourceID, Tables: tables})
	}
	if c.Engine() == "mysql" {
		b, _ = json.Marshal(struct {
			Engine   string
			Source   string
			ServerID uint32
			Tables   []Table
		}{Engine: c.Engine(), Source: c.SourceID, ServerID: c.MySQL.ServerID, Tables: tables})
	}
	if len(c.FileColumns) != 0 {
		fileColumns := slices.Clone(c.FileColumns)
		slices.SortFunc(fileColumns, func(a, b FileColumn) int {
			return strings.Compare(a.Schema+"\x00"+a.Table+"\x00"+a.Column, b.Schema+"\x00"+b.Table+"\x00"+b.Column)
		})
		b, _ = json.Marshal(struct {
			Engine      string
			Source      string
			Slot        string
			Tables      []Table
			FileColumns []FileColumn
		}{c.Engine(), c.SourceID, c.Slot, tables, fileColumns})
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// Engine treats legacy configurations without source_type as PostgreSQL.
func (c Config) Engine() string {
	if c.SourceType == "" {
		return "postgres"
	}
	return c.SourceType
}
