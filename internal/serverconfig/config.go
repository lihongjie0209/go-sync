package serverconfig

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
	_ "time/tzdata"

	"go-sync/internal/config"
)

type GRPC struct {
	ListenAddr      string `json:"listen_addr"`
	TLSCertFile     string `json:"tls_cert_file"`
	TLSKeyFile      string `json:"tls_key_file"`
	MaxMessageBytes int    `json:"max_message_bytes"`
}

type VictoriaMetrics struct {
	URL      string            `json:"url"`
	Interval string            `json:"interval,omitempty"`
	Timeout  string            `json:"timeout,omitempty"`
	Headers  map[string]string `json:"headers,omitempty"`
}

type Postgres struct {
	DSN            string `json:"dsn"`
	TargetSchema   string `json:"target_schema"`
	MetadataSchema string `json:"metadata_schema,omitempty"`
}

type Table struct {
	SourceSchema string `json:"source_schema"`
	Name         string `json:"name"`
}

type FileColumn struct {
	SourceSchema  string `json:"source_schema"`
	Table         string `json:"table"`
	SourceColumn  string `json:"source_column"`
	ContentColumn string `json:"content_column"`
}

type Syncer struct {
	ID          string       `json:"id"`
	Token       string       `json:"token"`
	Postgres    Postgres     `json:"postgres"`
	Tables      []Table      `json:"tables"`
	FileColumns []FileColumn `json:"file_columns,omitempty"`
}

type Reconcile struct {
	DailyAt    string `json:"daily_at,omitempty"`
	Timezone   string `json:"timezone,omitempty"`
	Buckets    int    `json:"buckets,omitempty"`
	AutoRepair bool   `json:"auto_repair"`
}

type Config struct {
	GRPC            GRPC            `json:"grpc"`
	AdminToken      string          `json:"admin_token"`
	MetricsAddr     string          `json:"metrics_addr,omitempty"`
	VictoriaMetrics VictoriaMetrics `json:"victoria_metrics"`
	Reconcile       Reconcile       `json:"reconcile,omitempty"`
	Log             config.Log      `json:"log,omitempty"`
	Syncers         []Syncer        `json:"syncers"`
}

func Defaults() Config {
	return Config{
		GRPC:            GRPC{ListenAddr: ":7443", MaxMessageBytes: 32 << 20},
		MetricsAddr:     "127.0.0.1:9100",
		VictoriaMetrics: VictoriaMetrics{Interval: "30s", Timeout: "15s"},
		Reconcile:       Reconcile{DailyAt: "02:00", Timezone: "Asia/Shanghai", Buckets: 256, AutoRepair: true},
		Log:             config.Log{Level: "info", MaxSizeMB: 100, MaxBackups: 10, MaxAgeDays: 30, Compress: true},
	}
}

func Load(path string) (Config, error) {
	c := Defaults()
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&c); err != nil {
		return c, fmt.Errorf("server config: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return c, errors.New("server config: trailing JSON")
	}
	c.AdminToken = os.ExpandEnv(c.AdminToken)
	for i := range c.Syncers {
		c.Syncers[i].Token = os.ExpandEnv(c.Syncers[i].Token)
		c.Syncers[i].Postgres.DSN = os.ExpandEnv(c.Syncers[i].Postgres.DSN)
	}
	for key, value := range c.VictoriaMetrics.Headers {
		c.VictoriaMetrics.Headers[key] = os.ExpandEnv(value)
	}
	return c, c.Validate()
}

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]{0,62}$`)
var syncerID = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)

func (c Config) Validate() error {
	if _, _, err := net.SplitHostPort(c.GRPC.ListenAddr); err != nil {
		return errors.New("grpc.listen_addr must be host:port")
	}
	if c.GRPC.MaxMessageBytes < 1<<20 {
		return errors.New("grpc.max_message_bytes must be at least 1 MiB")
	}
	if c.GRPC.TLSCertFile == "" || c.GRPC.TLSKeyFile == "" {
		return errors.New("grpc TLS certificate and key are required")
	}
	if _, err := tls.LoadX509KeyPair(c.GRPC.TLSCertFile, c.GRPC.TLSKeyFile); err != nil {
		return fmt.Errorf("load grpc TLS key pair: %w", err)
	}
	if strings.ContainsRune(c.GRPC.TLSCertFile+c.GRPC.TLSKeyFile, 0) {
		return errors.New("grpc TLS paths must not contain nul")
	}
	if c.AdminToken == "" {
		return errors.New("admin_token is required")
	}
	if strings.ContainsAny(c.AdminToken, "\r\n") || len(c.AdminToken) > 4096 {
		return errors.New("admin_token is invalid")
	}
	if c.MetricsAddr != "" {
		if _, _, err := net.SplitHostPort(c.MetricsAddr); err != nil {
			return errors.New("metrics_addr must be host:port")
		}
	}
	if c.VictoriaMetrics.URL == "" {
		return errors.New("victoria_metrics.url is required")
	}
	u, err := url.Parse(c.VictoriaMetrics.URL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return errors.New("victoria_metrics.url must be an http(s) URL without credentials")
	}
	for _, value := range []string{c.VictoriaMetrics.Interval, c.VictoriaMetrics.Timeout} {
		if duration, err := time.ParseDuration(value); err != nil || duration < time.Second {
			return errors.New("VictoriaMetrics interval and timeout must be at least 1s")
		}
	}
	for name, value := range c.VictoriaMetrics.Headers {
		if name == "" || strings.ContainsAny(name+value, "\r\n") {
			return errors.New("VictoriaMetrics headers are invalid")
		}
	}
	if _, err := time.Parse("15:04", c.Reconcile.DailyAt); err != nil {
		return errors.New("reconcile.daily_at must use HH:MM")
	}
	if _, err := time.LoadLocation(c.Reconcile.Timezone); err != nil {
		return errors.New("reconcile.timezone is invalid")
	}
	if c.Reconcile.Buckets < 1 || c.Reconcile.Buckets > 65536 {
		return errors.New("reconcile.buckets must be between 1 and 65536")
	}
	seenIDs := make(map[string]struct{}, len(c.Syncers))
	seenTargets := make(map[string]string)
	for i := range c.Syncers {
		s := &c.Syncers[i]
		if !syncerID.MatchString(s.ID) || s.Token == "" || s.Postgres.DSN == "" || strings.ContainsAny(s.Token, "\r\n") || len(s.Token) > 4096 {
			return errors.New("each syncer requires a valid id, token and postgres.dsn")
		}
		if _, ok := seenIDs[s.ID]; ok {
			return fmt.Errorf("duplicate syncer id %q", s.ID)
		}
		seenIDs[s.ID] = struct{}{}
		if s.Postgres.MetadataSchema == "" {
			s.Postgres.MetadataSchema = "go_sync_meta"
		}
		if !identifier.MatchString(s.Postgres.TargetSchema) || !identifier.MatchString(s.Postgres.MetadataSchema) {
			return fmt.Errorf("syncer %q has an invalid PostgreSQL schema", s.ID)
		}
		if len(s.Tables) == 0 {
			return fmt.Errorf("syncer %q has no tables", s.ID)
		}
		tables := make(map[string]struct{}, len(s.Tables))
		for _, table := range s.Tables {
			if !identifier.MatchString(table.SourceSchema) || !identifier.MatchString(table.Name) {
				return fmt.Errorf("syncer %q has an invalid table", s.ID)
			}
			if _, ok := tables[table.Name]; ok {
				return fmt.Errorf("syncer %q maps duplicate target table %q", s.ID, table.Name)
			}
			tables[table.Name] = struct{}{}
			target := s.Postgres.DSN + "\x00" + s.Postgres.TargetSchema + "\x00" + table.Name
			if other, ok := seenTargets[target]; ok {
				return fmt.Errorf("syncers %q and %q share a target table", other, s.ID)
			}
			seenTargets[target] = s.ID
		}
		for _, rule := range s.FileColumns {
			if !identifier.MatchString(rule.SourceSchema) || !identifier.MatchString(rule.Table) || !identifier.MatchString(rule.SourceColumn) || !identifier.MatchString(rule.ContentColumn) {
				return fmt.Errorf("syncer %q has an invalid file column mapping", s.ID)
			}
			if _, ok := tables[rule.Table]; !ok || rule.SourceColumn == rule.ContentColumn {
				return fmt.Errorf("syncer %q file mapping references an invalid table or column", s.ID)
			}
		}
	}
	return nil
}

func (c Config) Hash() string {
	b, _ := json.Marshal(c)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (c Config) StaticEqual(other Config) bool {
	return c.GRPC.ListenAddr == other.GRPC.ListenAddr && c.GRPC.MaxMessageBytes == other.GRPC.MaxMessageBytes &&
		c.MetricsAddr == other.MetricsAddr && c.Log.File == other.Log.File && c.Log.MaxSizeMB == other.Log.MaxSizeMB &&
		c.Log.MaxBackups == other.Log.MaxBackups && c.Log.MaxAgeDays == other.Log.MaxAgeDays && c.Log.Compress == other.Log.Compress
}

func (s Syncer) Allows(schema, table string) bool {
	for _, candidate := range s.Tables {
		if candidate.SourceSchema == schema && candidate.Name == table {
			return true
		}
	}
	return false
}

func (s Syncer) FileMapping(schema, table, column string) (string, bool) {
	for _, candidate := range s.FileColumns {
		if candidate.SourceSchema == schema && candidate.Table == table && candidate.SourceColumn == column {
			return candidate.ContentColumn, true
		}
	}
	return "", false
}

func SafeURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "invalid"
	}
	u.RawQuery = ""
	u.Fragment = ""
	return strings.TrimSuffix(u.String(), "/")
}
