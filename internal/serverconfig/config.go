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
	"path"
	"path/filepath"
	"regexp"
	"slices"
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

// FileStorage stores file-watch events. PostgreSQL remains the durable event
// checkpoint; the selected backend stores only committed file contents.
type FileStorage struct {
	Backend      string   `json:"backend,omitempty"`
	Directory    string   `json:"directory,omitempty"`
	Endpoint     string   `json:"endpoint,omitempty"`
	Region       string   `json:"region,omitempty"`
	Bucket       string   `json:"bucket,omitempty"`
	Prefix       string   `json:"prefix,omitempty"`
	AccessKey    string   `json:"access_key,omitempty"`
	SecretKey    string   `json:"secret_key,omitempty"`
	SessionToken string   `json:"session_token,omitempty"`
	Secure       bool     `json:"secure,omitempty"`
	Events       []string `json:"events,omitempty"`
	MaxFileBytes int64    `json:"max_file_bytes,omitempty"`
}

type Syncer struct {
	ID             string       `json:"id"`
	Token          string       `json:"token"`
	Postgres       Postgres     `json:"postgres"`
	Tables         []Table      `json:"tables"`
	FileColumns    []FileColumn `json:"file_columns,omitempty"`
	AutoAddColumns bool         `json:"auto_add_nullable_columns,omitempty"`
	FileStorage    FileStorage  `json:"file_storage,omitempty"`
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
		c.Syncers[i].FileStorage.AccessKey = os.ExpandEnv(c.Syncers[i].FileStorage.AccessKey)
		c.Syncers[i].FileStorage.SecretKey = os.ExpandEnv(c.Syncers[i].FileStorage.SecretKey)
		c.Syncers[i].FileStorage.SessionToken = os.ExpandEnv(c.Syncers[i].FileStorage.SessionToken)
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
	fileTargets := make(map[string]string)
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
		if len(s.Tables) == 0 && s.FileStorage.Backend == "" {
			return fmt.Errorf("syncer %q has no tables", s.ID)
		}
		if err := s.FileStorage.validate(s.ID); err != nil {
			return err
		}
		if s.FileStorage.Backend != "" {
			key := fileStorageKey(s.FileStorage)
			for existing, owner := range fileTargets {
				if storageKeysOverlap(existing, key) {
					return fmt.Errorf("syncers %q and %q have overlapping file storage", owner, s.ID)
				}
			}
			fileTargets[key] = s.ID
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

func fileStorageKey(storage FileStorage) string {
	if storage.Backend == "directory" {
		return "directory\x00" + filepath.Clean(filepath.Join(storage.Directory, filepath.FromSlash(storage.Prefix)))
	}
	return "object\x00" + strings.ToLower(storage.Endpoint) + "\x00" + storage.Bucket + "\x00" + path.Clean("/"+storage.Prefix)
}

func storageKeysOverlap(a, b string) bool {
	aParts, bParts := strings.Split(a, "\x00"), strings.Split(b, "\x00")
	if len(aParts) != len(bParts) || !slices.Equal(aParts[:len(aParts)-1], bParts[:len(bParts)-1]) {
		return false
	}
	if aParts[0] == "directory" {
		return filePathsOverlap(aParts[len(aParts)-1], bParts[len(bParts)-1])
	}
	aPrefix, bPrefix := strings.Trim(aParts[len(aParts)-1], "/"), strings.Trim(bParts[len(bParts)-1], "/")
	return aPrefix == bPrefix || strings.HasPrefix(aPrefix, bPrefix+"/") || strings.HasPrefix(bPrefix, aPrefix+"/")
}

func filePathsOverlap(a, b string) bool {
	contains := func(parent, child string) bool {
		rel, err := filepath.Rel(parent, child)
		return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
	}
	return contains(a, b) || contains(b, a)
}

func (f *FileStorage) validate(syncer string) error {
	if f.Backend == "" {
		return nil
	}
	f.Backend = strings.ToLower(f.Backend)
	if len(f.Events) == 0 {
		f.Events = []string{"create", "update", "delete"}
	}
	if f.MaxFileBytes == 0 {
		f.MaxFileBytes = 1 << 30
	}
	if f.MaxFileBytes < 1<<20 || f.MaxFileBytes > 1<<40 {
		return fmt.Errorf("syncer %q file_storage.max_file_bytes must be between 1 MiB and 1 TiB", syncer)
	}
	seen := make(map[string]bool, len(f.Events))
	for _, operation := range f.Events {
		if operation != "create" && operation != "update" && operation != "delete" || seen[operation] {
			return fmt.Errorf("syncer %q has invalid or duplicate file_storage event", syncer)
		}
		seen[operation] = true
	}
	if strings.ContainsRune(f.Prefix+f.Directory+f.Endpoint+f.Bucket, 0) || filepath.IsAbs(f.Prefix) || strings.Contains(f.Prefix, "..") {
		return fmt.Errorf("syncer %q has invalid file_storage paths", syncer)
	}
	switch f.Backend {
	case "directory":
		if !filepath.IsAbs(f.Directory) {
			return fmt.Errorf("syncer %q file_storage.directory must be absolute", syncer)
		}
	case "s3", "oss":
		if f.Endpoint == "" || f.Bucket == "" || f.AccessKey == "" || f.SecretKey == "" {
			return fmt.Errorf("syncer %q object storage requires endpoint, bucket, access_key and secret_key", syncer)
		}
		if strings.ContainsAny(f.AccessKey+f.SecretKey+f.SessionToken, "\r\n") {
			return fmt.Errorf("syncer %q has invalid object storage credentials", syncer)
		}
		if strings.ContainsAny(f.Endpoint, "\r\n") {
			return fmt.Errorf("syncer %q has an invalid object storage endpoint", syncer)
		}
		if f.Backend == "oss" {
			u, err := url.Parse(f.Endpoint)
			if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
				return fmt.Errorf("syncer %q OSS endpoint must be an http(s) URL without credentials", syncer)
			}
		}
	default:
		return fmt.Errorf("syncer %q file_storage.backend must be directory, s3 or oss", syncer)
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

func (s Syncer) AllowsFileOperation(operation string) bool {
	return s.FileStorage.Backend != "" && slices.Contains(s.FileStorage.Events, operation)
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
