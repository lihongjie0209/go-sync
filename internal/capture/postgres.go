package capture

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go-sync/internal/config"
)

type Column struct {
	Name    string  `json:"name"`
	Type    string  `json:"type"`
	OID     uint32  `json:"type_oid"`
	NotNull bool    `json:"not_null"`
	Default *string `json:"default"`
	Primary bool    `json:"primary_key"`
}
type Table struct {
	RowIdentity string   `json:"row_identity,omitempty"`
	Schema      string   `json:"schema"`
	Name        string   `json:"name"`
	OID         uint32   `json:"oid"`
	Identity    string   `json:"replica_identity"`
	Columns     []Column `json:"columns"`
}

func (t Table) SQL() string { return pgx.Identifier{t.Schema, t.Name}.Sanitize() }
func (t Table) KeyNames() []string {
	var keys []string
	for _, c := range t.Columns {
		if c.Primary {
			keys = append(keys, c.Name)
		}
	}
	return keys
}

type querier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func connect(ctx context.Context, c config.Config) (*pgx.Conn, error) {
	pc, e := pgx.ParseConfig(c.DSN)
	if e != nil {
		return nil, errors.New("invalid postgres dsn")
	}
	pc.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pc.ConnectTimeout = 10 * time.Second
	pc.RuntimeParams["application_name"] = "go-sync"
	pc.RuntimeParams["standard_conforming_strings"] = "on"
	pc.RuntimeParams["client_encoding"] = "UTF8"
	pc.RuntimeParams["DateStyle"] = "ISO, YMD"
	pc.RuntimeParams["TimeZone"] = "UTC"
	pc.RuntimeParams["IntervalStyle"] = "postgres"
	pc.RuntimeParams["bytea_output"] = "hex"
	pc.RuntimeParams["extra_float_digits"] = "3"
	return pgx.ConnectConfig(ctx, pc)
}
func replication(ctx context.Context, c config.Config) (*pgconn.PgConn, error) {
	pc, e := pgconn.ParseConfig(c.DSN)
	if e != nil {
		return nil, errors.New("invalid postgres dsn")
	}
	pc.ConnectTimeout = 10 * time.Second
	pc.RuntimeParams["replication"] = "database"
	pc.RuntimeParams["application_name"] = "go-sync"
	pc.RuntimeParams["standard_conforming_strings"] = "on"
	pc.RuntimeParams["client_encoding"] = "UTF8"
	pc.RuntimeParams["DateStyle"] = "ISO, YMD"
	pc.RuntimeParams["TimeZone"] = "UTC"
	pc.RuntimeParams["IntervalStyle"] = "postgres"
	pc.RuntimeParams["bytea_output"] = "hex"
	pc.RuntimeParams["extra_float_digits"] = "3"
	return pgconn.ConnectConfig(ctx, pc)
}

func catalog(ctx context.Context, q querier, tables []config.Table) ([]Table, error) {
	var out []Table
	for _, want := range tables {
		rows, err := q.Query(ctx, `SELECT c.oid, c.relkind::text, c.relpersistence::text,
 c.relreplident::text, EXISTS(SELECT 1 FROM pg_inherits i WHERE i.inhrelid=c.oid OR i.inhparent=c.oid),
 a.attname, format_type(a.atttypid,a.atttypmod), a.atttypid, a.attnotnull,
 pg_get_expr(d.adbin,d.adrelid),
 EXISTS(SELECT 1 FROM pg_index i WHERE i.indrelid=c.oid AND i.indisprimary AND a.attnum=ANY(i.indkey)),
 COALESCE((row_to_json(c)->>'relrowsecurity')::boolean,false),
 COALESCE(row_to_json(a)->>'attgenerated','')
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
 LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
 WHERE n.nspname=$1 AND c.relname=$2 ORDER BY a.attnum`, want.Schema, want.Name)
		if err != nil {
			return nil, err
		}
		t := Table{Schema: want.Schema, Name: want.Name}
		for rows.Next() {
			var col Column
			var kind, persistence string
			var inherited bool
			var rowSecurity bool
			var generated string
			if err := rows.Scan(&t.OID, &kind, &persistence, &t.Identity, &inherited, &col.Name, &col.Type, &col.OID, &col.NotNull, &col.Default, &col.Primary, &rowSecurity, &generated); err != nil {
				rows.Close()
				return nil, err
			}
			if kind != "r" || persistence != "p" || inherited {
				rows.Close()
				return nil, fmt.Errorf("%s must be an ordinary persistent, non-inherited table", want.String())
			}
			if rowSecurity || generated != "" {
				rows.Close()
				return nil, fmt.Errorf("%s: row security and generated columns are unsupported", want.String())
			}
			t.Columns = append(t.Columns, col)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
		if len(t.Columns) == 0 {
			return nil, fmt.Errorf("table %s missing or has no columns", want.String())
		}
		if len(t.KeyNames()) == 0 {
			if t.Identity != "f" {
				return nil, fmt.Errorf("table %s has no primary key; configure explicitly: ALTER TABLE %s REPLICA IDENTITY FULL;", want.String(), t.SQL())
			}
			t.RowIdentity = "full_row"
		} else if t.Identity != "d" && t.Identity != "f" {
			return nil, fmt.Errorf("table %s needs default/full replica identity", want.String())
		}
		out = append(out, t)
	}
	return out, nil
}
func schemaHash(tables []Table) string {
	b, _ := json.Marshal(tables)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

type Inspection struct {
	Version  int     `json:"version"`
	SystemID string  `json:"system_id"`
	Timeline int32   `json:"timeline"`
	Database string  `json:"database"`
	Tables   []Table `json:"tables"`
}

// Inspect is read-only: plugin loading and option negotiation are validated when
// the owned slot is first created/streamed, never through a destructive probe.
func Inspect(ctx context.Context, c config.Config) (Inspection, error) {
	var out Inspection
	conn, e := connect(ctx, c)
	if e != nil {
		return out, e
	}
	defer closeConn(ctx, conn)
	var level string
	var encoding string
	var replicationRole bool
	var slots, senders int
	e = conn.QueryRow(ctx, `SELECT current_setting('server_version_num')::int,current_setting('wal_level'),current_setting('max_replication_slots')::int,current_setting('max_wal_senders')::int,(SELECT rolsuper OR rolreplication FROM pg_roles WHERE rolname=current_user),current_setting('server_encoding')`).Scan(&out.Version, &level, &slots, &senders, &replicationRole, &encoding)
	if e != nil {
		return out, e
	}
	if out.Version < 90400 {
		return out, errors.New("postgresql 9.4 or newer is required")
	}
	if encoding == "SQL_ASCII" {
		return out, errors.New("sql_ascii databases are unsupported because json cannot preserve arbitrary text bytes")
	}
	if level != "logical" || slots < 1 || senders < 1 || !replicationRole {
		return out, errors.New("logical wal, replication role, slots and wal senders are required")
	}
	out.Tables, e = catalog(ctx, conn, c.Tables)
	if e != nil {
		return out, e
	}
	r, e := replication(ctx, c)
	if e != nil {
		return out, e
	}
	defer closeReplication(ctx, r)
	id, e := pglogrepl.IdentifySystem(ctx, r)
	if e != nil {
		return out, e
	}
	out.SystemID = id.SystemID
	out.Timeline = id.Timeline
	out.Database = id.DBName
	return out, nil
}

func closeConn(ctx context.Context, c *pgx.Conn) {
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	c.Close(closeCtx)
}
func closeReplication(ctx context.Context, c *pgconn.PgConn) {
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	c.Close(closeCtx)
}

func filterName(s string) string {
	var b strings.Builder
	for _, r := range s {
		if strings.ContainsRune(" \\.,*'\t\n\r", r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

func pluginArgs(c config.Config) []string {
	var tables []string
	for _, t := range c.Tables {
		tables = append(tables, filterName(t.Schema)+"."+filterName(t.Name))
	}
	value := strings.ReplaceAll(strings.Join(tables, ","), "'", "''")
	return []string{`"format-version" '2'`, `"include-transaction" 'true'`, `"include-lsn" 'true'`, `"include-xids" 'true'`, `"include-types" 'true'`, `"include-type-oids" 'true'`, `"numeric-data-types-as-string" 'true'`, `"add-tables" '` + value + `'`}
}
