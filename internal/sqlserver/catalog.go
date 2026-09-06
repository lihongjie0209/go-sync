package sqlserver

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"go-sync/internal/config"
)

// Column retains the common schema fields used by the PostgreSQL protocol.
type Column struct {
	Name      string  `json:"name"`
	Type      string  `json:"type"`
	NotNull   bool    `json:"not_null"`
	Default   *string `json:"default"`
	Primary   bool    `json:"primary_key"`
	BaseType  string  `json:"base_type"`
	Ordinal   int     `json:"capture_ordinal"`
	MaxLength int     `json:"max_length"`
	Collation *string `json:"collation,omitempty"`
}

// Table includes capture identity so a replaced CDC instance cannot be resumed.
type Table struct {
	SourceType      string   `json:"source_type"`
	Schema          string   `json:"schema"`
	Name            string   `json:"name"`
	RowIdentity     string   `json:"row_identity,omitempty"`
	Columns         []Column `json:"columns"`
	ObjectID        int      `json:"object_id"`
	CaptureID       int      `json:"capture_object_id"`
	CaptureInstance string   `json:"capture_instance"`
	CreatedAt       string   `json:"capture_created_at"`
	HasCommandID    bool     `json:"has_command_id"`
}

// Inspection is the read-only preflight output of go-sync check.
type Inspection struct {
	SourceType string  `json:"source_type"`
	Version    string  `json:"version"`
	Edition    string  `json:"edition"`
	Database   string  `json:"database"`
	SystemID   string  `json:"system_id"`
	Tables     []Table `json:"tables"`
	Fence      Table   `json:"fence_table"`
}

type querier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func quote(name string) string      { return "[" + strings.ReplaceAll(name, "]", "]]") + "]" }
func (t Table) sqlName() string     { return quote(t.Schema) + "." + quote(t.Name) }
func (t Table) changesName() string { return "[cdc]." + quote(t.CaptureInstance+"_CT") }

// Inspect validates configuration, database identity, CDC and captured columns.
// It does not acquire snapshot locks, insert markers, or enable CDC/settings.
func Inspect(ctx context.Context, c config.Config) (info Inspection, result error) {
	if err := c.Validate(); err != nil {
		return info, err
	}
	db, err := open(ctx, c)
	if err != nil {
		return info, err
	}
	defer func() { result = errors.Join(result, db.Close()) }()
	return inspect(ctx, db, c)
}

func inspect(ctx context.Context, q querier, c config.Config) (Inspection, error) {
	info := Inspection{SourceType: "sqlserver"}
	var enabled bool
	var snapshotState int
	err := q.QueryRowContext(ctx, `SELECT
 CONVERT(nvarchar(128),SERVERPROPERTY('ProductVersion')),
 CONVERT(nvarchar(128),SERVERPROPERTY('Edition')), DB_NAME(),
 CONVERT(nvarchar(128),SERVERPROPERTY('ServerName')) + N'/' +
 CONVERT(nvarchar(36),r.database_guid) + N'/' + CONVERT(nvarchar(36),r.recovery_fork_guid),
 d.is_cdc_enabled,d.snapshot_isolation_state
 FROM sys.databases d JOIN sys.database_recovery_status r ON d.database_id=r.database_id
 WHERE d.database_id=DB_ID()`).Scan(
		&info.Version, &info.Edition, &info.Database, &info.SystemID, &enabled, &snapshotState,
	)
	if err != nil {
		return info, dbError(err)
	}
	major, err := strconv.Atoi(strings.Split(info.Version, ".")[0])
	if err != nil || major < 10 {
		return info, errors.New("sqlserver 2008 or later is required")
	}
	if !enabled {
		return info, errors.New("sqlserver database cdc is not enabled")
	}
	if snapshotState != 1 {
		return info, errors.New("sqlserver requires allow_snapshot_isolation on for consistent cdc reads")
	}
	wanted := slices.Clone(c.Tables)
	slices.SortFunc(wanted, func(a, b config.Table) int {
		if n := strings.Compare(a.Schema, b.Schema); n != 0 {
			return n
		}
		return strings.Compare(a.Name, b.Name)
	})
	seen := map[int]bool{}
	for _, ref := range wanted {
		table, err := readTable(ctx, q, ref)
		if err != nil {
			return info, fmt.Errorf("inspect table %s: %w", ref.String(), err)
		}
		if seen[table.ObjectID] {
			return info, errors.New("duplicate sqlserver source object")
		}
		seen[table.ObjectID] = true
		info.Tables = append(info.Tables, table)
	}
	info.Fence, err = readTable(ctx, q, c.SQLServer.FenceTable)
	if err != nil {
		return info, fmt.Errorf("inspect fence table: %w", err)
	}
	if seen[info.Fence.ObjectID] {
		return info, errors.New("fence must not be a business table")
	}
	cols := info.Fence.Columns
	if len(cols) != 1 || cols[0].Name != "token" || cols[0].Type != "varchar(64)" || !cols[0].Primary {
		return info, errors.New("fence table must have only token varchar(64) primary key, with cdc enabled")
	}
	return info, nil
}

func readTable(ctx context.Context, q querier, ref config.Table) (table Table, result error) {
	table = Table{SourceType: "sqlserver", Schema: ref.Schema, Name: ref.Name, Columns: []Column{}}
	rows, err := q.QueryContext(ctx, `SELECT ct.source_object_id,ct.object_id,ct.capture_instance,
 CONVERT(nvarchar(30),ct.create_date,126),
 CASE WHEN EXISTS(SELECT 1 FROM sys.columns x WHERE x.object_id=ct.object_id AND x.name=N'__$command_id')
 THEN 1 ELSE 0 END,
 CASE WHEN EXISTS(SELECT 1 FROM sys.indexes i JOIN sys.data_spaces ds ON ds.data_space_id=i.data_space_id
 WHERE i.object_id=t.object_id AND ds.type='PS') THEN 1 ELSE 0 END
 FROM cdc.change_tables ct JOIN sys.tables t ON t.object_id=ct.source_object_id
 JOIN sys.schemas s ON s.schema_id=t.schema_id
 WHERE s.name=@p1 AND t.name=@p2`, ref.Schema, ref.Name)
	if err != nil {
		return table, dbError(err)
	}
	defer func() { result = errors.Join(result, dbError(rows.Close())) }()
	count := 0
	var isPartitioned bool
	for rows.Next() {
		count++
		if err := rows.Scan(&table.ObjectID, &table.CaptureID, &table.CaptureInstance,
			&table.CreatedAt, &table.HasCommandID, &isPartitioned); err != nil {
			return table, dbError(err)
		}
	}
	if err := rows.Err(); err != nil {
		return table, dbError(err)
	}
	if count != 1 {
		return table, schemaError("exactly one cdc capture instance per table is required")
	}
	if isPartitioned {
		return table, errors.New("partitioned sqlserver tables are unsupported; partition switching is not captured by cdc")
	}
	if err := rows.Close(); err != nil {
		return table, dbError(err)
	}
	columns, err := q.QueryContext(ctx, `SELECT c.name,TYPE_NAME(c.system_type_id),c.max_length,
 c.precision,c.scale,c.is_nullable,OBJECT_DEFINITION(c.default_object_id),c.is_computed,
 CASE WHEN EXISTS(SELECT 1 FROM sys.indexes i JOIN sys.index_columns ic
 ON ic.object_id=i.object_id AND ic.index_id=i.index_id
 WHERE i.object_id=c.object_id AND i.is_primary_key=1 AND ic.column_id=c.column_id) THEN 1 ELSE 0 END,
 cc.column_ordinal,c.collation_name,
 CASE WHEN dest.system_type_id=c.system_type_id AND dest.max_length=c.max_length
 AND dest.precision=c.precision AND dest.scale=c.scale
 AND ISNULL(dest.collation_name,N'')=ISNULL(c.collation_name,N'') THEN 1
 WHEN c.system_type_id=189 AND dest.system_type_id=173 THEN 1 ELSE 0 END
 FROM sys.columns c
 LEFT JOIN cdc.captured_columns cc ON cc.object_id=@p2 AND cc.column_id=c.column_id
 LEFT JOIN sys.columns dest ON dest.object_id=@p2 AND dest.name=c.name
 WHERE c.object_id=@p1 ORDER BY c.column_id`, table.ObjectID, table.CaptureID)
	if err != nil {
		return table, dbError(err)
	}
	defer func() { result = errors.Join(result, dbError(columns.Close())) }()
	hasPrimary := false
	for columns.Next() {
		var col Column
		var precision, scale int
		var nullable, computed, compatible bool
		var ordinal sql.NullInt64
		if err := columns.Scan(
			&col.Name, &col.BaseType, &col.MaxLength, &precision, &scale, &nullable,
			&col.Default, &computed, &col.Primary, &ordinal, &col.Collation, &compatible,
		); err != nil {
			return table, dbError(err)
		}
		if computed || !ordinal.Valid || !compatible {
			return table, schemaError("cdc must capture every noncomputed source column with matching types")
		}
		col.Ordinal = int(ordinal.Int64)
		col.NotNull = !nullable
		col.Type, err = typeName(col.BaseType, col.MaxLength, precision, scale)
		if err != nil {
			return table, err
		}
		hasPrimary = hasPrimary || col.Primary
		table.Columns = append(table.Columns, col)
	}
	if err := columns.Err(); err != nil {
		return table, dbError(err)
	}
	if len(table.Columns) == 0 {
		return table, errors.New("table has no visible columns")
	}
	if !hasPrimary {
		table.RowIdentity = "full_row"
		for _, col := range table.Columns {
			if legacyLOB(col.BaseType) || col.BaseType == "xml" {
				return table, errors.New("keyless cdc tables cannot contain text, ntext, image or xml: complete old identity unavailable")
			}
		}
	}
	return table, nil
}

func typeName(base string, length, precision, scale int) (string, error) {
	switch base {
	case "varchar", "nvarchar", "char", "nchar", "binary", "varbinary":
		if length == -1 {
			return base + "(max)", nil
		}
		if base == "nvarchar" || base == "nchar" {
			length /= 2
		}
		return fmt.Sprintf("%s(%d)", base, length), nil
	case "decimal", "numeric":
		return fmt.Sprintf("%s(%d,%d)", base, precision, scale), nil
	case "datetime2", "datetimeoffset", "time":
		return fmt.Sprintf("%s(%d)", base, scale), nil
	case "bigint", "int", "smallint", "tinyint", "bit", "money", "smallmoney", "float", "real",
		"date", "datetime", "smalldatetime", "uniqueidentifier", "timestamp", "text", "ntext", "image", "xml":
		return base, nil
	default:
		return "", fmt.Errorf("unsupported sqlserver type %q", base)
	}
}

func schemaHash(tables []Table) string {
	b, _ := json.Marshal(tables)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
