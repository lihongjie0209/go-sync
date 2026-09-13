// Package sqlserverlegacy captures SQL Server 2000 changes through owned DML
// triggers and an outbox. It intentionally does not use SQL Server CDC.
package sqlserverlegacy

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"go-sync/internal/config"
)

const schemaVersion = 3

type Column struct {
	Name      string  `json:"name"`
	Type      string  `json:"type"`
	NotNull   bool    `json:"not_null"`
	Default   *string `json:"default"`
	Primary   bool    `json:"primary_key"`
	BaseType  string  `json:"base_type"`
	Ordinal   int     `json:"capture_ordinal"`
	MaxLength int     `json:"max_length"`
}

type Table struct {
	SourceType  string   `json:"source_type"`
	Schema      string   `json:"schema"`
	Name        string   `json:"name"`
	RowIdentity string   `json:"row_identity,omitempty"`
	Columns     []Column `json:"columns"`
	ObjectID    int      `json:"object_id"`
	TriggerName string   `json:"trigger_name"`
	Modern      bool     `json:"modern,omitempty"`
}

func (t Table) sqlName() string { return quote(t.Schema) + "." + quote(t.Name) }

type Inspection struct {
	SourceType string  `json:"source_type"`
	Version    string  `json:"version"`
	Edition    string  `json:"edition"`
	Database   string  `json:"database"`
	SystemID   string  `json:"system_id,omitempty"`
	Installed  bool    `json:"installed"`
	Tables     []Table `json:"tables"`
}

type querier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func quote(name string) string { return "[" + strings.ReplaceAll(name, "]", "]]") + "]" }

func names(c config.Config) (control, events, values string) {
	p := c.SQLServerLegacy.Prefix
	o := quote(c.SQLServerLegacy.Owner) + "."
	return o + quote(p+"_control"), o + quote(p+"_events"), o + quote(p+"_values")
}

func manifestName(c config.Config) string {
	return quote(c.SQLServerLegacy.Owner) + "." + quote(c.SQLServerLegacy.Prefix+"_tables")
}

// Inspect is read-only. Missing managed objects are reported, not installed.
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
	info := Inspection{SourceType: "sqlserver_legacy"}
	if err := q.QueryRowContext(ctx, `SELECT CONVERT(varchar(128),SERVERPROPERTY('ProductVersion')),
 CONVERT(varchar(128),SERVERPROPERTY('Edition')),DB_NAME()`).Scan(&info.Version, &info.Edition, &info.Database); err != nil {
		return info, dbError(err)
	}
	modern := strings.HasPrefix(info.Version, "10.50.")
	if !strings.HasPrefix(info.Version, "8.") && !modern {
		return info, errors.New("sqlserver_legacy requires sql server 2000 or 2008 R2")
	}
	wanted := slices.Clone(c.Tables)
	slices.SortFunc(wanted, func(a, b config.Table) int { return strings.Compare(a.String(), b.String()) })
	seen := map[int]bool{}
	for _, ref := range wanted {
		table, err := readTable(ctx, q, c, ref, modern)
		if err != nil {
			return info, fmt.Errorf("inspect legacy table %s: %w", ref.String(), err)
		}
		if seen[table.ObjectID] {
			return info, errors.New("duplicate sqlserver legacy source object")
		}
		seen[table.ObjectID] = true
		info.Tables = append(info.Tables, table)
	}
	control, events, values := names(c)
	manifest := manifestName(c)
	var installed int
	if err := q.QueryRowContext(ctx, `SELECT CASE WHEN OBJECT_ID(@p1,'U') IS NOT NULL
	 AND OBJECT_ID(@p2,'U') IS NOT NULL AND OBJECT_ID(@p3,'U') IS NOT NULL
	 AND OBJECT_ID(@p4,'U') IS NOT NULL THEN 1 ELSE 0 END`,
		control, events, values, manifest).Scan(&installed); err != nil {
		return info, dbError(err)
	}
	info.Installed = installed == 1
	if info.Installed {
		for _, table := range info.Tables {
			var exists int
			trigger := quote(table.Schema) + "." + quote(table.TriggerName)
			if err := q.QueryRowContext(ctx, "SELECT CASE WHEN OBJECT_ID(@p1,'TR') IS NULL THEN 0 ELSE 1 END", trigger).Scan(&exists); err != nil {
				return info, dbError(err)
			}
			if exists == 0 {
				info.Installed = false
				break
			}
		}
	}
	if info.Installed {
		var version int
		var product string
		var installedHash string
		if err := q.QueryRowContext(ctx, "SELECT product_id,schema_version,CONVERT(varchar(36),source_guid),scope_hash FROM "+control+" WHERE singleton=1").Scan(&product, &version, &info.SystemID, &installedHash); err != nil {
			return info, dbError(err)
		}
		if product != "go-sync/sqlserver-legacy" {
			return info, errors.New("sqlserver legacy object names collide with an unmanaged installation")
		}
		if version != schemaVersion {
			return info, errors.New("sqlserver legacy outbox schema version is unsupported")
		}
		if installedHash != schemaHash(info.Tables) {
			info.Installed = false
		}
	}
	return info, nil
}

func readTable(ctx context.Context, q querier, c config.Config, ref config.Table, modern bool) (Table, error) {
	t := Table{SourceType: "sqlserver_legacy", Schema: ref.Schema, Name: ref.Name, Columns: []Column{}, Modern: modern}
	if err := q.QueryRowContext(ctx, `SELECT o.id FROM sysobjects o JOIN sysusers u ON u.uid=o.uid
 WHERE o.xtype='U' AND u.name=@p1 AND o.name=@p2`, ref.Schema, ref.Name).Scan(&t.ObjectID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return t, errors.New("source table does not exist")
		}
		return t, dbError(err)
	}
	t.TriggerName = triggerName(c, t.ObjectID)
	rows, err := q.QueryContext(ctx, `SELECT c.colid,c.name,ty.name,c.length,ISNULL(c.prec,0),ISNULL(c.scale,0),c.isnullable,
 COLUMNPROPERTY(c.id,c.name,'IsComputed'),
 CASE WHEN EXISTS(SELECT 1 FROM sysindexes i JOIN sysindexkeys k ON k.id=i.id AND k.indid=i.indid
 WHERE i.id=c.id AND (i.status & 2048) <> 0 AND k.colid=c.colid) THEN 1 ELSE 0 END
	 FROM syscolumns c JOIN systypes ty ON c.xtype=ty.xtype AND ty.xusertype=ty.xtype
 WHERE c.id=@p1 ORDER BY c.colid`, t.ObjectID)
	if err != nil {
		return t, dbError(err)
	}
	defer rows.Close()
	hasPrimary := false
	for rows.Next() {
		var col Column
		var precision, scale int
		var nullable, computed bool
		if err := rows.Scan(&col.Ordinal, &col.Name, &col.BaseType, &col.MaxLength, &precision, &scale, &nullable, &computed, &col.Primary); err != nil {
			return t, dbError(err)
		}
		if computed {
			continue
		}
		if legacyLOB(col.BaseType) && !modern {
			return t, errors.New("text, ntext and image columns are unsupported in strict legacy capture")
		}
		var typeErr error
		col.Type, typeErr = typeName(col.BaseType, col.MaxLength, precision, scale)
		if typeErr != nil {
			return t, typeErr
		}
		col.NotNull = !nullable
		hasPrimary = hasPrimary || col.Primary
		t.Columns = append(t.Columns, col)
	}
	if err := rows.Err(); err != nil {
		return t, dbError(err)
	}
	if len(t.Columns) == 0 {
		return t, errors.New("table has no capturable columns")
	}
	if !hasPrimary {
		t.RowIdentity = "full_row"
		for _, col := range t.Columns {
			if legacyLOB(col.BaseType) {
				return t, errors.New("text, ntext and image require a primary key on SQL Server 2008 R2")
			}
		}
	}
	return t, nil
}

func typeName(base string, length, precision, scale int) (string, error) {
	switch base {
	case "varchar", "nvarchar", "char", "nchar", "binary", "varbinary":
		if base == "nvarchar" || base == "nchar" {
			length /= 2
		}
		return fmt.Sprintf("%s(%d)", base, length), nil
	case "decimal", "numeric":
		return fmt.Sprintf("%s(%d,%d)", base, precision, scale), nil
	case "bigint", "int", "smallint", "tinyint", "bit", "money", "smallmoney", "float", "real", "datetime", "smalldatetime", "uniqueidentifier", "timestamp":
		return base, nil
	case "text", "ntext", "image":
		return base, nil
	default:
		return "", fmt.Errorf("unsupported sqlserver 2000 type %q", base)
	}
}

func legacyLOB(base string) bool { return base == "text" || base == "ntext" || base == "image" }

func schemaHash(tables []Table) string {
	b, _ := json.Marshal(tables)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
