package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"go-sync/internal/event"
)

type sourceSchemaTable struct {
	SourceType string               `json:"source_type"`
	Schema     string               `json:"schema"`
	Name       string               `json:"name"`
	Engine     string               `json:"engine"`
	Columns    []sourceSchemaColumn `json:"columns"`
}

type sourceSchemaColumn struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	ColumnType string `json:"column_type"`
	BaseType   string `json:"base_type"`
	NotNull    bool   `json:"not_null"`
	Nullable   *bool  `json:"nullable"`
	Primary    bool   `json:"primary_key"`
}

func (t *target) stageSchema(ctx context.Context, tx pgx.Tx, message event.Message) error {
	var schema sourceSchemaTable
	if err := json.Unmarshal(message.Schema, &schema); err != nil {
		return errors.New("invalid schema payload")
	}
	if schema.Schema == "" || schema.Name == "" || !t.cfg.Allows(schema.Schema, schema.Name) {
		return fmt.Errorf("schema references unconfigured table %s.%s", schema.Schema, schema.Name)
	}
	if len(schema.Columns) == 0 {
		return fmt.Errorf("schema for %s.%s has no columns", schema.Schema, schema.Name)
	}
	_, err := tx.Exec(ctx, "INSERT INTO "+quote(t.cfg.Postgres.MetadataSchema)+`.schema_stage
        (syncer_id,generation,schema_version,source_schema,table_name,payload)
        VALUES($1,$2,$3,$4,$5,$6)`, t.cfg.ID, message.Generation, message.SchemaVersion, schema.Schema, schema.Name, message.Schema)
	return err
}

func (t *target) applyStagedSchemas(ctx context.Context, tx pgx.Tx, generation, version string) error {
	meta := quote(t.cfg.Postgres.MetadataSchema)
	rows, err := tx.Query(ctx, "SELECT payload FROM "+meta+`.schema_stage
        WHERE syncer_id=$1 AND generation=$2 AND schema_version=$3 ORDER BY source_schema,table_name`, t.cfg.ID, generation, version)
	if err != nil {
		return err
	}
	schemas := make([]sourceSchemaTable, 0, len(t.cfg.Tables))
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			rows.Close()
			return err
		}
		var schema sourceSchemaTable
		if err := json.Unmarshal(payload, &schema); err != nil {
			rows.Close()
			return errors.New("invalid staged schema payload")
		}
		schemas = append(schemas, schema)
	}
	iterationErr := rows.Err()
	rows.Close()
	if iterationErr != nil {
		return iterationErr
	}
	if len(schemas) != len(t.cfg.Tables) {
		return fmt.Errorf("schema refresh contains %d tables, want %d", len(schemas), len(t.cfg.Tables))
	}
	for _, schema := range schemas {
		types, err := targetColumnTypes(ctx, tx, t.cfg.Postgres.TargetSchema, schema.Name)
		if err != nil {
			return err
		}
		for _, column := range schema.Columns {
			if _, exists := types[column.Name]; exists {
				continue
			}
			nullable := !column.NotNull
			if column.Nullable != nil {
				nullable = *column.Nullable
			}
			if !t.cfg.AutoAddColumns || !nullable || column.Primary {
				return fmt.Errorf("target column %s.%s is missing and cannot be added safely", schema.Name, column.Name)
			}
			typeName, err := safeTargetType(schema, column)
			if err != nil {
				return fmt.Errorf("target column %s.%s: %w", schema.Name, column.Name, err)
			}
			if _, err := tx.Exec(ctx, "ALTER TABLE "+quote(t.cfg.Postgres.TargetSchema)+"."+quote(schema.Name)+" ADD COLUMN "+quote(column.Name)+" "+typeName); err != nil {
				return err
			}
			types[column.Name] = typeName
		}
	}
	_, err = tx.Exec(ctx, "DELETE FROM "+meta+".schema_stage WHERE syncer_id=$1 AND generation=$2 AND schema_version=$3", t.cfg.ID, generation, version)
	return err
}

func targetColumnTypes(ctx context.Context, tx pgx.Tx, schema, table string) (map[string]string, error) {
	rows, err := tx.Query(ctx, `SELECT a.attname, format_type(a.atttypid,a.atttypmod)
        FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=$1 AND c.relname=$2 AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum`, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	types := make(map[string]string)
	for rows.Next() {
		var name, typeName string
		if err := rows.Scan(&name, &typeName); err != nil {
			return nil, err
		}
		types[name] = typeName
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(types) == 0 {
		return nil, fmt.Errorf("target table %s.%s does not exist", schema, table)
	}
	return types, nil
}

var postgresType = regexp.MustCompile(`^(smallint|integer|bigint|real|double precision|boolean|text|bytea|date|uuid|json|jsonb|numeric(?:\([1-9][0-9]{0,3}(?:,[0-9]{1,4})?\))?|character(?: varying)?\([1-9][0-9]{0,6}\)|timestamp(?:\([0-6]\))? (?:with|without) time zone|time(?:\([0-6]\))? (?:with|without) time zone)$`)
var sizedType = regexp.MustCompile(`^[a-z]+\(([1-9][0-9]{0,6})(?:,([0-9]{1,4}))?\)$`)

func safeTargetType(table sourceSchemaTable, column sourceSchemaColumn) (string, error) {
	if table.Engine != "" {
		return mysqlTargetType(column)
	}
	if table.SourceType == "sqlserver" || table.SourceType == "sqlserver_legacy" {
		return sqlServerTargetType(column)
	}
	typeName := strings.ToLower(strings.TrimSpace(column.Type))
	if !postgresType.MatchString(typeName) {
		return "", fmt.Errorf("PostgreSQL source type %q is not in the automatic-add allowlist", column.Type)
	}
	return typeName, nil
}

func mysqlTargetType(column sourceSchemaColumn) (string, error) {
	base := strings.ToLower(column.Type)
	switch base {
	case "tinyint", "smallint":
		return "smallint", nil
	case "mediumint", "int", "integer":
		return "integer", nil
	case "bigint":
		return "bigint", nil
	case "float":
		return "real", nil
	case "double":
		return "double precision", nil
	case "decimal", "numeric":
		return numericTargetType(column.ColumnType)
	case "char", "varchar":
		match := sizedType.FindStringSubmatch(strings.ToLower(column.ColumnType))
		if match == nil {
			return "", fmt.Errorf("unsupported MySQL character type %q", column.ColumnType)
		}
		length, _ := strconv.Atoi(match[1])
		if base == "char" {
			return fmt.Sprintf("character(%d)", length), nil
		}
		return fmt.Sprintf("character varying(%d)", length), nil
	case "tinytext", "text", "mediumtext", "longtext":
		return "text", nil
	case "tinyblob", "blob", "mediumblob", "longblob", "binary", "varbinary":
		return "bytea", nil
	case "date":
		return "date", nil
	case "datetime", "timestamp":
		return "timestamp without time zone", nil
	case "time":
		return "time without time zone", nil
	case "json":
		return "jsonb", nil
	default:
		return "", fmt.Errorf("MySQL source type %q is not in the automatic-add allowlist", column.Type)
	}
}

func numericTargetType(source string) (string, error) {
	match := sizedType.FindStringSubmatch(strings.ToLower(source))
	if match == nil {
		return "", fmt.Errorf("unsupported numeric type %q", source)
	}
	precision, _ := strconv.Atoi(match[1])
	scale := 0
	if match[2] != "" {
		scale, _ = strconv.Atoi(match[2])
	}
	if precision > 1000 || scale > precision {
		return "", errors.New("invalid numeric precision or scale")
	}
	return fmt.Sprintf("numeric(%d,%d)", precision, scale), nil
}

func sqlServerTargetType(column sourceSchemaColumn) (string, error) {
	base := strings.ToLower(column.BaseType)
	switch base {
	case "tinyint", "smallint":
		return "smallint", nil
	case "int":
		return "integer", nil
	case "bigint":
		return "bigint", nil
	case "bit":
		return "boolean", nil
	case "real":
		return "real", nil
	case "float":
		return "double precision", nil
	case "text", "ntext", "varchar", "nvarchar", "char", "nchar":
		return "text", nil
	case "binary", "varbinary", "image", "timestamp", "rowversion":
		return "bytea", nil
	case "decimal", "numeric":
		return numericTargetType(column.Type)
	case "money":
		return "numeric(19,4)", nil
	case "smallmoney":
		return "numeric(10,4)", nil
	case "uniqueidentifier":
		return "uuid", nil
	case "date":
		return "date", nil
	case "datetime", "datetime2", "smalldatetime":
		return "timestamp without time zone", nil
	case "time":
		return "time without time zone", nil
	default:
		return "", fmt.Errorf("SQL Server source type %q is not in the automatic-add allowlist", column.BaseType)
	}
}

func (t *target) invalidateTypes() {
	t.mu.Lock()
	t.types = make(map[string]map[string]string)
	t.mu.Unlock()
}
