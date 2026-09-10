package server

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go-sync/internal/event"
	"go-sync/internal/serverconfig"
)

type target struct {
	cfg   serverconfig.Syncer
	pool  *pgxpool.Pool
	mu    sync.RWMutex
	types map[string]map[string]string
}

type progress struct {
	Generation string
	Received   uint64
	Applied    uint64
}

func openTarget(ctx context.Context, cfg serverconfig.Syncer) (*target, error) {
	pool, err := pgxpool.New(ctx, cfg.Postgres.DSN)
	if err != nil {
		return nil, err
	}
	t := &target{cfg: cfg, pool: pool, types: make(map[string]map[string]string)}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	var version int
	if err := pool.QueryRow(ctx, "SELECT current_setting('server_version_num')::integer").Scan(&version); err != nil || version < 150000 {
		pool.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("target PostgreSQL 15 or newer is required; got server_version_num %d", version)
	}
	if err := t.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	for _, table := range cfg.Tables {
		if _, err := t.columnTypes(ctx, table.Name); err != nil {
			pool.Close()
			return nil, err
		}
	}
	return t, nil
}

func (t *target) Close() { t.pool.Close() }

func quote(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

func (t *target) migrate(ctx context.Context) error {
	meta := quote(t.cfg.Postgres.MetadataSchema)
	statements := []string{
		"CREATE SCHEMA IF NOT EXISTS " + meta,
		"CREATE TABLE IF NOT EXISTS " + meta + `.sync_state (
            syncer_id text PRIMARY KEY,
            generation text NOT NULL DEFAULT '',
            received_seq bigint NOT NULL DEFAULT 0,
            applied_seq bigint NOT NULL DEFAULT 0,
            phase text NOT NULL DEFAULT '',
            updated_at timestamptz NOT NULL DEFAULT clock_timestamp())`,
		"CREATE TABLE IF NOT EXISTS " + meta + `.server_instance (
            singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), instance_id text NOT NULL)`,
		"CREATE TABLE IF NOT EXISTS " + meta + `.inbox (
            syncer_id text NOT NULL, generation text NOT NULL, seq bigint NOT NULL,
            message_id text NOT NULL, payload bytea NOT NULL,
            PRIMARY KEY(syncer_id, generation, seq))`,
		"CREATE TABLE IF NOT EXISTS " + meta + `.snapshot_scope (
            syncer_id text NOT NULL, source_schema text NOT NULL, table_name text NOT NULL, ordinal integer NOT NULL DEFAULT 0,
            PRIMARY KEY(syncer_id, source_schema, table_name))`,
		"ALTER TABLE " + meta + ".snapshot_scope ADD COLUMN IF NOT EXISTS ordinal integer NOT NULL DEFAULT 0",
		"CREATE TABLE IF NOT EXISTS " + meta + `.snapshot_rows (
            syncer_id text NOT NULL, generation text NOT NULL, seq bigint NOT NULL,
            row_ordinal bigint NOT NULL, payload bytea NOT NULL,
            PRIMARY KEY(syncer_id, generation, seq, row_ordinal))`,
	}
	for _, statement := range statements {
		if _, err := t.pool.Exec(ctx, statement); err != nil {
			return fmt.Errorf("initialize server metadata: %w", err)
		}
	}
	if _, err := t.pool.Exec(ctx, "INSERT INTO "+meta+".server_instance(singleton,instance_id) VALUES(true,$1) ON CONFLICT(singleton) DO NOTHING", uuid.NewString()); err != nil {
		return err
	}
	_, err := t.pool.Exec(ctx, "INSERT INTO "+meta+".sync_state(syncer_id) VALUES($1) ON CONFLICT(syncer_id) DO NOTHING", t.cfg.ID)
	return err
}

func (t *target) identity(ctx context.Context) (string, error) {
	var id string
	err := t.pool.QueryRow(ctx, "SELECT instance_id FROM "+quote(t.cfg.Postgres.MetadataSchema)+".server_instance WHERE singleton").Scan(&id)
	return id, err
}

func (t *target) Progress(ctx context.Context) (progress, error) {
	var p progress
	err := t.pool.QueryRow(ctx, "SELECT generation, received_seq, applied_seq FROM "+quote(t.cfg.Postgres.MetadataSchema)+".sync_state WHERE syncer_id=$1", t.cfg.ID).
		Scan(&p.Generation, &p.Received, &p.Applied)
	return p, err
}

func (t *target) Store(ctx context.Context, message event.Message, raw []byte) error {
	if message.Seq > math.MaxInt64 {
		return errors.New("message sequence exceeds PostgreSQL bigint")
	}
	tx, err := t.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	meta := quote(t.cfg.Postgres.MetadataSchema)
	var current progress
	var phase string
	if err := tx.QueryRow(ctx, "SELECT generation, received_seq, applied_seq, phase FROM "+meta+".sync_state WHERE syncer_id=$1 FOR UPDATE", t.cfg.ID).
		Scan(&current.Generation, &current.Received, &current.Applied, &phase); err != nil {
		return err
	}
	if message.Generation != current.Generation {
		if message.Seq != 1 || message.Kind != "snapshot_begin" {
			return errors.New("new generation must begin with snapshot_begin sequence 1")
		}
		if _, err := tx.Exec(ctx, "DELETE FROM "+meta+".inbox WHERE syncer_id=$1", t.cfg.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "DELETE FROM "+meta+".snapshot_rows WHERE syncer_id=$1", t.cfg.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "DELETE FROM "+meta+".snapshot_scope WHERE syncer_id=$1", t.cfg.ID); err != nil {
			return err
		}
		current = progress{Generation: message.Generation}
		phase = ""
	}
	if message.Seq <= current.Received {
		return tx.Commit(ctx)
	}
	if message.Seq != current.Received+1 {
		return fmt.Errorf("non-contiguous message: got %d, want %d", message.Seq, current.Received+1)
	}
	if err := t.applyMessage(ctx, tx, message, raw, &phase); err != nil {
		return err
	}
	applied := current.Applied
	if message.Kind == "snapshot_end" || message.Kind == "transaction_end" || (phase == "stream" && message.Kind == "schema") {
		applied = message.Seq
	}
	if _, err := tx.Exec(ctx, "UPDATE "+meta+".sync_state SET generation=$2, received_seq=$3, applied_seq=$4, phase=$5, updated_at=clock_timestamp() WHERE syncer_id=$1",
		t.cfg.ID, message.Generation, int64(message.Seq), int64(applied), phase); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (t *target) applyMessage(ctx context.Context, tx pgx.Tx, message event.Message, raw []byte, phase *string) error {
	meta := quote(t.cfg.Postgres.MetadataSchema)
	switch message.Kind {
	case "snapshot_begin":
		*phase = "snapshot"
		if _, err := tx.Exec(ctx, "DELETE FROM "+meta+".snapshot_rows WHERE syncer_id=$1", t.cfg.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "DELETE FROM "+meta+".snapshot_scope WHERE syncer_id=$1", t.cfg.ID); err != nil {
			return err
		}
		for ordinal, table := range message.Tables {
			if !t.cfg.Allows(table.Schema, table.Name) {
				return fmt.Errorf("snapshot contains unconfigured table %s.%s", table.Schema, table.Name)
			}
			if _, err := tx.Exec(ctx, "INSERT INTO "+meta+".snapshot_scope(syncer_id,source_schema,table_name,ordinal) VALUES($1,$2,$3,$4)", t.cfg.ID, table.Schema, table.Name, ordinal); err != nil {
				return err
			}
		}
	case "schema":
		if *phase != "snapshot" && *phase != "stream" {
			return errors.New("schema received outside a synchronization phase")
		}
	case "snapshot_rows":
		if *phase != "snapshot" {
			return errors.New("snapshot rows received outside snapshot")
		}
		for i, row := range message.Rows {
			payload, err := json.Marshal(row)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, "INSERT INTO "+meta+".snapshot_rows(syncer_id,generation,seq,row_ordinal,payload) VALUES($1,$2,$3,$4,$5)",
				t.cfg.ID, message.Generation, int64(message.Seq), i, payload); err != nil {
				return err
			}
		}
	case "snapshot_end":
		if *phase != "snapshot" {
			return errors.New("snapshot_end received outside snapshot")
		}
		rows, err := tx.Query(ctx, "SELECT source_schema,table_name FROM "+meta+".snapshot_scope WHERE syncer_id=$1 ORDER BY ordinal", t.cfg.ID)
		if err != nil {
			return err
		}
		var scope []serverconfig.Table
		for rows.Next() {
			var table serverconfig.Table
			if err := rows.Scan(&table.SourceSchema, &table.Name); err != nil {
				rows.Close()
				return err
			}
			scope = append(scope, table)
		}
		rows.Close()
		for i := len(scope) - 1; i >= 0; i-- {
			table := scope[i]
			if _, err := tx.Exec(ctx, "DELETE FROM "+quote(t.cfg.Postgres.TargetSchema)+"."+quote(table.Name)); err != nil {
				return err
			}
		}
		staged, err := tx.Query(ctx, "SELECT payload FROM "+meta+".snapshot_rows WHERE syncer_id=$1 AND generation=$2 ORDER BY seq,row_ordinal", t.cfg.ID, message.Generation)
		if err != nil {
			return err
		}
		var snapshot []event.Row
		for staged.Next() {
			var payload []byte
			if err := staged.Scan(&payload); err != nil {
				staged.Close()
				return err
			}
			var row event.Row
			if err := json.Unmarshal(payload, &row); err != nil {
				staged.Close()
				return err
			}
			snapshot = append(snapshot, row)
		}
		staged.Close()
		for _, row := range snapshot {
			row.Operation = "insert"
			if err := t.applyRow(ctx, tx, row); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, "DELETE FROM "+meta+".snapshot_rows WHERE syncer_id=$1", t.cfg.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "DELETE FROM "+meta+".snapshot_scope WHERE syncer_id=$1", t.cfg.ID); err != nil {
			return err
		}
		*phase = "stream"
	case "transaction_rows":
		if *phase != "stream" {
			return errors.New("transaction rows received before snapshot publication")
		}
		_, err := tx.Exec(ctx, "INSERT INTO "+meta+".inbox(syncer_id,generation,seq,message_id,payload) VALUES($1,$2,$3,$4,$5)",
			t.cfg.ID, message.Generation, int64(message.Seq), message.ID, raw)
		return err
	case "transaction_end":
		if *phase != "stream" {
			return errors.New("transaction end received before snapshot publication")
		}
		rows, err := tx.Query(ctx, "SELECT seq,payload FROM "+meta+".inbox WHERE syncer_id=$1 AND generation=$2 ORDER BY seq", t.cfg.ID, message.Generation)
		if err != nil {
			return err
		}
		var chunks []event.Message
		var sequences []int64
		for rows.Next() {
			var seq int64
			var payload []byte
			if err := rows.Scan(&seq, &payload); err != nil {
				rows.Close()
				return err
			}
			var chunk event.Message
			if err := json.Unmarshal(payload, &chunk); err != nil {
				rows.Close()
				return err
			}
			if chunk.Transaction == message.Transaction {
				chunks, sequences = append(chunks, chunk), append(sequences, seq)
			}
		}
		rows.Close()
		for _, chunk := range chunks {
			for _, row := range chunk.Rows {
				if err := t.applyRow(ctx, tx, row); err != nil {
					return err
				}
			}
		}
		for _, seq := range sequences {
			if _, err := tx.Exec(ctx, "DELETE FROM "+meta+".inbox WHERE syncer_id=$1 AND generation=$2 AND seq=$3", t.cfg.ID, message.Generation, seq); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported message kind %q", message.Kind)
	}
	return nil
}

type appliedColumn struct {
	name   string
	value  any
	binary bool
}

func (t *target) transform(row event.Row, columns []event.Column) ([]appliedColumn, error) {
	result := make([]appliedColumn, 0, len(columns)+1)
	for _, column := range columns {
		if content, ok := t.cfg.FileMapping(row.Schema, row.Table, column.Name); ok && column.Encoding == "base64" {
			var decoded []byte
			var err error
			if column.Value != nil {
				decoded, err = base64.StdEncoding.DecodeString(*column.Value)
				if err != nil {
					return nil, fmt.Errorf("decode embedded file column %s: %w", column.Name, err)
				}
			}
			result = append(result, appliedColumn{name: column.Name, value: column.SourceValue}, appliedColumn{name: content, value: decoded, binary: true})
			continue
		}
		result = append(result, appliedColumn{name: column.Name, value: column.Value})
	}
	return result, nil
}

func (t *target) applyRow(ctx context.Context, tx pgx.Tx, row event.Row) error {
	if !t.cfg.Allows(row.Schema, row.Table) {
		return fmt.Errorf("row references unconfigured table %s.%s", row.Schema, row.Table)
	}
	types, err := t.columnTypes(ctx, row.Table)
	if err != nil {
		return err
	}
	table := quote(t.cfg.Postgres.TargetSchema) + "." + quote(row.Table)
	columns, err := t.transform(row, row.Columns)
	if err != nil {
		return err
	}
	keySource := row.Key
	if row.Operation == "update" && len(row.OldKey) != 0 {
		keySource = row.OldKey
	}
	keys, err := t.transform(row, keySource)
	if err != nil {
		return err
	}
	switch row.Operation {
	case "read", "insert":
		if len(columns) == 0 {
			return errors.New("insert has no columns")
		}
		names, values, args, err := expressions(columns, types, 1)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, "INSERT INTO "+table+" ("+strings.Join(names, ",")+") VALUES ("+strings.Join(values, ",")+")", args...)
		return err
	case "update":
		if len(columns) == 0 || len(keys) == 0 {
			return errors.New("update requires changed columns and identity")
		}
		_, values, args, err := expressions(columns, types, 1)
		if err != nil {
			return err
		}
		set := make([]string, len(columns))
		for i := range columns {
			set[i] = quote(columns[i].name) + "=" + values[i]
		}
		where, keyArgs, err := predicates(keys, types, len(args)+1)
		if err != nil {
			return err
		}
		args = append(args, keyArgs...)
		command, err := tx.Exec(ctx, "UPDATE "+table+" SET "+strings.Join(set, ",")+" WHERE ctid IN (SELECT ctid FROM "+table+" WHERE "+where+" LIMIT 1)", args...)
		if err == nil && command.RowsAffected() != 1 {
			return errors.New("update identity did not match exactly one row")
		}
		return err
	case "delete":
		if len(keys) == 0 {
			return errors.New("delete requires identity")
		}
		where, args, err := predicates(keys, types, 1)
		if err != nil {
			return err
		}
		command, err := tx.Exec(ctx, "DELETE FROM "+table+" WHERE ctid IN (SELECT ctid FROM "+table+" WHERE "+where+" LIMIT 1)", args...)
		if err == nil && command.RowsAffected() != 1 {
			return errors.New("delete identity did not match exactly one row")
		}
		return err
	default:
		return fmt.Errorf("unsupported row operation %q", row.Operation)
	}
}

func expressions(columns []appliedColumn, types map[string]string, start int) ([]string, []string, []any, error) {
	names, values, args := make([]string, len(columns)), make([]string, len(columns)), make([]any, len(columns))
	for i, column := range columns {
		typeName, ok := types[column.name]
		if !ok {
			return nil, nil, nil, fmt.Errorf("target column %q does not exist", column.name)
		}
		names[i], args[i] = quote(column.name), column.value
		if column.binary {
			if typeName != "bytea" {
				return nil, nil, nil, fmt.Errorf("file content column %q must be bytea", column.name)
			}
			values[i] = fmt.Sprintf("$%d::bytea", start+i)
		} else {
			if value, ok := column.value.(*string); ok && value != nil && typeName == "bytea" && strings.HasPrefix(*value, "0x") {
				decoded, err := hex.DecodeString((*value)[2:])
				if err != nil {
					return nil, nil, nil, fmt.Errorf("invalid binary value for %q", column.name)
				}
				args[i], values[i] = decoded, fmt.Sprintf("$%d::bytea", start+i)
			} else {
				values[i] = fmt.Sprintf("$%d::text::%s", start+i, typeName)
			}
		}
	}
	return names, values, args, nil
}

func predicates(columns []appliedColumn, types map[string]string, start int) (string, []any, error) {
	_, values, args, err := expressions(columns, types, start)
	if err != nil {
		return "", nil, err
	}
	parts := make([]string, len(columns))
	for i := range columns {
		parts[i] = quote(columns[i].name) + " IS NOT DISTINCT FROM " + values[i]
	}
	return strings.Join(parts, " AND "), args, nil
}

func (t *target) columnTypes(ctx context.Context, table string) (map[string]string, error) {
	t.mu.RLock()
	cached := t.types[table]
	t.mu.RUnlock()
	if cached != nil {
		return cached, nil
	}
	rows, err := t.pool.Query(ctx, `SELECT a.attname, format_type(a.atttypid,a.atttypmod)
        FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=$1 AND c.relname=$2 AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum`, t.cfg.Postgres.TargetSchema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]string)
	for rows.Next() {
		var name, typeName string
		if err := rows.Scan(&name, &typeName); err != nil {
			return nil, err
		}
		result[name] = typeName
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("target table %s.%s does not exist", t.cfg.Postgres.TargetSchema, table)
	}
	t.mu.Lock()
	t.types[table] = result
	t.mu.Unlock()
	return result, rows.Err()
}
