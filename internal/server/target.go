package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go-sync/internal/event"
	"go-sync/internal/filestore"
	"go-sync/internal/serverconfig"
)

const applyPageSize = 512

var errTargetLeaseHeld = errors.New("syncer is owned by another server instance")

type target struct {
	cfg   serverconfig.Syncer
	pool  *pgxpool.Pool
	mu    sync.RWMutex
	types map[string]map[string]string
	files filestore.Store
}

type progress struct {
	Generation    string
	Received      uint64
	Applied       uint64
	SchemaVersion string
}

type targetLease struct {
	conn *pgxpool.Conn
	key  string
}

func (t *target) acquireLease(ctx context.Context) (*targetLease, error) {
	conn, err := t.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	key := t.cfg.Postgres.MetadataSchema + ":" + t.cfg.ID
	var locked bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock(hashtextextended($1,0))", key).Scan(&locked); err != nil {
		conn.Release()
		return nil, err
	}
	if !locked {
		conn.Release()
		return nil, errTargetLeaseHeld
	}
	return &targetLease{conn: conn, key: key}, nil
}

func (l *targetLease) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var unlocked bool
	if err := l.conn.QueryRow(ctx, "SELECT pg_advisory_unlock(hashtextextended($1,0))", l.key).Scan(&unlocked); err != nil || !unlocked {
		connection := l.conn.Hijack()
		_ = connection.Close(ctx)
		return
	}
	l.conn.Release()
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
	if cfg.FileStorage.Backend != "" {
		t.files, err = filestore.Open(cfg.FileStorage)
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("open file storage: %w", err)
		}
	}
	for _, table := range cfg.Tables {
		if _, err := t.columnTypes(ctx, table.Name); err != nil {
			if t.files != nil {
				_ = t.files.Close()
			}
			pool.Close()
			return nil, err
		}
	}
	return t, nil
}

func (t *target) Close() {
	if t.files != nil {
		_ = t.files.Close()
	}
	t.pool.Close()
}

func quote(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

func (t *target) migrate(ctx context.Context) error {
	meta := quote(t.cfg.Postgres.MetadataSchema)
	var schemaExists bool
	if err := t.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=$1)", t.cfg.Postgres.MetadataSchema).Scan(&schemaExists); err != nil {
		return fmt.Errorf("inspect server metadata schema: %w", err)
	}
	if !schemaExists {
		if _, err := t.pool.Exec(ctx, "CREATE SCHEMA "+meta); err != nil {
			return fmt.Errorf("create server metadata schema: %w", err)
		}
	}
	statements := []string{
		"CREATE TABLE IF NOT EXISTS " + meta + `.sync_state (
            syncer_id text PRIMARY KEY,
            generation text NOT NULL DEFAULT '',
            received_seq bigint NOT NULL DEFAULT 0,
            applied_seq bigint NOT NULL DEFAULT 0,
            phase text NOT NULL DEFAULT '',
		    open_transaction text NOT NULL DEFAULT '',
		    schema_version text NOT NULL DEFAULT '',
		    pending_schema_version text NOT NULL DEFAULT '',
            updated_at timestamptz NOT NULL DEFAULT clock_timestamp())`,
		"ALTER TABLE " + meta + ".sync_state ADD COLUMN IF NOT EXISTS open_transaction text NOT NULL DEFAULT ''",
		"ALTER TABLE " + meta + ".sync_state ADD COLUMN IF NOT EXISTS schema_version text NOT NULL DEFAULT ''",
		"ALTER TABLE " + meta + ".sync_state ADD COLUMN IF NOT EXISTS pending_schema_version text NOT NULL DEFAULT ''",
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
		"CREATE TABLE IF NOT EXISTS " + meta + `.message_hash (
            syncer_id text NOT NULL, generation text NOT NULL, seq bigint NOT NULL,
            payload_hash bytea NOT NULL,
            PRIMARY KEY(syncer_id, generation, seq))`,
		"CREATE TABLE IF NOT EXISTS " + meta + `.schema_stage (
            syncer_id text NOT NULL, generation text NOT NULL, schema_version text NOT NULL,
            source_schema text NOT NULL, table_name text NOT NULL, payload bytea NOT NULL,
            PRIMARY KEY(syncer_id, generation, schema_version, source_schema, table_name))`,
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
	err := t.pool.QueryRow(ctx, "SELECT generation, received_seq, applied_seq, schema_version FROM "+quote(t.cfg.Postgres.MetadataSchema)+".sync_state WHERE syncer_id=$1", t.cfg.ID).
		Scan(&p.Generation, &p.Received, &p.Applied, &p.SchemaVersion)
	return p, err
}

func (t *target) Store(ctx context.Context, message event.Message, raw []byte) error {
	return t.store(ctx, t.pool, message, raw)
}

type transactionBeginner interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

func (l *targetLease) Store(ctx context.Context, target *target, message event.Message, raw []byte) error {
	return target.store(ctx, l.conn, message, raw)
}

func (t *target) store(ctx context.Context, beginner transactionBeginner, message event.Message, raw []byte) error {
	if message.Seq > math.MaxInt64 {
		return errors.New("message sequence exceeds PostgreSQL bigint")
	}
	tx, err := beginner.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	meta := quote(t.cfg.Postgres.MetadataSchema)
	payloadHash := sha256.Sum256(raw)
	var current progress
	var phase string
	var openTransaction string
	var pendingSchemaVersion string
	if err := tx.QueryRow(ctx, "SELECT generation, received_seq, applied_seq, phase, open_transaction, schema_version, pending_schema_version FROM "+meta+".sync_state WHERE syncer_id=$1 FOR UPDATE", t.cfg.ID).
		Scan(&current.Generation, &current.Received, &current.Applied, &phase, &openTransaction, &current.SchemaVersion, &pendingSchemaVersion); err != nil {
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
		if _, err := tx.Exec(ctx, "DELETE FROM "+meta+".message_hash WHERE syncer_id=$1", t.cfg.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "DELETE FROM "+meta+".schema_stage WHERE syncer_id=$1", t.cfg.ID); err != nil {
			return err
		}
		current = progress{Generation: message.Generation, SchemaVersion: message.SchemaVersion}
		phase = ""
		openTransaction = ""
		pendingSchemaVersion = ""
	}
	if message.Seq <= current.Received {
		var storedHash []byte
		err := tx.QueryRow(ctx, "SELECT payload_hash FROM "+meta+".message_hash WHERE syncer_id=$1 AND generation=$2 AND seq=$3", t.cfg.ID, message.Generation, int64(message.Seq)).Scan(&storedHash)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err == nil && !bytes.Equal(storedHash, payloadHash[:]) {
			return fmt.Errorf("sequence %d was already stored with a different payload", message.Seq)
		}
		return tx.Commit(ctx)
	}
	if message.Seq != current.Received+1 {
		return fmt.Errorf("non-contiguous message: got %d, want %d", message.Seq, current.Received+1)
	}
	if strings.HasPrefix(message.Kind, "file_") {
		if err := t.validateFileMessage(message); err != nil {
			return err
		}
	}
	if message.Kind == "schema" && phase == "stream" {
		if message.SchemaVersion == "" {
			return errors.New("stream schema refresh requires schema_version")
		}
		if pendingSchemaVersion == "" {
			pendingSchemaVersion = message.SchemaVersion
		} else if pendingSchemaVersion != message.SchemaVersion {
			return errors.New("schema refresh version changed before schema_end")
		}
	} else if message.Kind == "schema_end" {
		if phase != "stream" || message.SchemaVersion == "" || message.SchemaVersion != pendingSchemaVersion {
			return errors.New("schema_end does not match pending schema refresh")
		}
	} else {
		if pendingSchemaVersion != "" {
			return errors.New("schema refresh must finish before row delivery")
		}
		if message.SchemaVersion != current.SchemaVersion {
			return fmt.Errorf("message schema_version %q does not match active version %q", message.SchemaVersion, current.SchemaVersion)
		}
	}
	switch message.Kind {
	case "transaction_rows":
		if message.Transaction == "" {
			return errors.New("transaction_rows requires a transaction identifier")
		}
		if openTransaction == "" {
			openTransaction = message.Transaction
		} else if openTransaction != message.Transaction {
			return fmt.Errorf("transaction %q is still open", openTransaction)
		}
	case "transaction_end":
		if message.Transaction == "" || openTransaction != message.Transaction {
			return fmt.Errorf("transaction_end %q does not match open transaction %q", message.Transaction, openTransaction)
		}
	case "file_begin", "file_chunk":
		if message.Transaction == "" || message.File == nil {
			return errors.New("file batch requires a transaction and file payload")
		}
		if openTransaction == "" {
			openTransaction = message.Transaction
		} else if openTransaction != message.Transaction {
			return fmt.Errorf("transaction %q is still open", openTransaction)
		}
	case "file_end":
		if message.Transaction == "" || message.File == nil || openTransaction != message.Transaction {
			return errors.New("file_end does not match the open file batch")
		}
	case "file_delete":
		if message.File == nil || openTransaction != "" {
			return errors.New("file_delete requires a payload and no open batch")
		}
	case "schema":
		if openTransaction != "" {
			return errors.New("schema cannot be applied inside a transaction")
		}
	}
	if err := t.applyMessage(ctx, tx, message, raw, &phase); err != nil {
		return err
	}
	if message.Kind == "transaction_end" || message.Kind == "file_end" {
		openTransaction = ""
	}
	if message.Kind == "schema_end" {
		current.SchemaVersion = pendingSchemaVersion
		pendingSchemaVersion = ""
	}
	if _, err := tx.Exec(ctx, "INSERT INTO "+meta+".message_hash(syncer_id,generation,seq,payload_hash) VALUES($1,$2,$3,$4)", t.cfg.ID, message.Generation, int64(message.Seq), payloadHash[:]); err != nil {
		return err
	}
	applied := current.Applied
	if message.Kind == "snapshot_end" || message.Kind == "transaction_end" || message.Kind == "schema_end" || message.Kind == "file_end" || message.Kind == "file_delete" {
		applied = message.Seq
	}
	if _, err := tx.Exec(ctx, "UPDATE "+meta+".sync_state SET generation=$2, received_seq=$3, applied_seq=$4, phase=$5, open_transaction=$6, schema_version=$7, pending_schema_version=$8, updated_at=clock_timestamp() WHERE syncer_id=$1",
		t.cfg.ID, message.Generation, int64(message.Seq), int64(applied), phase, openTransaction, current.SchemaVersion, pendingSchemaVersion); err != nil {
		return err
	}
	err = tx.Commit(ctx)
	if err == nil && (message.Kind == "snapshot_end" || message.Kind == "schema_end") {
		t.invalidateTypes()
	}
	return err
}

func (t *target) validateFileMessage(message event.Message) error {
	if t.files == nil || message.File == nil || message.File.Path == "" || !filepath.IsLocal(filepath.FromSlash(message.File.Path)) || strings.ContainsRune(message.File.Path, 0) {
		return errors.New("file event has an invalid or unconfigured path")
	}
	file := message.File
	switch message.Kind {
	case "file_delete":
		if file.Operation != "delete" || message.Transaction != "" || file.Data != "" {
			return errors.New("invalid file_delete envelope")
		}
		return nil
	case "file_begin", "file_chunk", "file_end":
		if file.Operation != "create" && file.Operation != "update" {
			return errors.New("file batch operation must be create or update")
		}
		if message.Transaction == "" || file.Size < 0 || file.Size > t.cfg.FileStorage.MaxFileBytes || file.Chunks == 0 {
			return errors.New("file batch metadata exceeds configured limits")
		}
		digest, err := hex.DecodeString(file.SHA256)
		if err != nil || len(digest) != sha256.Size {
			return errors.New("file batch SHA-256 is invalid")
		}
		if message.Kind != "file_chunk" && file.Data != "" {
			return errors.New("file content is only allowed in file_chunk")
		}
		return nil
	default:
		return errors.New("unsupported file message")
	}
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
		if message.SchemaVersion != "" {
			return t.stageSchema(ctx, tx, message)
		}
	case "snapshot_rows":
		if *phase != "snapshot" {
			return errors.New("snapshot rows received outside snapshot")
		}
		batch := &pgx.Batch{}
		for i, row := range message.Rows {
			payload, err := json.Marshal(row)
			if err != nil {
				return err
			}
			batch.Queue("INSERT INTO "+meta+".snapshot_rows(syncer_id,generation,seq,row_ordinal,payload) VALUES($1,$2,$3,$4,$5)",
				t.cfg.ID, message.Generation, int64(message.Seq), i, payload)
		}
		results := tx.SendBatch(ctx, batch)
		for range message.Rows {
			if _, err := results.Exec(); err != nil {
				_ = results.Close()
				return err
			}
		}
		if err := results.Close(); err != nil {
			return err
		}
	case "snapshot_end":
		if *phase != "snapshot" {
			return errors.New("snapshot_end received outside snapshot")
		}
		if message.SchemaVersion != "" {
			if err := t.applyStagedSchemas(ctx, tx, message.Generation, message.SchemaVersion); err != nil {
				return err
			}
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
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		for i := len(scope) - 1; i >= 0; i-- {
			table := scope[i]
			if _, err := tx.Exec(ctx, "DELETE FROM "+quote(t.cfg.Postgres.TargetSchema)+"."+quote(table.Name)); err != nil {
				return err
			}
		}
		var lastSeq, lastOrdinal int64 = -1, -1
		for {
			staged, err := tx.Query(ctx, "SELECT seq,row_ordinal,payload FROM "+meta+`.snapshot_rows
                WHERE syncer_id=$1 AND generation=$2 AND (seq,row_ordinal)>($3,$4)
                ORDER BY seq,row_ordinal LIMIT $5`, t.cfg.ID, message.Generation, lastSeq, lastOrdinal, applyPageSize)
			if err != nil {
				return err
			}
			batch := make([]event.Row, 0, applyPageSize)
			for staged.Next() {
				var payload []byte
				if err := staged.Scan(&lastSeq, &lastOrdinal, &payload); err != nil {
					staged.Close()
					return err
				}
				var row event.Row
				if err := json.Unmarshal(payload, &row); err != nil {
					staged.Close()
					return err
				}
				batch = append(batch, row)
			}
			iterationErr := staged.Err()
			staged.Close()
			if iterationErr != nil {
				return iterationErr
			}
			for _, row := range batch {
				row.Operation = "insert"
				if err := t.applyRow(ctx, tx, row); err != nil {
					return err
				}
			}
			if len(batch) < applyPageSize {
				break
			}
		}
		if _, err := tx.Exec(ctx, "DELETE FROM "+meta+".snapshot_rows WHERE syncer_id=$1", t.cfg.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "DELETE FROM "+meta+".snapshot_scope WHERE syncer_id=$1", t.cfg.ID); err != nil {
			return err
		}
		*phase = "stream"
	case "schema_end":
		if *phase != "stream" {
			return errors.New("schema_end received outside stream")
		}
		return t.applyStagedSchemas(ctx, tx, message.Generation, message.SchemaVersion)
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
		var lastSeq int64 = -1
		for {
			rows, err := tx.Query(ctx, "SELECT seq,payload FROM "+meta+".inbox WHERE syncer_id=$1 AND generation=$2 AND seq>$3 ORDER BY seq LIMIT $4", t.cfg.ID, message.Generation, lastSeq, applyPageSize)
			if err != nil {
				return err
			}
			type inboxChunk struct {
				seq     int64
				message event.Message
			}
			batch := make([]inboxChunk, 0, applyPageSize)
			for rows.Next() {
				var item inboxChunk
				var payload []byte
				if err := rows.Scan(&item.seq, &payload); err != nil {
					rows.Close()
					return err
				}
				lastSeq = item.seq
				if err := json.Unmarshal(payload, &item.message); err != nil {
					rows.Close()
					return err
				}
				batch = append(batch, item)
			}
			iterationErr := rows.Err()
			rows.Close()
			if iterationErr != nil {
				return iterationErr
			}
			for _, item := range batch {
				if item.message.Transaction != message.Transaction {
					continue
				}
				for _, row := range item.message.Rows {
					if err := t.applyRow(ctx, tx, row); err != nil {
						return err
					}
				}
				if _, err := tx.Exec(ctx, "DELETE FROM "+meta+".inbox WHERE syncer_id=$1 AND generation=$2 AND seq=$3", t.cfg.ID, message.Generation, item.seq); err != nil {
					return err
				}
			}
			if len(batch) < applyPageSize {
				break
			}
		}
	case "file_begin", "file_chunk":
		if *phase != "stream" || t.files == nil {
			return errors.New("file event received before snapshot or without file storage")
		}
		_, err := tx.Exec(ctx, "INSERT INTO "+meta+".inbox(syncer_id,generation,seq,message_id,payload) VALUES($1,$2,$3,$4,$5)",
			t.cfg.ID, message.Generation, int64(message.Seq), message.ID, raw)
		return err
	case "file_end":
		if *phase != "stream" || t.files == nil {
			return errors.New("file_end received before snapshot or without file storage")
		}
		if err := t.applyFile(ctx, tx, message); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "DELETE FROM "+meta+".inbox WHERE syncer_id=$1 AND generation=$2", t.cfg.ID, message.Generation)
		return err
	case "file_delete":
		if *phase != "stream" || t.files == nil || message.File.Operation != "delete" {
			return errors.New("invalid file_delete event")
		}
		if !t.cfg.AllowsFileOperation("delete") {
			return nil
		}
		return t.files.Delete(ctx, message.File.Path)
	default:
		return fmt.Errorf("unsupported message kind %q", message.Kind)
	}
	return nil
}

func (t *target) applyFile(ctx context.Context, tx pgx.Tx, end event.Message) (result error) {
	if end.File.Operation != "create" && end.File.Operation != "update" {
		return errors.New("file_end operation must be create or update")
	}
	if end.File.Size < 0 || end.File.Size > t.cfg.FileStorage.MaxFileBytes || end.File.SHA256 == "" || end.File.Chunks == 0 {
		return errors.New("file metadata exceeds configured limits")
	}
	temporary, err := os.CreateTemp("", ".go-sync-file-*")
	if err != nil {
		return err
	}
	defer func() {
		_ = temporary.Close()
		if err := os.Remove(temporary.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}()
	hash := sha256.New()
	written := int64(0)
	expectedChunk := uint32(0)
	rows, err := tx.Query(ctx, "SELECT payload FROM "+quote(t.cfg.Postgres.MetadataSchema)+".inbox WHERE syncer_id=$1 AND generation=$2 ORDER BY seq", t.cfg.ID, end.Generation)
	if err != nil {
		return err
	}
	defer rows.Close()
	seenBegin := false
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return err
		}
		var message event.Message
		if err := json.Unmarshal(payload, &message); err != nil {
			return err
		}
		if message.Transaction != end.Transaction || message.File == nil || message.File.Path != end.File.Path || message.File.Operation != end.File.Operation || message.File.Size != end.File.Size || message.File.SHA256 != end.File.SHA256 || message.File.Chunks != end.File.Chunks {
			return errors.New("file batch metadata is inconsistent")
		}
		switch message.Kind {
		case "file_begin":
			if seenBegin || expectedChunk != 0 {
				return errors.New("file batch has duplicate begin")
			}
			seenBegin = true
		case "file_chunk":
			if !seenBegin || message.File.Chunk != expectedChunk || expectedChunk >= end.File.Chunks {
				return errors.New("file chunks are not contiguous")
			}
			decoded, err := base64.StdEncoding.DecodeString(message.File.Data)
			if err != nil {
				return errors.New("file chunk is not valid base64")
			}
			if int64(len(decoded))+written > t.cfg.FileStorage.MaxFileBytes {
				return errors.New("file content exceeds configured limit")
			}
			if _, err := temporary.Write(decoded); err != nil {
				return err
			}
			_, _ = hash.Write(decoded)
			written += int64(len(decoded))
			expectedChunk++
		default:
			return errors.New("unexpected message in file batch")
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if !seenBegin || expectedChunk != end.File.Chunks || written != end.File.Size || hex.EncodeToString(hash.Sum(nil)) != end.File.SHA256 {
		return errors.New("file size, chunk count or SHA-256 does not match")
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if !t.cfg.AllowsFileOperation(end.File.Operation) {
		return nil
	}
	return t.files.Put(ctx, end.File.Path, temporary.Name(), os.FileMode(end.File.Mode), time.Unix(0, end.File.ModTimeUnixNano))
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
