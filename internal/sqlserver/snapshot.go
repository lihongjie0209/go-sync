package sqlserver

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"go-sync/internal/delivery"
	"go-sync/internal/event"
	"go-sync/internal/queue"
)

func (c *Collector) snapshot(ctx context.Context, db *sql.DB, info Inspection) (result error) {
	if !c.cfg.SQLServer.AllowSnapshotLocks {
		return errors.New("initial sqlserver snapshot requires explicit sqlserver.allow_snapshot_locks=true in a maintenance window")
	}
	duration, _ := time.ParseDuration(c.cfg.SQLServer.SnapshotTimeout)
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	started := time.Now()
	parentCtx := ctx
	c.metrics.SQLSnapshotStarted()
	defer func() { c.metrics.SQLSnapshotFinished(result != nil && parentCtx.Err() != context.Canceled) }()
	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	generation := hex.EncodeToString(token[:])
	st := queue.State{SourceID: c.cfg.SourceID, Generation: generation, Fingerprint: c.cfg.Fingerprint(),
		SystemID: info.SystemID, Database: info.Database, SchemaHash: schemaHash(info.Tables)}
	for _, table := range info.Tables {
		if table.RowIdentity == "full_row" {
			st.ProtocolVersion = "v2"
		}
	}
	if err := c.q.Initialize(st); err != nil {
		return err
	}
	// HOLDLOCK applies only to business tables below. A SERIALIZABLE transaction
	// would also retain catalog/CDC metadata locks and can prevent the capture
	// job from initializing its low watermark while we wait for the fence.
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return dbError(err)
	}
	defer rollback(tx, &result)
	c.log.WarnContext(ctx, "sqlserver initial snapshot acquiring table locks; selected-table writes may block")
	if err := c.snapshotLocks(ctx, tx, info.Tables); err != nil {
		return err
	}
	current, err := inspect(ctx, tx, c.cfg)
	if err != nil {
		c.observeSchemaError(err)
		return err
	}
	if err := c.sameSource(info, current); err != nil {
		return err
	}
	if schemaHash([]Table{info.Fence}) != schemaHash([]Table{current.Fence}) {
		c.metrics.SchemaChanged()
		return errors.New("sqlserver fence capture instance changed")
	}
	// Target writers are blocked. The independently committed marker identifies
	// the exact snapshot boundary even when CDC is behind the transaction log.
	if err := c.insertFence(ctx, db, info.Fence, generation); err != nil {
		return err
	}
	b := batch{cfg: c.cfg, q: c.q, metrics: c.metrics}
	refs := make([]event.TableRef, 0, len(info.Tables))
	for _, table := range info.Tables {
		refs = append(refs, event.TableRef{Schema: table.Schema, Name: table.Name})
	}
	if err := b.append(ctx, event.Message{Kind: "snapshot_begin", Tables: refs}); err != nil {
		return err
	}
	var total uint64
	for index, table := range info.Tables {
		tableStarted := time.Now()
		c.log.InfoContext(ctx, "sqlserver snapshot table started", "table_index", index+1, "tables", len(info.Tables))
		schema, err := json.Marshal(table)
		if err != nil {
			return err
		}
		if err := b.append(ctx, event.Message{Kind: "schema", Schema: schema}); err != nil {
			return err
		}
		b.message = event.Message{Kind: "snapshot_rows"}
		count, err := snapshotRows(ctx, tx, table, &b)
		if err != nil {
			return fmt.Errorf("sqlserver snapshot table %d: %w", index+1, err)
		}
		total += count
		if err := b.flush(ctx); err != nil {
			return err
		}
		c.metrics.SQLSnapshotTableCompleted()
		c.log.InfoContext(ctx, "sqlserver snapshot table staged", "table_index", index+1,
			"rows", count, "duration_seconds", time.Since(tableStarted).Seconds())
	}
	if err := b.append(ctx, event.Message{Kind: "snapshot_end"}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return dbError(err)
	}
	c.log.InfoContext(ctx, "sqlserver snapshot rows staged; table locks released", "rows", total)
	c.log.InfoContext(ctx, "sqlserver snapshot waiting for cdc fence")
	endWait := c.metrics.SQLSnapshotWait("fence")
	end, err := c.waitFence(ctx, db, info.Fence, generation)
	endWait(err == nil)
	if err != nil {
		return fmt.Errorf("sqlserver snapshot fence: %w", err)
	}
	if err := c.q.FinalizeSnapshotLSN(end.String()); err != nil {
		return err
	}
	c.log.InfoContext(ctx, "sqlserver snapshot fence captured", "lsn", end.String())
	if err := c.q.Publish(end.String(), true); err != nil {
		return err
	}
	c.metrics.SnapshotCommitted(total, time.Since(started))
	c.log.InfoContext(ctx, "sqlserver snapshot sealed", "rows", total, "lsn", end.String(),
		"duration_seconds", time.Since(started).Seconds())
	return nil
}

func (c *Collector) snapshotLocks(ctx context.Context, tx *sql.Tx, tables []Table) (result error) {
	finished := c.metrics.SQLSnapshotWait("locks")
	defer func() { finished(result == nil) }()
	for index, table := range tables {
		var one int
		err := tx.QueryRowContext(ctx, "SELECT TOP (1) 1 FROM "+table.sqlName()+" WITH (TABLOCKX,HOLDLOCK)").Scan(&one)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("sqlserver snapshot lock table %d: %w", index+1, dbError(err))
		}
	}
	c.log.InfoContext(ctx, "sqlserver snapshot table locks acquired", "tables", len(tables))
	return nil
}

func (c *Collector) insertFence(ctx context.Context, db *sql.DB, table Table, token string) error {
	if _, err := db.ExecContext(ctx, "INSERT INTO "+table.sqlName()+" ([token]) VALUES (@p1)", token); err != nil {
		return dbError(err)
	}
	return nil
}

func (c *Collector) waitFence(ctx context.Context, db *sql.DB, table Table, token string) (lsn, error) {
	duration, _ := time.ParseDuration(c.cfg.SQLServer.FenceTimeout)
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	// Markers are retained for diagnosis; only DBA cleanup may remove them.
	for {
		var raw []byte
		err := db.QueryRowContext(ctx, "SELECT [__$start_lsn] FROM "+table.changesName()+
			" WHERE [token]=@p1 AND [__$operation]=2", token).Scan(&raw)
		if err == nil {
			return binaryLSN(raw)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return lsn{}, dbError(err)
		}
		if err := delivery.Wait(ctx, 200*time.Millisecond); err != nil {
			return lsn{}, errors.New("cdc snapshot fence timed out; verify sql server agent and capture job")
		}
	}
}

func snapshotRows(ctx context.Context, q querier, table Table, b *batch) (count uint64, result error) {
	fields := make([]string, 0, len(table.Columns))
	for _, col := range table.Columns {
		fields = append(fields, projection(col))
	}
	rows, err := q.QueryContext(ctx, "SELECT "+strings.Join(fields, ",")+" FROM "+table.sqlName())
	if err != nil {
		return 0, dbError(err)
	}
	defer func() { result = errors.Join(result, dbError(rows.Close())) }()
	for rows.Next() {
		values := make([]*string, len(table.Columns))
		args := make([]any, len(values))
		for i := range values {
			args[i] = &values[i]
		}
		if err := rows.Scan(args...); err != nil {
			return count, dbError(err)
		}
		cols, err := toColumns(table, values)
		if err != nil {
			return count, err
		}
		key, err := rowKey(table, cols)
		if err != nil {
			return count, err
		}
		row := event.Row{Schema: table.Schema, Table: table.Name, Identity: table.RowIdentity,
			Operation: "read", Key: key, Columns: cols}
		if err := b.add(ctx, row); err != nil {
			return count, err
		}
		count++
		b.metrics.SQLSnapshotScanned()
	}
	return count, dbError(rows.Err())
}
