package sqlserverlegacy

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"go-sync/internal/event"
)

func (c *Collector) poll(ctx context.Context, db *sql.DB, expected Inspection) (progress bool, result error) {
	started := time.Now()
	defer func() {
		c.metrics.LegacyPoll(time.Since(started), progress, result, ctx.Err() != nil)
	}()
	current, err := inspect(ctx, db, c.cfg)
	if err != nil {
		return false, err
	}
	if current.SystemID != expected.SystemID || current.Database != expected.Database {
		return false, errors.New("sqlserver legacy source identity changed")
	}
	if schemaHash(current.Tables) != schemaHash(expected.Tables) {
		c.metrics.SchemaChanged()
		return false, errors.New("sqlserver legacy schema changed; explicit reinitialization is required")
	}
	_, events, values := names(c.cfg)
	state, err := c.q.State()
	if err != nil {
		return false, err
	}
	durable, err := parsePosition(state.DurableLSN)
	if err != nil {
		return false, err
	}
	b := batch{cfg: c.cfg, q: c.q, metrics: c.metrics}
	var ordinal uint64
	statement, maximum, count, err := readStatement(ctx, db, events, values, expected.Tables,
		func(statement string, statementMaximum int64, row event.Row) error {
			if ordinal == 0 {
				b.message = event.Message{Kind: "transaction_rows", Transaction: "legacy-statement:" + statement,
					LSN: position(max(durable, statementMaximum))}
			}
			ordinal++
			row.Ordinal = ordinal
			return b.add(ctx, row)
		})
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	checkpoint := position(max(durable, maximum))
	if err := b.flush(ctx); err != nil {
		return false, err
	}
	transaction := "legacy-statement:" + statement
	if err := b.append(ctx, event.Message{Kind: "transaction_end", Transaction: transaction, LSN: checkpoint, Chunk: b.message.Chunk}); err != nil {
		return false, err
	}
	if err := c.q.PublishSource(checkpoint, false, statement); err != nil {
		return false, err
	}
	// Publication must precede deletion. A crash in this window only replays the
	// source event, while delete-before-publish could lose it permanently.
	if err := deleteStatement(ctx, db, events, values, statement, false); err != nil {
		c.metrics.LegacyOutboxDelete(false)
		return false, err
	}
	if err := c.q.ClearSourceCleanup(statement); err != nil {
		return false, err
	}
	c.metrics.LegacyOutboxDelete(true)
	c.metrics.TransactionCommitted(count)
	return true, nil
}

func deleteStatement(ctx context.Context, db *sql.DB, events, values string, statement string, allowMissing bool) (result error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return dbError(err)
	}
	defer rollback(tx, &result)
	where := " WHERE change_id IN (SELECT change_id FROM " + events + " WHERE statement_id=CONVERT(uniqueidentifier,@p1))"
	if _, err := tx.ExecContext(ctx, "DELETE FROM "+values+where, statement); err != nil {
		return dbError(err)
	}
	deleted, err := tx.ExecContext(ctx, "DELETE FROM "+events+" WHERE statement_id=CONVERT(uniqueidentifier,@p1)", statement)
	if err != nil {
		return dbError(err)
	}
	count, err := deleted.RowsAffected()
	if err != nil {
		return dbError(err)
	}
	if count < 1 && !allowMissing {
		return errors.New("sqlserver legacy statement disappeared before acknowledgement")
	}
	if err := tx.Commit(); err != nil {
		return dbError(err)
	}
	return nil
}

func (c *Collector) finishSourceCleanup(ctx context.Context, db *sql.DB) error {
	state, err := c.q.State()
	if err != nil {
		return err
	}
	if state.SourceCleanup == "" {
		return nil
	}
	_, events, values := names(c.cfg)
	if err := deleteStatement(ctx, db, events, values, state.SourceCleanup, true); err != nil {
		c.metrics.LegacyOutboxDelete(false)
		return err
	}
	if err := c.q.ClearSourceCleanup(state.SourceCleanup); err != nil {
		return err
	}
	c.metrics.LegacyOutboxDelete(true)
	c.log.InfoContext(ctx, "sqlserver legacy source cleanup recovered")
	return nil
}

func deleteEvent(ctx context.Context, db *sql.DB, events, values string, id int64) (result error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return dbError(err)
	}
	defer rollback(tx, &result)
	if _, err := tx.ExecContext(ctx, "DELETE FROM "+values+" WHERE change_id=@p1", id); err != nil {
		return dbError(err)
	}
	resultExec, err := tx.ExecContext(ctx, "DELETE FROM "+events+" WHERE change_id=@p1", id)
	if err != nil {
		return dbError(err)
	}
	affected, err := resultExec.RowsAffected()
	if err != nil {
		return dbError(err)
	}
	if affected != 1 {
		return errors.New("sqlserver legacy outbox event disappeared before acknowledgement")
	}
	if err := tx.Commit(); err != nil {
		return dbError(err)
	}
	return nil
}

func (c *Collector) discardSnapshotEvents(ctx context.Context, db *sql.DB, checkpoint string) error {
	boundary, err := parsePosition(checkpoint)
	if err != nil {
		return err
	}
	_, events, values := names(c.cfg)
	for {
		var id int64
		err := db.QueryRowContext(ctx, "SELECT TOP 1 change_id FROM "+events+" WHERE change_id<=@p1 ORDER BY change_id", boundary).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return dbError(err)
		}
		if err := deleteEvent(ctx, db, events, values, id); err != nil {
			return err
		}
	}
}
