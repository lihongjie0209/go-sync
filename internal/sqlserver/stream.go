package sqlserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"go-sync/internal/event"
)

var errRetentionGap = errors.New("sqlserver cdc retention gap; stop and explicitly reinitialize from a new snapshot")

func getLSN(ctx context.Context, q querier, query string, args ...any) (lsn, error) {
	var b []byte
	if err := q.QueryRowContext(ctx, query, args...).Scan(&b); err != nil {
		return lsn{}, dbError(err)
	}
	return binaryLSN(b)
}

func validateRange(from, minimum lsn) error {
	if minimum == (lsn{}) {
		return errors.New("cdc minimum lsn unavailable; check capture instance and permissions")
	}
	if from.compare(minimum) < 0 {
		return errRetentionGap
	}
	return nil
}

func (c *Collector) poll(ctx context.Context, db *sql.DB, expected Inspection) (progress bool, result error) {
	started := time.Now()
	defer func() {
		c.metrics.CDCPoll(time.Since(started), result == nil, errors.Is(result, errRetentionGap))
		c.metrics.CDCPollOutcome(progress, result != nil, ctx.Err() != nil)
	}()
	st, err := c.q.State()
	if err != nil {
		return false, err
	}
	last, err := parseLSN(st.DurableLSN)
	if err != nil {
		return false, err
	}
	// Cleanup advances low watermarks before deleting CT rows. A single SNAPSHOT
	// transaction must cover validation and ALL tables; RCSI is insufficient.
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		return false, dbError(err)
	}
	defer rollback(tx, &result)
	current, err := inspect(ctx, tx, c.cfg)
	if err != nil {
		c.observeSchemaError(err)
		return false, err
	}
	if err := c.sameSource(expected, current); err != nil {
		return false, err
	}
	upper, err := getLSN(ctx, tx, "SELECT sys.fn_cdc_get_max_lsn()")
	if err != nil {
		return false, err
	}
	if upper.compare(last) < 0 {
		return false, errors.New("cdc high watermark moved behind local checkpoint")
	}
	if upper == last {
		return false, dbError(tx.Commit())
	}
	from, err := getLSN(ctx, tx, "SELECT sys.fn_cdc_increment_lsn(@p1)", last[:])
	if err != nil {
		return false, err
	}
	for _, table := range expected.Tables {
		minimum, err := getLSN(ctx, tx, "SELECT sys.fn_cdc_get_min_lsn(@p1)", table.CaptureInstance)
		if err != nil {
			return false, err
		}
		if err := validateRange(from, minimum); err != nil {
			return false, err
		}
	}
	parts := make([]string, 0, len(expected.Tables))
	for _, table := range expected.Tables {
		// Security: identifiers come from inspected metadata and are quoted. LSNs
		// are parameters. Direct CT access retains command_id on patched engines;
		// the all-changes TVF does not expose it. No CT table is ever modified.
		parts = append(parts, "SELECT MIN([__$start_lsn]) AS lsn FROM "+table.changesName()+
			" WHERE [__$start_lsn]>=@p1 AND [__$start_lsn]<=@p2")
	}
	var nextBytes []byte
	err = tx.QueryRowContext(ctx, "SELECT MIN(lsn) FROM ("+strings.Join(parts, " UNION ALL ")+") AS changes",
		from[:], upper[:]).Scan(&nextBytes)
	if err != nil {
		return false, dbError(err)
	}
	if len(nextBytes) == 0 {
		if err := tx.Commit(); err != nil {
			return false, dbError(err)
		}
		return false, c.q.Publish(upper.String(), false)
	}
	next, err := binaryLSN(nextBytes)
	if err != nil {
		return false, err
	}
	if next.compare(from) < 0 || next.compare(upper) > 0 {
		return false, errors.New("cdc transaction outside requested range")
	}
	b := batch{cfg: c.cfg, q: c.q, metrics: c.metrics,
		message: event.Message{Kind: "transaction_rows", Transaction: next.String(), LSN: next.String()}}
	count, err := readChanges(ctx, tx, expected.Tables, next, &b)
	if err != nil {
		return false, err
	}
	if count == 0 {
		return false, errors.New("cdc transaction disappeared during consistent read")
	}
	current, err = inspect(ctx, tx, c.cfg)
	if err != nil {
		c.observeSchemaError(err)
		return false, err
	}
	if err := c.sameSource(expected, current); err != nil {
		return false, err
	}
	if err := b.flush(ctx); err != nil {
		return false, err
	}
	if err := b.append(ctx, event.Message{Kind: "transaction_end", Transaction: next.String(),
		LSN: next.String(), Chunk: b.message.Chunk}); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, dbError(err)
	}
	if err := c.q.Publish(next.String(), false); err != nil {
		return false, err
	}
	c.metrics.TransactionCommitted(count)
	return true, nil
}

func (c *Collector) sameSource(expected, current Inspection) error {
	if expected.SystemID != current.SystemID || expected.Database != current.Database {
		return errors.New("sqlserver source identity changed")
	}
	if schemaHash(expected.Tables) != schemaHash(current.Tables) {
		c.metrics.SchemaChanged()
		return errors.New("sqlserver schema or capture instance changed; explicit reinitialization required")
	}
	return nil
}

func changesQuery(tables []Table) (string, int) {
	width := 0
	for _, table := range tables {
		width = max(width, len(table.Columns))
	}
	parts := make([]string, 0, len(tables))
	for i, table := range tables {
		command := "0"
		if table.HasCommandID {
			command = "[__$command_id]"
		}
		fields := []string{fmt.Sprintf("%d AS table_number", i), "[__$seqval] AS seqval",
			command + " AS command_id", "[__$operation] AS operation", "[__$update_mask] AS update_mask"}
		for n := range width {
			value := "CAST(NULL AS nvarchar(max))"
			if n < len(table.Columns) {
				value = projection(table.Columns[n])
			}
			fields = append(fields, fmt.Sprintf("%s COLLATE Latin1_General_BIN2 AS v%d", value, n))
		}
		parts = append(parts, "SELECT "+strings.Join(fields, ",")+" FROM "+table.changesName()+
			" WHERE [__$start_lsn]=@p1")
	}
	return strings.Join(parts, " UNION ALL ") + " ORDER BY command_id,seqval,table_number,operation", width
}

func readChanges(ctx context.Context, q querier, tables []Table, end lsn, b *batch) (count uint64, result error) {
	query, width := changesQuery(tables)
	rows, err := q.QueryContext(ctx, query, end[:])
	if err != nil {
		return 0, dbError(err)
	}
	defer func() { result = errors.Join(result, dbError(rows.Close())) }()
	var before, previous *change
	for rows.Next() {
		r := change{values: make([]*string, width)}
		var seq []byte
		args := []any{&r.table, &seq, &r.command, &r.operation, &r.mask}
		for i := range r.values {
			args = append(args, &r.values[i])
		}
		if err := rows.Scan(args...); err != nil {
			return count, dbError(err)
		}
		if r.table < 0 || r.table >= len(tables) {
			return count, errors.New("unexpected cdc table index")
		}
		r.sequence, err = binaryLSN(seq)
		if err != nil {
			return count, err
		}
		r.values = r.values[:len(tables[r.table].Columns)]
		// SQL Server 2008 R2 may encode a primary-key update as DELETE and
		// INSERT at the same seqval. operation is the final stable tie-breaker,
		// so only two rows with the same operation remain ambiguous.
		if previous != nil && samePosition(*previous, r) && previous.operation == r.operation {
			return count, errors.New("ambiguous cdc row ordering")
		}
		previous = &r
		if r.operation == 3 {
			if before != nil {
				return count, errors.New("cdc update before image without after image")
			}
			before = &r
			continue
		}
		if before != nil && (r.operation != 4 || !samePosition(*before, r)) {
			return count, errors.New("cdc update images do not match")
		}
		row, err := convertChange(tables[r.table], r, before)
		if err != nil {
			return count, err
		}
		before = nil
		count++
		row.Ordinal = count
		if err := b.add(ctx, row); err != nil {
			return count, err
		}
	}
	if err := rows.Err(); err != nil {
		return count, dbError(err)
	}
	if before != nil {
		return count, errors.New("cdc transaction ended without update after image")
	}
	return count, nil
}

func samePosition(a, b change) bool {
	return a.table == b.table && a.sequence == b.sequence && a.command == b.command
}
