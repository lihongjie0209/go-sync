//go:build integration

package sqlserverlegacy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"go-sync/internal/config"
	"go-sync/internal/event"
	"go-sync/internal/queue"
	"go-sync/internal/testsqlserver"
)

// A modern disposable engine cannot certify SQL Server 2000 TDS compatibility,
// but it does execute the legacy catalog SQL, DDL and generated triggers.
func TestLegacyOutboxInstallationAndRollback(t *testing.T) {
	dsn, db := testsqlserver.Start(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	if _, err := db.ExecContext(ctx, `CREATE TABLE dbo.legacy_items(
 id int NOT NULL PRIMARY KEY,label nvarchar(40) NULL,amount decimal(18,4) NULL,payload varbinary(20) NULL)`); err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.SourceType, cfg.SourceID, cfg.DSN = "sqlserver_legacy", "legacy-test", dsn
	cfg.URL = "http://127.0.0.1/unused"
	cfg.Tables = []config.Table{{Schema: "dbo", Name: "legacy_items"}}
	table, err := readTable(ctx, db, cfg, cfg.Tables[0], true)
	if err != nil {
		var databaseErr *databaseError
		if errors.As(err, &databaseErr) {
			t.Fatalf("read legacy catalog: %v", databaseErr.cause)
		}
		t.Fatal(err)
	}
	if err := install(ctx, db, cfg, []Table{table}); err != nil {
		t.Fatal(err)
	}
	_, events, values := names(cfg)
	if _, err := db.ExecContext(ctx, `BEGIN TRANSACTION
 INSERT dbo.legacy_items VALUES(1,N'rolled back',1.2500,0x00ff)
 ROLLBACK TRANSACTION`); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+events).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("rollback left %d outbox events", count)
	}
	if _, err := db.ExecContext(ctx, `INSERT dbo.legacy_items VALUES(1,N'中文',1.2500,0x00ff)`); err != nil {
		t.Fatal(err)
	}
	id, row, err := readEvent(ctx, db, events, values, []Table{table})
	if err != nil {
		t.Fatal(err)
	}
	if id < 1 || row.Operation != "insert" || len(row.Columns) != 4 || row.Columns[1].Value == nil || *row.Columns[1].Value != "中文" {
		t.Fatalf("unexpected captured row: id=%d row=%+v", id, row)
	}
	if row.Columns[2].Value == nil || *row.Columns[2].Value != "1.2500" || row.Columns[3].Value == nil || *row.Columns[3].Value != "0x00ff" {
		t.Fatalf("decimal or binary value was not lossless: %+v", row.Columns)
	}
	if err := deleteEvent(ctx, db, events, values, id); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+events).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("delete left %d outbox events", count)
	}
	if _, err := db.ExecContext(ctx, `INSERT dbo.legacy_items VALUES
 (2,N'a',2.0000,0x02),(3,N'b',3.0000,0x03);
 UPDATE dbo.legacy_items SET amount=amount+1 WHERE id IN (2,3)`); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+events).Scan(&count); err != nil {
		t.Fatal(err)
	}
	// Two inserts plus before/after images for both updated rows.
	if count != 6 {
		t.Fatalf("multirow insert/update produced %d events, want 6", count)
	}
	var rows int
	statement, _, captured, err := readStatement(ctx, db, events, values, []Table{table}, func(gotStatement string, _ int64, row event.Row) error {
		if gotStatement == "" || row.Operation == "" {
			t.Fatal("statement identity or operation missing")
		}
		rows++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// The oldest statement is the two-row INSERT; its rows stay in one batch.
	if captured != 2 || rows != 2 {
		t.Fatalf("statement captured %d/%d rows", captured, rows)
	}
	if err := deleteStatement(ctx, db, events, values, statement, false); err != nil {
		t.Fatal(err)
	}
	var pendingStatement string
	if err := db.QueryRowContext(ctx, "SELECT TOP 1 CONVERT(varchar(36),statement_id) FROM "+events+" ORDER BY change_id").Scan(&pendingStatement); err != nil {
		t.Fatal(err)
	}
	store, err := queue.Open(t.TempDir(), 1<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := store.Initialize(queue.State{SourceID: "legacy", Generation: "g"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(event.Message{Kind: "transaction_end"}); err != nil {
		t.Fatal(err)
	}
	if err := store.PublishSource(position(100), true, pendingStatement); err != nil {
		t.Fatal(err)
	}
	collector := New(cfg, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := collector.finishSourceCleanup(ctx, db); err != nil {
		t.Fatal(err)
	}
	state, err := store.State()
	if err != nil || state.SourceCleanup != "" || state.ReadySeq != 1 || state.NextSeq != 1 {
		t.Fatalf("cleanup recovery changed local publication: %+v, %v", state, err)
	}
}
