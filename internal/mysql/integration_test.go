//go:build integration

package mysql

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go-sync/internal/config"
	"go-sync/internal/event"
	"go-sync/internal/queue"
	syncv1 "go-sync/internal/rpc/syncv1"
	"go-sync/internal/telemetry"
	"go-sync/internal/testmysql"
)

func TestMySQL56And57SnapshotAndBinlog(t *testing.T) {
	for _, version := range []string{"5.6", "5.7"} {
		version := version
		t.Run(version, func(t *testing.T) { testMySQL(t, version) })
	}
}

func TestMySQL55CompatibilityIsRejected(t *testing.T) {
	dsn, db := testmysql.Start(t, "5.5")
	if _, err := db.Execute("SELECT @@binlog_row_image"); err == nil {
		t.Fatal("MySQL 5.5 unexpectedly exposes binlog_row_image")
	}
	cfg := config.Defaults()
	cfg.SourceType, cfg.SourceID, cfg.DSN = "mysql", "mysql-5.5", dsn
	cfg.URL, cfg.DataDir = "http://127.0.0.1/unused", t.TempDir()
	cfg.Tables = []config.Table{{Schema: "go_sync_test", Name: "missing"}}
	if _, err := Inspect(t.Context(), cfg); err == nil {
		t.Fatal("collector accepted unsupported MySQL 5.5")
	}
}

func TestMySQL50And51OfficialImagesUnavailable(t *testing.T) {
	for _, version := range []string{"5.0", "5.1"} {
		version := version
		t.Run(version, func(t *testing.T) {
			t.Skipf("official mysql container registry has no %s image; CI cannot perform a genuine Testcontainers engine test", version)
		})
	}
}

func testMySQL(t *testing.T, version string) {
	fileRoot := t.TempDir()
	for name, content := range map[string]string{"same": "file-content", "after-ddl": "new-content"} {
		if err := os.WriteFile(filepath.Join(fileRoot, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	dsn, db := testmysql.Start(t, version)
	exec := func(sql string) {
		t.Helper()
		if _, err := db.Execute(sql); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE TABLE items(id BIGINT UNSIGNED NOT NULL PRIMARY KEY, body VARCHAR(100), amount DECIMAL(30,10), payload VARBINARY(20)) ENGINE=InnoDB")
	exec("CREATE TABLE bag(n INT, body VARCHAR(30)) ENGINE=InnoDB")
	exec("INSERT INTO items VALUES(18446744073709551610,'中文',12345678901234567890.1234567890,X'00FF')")
	exec("INSERT INTO bag VALUES(1,'same'),(1,'same')")
	cfg := config.Defaults()
	cfg.SourceType = "mysql"
	cfg.Transport = "grpc"
	cfg.SourceID = "mysql-" + version
	cfg.DSN = dsn
	cfg.URL = "http://127.0.0.1/unused"
	cfg.DataDir = t.TempDir()
	cfg.ReserveBytes = 0
	cfg.BatchRows = 1
	cfg.MySQL.ServerID = 177002
	cfg.MySQL.AllowSnapshotLock = true
	cfg.Tables = []config.Table{{Schema: "go_sync_test", Name: "items"}, {Schema: "go_sync_test", Name: "bag"}}
	cfg.FileColumns = []config.FileColumn{{Schema: "go_sync_test", Table: "bag", Column: "body", RootDir: fileRoot, MaxBytes: 1024}}
	q, err := queue.Open(cfg.DataDir, cfg.QueueBytes, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	collector := New(cfg, q, slog.New(slog.NewTextHandler(io.Discard, nil))).WithMetrics(telemetry.New(q, cfg))
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	info, err := Inspect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	reconciled, err := Reconcile(ctx, cfg, &syncv1.ReconcileRequest{RequestId: "mysql-reconcile", SourceSchema: "go_sync_test", Table: "bag", Buckets: 16})
	if err != nil {
		t.Fatal(err)
	}
	var reconciledRows uint64
	for _, digest := range reconciled.Digests {
		reconciledRows += digest.Rows
	}
	if reconciledRows != 2 || len(reconciled.PrimaryKeys) != 0 {
		t.Fatalf("unexpected keyless reconcile result: rows=%d keys=%v", reconciledRows, reconciled.PrimaryKeys)
	}
	if err = collector.snapshot(ctx, info); err != nil {
		t.Fatal(err)
	}
	messages := drain(t, q)
	if len(messages) < 6 || messages[0].Kind != "snapshot_begin" || messages[len(messages)-1].Kind != "snapshot_end" {
		t.Fatalf("invalid snapshot boundaries: %d messages", len(messages))
	}
	var snapshotRows int
	embedded := false
	for _, m := range messages {
		for _, row := range m.Rows {
			snapshotRows++
			if row.Table == "bag" && row.Identity != "full_row" {
				t.Fatal("keyless table missing full_row identity")
			}
			if row.Table == "bag" {
				for _, column := range row.Columns {
					if column.Name == "body" && column.Encoding == "base64" && column.SourceValue != nil && *column.SourceValue == "same" && column.Value != nil && *column.Value == "ZmlsZS1jb250ZW50" {
						embedded = true
					}
				}
			}
		}
	}
	if snapshotRows != 3 {
		t.Fatalf("snapshot rows=%d", snapshotRows)
	}
	if !embedded {
		t.Fatal("snapshot did not retain the path and embed file content")
	}
	st, _ := q.State()
	position, err := decodePosition(st.DurableLSN)
	if err != nil {
		t.Fatal(err)
	}
	streamCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- collector.stream(streamCtx, info, position) }()
	exec("START TRANSACTION")
	exec("UPDATE items SET id=18446744073709551609,body=NULL WHERE id=18446744073709551610")
	exec("UPDATE bag SET n=2 LIMIT 1")
	exec("COMMIT")
	exec("START TRANSACTION")
	exec("DELETE FROM bag")
	exec("ROLLBACK")
	deadline := time.Now().Add(30 * time.Second)
	for {
		state, _ := q.State()
		if state.ReadySeq > state.DeliveredSeq {
			break
		}
		select {
		case streamErr := <-done:
			t.Fatalf("stream stopped before publishing: %v", streamErr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for binlog transaction")
		}
		time.Sleep(100 * time.Millisecond)
	}
	changes := drain(t, q)
	rows := 0
	ends := 0
	for _, m := range changes {
		rows += len(m.Rows)
		if m.Kind == "transaction_end" {
			ends++
		}
	}
	if rows != 2 || ends != 1 {
		t.Fatalf("committed rows=%d ends=%d; rollback may have leaked", rows, ends)
	}
	exec("ALTER TABLE bag ADD COLUMN extra VARCHAR(20) NULL")
	exec("INSERT INTO bag VALUES(9,'after-ddl','new')")
	deadline = time.Now().Add(30 * time.Second)
	for {
		state, _ := q.State()
		// Full schema set, schema_end, one row chunk and transaction_end.
		if state.ReadySeq-state.DeliveredSeq >= uint64(len(cfg.Tables)+3) {
			break
		}
		select {
		case streamErr := <-done:
			t.Fatalf("stream stopped after DDL: %v", streamErr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for schema refresh")
		}
		time.Sleep(100 * time.Millisecond)
	}
	afterDDL := drain(t, q)
	schemaSeen, schemaEndSeen, rowSeen := false, false, false
	for _, message := range afterDDL {
		if message.Kind == "schema" {
			schemaSeen = message.SchemaVersion != ""
		}
		if message.Kind == "schema_end" {
			schemaEndSeen = message.SchemaVersion != ""
		}
		for _, row := range message.Rows {
			if row.Table == "bag" && len(row.Columns) == 3 {
				rowSeen = true
			}
		}
	}
	if !schemaSeen || !schemaEndSeen || !rowSeen {
		t.Fatalf("schema refresh or adapted row missing: %+v", afterDDL)
	}
	stop()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("stream shutdown: %v", err)
	}
}

func drain(t *testing.T, q *queue.Store) []event.Message {
	t.Helper()
	var out []event.Message
	for {
		m, _, err := q.Peek()
		if errors.Is(err, queue.ErrEmpty) {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, m)
		if err = q.Ack(m.Seq, m.ID); err != nil {
			t.Fatal(err)
		}
	}
}
