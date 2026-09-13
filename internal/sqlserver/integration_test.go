//go:build integration

package sqlserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go-sync/internal/config"
	"go-sync/internal/delivery"
	"go-sync/internal/event"
	"go-sync/internal/queue"
	syncv1 "go-sync/internal/rpc/syncv1"
	"go-sync/internal/telemetry"
	"go-sync/internal/testsqlserver"
)

func TestSQLServerSnapshotCDCRecovery(t *testing.T) {
	runSQLServerSnapshotCDCRecovery(t, testsqlserver.Start)
}

func TestSQLServer2008R2SnapshotCDCRecovery(t *testing.T) {
	runSQLServerSnapshotCDCRecovery(t, testsqlserver.StartExternal2008)
}

func runSQLServerSnapshotCDCRecovery(t *testing.T, start func(*testing.T) (string, *sql.DB)) {
	t.Helper()
	dsn, db := start(t)
	// The parent covers container startup plus deliberate fence failure and the
	// successful retry. Each production operation still uses its own timeout.
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	exec := func(statement string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	exec(`CREATE TABLE dbo.go_sync_fence(token varchar(64) NOT NULL PRIMARY KEY);
 CREATE TABLE dbo.items(id bigint NOT NULL PRIMARY KEY,body nvarchar(max),amount decimal(38,18),
 payload varbinary(max),score float,flag bit,rv rowversion);
 CREATE TABLE dbo.details(id int NOT NULL PRIMARY KEY,label nvarchar(40));
 CREATE TABLE dbo.bag(n int,body nvarchar(max));
 INSERT dbo.items(id,body,amount,payload,score,flag) VALUES
 (9007199254740993,N'中文 😀  ',12345678901234567890.123456789012345678,0x00ff,0.1,1);
 INSERT dbo.details VALUES(1,N'before');
 INSERT dbo.bag VALUES(1,N'unchanged'),(1,N'unchanged');`)
	for _, name := range []string{"go_sync_fence", "items", "details", "bag"} {
		_, err := db.ExecContext(ctx, `EXEC sys.sp_cdc_enable_table @source_schema=N'dbo',
 @source_name=@p1,@role_name=NULL,@supports_net_changes=0`, name)
		if err != nil {
			t.Fatal(err)
		}
	}
	cfg := config.Defaults()
	cfg.SourceType, cfg.SourceID, cfg.DSN = "sqlserver", "sqlserver-test", dsn
	cfg.URL, cfg.DataDir = "http://127.0.0.1/unused", t.TempDir()
	cfg.ReserveBytes, cfg.BatchRows = 0, 1
	cfg.SQLServer.FenceTable = config.Table{Schema: "dbo", Name: "go_sync_fence"}
	cfg.Tables = []config.Table{{Schema: "dbo", Name: "items"}, {Schema: "dbo", Name: "details"}, {Schema: "dbo", Name: "bag"}}
	reconciled, err := Reconcile(ctx, cfg, &syncv1.ReconcileRequest{RequestId: "sqlserver-reconcile", SourceSchema: "dbo", Table: "bag", Buckets: 16})
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
	q, err := queue.Open(cfg.DataDir, cfg.QueueBytes, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := q.Close(); err != nil {
			t.Error(err)
		}
	})
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	meter := telemetry.New(q, cfg)
	c := New(cfg, q, log).WithMetrics(meter)
	info, err := inspect(ctx, db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.snapshot(ctx, db, info); err == nil {
		t.Fatal("snapshot acquired locks without opt-in")
	}
	st, err := q.State()
	if err != nil || st.Phase != "" {
		t.Fatal("disabled snapshot changed state")
	}
	c.cfg.SQLServer.AllowSnapshotLocks = true
	c.cfg.SQLServer.FenceTimeout = "1ms"
	if err := c.snapshot(ctx, db, info); err == nil {
		t.Fatal("expired fence attempt unexpectedly published a snapshot")
	}
	if _, _, err := q.Peek(); !errors.Is(err, queue.ErrEmpty) {
		t.Fatal("failed snapshot was published")
	}
	// Cancellation must release the table locks before retrying the baseline.
	lockCtx, release := context.WithTimeout(ctx, 3*time.Second)
	_, err = db.ExecContext(lockCtx, "UPDATE dbo.details SET label=label WHERE id=1")
	release()
	if err != nil {
		t.Fatalf("failed snapshot retained table locks: %v", err)
	}
	c.cfg.SQLServer.FenceTimeout = "2m"
	if err := c.snapshot(ctx, db, info); err != nil {
		t.Fatal(err)
	}
	assertMetric := func(sample string) {
		t.Helper()
		response := httptest.NewRecorder()
		meter.ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
		if response.Code != 200 || !strings.Contains("\n"+response.Body.String(), "\n"+sample+"\n") {
			t.Fatalf("missing metric %s", sample)
		}
	}
	assertMetric("go_sync_sqlserver_snapshot_active 0")
	assertMetric("go_sync_sqlserver_snapshot_failures_total 1")
	assertMetric("go_sync_sqlserver_snapshot_scanned_rows 4")
	assertMetric("go_sync_capture_snapshot_rows_total 4")
	assertMetric(`go_sync_sqlserver_snapshot_wait_duration_seconds_count{result="error",stage="fence"} 1`)
	stateAfterSnapshot, err := q.State()
	if err != nil {
		t.Fatal(err)
	}
	durable, err := parseLSN(stateAfterSnapshot.DurableLSN)
	if err != nil {
		t.Fatal(err)
	}
	health, err := readHealth(ctx, db, info.Tables, durable)
	if err != nil {
		t.Fatal(err)
	}
	if !health.CheckpointLagSeconds.Valid || !health.RetentionMarginSeconds.Valid {
		t.Fatalf("SQL Server health mapping unavailable: %+v", health)
	}
	c.publishHealth(health)
	assertMetric("go_sync_sqlserver_health_errors_total 0")
	drain := func() []event.Message {
		t.Helper()
		messages := []event.Message{}
		for {
			m, _, err := q.Peek()
			if errors.Is(err, queue.ErrEmpty) {
				return messages
			}
			if err != nil {
				t.Fatal(err)
			}
			messages = append(messages, m)
			if err := q.Ack(m.Seq, m.ID); err != nil {
				t.Fatal(err)
			}
		}
	}
	snapshot := drain()
	if snapshot[0].Kind != "snapshot_begin" || snapshot[len(snapshot)-1].Kind != "snapshot_end" {
		t.Fatal("snapshot boundaries missing")
	}
	seenItems, bagRows := false, 0
	for _, m := range snapshot {
		if m.Version != "v2" {
			t.Fatal("keyless source did not negotiate v2")
		}
		for _, row := range m.Rows {
			if row.Table == "bag" {
				bagRows++
			}
			if row.Table != "items" {
				continue
			}
			seenItems = true
			values := map[string]*string{}
			for _, col := range row.Columns {
				values[col.Name] = col.Value
			}
			for name, want := range map[string]string{"id": "9007199254740993", "body": "中文 😀  ",
				"amount": "12345678901234567890.123456789012345678", "payload": "0x00ff", "score": "0.1", "flag": "true"} {
				if values[name] == nil || *values[name] != want {
					t.Fatalf("snapshot %s = %v, want %s", name, values[name], want)
				}
			}
		}
	}
	if !seenItems || bagRows != 2 {
		t.Fatal("snapshot lost rows or keyless duplicates")
	}
	exec(`BEGIN TRANSACTION;
 UPDATE dbo.details SET label=N'after' WHERE id=1;
 UPDATE dbo.items SET id=9007199254740994,body=NULL WHERE id=9007199254740993;
 UPDATE TOP (1) dbo.bag SET n=2 WHERE n=1;
 SAVE TRANSACTION discarded;
 INSERT dbo.details VALUES(99,N'rolled back savepoint');
 ROLLBACK TRANSACTION discarded;
 COMMIT;
 BEGIN TRANSACTION;
 DELETE dbo.details WHERE id=1;
 ROLLBACK;`)
	for {
		progress, err := c.poll(ctx, db, info)
		if err != nil {
			t.Fatal(err)
		}
		if progress {
			break
		}
		if err := delivery.Wait(ctx, 100*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	st, err = q.State()
	if err != nil {
		t.Fatal(err)
	}
	pending := int(st.ReadySeq - st.DeliveredSeq)
	// SQL Server may encode a clustered primary-key update as DELETE + INSERT.
	if pending != 4 && pending != 5 {
		t.Fatalf("expected 3 updates (or PK delete/insert) and one transaction end: %+v", st)
	}
	// Resume published messages after reopening; preserve IDs and byte content.
	first, body, err := q.Peek()
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = queue.Open(cfg.DataDir, cfg.QueueBytes, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Recover(); err != nil {
		t.Fatal(err)
	}
	c.q = q
	after, restored, err := q.Peek()
	if err != nil || after.ID != first.ID || string(restored) != string(body) {
		t.Fatal("restart changed queued message")
	}
	var mu sync.Mutex
	requests := []event.Message{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m event.Message
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		mu.Lock()
		requests = append(requests, m)
		n := len(requests)
		mu.Unlock()
		if r.Header.Get("Idempotency-Key") != m.ID {
			t.Error("missing idempotency key")
		}
		if n == 1 {
			w.WriteHeader(503)
			return
		}
		if err := json.NewEncoder(w).Encode(event.Ack{ID: m.ID, Seq: m.Seq}); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	cfg.URL, cfg.RetryMin, cfg.RetryMax = server.URL, "1ms", "2ms"
	sendCtx, stopSender := context.WithCancel(ctx)
	finished := make(chan error, 1)
	go func() { finished <- delivery.New(cfg, log).Run(sendCtx, q) }()
	defer func() { stopSender(); <-finished }()
	for {
		current, err := q.State()
		if err != nil {
			t.Fatal(err)
		}
		if current.DeliveredSeq == st.ReadySeq {
			break
		}
		if err := delivery.Wait(ctx, 10*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	got := append([]event.Message{}, requests...)
	mu.Unlock()
	if len(got) != pending+1 || got[0].ID != got[1].ID {
		t.Fatalf("HTTP retry did not reuse ID: %d requests", len(got))
	}
	for i, m := range got[1:] {
		if m.Transaction != st.DurableLSN || m.Chunk != uint64(i) {
			t.Fatal("cross-table transaction split")
		}
		if i == pending-1 {
			if m.Kind != "transaction_end" {
				t.Fatal("missing transaction end")
			}
			continue
		}
		if len(m.Rows) != 1 || m.Rows[0].Ordinal != uint64(i+1) {
			t.Fatal("wrong ordinal")
		}
	}
	if got[1].Rows[0].Table != "details" || got[pending-1].Rows[0].Table != "bag" {
		t.Fatal("cross-table source order changed")
	}
	if got[1].Rows[0].Operation != "update" || *got[1].Rows[0].Key[0].Value != "1" {
		t.Fatal("rollback/savepoint changes leaked")
	}
	for _, message := range got[2 : pending-1] {
		row := message.Rows[0]
		if row.Table != "items" {
			t.Fatal("unexpected row in transaction")
		}
		if row.Operation == "delete" {
			if *row.Key[0].Value != "9007199254740993" {
				t.Fatal("wrong deleted primary key")
			}
			continue
		}
		if *row.Key[0].Value != "9007199254740994" {
			t.Fatal("wrong new primary key")
		}
		if row.Columns[1].Value != nil {
			t.Fatal("updated NULL not preserved")
		}
	}
	bag := got[pending-1].Rows[0]
	if bag.Identity != "full_row" || *bag.OldKey[1].Value != "unchanged" {
		t.Fatal("keyless old max value lost")
	}
	// Move one capture instance's retention floor past the persisted position.
	if err := c.insertFence(ctx, db, info.Fence, "retention-check"); err != nil {
		t.Fatal(err)
	}
	cut, err := c.waitFence(ctx, db, info.Fence, "retention-check")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `EXEC sys.sp_cdc_cleanup_change_table
 @capture_instance=N'dbo_items',@low_water_mark=@p1,@threshold=5000`, cut[:])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.poll(ctx, db, info); !errors.Is(err, errRetentionGap) {
		t.Fatalf("retention gap did not stop capture: %v", err)
	}
	afterGap, err := q.State()
	if err != nil || afterGap.DurableLSN != st.DurableLSN {
		t.Fatal("retention gap advanced checkpoint")
	}
	// Partitioned storage is rejected even when partition switching is disabled.
	ddl, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer ddl.Close()
	_, err = ddl.ExecContext(ctx, `SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
 CREATE PARTITION FUNCTION sync_pf(int) AS RANGE LEFT FOR VALUES(10);
 CREATE PARTITION SCHEME sync_ps AS PARTITION sync_pf ALL TO ([PRIMARY]);
 CREATE TABLE dbo.partitioned(n int) ON sync_ps(n);
 EXEC sys.sp_cdc_enable_table @source_schema=N'dbo',@source_name=N'partitioned',
 @role_name=NULL,@supports_net_changes=0,@allow_partition_switch=0;`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := readTable(ctx, db, config.Table{Schema: "dbo", Name: "partitioned"}); err == nil ||
		!strings.Contains(err.Error(), "partitioned") {
		t.Fatalf("partitioned source accepted: %v", err)
	}
	// A source schema change must fail before new data can be published.
	exec("ALTER TABLE dbo.details ADD extra int NULL")
	if _, err := c.poll(ctx, db, info); err == nil {
		t.Fatal("schema drift accepted")
	}
}
