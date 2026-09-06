//go:build integration

package app_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"go-sync/internal/app"
	"go-sync/internal/capture"
	"go-sync/internal/config"
	"go-sync/internal/event"
	"go-sync/internal/testpg"
)

type replica struct {
	mu          sync.Mutex
	seen        map[string]bool
	rows        map[string]map[string]*string
	fullRows    map[string][]map[string]*string
	schemas     map[string]capture.Table
	versions    map[string]bool
	pending     map[string][]event.Row
	ended       bool
	schemaCount int
	dropped     bool
	duplicates  int
	last        uint64
	err         string
}

func (r *replica) serve(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var m event.Message
	if e := json.NewDecoder(req.Body).Decode(&m); e != nil {
		r.err = e.Error()
		w.WriteHeader(400)
		return
	}
	if m.Version != "v1" && m.Version != "v2" {
		r.err = "unsupported protocol version"
		w.WriteHeader(400)
		return
	}
	if r.versions == nil {
		r.versions = map[string]bool{}
	}
	r.versions[m.Version] = true
	if !r.seen[m.ID] {
		if m.Seq != r.last+1 {
			r.err = fmt.Sprintf("sequence gap: %d after %d", m.Seq, r.last)
			w.WriteHeader(400)
			return
		}
		r.seen[m.ID] = true
		r.last = m.Seq
		switch m.Kind {
		case "schema":
			r.schemaCount++
			var table capture.Table
			if err := json.Unmarshal(m.Schema, &table); err != nil {
				r.err = err.Error()
			}
			if r.schemas == nil {
				r.schemas = map[string]capture.Table{}
			}
			r.schemas[table.Name] = table
		case "snapshot_rows":
			for _, row := range m.Rows {
				r.apply(row)
			}
		case "snapshot_end":
			r.ended = true
		case "transaction_rows":
			r.pending[m.Transaction] = append(r.pending[m.Transaction], m.Rows...)
		case "transaction_end":
			for _, row := range r.pending[m.Transaction] {
				r.apply(row)
			}
			delete(r.pending, m.Transaction)
		}
	} else {
		r.duplicates++
	}
	if r.err != "" {
		w.WriteHeader(400)
		return
	}
	if m.Kind == "transaction_end" && !r.dropped {
		r.dropped = true
		conn, _, e := w.(http.Hijacker).Hijack()
		if e != nil {
			r.err = e.Error()
			return
		}
		conn.Close()
		return
	}
	json.NewEncoder(w).Encode(event.Ack{ID: m.ID, Seq: m.Seq})
}
func key(row event.Row, cols []event.Column) string {
	v := row.Table
	for _, c := range cols {
		if c.Value != nil {
			v += "/" + *c.Value
		}
	}
	return v
}
func (r *replica) apply(row event.Row) {
	if row.Identity == "full_row" {
		r.applyFull(row)
		return
	}
	k := key(row, row.Key)
	if row.Operation == "delete" {
		delete(r.rows, k)
		return
	}
	var values map[string]*string
	if len(row.OldKey) > 0 {
		oldKey := key(row, row.OldKey)
		values = r.rows[oldKey]
		delete(r.rows, oldKey)
	} else {
		values = r.rows[k]
	}
	if values == nil {
		values = map[string]*string{}
	}
	for _, c := range row.Columns {
		values[c.Name] = c.Value
	}
	r.rows[k] = values
}

// Full-row matching uses a multiset. An UPDATE/DELETE must affect exactly one
// matching row, even if multiple equal rows exist; NULL equals NULL for matching.
func (r *replica) applyFull(row event.Row) {
	if r.fullRows == nil {
		r.fullRows = map[string][]map[string]*string{}
	}
	rows := r.fullRows[row.Table]
	values := map[string]*string{}
	if row.Operation == "update" || row.Operation == "delete" {
		match := row.Key
		if row.Operation == "update" {
			match = row.OldKey
		}
		found := -1
		for i, candidate := range rows {
			equal := len(candidate) == len(match)
			for _, col := range match {
				value, ok := candidate[col.Name]
				if !ok || (value == nil) != (col.Value == nil) || (value != nil && col.Value != nil && *value != *col.Value) {
					equal = false
					break
				}
			}
			if equal {
				found = i
				values = candidate
				break
			}
		}
		if found < 0 {
			r.err = "full-row event did not match an existing row"
			return
		}
		rows = append(rows[:found], rows[found+1:]...)
	}
	if row.Operation != "delete" {
		for _, col := range row.Columns {
			values[col.Name] = col.Value
		}
		rows = append(rows, values)
	}
	r.fullRows[row.Table] = rows
}

func eventually(t *testing.T, ctx context.Context, fn func() bool) {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if fn() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("condition timed out")
		case <-ticker.C:
		}
	}
}

func TestPostgresSnapshotStreamAndRecovery(t *testing.T) {
	database := testpg.Start(t)
	dsn := database.DSN
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	conn, e := pgx.Connect(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { conn.Close(context.WithoutCancel(ctx)) }()
	id := strconv.FormatInt(time.Now().UnixNano(), 36)
	schema := "sync_test_" + id
	slot := "sync_test_" + id
	quoted := pgx.Identifier{schema}.Sanitize()
	_, e = conn.Exec(ctx, `CREATE SCHEMA `+quoted+`; CREATE TABLE `+quoted+`.items(id bigint PRIMARY KEY,body text,amount numeric,bin bytea,enabled boolean,info jsonb); ALTER TABLE `+quoted+`.items ALTER COLUMN body SET STORAGE EXTERNAL; CREATE TABLE `+quoted+`.pairs(a integer,b text,val text,PRIMARY KEY(a,b))`)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		endCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if _, e := conn.Exec(endCtx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1", slot); e != nil {
			t.Error(e)
		}
		if _, e := conn.Exec(endCtx, "DROP SCHEMA "+quoted+" CASCADE"); e != nil {
			t.Error(e)
		}
	}()
	_, e = conn.Exec(ctx, `INSERT INTO `+quoted+`.items VALUES (1,repeat('body',20000),12345678901234567890.123456789,'\x00ff',true,'{"x":1}'),(2,'delete me',null,null,false,null); INSERT INTO `+quoted+`.pairs VALUES (1,'a','old')`)
	if e != nil {
		t.Fatal(e)
	}
	r := &replica{seen: map[string]bool{}, rows: map[string]map[string]*string{}, pending: map[string][]event.Row{}}
	srv := httptest.NewServer(http.HandlerFunc(r.serve))
	defer srv.Close()
	c := config.Defaults()
	c.SourceID = "integration"
	c.DSN = dsn
	c.Slot = slot
	c.URL = srv.URL
	c.DataDir = t.TempDir()
	c.ReserveBytes = 0
	c.QueueBytes = 16 << 20
	c.MaxRowBytes = 1 << 20
	c.BatchBytes = 4096
	c.BatchRows = 1
	c.Tables = []config.Table{{Schema: schema, Name: "items"}, {Schema: schema, Name: "pairs"}}
	c.RetryMin = "20ms"
	c.RetryMax = "100ms"
	metricsPort, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c.MetricsAddr = metricsPort.Addr().String()
	if err := metricsPort.Close(); err != nil {
		t.Fatal(err)
	}
	metricsClient := &http.Client{Timeout: time.Second}
	defer metricsClient.CloseIdleConnections()
	metric := func(sample string) bool {
		req, err := http.NewRequestWithContext(ctx, "GET", "http://"+c.MetricsAddr+"/metrics", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := metricsClient.Do(req)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		return err == nil && resp.StatusCode == 200 && strings.Contains("\n"+string(body), "\n"+sample+"\n")
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	start := func() (context.CancelFunc, chan error) {
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- app.Run(runCtx, c, log) }()
		return stop, done
	}
	stop, done := start()
	defer func() {
		stop()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("collector did not stop")
		}
	}()
	wait := func(fn func() bool) {
		t.Helper()
		eventually(t, ctx, func() bool {
			select {
			case e := <-done:
				t.Fatalf("collector exited early: %v", e)
			default:
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.err != "" {
				t.Fatal(r.err)
			}
			return fn()
		})
	}
	wait(func() bool { return r.ended })
	eventually(t, ctx, func() bool {
		return metric("go_sync_capture_snapshot_rows_total 3") && metric("go_sync_schema_delivered_total 2") && metric("go_sync_capture_streaming 1")
	})
	r.mu.Lock()
	if r.schemaCount != 2 || len(r.rows) != 3 || *r.rows["items/1"]["amount"] != "12345678901234567890.123456789" || *r.rows["items/1"]["bin"] != "\\x00ff" || *r.rows["items/1"]["enabled"] != "true" {
		t.Errorf("incorrect snapshot: schemas=%d rows=%d", r.schemaCount, len(r.rows))
	}
	r.mu.Unlock()
	_, e = conn.Exec(ctx, `BEGIN; UPDATE `+quoted+`.items SET id=3,amount='NaN' WHERE id=1; DELETE FROM `+quoted+`.items WHERE id=2; INSERT INTO `+quoted+`.items(id,body) VALUES(9007199254740993,'new'); UPDATE `+quoted+`.pairs SET b='b',val='new' WHERE a=1; COMMIT; BEGIN; INSERT INTO `+quoted+`.items(id) VALUES(99); ROLLBACK`)
	if e != nil {
		t.Fatal(e)
	}
	wait(func() bool {
		return r.rows["items/3"] != nil && r.rows["items/9007199254740993"] != nil && r.rows["pairs/1/b"] != nil && r.duplicates > 0
	})
	r.mu.Lock()
	if len(r.rows) != 3 || len(*r.rows["items/3"]["body"]) != 80000 || *r.rows["items/3"]["amount"] != "NaN" {
		t.Errorf("bad update / toast preservation")
	}
	r.mu.Unlock()
	stop()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("stop timed out")
	}
	_, e = conn.Exec(ctx, "UPDATE "+quoted+".items SET body='while offline' WHERE id=3")
	if e != nil {
		t.Fatal(e)
	}
	stop, done = start()
	wait(func() bool { return r.rows["items/3"] != nil && *r.rows["items/3"]["body"] == "while offline" })
	eventually(t, ctx, func() bool {
		return metric("go_sync_capture_snapshots_total 0") && metric("go_sync_capture_streaming 1")
	})
	// Terminate only this test's WAL sender, then write while it reconnects.
	_, e = conn.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='go-sync' AND query LIKE $1`, "START_REPLICATION SLOT "+slot+" %")
	if e != nil {
		t.Fatal(e)
	}
	_, e = conn.Exec(ctx, "UPDATE "+quoted+".items SET body='after disconnect' WHERE id=3")
	if e != nil {
		t.Fatal(e)
	}
	wait(func() bool { return r.rows["items/3"] != nil && *r.rows["items/3"]["body"] == "after disconnect" })
	{
		if err := database.CrashRestart(ctx); err != nil {
			t.Fatal(err)
		}
		conn.Close(context.WithoutCancel(ctx))
		eventually(t, ctx, func() bool {
			candidate, e := pgx.Connect(ctx, dsn)
			if e != nil {
				return false
			}
			conn = candidate
			return true
		})
		_, e = conn.Exec(ctx, "UPDATE "+quoted+".items SET body='after postgres crash' WHERE id=3")
		if e != nil {
			t.Fatal(e)
		}
		wait(func() bool { return r.rows["items/3"] != nil && *r.rows["items/3"]["body"] == "after postgres crash" })
	}
	// Schema drift must fail visibly instead of acknowledging changed-shape rows.
	_, e = conn.Exec(ctx, "ALTER TABLE "+quoted+".items ADD COLUMN added text")
	if e != nil {
		t.Fatal(e)
	}
	select {
	case e := <-done:
		if e == nil {
			t.Error("schema drift did not fail")
		}
		done <- e
	case <-ctx.Done():
		t.Fatal("schema drift not detected")
	}
}
