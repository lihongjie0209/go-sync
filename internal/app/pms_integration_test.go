//go:build integration

package app_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"go-sync/internal/app"
	"go-sync/internal/capture"
	"go-sync/internal/config"
	"go-sync/internal/event"
	"go-sync/internal/testpg"
)

func testConnection(t *testing.T, ctx context.Context, dsn string) *pgx.Conn {
	t.Helper()
	c, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	c.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	c.RuntimeParams["DateStyle"], c.RuntimeParams["TimeZone"] = "ISO, YMD", "UTC"
	c.RuntimeParams["extra_float_digits"], c.RuntimeParams["bytea_output"] = "3", "hex"
	c.RuntimeParams["standard_conforming_strings"] = "on"
	conn, err := pgx.ConnectConfig(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		end, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := conn.Close(end); err != nil {
			t.Error(err)
		}
	})
	return conn
}

func integrationConfig(t *testing.T, dsn, receiver string, tables []config.Table) config.Config {
	t.Helper()
	c := config.Defaults()
	c.SourceID, c.Slot, c.DSN, c.URL = "synthetic", "synthetic", dsn, receiver
	c.DataDir, c.Tables = t.TempDir(), tables
	c.QueueBytes, c.ReserveBytes = 64<<20, 0
	c.BatchRows, c.BatchBytes, c.MaxRowBytes = 7, 4096, 2<<20
	c.RetryMin, c.RetryMax = "10ms", "100ms"
	return c
}

// startApp always joins the collector before test cleanup can close the queue,
// receiver or Testcontainer, including a failed assertion in the test body.
func startApp(t *testing.T, ctx context.Context, c config.Config) func() {
	t.Helper()
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- app.Run(runCtx, c, slog.New(slog.NewTextHandler(io.Discard, nil))) }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("collector exited: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("collector did not stop")
		}
	}
	t.Cleanup(stop)
	return stop
}

func syntheticReplica(t *testing.T) (*replica, *httptest.Server) {
	t.Helper()
	r := &replica{seen: map[string]bool{}, rows: map[string]map[string]*string{}, pending: map[string][]event.Row{}}
	srv := httptest.NewServer(http.HandlerFunc(r.serve))
	t.Cleanup(srv.Close)
	return r, srv
}

func seedTable(t *testing.T, ctx context.Context, conn *pgx.Conn, table capture.Table, seed int) {
	t.Helper()
	var columns, placeholders []string
	var values []any
	for i, col := range table.Columns {
		columns = append(columns, pgx.Identifier{col.Name}.Sanitize())
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		var value any
		switch col.OID {
		case 20:
			value = int64(9007199254740993) + int64(seed)
		case 21, 23:
			value = seed + 10
		case 700:
			value = float32(seed) + .25
		case 25:
			value = fmt.Sprintf("人工测试-%d-quote'\\path\nline", seed)
		case 1043:
			value = fmt.Sprintf("测试%d", seed)
		case 17:
			value = []byte{0, byte(seed), 127, 255}
		case 1082:
			value = "2024-02-29"
		case 1114:
			value = fmt.Sprintf("2024-02-29 12:34:%02d.123456", seed)
		default:
			t.Fatalf("no synthetic generator for %s.%s type %s", table.Name, col.Name, col.Type)
		}
		if col.Name == "id" {
			value = seed
		}
		values = append(values, value)
	}
	query := "INSERT INTO " + table.SQL() + " (" + strings.Join(columns, ",") + ") VALUES (" + strings.Join(placeholders, ",") + ")"
	if _, err := conn.Exec(ctx, query, values...); err != nil {
		t.Fatalf("seed %s: %v", table.Name, err)
	}
}

func canonical(rows []map[string]*string) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		b, _ := json.Marshal(row)
		out = append(out, string(b))
	}
	sort.Strings(out)
	return out
}

func expectedRows(t *testing.T, ctx context.Context, conn *pgx.Conn, tables []capture.Table) map[string][]string {
	t.Helper()
	result := map[string][]string{}
	for _, table := range tables {
		var columns []string
		for _, col := range table.Columns {
			columns = append(columns, pgx.Identifier{col.Name}.Sanitize())
		}
		rows, err := conn.Query(ctx, "SELECT "+strings.Join(columns, ",")+" FROM "+table.SQL())
		if err != nil {
			t.Fatal(err)
		}
		var all []map[string]*string
		for rows.Next() {
			row := map[string]*string{}
			for i, raw := range rows.RawValues() {
				var value *string
				if raw != nil {
					v := string(raw)
					value = &v
				}
				row[table.Columns[i].Name] = value
			}
			all = append(all, row)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		result[table.Name] = canonical(all)
	}
	return result
}

func awaitReplica(t *testing.T, ctx context.Context, r *replica, expected map[string][]string) {
	t.Helper()
	eventually(t, ctx, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.err != "" {
			t.Fatal(r.err)
		}
		if !r.ended {
			return false
		}
		for table, want := range expected {
			all := append([]map[string]*string(nil), r.fullRows[table]...)
			for key, row := range r.rows {
				if strings.HasPrefix(key, table+"/") {
					all = append(all, row)
				}
			}
			if !reflect.DeepEqual(canonical(all), want) {
				return false
			}
		}
		return true
	})
}

func TestPMSSchemaAllTables(t *testing.T) {
	database := testpg.Start(t)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	conn := testConnection(t, ctx, database.DSN)
	if err := testpg.LoadPMS(ctx, conn); err != nil {
		t.Fatal(err)
	}
	r, srv := syntheticReplica(t)
	c := integrationConfig(t, database.DSN, srv.URL, testpg.PMSTables())
	if len(c.Tables) != 57 {
		t.Fatalf("PMS tables=%d", len(c.Tables))
	}
	for _, name := range []string{"cardceilpayinfo", "metainfo", "syncskipinfo"} {
		probe := c
		probe.Tables = []config.Table{{Schema: "public", Name: name}}
		if _, err := capture.Inspect(ctx, probe); err == nil || !strings.Contains(err.Error(), "REPLICA IDENTITY FULL") {
			t.Fatalf("keyless DEFAULT must be rejected with guidance: %v", err)
		}
		if _, err := conn.Exec(ctx, "ALTER TABLE "+pgx.Identifier{"public", name}.Sanitize()+" REPLICA IDENTITY FULL"); err != nil {
			t.Fatal(err)
		}
	}
	info, err := capture.Inspect(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	keyless, columns := 0, 0
	for _, table := range info.Tables {
		if table.RowIdentity == "full_row" {
			keyless++
		}
		columns += len(table.Columns)
		seedTable(t, ctx, conn, table, 1)
		seedTable(t, ctx, conn, table, 2)
	}
	if keyless != 3 || columns != 721 {
		t.Fatalf("schema changed: keyless=%d columns=%d", keyless, columns)
	}
	stop := startApp(t, ctx, c)
	awaitReplica(t, ctx, r, expectedRows(t, ctx, conn, info.Tables))
	r.mu.Lock()
	if r.schemaCount != 57 || !r.versions["v2"] || r.versions["v1"] || len(r.schemas) != 57 {
		t.Error("missing v2 schema metadata")
	}
	for _, table := range info.Tables {
		if !reflect.DeepEqual(r.schemas[table.Name], table) {
			t.Errorf("metadata differs for %s", table.Name)
		}
	}
	r.mu.Unlock()
	// Change primary keys and full-row identities, insert/delete, and roll back
	// synthetic work for every table in the supplied schema.
	if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	for _, table := range info.Tables {
		if _, err := conn.Exec(ctx, "UPDATE "+table.SQL()+" SET id=101 WHERE id=1; DELETE FROM "+table.SQL()+" WHERE id=2"); err != nil {
			t.Fatal(err)
		}
		seedTable(t, ctx, conn, table, 3)
	}
	if _, err := conn.Exec(ctx, "COMMIT; BEGIN; UPDATE public.metainfo SET id=999; ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	awaitReplica(t, ctx, r, expectedRows(t, ctx, conn, info.Tables))
	eventually(t, ctx, func() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.duplicates > 0 })
	stop()
	if _, err := conn.Exec(ctx, "UPDATE public.metainfo SET metavalue=NULL WHERE id=101; DELETE FROM public.syncskipinfo WHERE id=101"); err != nil {
		t.Fatal(err)
	}
	startApp(t, ctx, c)
	awaitReplica(t, ctx, r, expectedRows(t, ctx, conn, info.Tables))
	for _, name := range []string{"metainfo", "syncskipinfo"} {
		if _, err := conn.Exec(ctx, "INSERT INTO public."+name+" (id) VALUES(500)"); err != nil {
			t.Fatal(err)
		}
	}
	awaitReplica(t, ctx, r, expectedRows(t, ctx, conn, info.Tables))
}

func TestKeylessDuplicatesNullAndToast(t *testing.T) {
	database := testpg.Start(t)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	conn := testConnection(t, ctx, database.DSN)
	if _, err := conn.Exec(ctx, `CREATE TABLE public.bag(n integer, body text, blob bytea);
ALTER TABLE public.bag REPLICA IDENTITY FULL;
ALTER TABLE public.bag ALTER COLUMN body SET STORAGE EXTERNAL;
ALTER TABLE public.bag ALTER COLUMN blob SET STORAGE EXTERNAL;
INSERT INTO public.bag VALUES(NULL,NULL,NULL),(NULL,NULL,NULL),
(1,repeat('synthetic-toast',5000),decode(repeat('00ff',4000),'hex'));`); err != nil {
		t.Fatal(err)
	}
	r, srv := syntheticReplica(t)
	c := integrationConfig(t, database.DSN, srv.URL, []config.Table{{Schema: "public", Name: "bag"}})
	info, err := capture.Inspect(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	startApp(t, ctx, c)
	awaitReplica(t, ctx, r, expectedRows(t, ctx, conn, info.Tables))
	// ctid is used only by this test to select one source row. It is never part
	// of the collector protocol or persisted as a replication identity.
	for _, sql := range []string{
		"DELETE FROM public.bag WHERE ctid=(SELECT ctid FROM public.bag WHERE n IS NULL LIMIT 1)",
		"INSERT INTO public.bag VALUES(NULL,NULL,NULL); UPDATE public.bag SET n=2 WHERE ctid=(SELECT ctid FROM public.bag WHERE n IS NULL LIMIT 1)",
		"UPDATE public.bag SET n=3 WHERE n=1",
		"UPDATE public.bag SET body=NULL WHERE n=3",
		"UPDATE public.bag SET body=repeat('new-toast',9000) WHERE n=3",
		"BEGIN; UPDATE public.bag SET n=4 WHERE n=3; UPDATE public.bag SET n=5 WHERE n=4; INSERT INTO public.bag VALUES(9,NULL,NULL); UPDATE public.bag SET n=10 WHERE n=9; DELETE FROM public.bag WHERE n=10; COMMIT",
		"BEGIN; DELETE FROM public.bag; ROLLBACK",
		"DELETE FROM public.bag WHERE n=5",
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
		awaitReplica(t, ctx, r, expectedRows(t, ctx, conn, info.Tables))
	}
	eventually(t, ctx, func() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.duplicates > 0 })
}
