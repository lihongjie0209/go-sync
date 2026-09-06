package sqlserver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"

	mssql "github.com/microsoft/go-mssqldb"
	"go-sync/internal/config"
	"go-sync/internal/event"
	"go-sync/internal/queue"
)

// A database/sql row-stream double; this tests assembly, not SQL correctness.
type rowScript struct {
	data     [][]driver.Value
	terminal error
}
type scriptDriver struct{}
type scriptConn struct{ script rowScript }
type scriptConnector struct{ script rowScript }
type scriptRows struct {
	script rowScript
	index  int
}

func (scriptDriver) Open(string) (driver.Conn, error) { return nil, errors.New("not used") }
func (c scriptConnector) Driver() driver.Driver       { return scriptDriver{} }
func (c scriptConnector) Connect(context.Context) (driver.Conn, error) {
	return &scriptConn{script: c.script}, nil
}
func (c *scriptConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not used") }
func (c *scriptConn) Begin() (driver.Tx, error)           { return nil, errors.New("not used") }
func (c *scriptConn) Close() error                        { return nil }
func (c *scriptConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &scriptRows{script: c.script}, nil
}
func (r *scriptRows) Columns() []string {
	return []string{"table_number", "seqval", "command_id", "operation", "mask", "v0"}
}
func (r *scriptRows) Close() error { return nil }
func (r *scriptRows) Next(dest []driver.Value) error {
	if r.index < len(r.script.data) {
		copy(dest, r.script.data[r.index])
		r.index++
		return nil
	}
	if r.script.terminal != nil {
		return r.script.terminal
	}
	return io.EOF
}

func TestCDCAssemblyNeverPublishesPartialTransaction(t *testing.T) {
	record := func(table, seq, op int) []driver.Value {
		position := lsn{9: byte(seq)}
		return []driver.Value{int64(table), position[:], int64(0), int64(op), []byte{1}, "1"}
	}
	for _, tc := range []struct {
		name      string
		script    rowScript
		wantRows  uint64
		wantError bool
	}{
		{name: "cross table", script: rowScript{data: [][]driver.Value{record(0, 1, 2), record(1, 2, 3), record(1, 2, 4)}}, wantRows: 2},
		{name: "orphan after", script: rowScript{data: [][]driver.Value{record(0, 1, 4)}}, wantError: true},
		{name: "orphan before", script: rowScript{data: [][]driver.Value{record(0, 1, 3)}}, wantError: true},
		{name: "wrong update table", script: rowScript{data: [][]driver.Value{record(0, 1, 3), record(1, 1, 4)}}, wantError: true},
		{name: "duplicate position", script: rowScript{data: [][]driver.Value{record(0, 1, 2), record(0, 1, 2)}}, wantRows: 1, wantError: true},
		{name: "connection interrupted", script: rowScript{data: [][]driver.Value{record(0, 1, 2)}, terminal: io.ErrUnexpectedEOF}, wantRows: 1, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := sql.OpenDB(scriptConnector{script: tc.script})
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			cfg := config.Defaults()
			cfg.BatchRows = 1
			q, err := queue.Open(t.TempDir(), 1<<20, 0)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := q.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := q.Initialize(queue.State{SourceID: "test", Generation: "g"}); err != nil {
				t.Fatal(err)
			}
			start, end := lsn{9: 1}, lsn{9: 2}
			if err := q.Publish(start.String(), true); err != nil {
				t.Fatal(err)
			}
			b := batch{cfg: cfg, q: q, message: event.Message{Kind: "transaction_rows", Transaction: end.String()}}
			tables := []Table{
				{Schema: "dbo", Name: "a", Columns: []Column{{Name: "id", Type: "int", BaseType: "int", Ordinal: 1, Primary: true}}},
				{Schema: "dbo", Name: "b", Columns: []Column{{Name: "id", Type: "int", BaseType: "int", Ordinal: 1, Primary: true}}},
			}
			count, err := readChanges(t.Context(), db, tables, end, &b)
			if (err != nil) != tc.wantError || count != tc.wantRows {
				t.Fatalf("count=%d error=%v", count, err)
			}
			if _, _, err := q.Peek(); !errors.Is(err, queue.ErrEmpty) {
				t.Fatal("transaction was published by row reader")
			}
			if err := q.Recover(); err != nil {
				t.Fatal(err)
			}
			st, err := q.State()
			if err != nil {
				t.Fatal(err)
			}
			if st.Bytes != 0 || st.NextSeq != 0 || st.DurableLSN != start.String() {
				t.Fatalf("incomplete transaction survived: %+v", st)
			}
		})
	}
}

func TestDatabaseErrorsAreRedactedAndRetryClassified(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		cause error
		retry bool
	}{
		{name: "server error", cause: mssql.Error{Number: 229, Message: "secret-row-value"}},
		{name: "deadlock", cause: mssql.Error{Number: 1205, Message: "secret-row-value"}, retry: true},
		{name: "disconnect", cause: io.ErrUnexpectedEOF, retry: true},
		{name: "dsn error", cause: errors.New("password=secret-row-value")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := dbError(tc.cause)
			if strings.Contains(err.Error(), "secret-row-value") {
				t.Fatal("database error exposed sensitive data")
			}
			if retryable(err) != tc.retry {
				t.Fatal("wrong retry classification")
			}
		})
	}
	if retryable(context.DeadlineExceeded) {
		t.Fatal("non-database timeout retried")
	}
	if retryable(errRetentionGap) {
		t.Fatal("retention gap retried")
	}
}
