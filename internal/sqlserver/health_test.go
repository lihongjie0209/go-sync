package sqlserver

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
)

type healthResponse struct {
	values []driver.Value
	err    error
}

type healthConnector struct {
	responses []healthResponse
	queries   []string
	args      [][]driver.NamedValue
}

func (*healthConnector) Driver() driver.Driver { return scriptDriver{} }
func (c *healthConnector) Connect(context.Context) (driver.Conn, error) {
	return &healthConn{connector: c}, nil
}

type healthConn struct{ connector *healthConnector }

func (*healthConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not used") }
func (*healthConn) Begin() (driver.Tx, error)           { return nil, errors.New("read only") }
func (*healthConn) Close() error                        { return nil }
func (c *healthConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	index := len(c.connector.queries)
	c.connector.queries = append(c.connector.queries, query)
	c.connector.args = append(c.connector.args, args)
	if index >= len(c.connector.responses) {
		return nil, errors.New("unexpected query")
	}
	response := c.connector.responses[index]
	if response.err != nil {
		return nil, response.err
	}
	return &healthRows{values: response.values}, nil
}

type healthRows struct {
	values []driver.Value
	done   bool
}

func (*healthRows) Columns() []string { return []string{"first", "second"} }
func (*healthRows) Close() error      { return nil }
func (r *healthRows) Next(dest []driver.Value) error {
	if r.done || r.values == nil {
		return io.EOF
	}
	r.done = true
	copy(dest, r.values)
	return nil
}

func TestReadHealth(t *testing.T) {
	known := func(value float64) sql.NullFloat64 { return sql.NullFloat64{Float64: value, Valid: true} }
	durable := lsn{9: 5}
	for _, tc := range []struct {
		name      string
		responses []healthResponse
		want      healthSample
		wantError bool
	}{
		{
			name: "active capture",
			responses: []healthResponse{
				{values: []driver.Value{float64(12), float64(3600)}},
				{values: []driver.Value{float64(2), float64(3)}},
			},
			want: healthSample{known(2), known(3), known(12), known(3600)},
		},
		{
			name: "empty scan has no transaction latency",
			responses: []healthResponse{
				{values: []driver.Value{float64(0), float64(3600)}},
				{values: []driver.Value{float64(1), nil}},
			},
			want: healthSample{ScanAgeSeconds: known(1), CheckpointLagSeconds: known(0), RetentionMarginSeconds: known(3600)},
		},
		{
			name: "no mappings or scan sessions",
			responses: []healthResponse{
				{values: []driver.Value{nil, nil}}, {},
			},
		},
		{
			name: "unknown minimum stays unknown",
			responses: []healthResponse{
				{values: []driver.Value{float64(5), nil}}, {},
			},
			want: healthSample{CheckpointLagSeconds: known(5)},
		},
		{
			name: "dmv permission denied preserves position metrics",
			responses: []healthResponse{
				{values: []driver.Value{float64(5), float64(300)}},
				{err: errors.New("permission denied synthetic-secret")},
			},
			want: healthSample{CheckpointLagSeconds: known(5), RetentionMarginSeconds: known(300)}, wantError: true,
		},
		{
			name: "mapping failure still samples scan",
			responses: []healthResponse{
				{err: errors.New("mapping unavailable synthetic-secret")},
				{values: []driver.Value{float64(9), nil}},
			},
			want: healthSample{ScanAgeSeconds: known(9)}, wantError: true,
		},
		{
			name: "negative clock durations unknown and exhausted margin zero",
			responses: []healthResponse{
				{values: []driver.Value{float64(-4), float64(-10)}},
				{values: []driver.Value{float64(-2), float64(-3)}},
			},
			want: healthSample{RetentionMarginSeconds: known(0)},
		},
		{
			name: "partially scanned bad row discarded",
			responses: []healthResponse{
				{values: []driver.Value{float64(5), "invalid"}},
				{values: []driver.Value{float64(9), "invalid"}},
			},
			wantError: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			connector := &healthConnector{responses: tc.responses}
			db := sql.OpenDB(connector)
			t.Cleanup(func() { _ = db.Close() })
			sample, err := readHealth(t.Context(), db, []Table{{CaptureInstance: "dbo_items"}}, durable)
			if (err != nil) != tc.wantError {
				t.Fatalf("readHealth error = %v, want error %v", err, tc.wantError)
			}
			if err != nil && strings.Contains(err.Error(), "synthetic-secret") {
				t.Fatal("database diagnostic leaked into safe error")
			}
			if sample != tc.want {
				t.Errorf("sample = %+v, want %+v", sample, tc.want)
			}
		})
	}
}

func TestHealthQueriesParameterizeCaptureInstances(t *testing.T) {
	t.Parallel()
	durable := lsn{9: 9}
	name := "odd'); DELETE FROM dbo.items;--"
	query, args := healthPositionQuery([]Table{{CaptureInstance: name}, {CaptureInstance: "dbo_second"}}, durable)
	if strings.Contains(query, name) || !strings.Contains(query, "min_lsn(@p2)") || !strings.Contains(query, "min_lsn(@p3)") {
		t.Fatalf("capture instance is not safely parameterized: %s", query)
	}
	if len(args) != 3 || !bytes.Equal(args[0].([]byte), durable[:]) || args[1] != name || args[2] != "dbo_second" {
		t.Fatalf("unexpected query arguments: %#v", args)
	}
	if !strings.Contains(query, "m.total=m.known") || !strings.Contains(query, "MAX(min_time)") {
		t.Fatal("retention margin must include every table and use the tightest boundary")
	}
}

func TestReadHealthWithoutCheckpoint(t *testing.T) {
	t.Parallel()
	connector := &healthConnector{responses: []healthResponse{{values: []driver.Value{float64(1), nil}}}}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	sample, err := readHealth(t.Context(), db, nil, lsn{})
	if err != nil || !sample.ScanAgeSeconds.Valid || sample.CheckpointLagSeconds.Valid || sample.RetentionMarginSeconds.Valid {
		t.Fatalf("sample = %+v, error = %v", sample, err)
	}
	if len(connector.queries) != 1 || !strings.Contains(connector.queries[0], "sys.dm_cdc_log_scan_sessions") {
		t.Fatal("zero checkpoint should only query the scan DMV")
	}
}

func TestReadHealthCancellation(t *testing.T) {
	t.Parallel()
	connector := &healthConnector{}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	sample, err := readHealth(ctx, db, nil, lsn{9: 1})
	if !errors.Is(err, context.Canceled) || sample != (healthSample{}) {
		t.Fatalf("sample = %+v, error = %v", sample, err)
	}
}
