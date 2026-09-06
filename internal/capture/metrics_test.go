package capture

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go-sync/internal/config"
	"go-sync/internal/event"
	"go-sync/internal/queue"
	"go-sync/internal/telemetry"
)

func TestBackpressureMetricClearsOnCancellation(t *testing.T) {
	c := config.Defaults()
	c.DataDir = t.TempDir()
	c.QueueBytes = 1200
	q, err := queue.Open(c.DataDir, c.QueueBytes, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.Initialize(queue.State{SourceID: "test", Generation: "g"}); err != nil {
		t.Fatal(err)
	}
	value := strings.Repeat("x", 600)
	msg := event.Message{Kind: "transaction_rows", Rows: []event.Row{{Columns: []event.Column{{Name: "body", Value: &value}}}}}
	if err := q.Append(msg); err != nil {
		t.Fatal(err)
	}
	if err := q.Publish("0/10", true); err != nil {
		t.Fatal(err)
	}
	meter := telemetry.New(q, c)
	b := batcher{cfg: c, q: q, metrics: meter}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- b.append(ctx, msg) }()
	active := false
	for ctx.Err() == nil {
		w := httptest.NewRecorder()
		meter.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
		if strings.Contains(w.Body.String(), "\ngo_sync_capture_backpressure 1\n") {
			active = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("unexpected append result %v", err)
	}
	if !active {
		t.Fatal("backpressure was never observable")
	}
	w := httptest.NewRecorder()
	meter.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(w.Body.String(), "\ngo_sync_capture_backpressure 0\n") {
		t.Fatal("backpressure did not clear")
	}
}

func TestSchemaMismatchMetric(t *testing.T) {
	d, q := fixture(t)
	meter := telemetry.New(q, d.b.cfg)
	d.b.metrics = meter
	if err := d.consume(t.Context(), []byte(`{"action":"B","xid":1,"nextlsn":"0/30"}`)); err != nil {
		t.Fatal(err)
	}
	err := d.consume(t.Context(), []byte(`{"action":"I","xid":1,"schema":"public","table":"items","columns":[{"name":"id","type":"text","typeoid":25,"value":"1"}]}`))
	if !errors.Is(err, errSchemaMismatch) {
		t.Fatalf("unexpected error: %v", err)
	}
	w := httptest.NewRecorder()
	meter.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	for _, sample := range []string{"go_sync_schema_changes_total 1", "go_sync_capture_transactions_total 0"} {
		if !strings.Contains(w.Body.String(), "\n"+sample+"\n") {
			t.Errorf("missing metric %s", sample)
		}
	}
}
