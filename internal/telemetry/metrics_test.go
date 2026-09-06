package telemetry

import (
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go-sync/internal/config"
	"go-sync/internal/event"
	"go-sync/internal/queue"
)

func metricsFixture(t *testing.T) (*Metrics, *queue.Store, config.Config) {
	t.Helper()
	c := config.Defaults()
	c.DataDir = t.TempDir()
	c.DSN = "postgres://secret:password@private-host/private-db"
	c.URL = "https://private-receiver/cdc"
	c.Headers = map[string]string{"Authorization": "Bearer private-token"}
	q, err := queue.Open(c.DataDir, c.QueueBytes, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	if err := q.Initialize(queue.State{SourceID: "secret-source", Generation: "secret-generation"}); err != nil {
		t.Fatal(err)
	}
	return New(q, c), q, c
}

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	w := httptest.NewRecorder()
	m.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if w.Code != 200 {
		t.Fatalf("scrape: %d %s", w.Code, w.Body.String())
	}
	if !strings.HasPrefix(w.Header().Get("Content-Type"), "text/plain") {
		t.Fatal("missing content type")
	}
	return w.Body.String()
}

func requireSample(t *testing.T, body, sample string) {
	t.Helper()
	if !strings.Contains("\n"+body, "\n"+sample+"\n") {
		t.Errorf("missing metric %s", sample)
	}
}

func TestSQLServerCDCObservations(t *testing.T) {
	t.Parallel()
	m, _, _ := metricsFixture(t)
	m.CDCPoll(time.Millisecond, true, false)
	m.CDCPoll(time.Millisecond, false, true)
	body := scrape(t, m)
	requireSample(t, body, "go_sync_sqlserver_cdc_poll_duration_seconds_count 2")
	requireSample(t, body, "go_sync_sqlserver_cdc_retention_gaps_total 1")
	requireSample(t, body, "go_sync_capture_wal_payload_bytes_total 0")
	requireSample(t, body, "go_sync_wal_sample_timestamp_seconds 0")
	var disabled *Metrics
	disabled.CDCPoll(time.Second, false, true)
}

func TestQueueMetricsAndRegistryIsolation(t *testing.T) {
	m, q, c := metricsFixture(t)
	if err := q.Append(event.Message{Kind: "schema"}); err != nil {
		t.Fatal(err)
	}
	body := scrape(t, m)
	requireSample(t, body, "go_sync_queue_staged_messages 1")
	requireSample(t, body, "go_sync_queue_pending_messages 0")
	requireSample(t, body, "go_sync_queue_oldest_pending_age_seconds 0")
	if err := q.Publish("0/10", true); err != nil {
		t.Fatal(err)
	}
	body = scrape(t, m)
	requireSample(t, body, "go_sync_queue_staged_messages 0")
	requireSample(t, body, "go_sync_queue_pending_messages 1")
	requireSample(t, body, `go_sync_capture_phase{phase="stream"} 1`)
	m.TransactionCommitted(3)
	requireSample(t, scrape(t, m), "go_sync_capture_transaction_rows_total 3")
	// Recreating the registry resets counters without losing durable queue gauges.
	m2 := New(q, c)
	body = scrape(t, m2)
	requireSample(t, body, "go_sync_queue_pending_messages 1")
	requireSample(t, body, "go_sync_capture_transaction_rows_total 0")
	msg, _, err := q.Peek()
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Ack(msg.Seq, msg.ID); err != nil {
		t.Fatal(err)
	}
	body = scrape(t, m)
	requireSample(t, body, "go_sync_queue_bytes 0")
	requireSample(t, body, "go_sync_queue_pending_messages 0")
	requireSample(t, body, "go_sync_queue_oldest_pending_age_seconds 0")
	for _, secret := range []string{c.DSN, c.URL, c.DataDir, "private-token", "secret-source", "secret-generation"} {
		if strings.Contains(body, secret) {
			t.Errorf("metrics leaked private value")
		}
	}
	if !strings.Contains(body, "go_goroutines ") || !strings.Contains(body, "process_start_time_seconds ") {
		t.Fatal("missing runtime/process metrics")
	}
	problems, err := testutil.GatherAndLint(m.registry)
	if err != nil || len(problems) > 0 {
		t.Fatalf("metric lint: %v %v", problems, err)
	}
}

func TestObservationsAndConcurrentScrapes(t *testing.T) {
	m, _, _ := metricsFixture(t)
	m.CaptureAttempt()
	m.CaptureError(true)
	m.Streaming(true)
	m.Received(100)
	m.WAL(1000, 500)
	m.SchemaChanged()
	m.QueueFull()
	m.Backpressure(true)
	m.SnapshotCommitted(7, 2*time.Second)
	m.DeliveryRetry()
	m.InvalidACK()
	m.Delivered(128, true)
	failure := errors.New("private error contents")
	m.Delivery(time.Second, true, failure, false)
	m.Delivery(2*time.Second, false, failure, false)
	m.Delivery(3*time.Second, true, failure, true)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 10 {
				m.TransactionCommitted(2)
				m.Delivery(time.Second, false, nil, false)
				if _, err := m.registry.Gather(); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	body := scrape(t, m)
	for _, sample := range []string{
		"go_sync_capture_attempts_total 1", "go_sync_capture_errors_total 1", "go_sync_capture_reconnects_total 1",
		"go_sync_capture_streaming 1", "go_sync_capture_backpressure 1", "go_sync_capture_wal_payload_bytes_total 100",
		"go_sync_wal_retained_bytes 1000", "go_sync_capture_lag_bytes 500", "go_sync_schema_changes_total 1",
		"go_sync_capture_snapshots_total 1", "go_sync_capture_snapshot_rows_total 7",
		"go_sync_capture_transactions_total 40", "go_sync_capture_transaction_rows_total 80",
		`go_sync_http_requests_total{result="success"} 40`, `go_sync_http_requests_total{result="retryable_error"} 1`,
		`go_sync_http_requests_total{result="permanent_error"} 1`, `go_sync_http_requests_total{result="canceled"} 1`,
		"go_sync_http_retries_total 1", "go_sync_http_invalid_acks_total 1", "go_sync_delivery_messages_total 1",
		"go_sync_delivery_bytes_total 128", "go_sync_schema_delivered_total 1", "go_sync_http_request_duration_seconds_count 43",
		"go_sync_http_request_duration_seconds_sum 46", `go_sync_http_request_duration_seconds_bucket{le="+Inf"} 43`,
	} {
		requireSample(t, body, sample)
	}
}

func TestOpenMetricsAndScrapeFailure(t *testing.T) {
	m, q, c := metricsFixture(t)
	r := httptest.NewRequest("GET", "/metrics", nil)
	r.Header.Set("Accept", "application/openmetrics-text; version=1.0.0")
	w := httptest.NewRecorder()
	m.ServeHTTP(w, r)
	if w.Code != 200 || !strings.HasPrefix(w.Header().Get("Content-Type"), "application/openmetrics-text") || !strings.HasSuffix(w.Body.String(), "# EOF\n") {
		t.Fatal("OpenMetrics negotiation failed")
	}
	q.Close()
	w = httptest.NewRecorder()
	m.ServeHTTP(w, r)
	if w.Code != 500 {
		t.Fatalf("failed scrape returned %d", w.Code)
	}
	if strings.Contains(w.Body.String(), c.DataDir) {
		t.Fatal("error leaked queue path")
	}
}

func TestNilObserver(t *testing.T) {
	var m *Metrics
	m.CaptureAttempt()
	m.CaptureError(true)
	m.Streaming(true)
	m.Received(1)
	m.WAL(1, 1)
	m.SnapshotCommitted(1, time.Second)
	m.TransactionCommitted(1)
	m.SchemaChanged()
	m.QueueFull()
	m.Backpressure(true)
	m.DeliveryRetry()
	m.InvalidACK()
	m.Delivered(1, true)
	m.Delivery(time.Second, false, nil, false)
}
