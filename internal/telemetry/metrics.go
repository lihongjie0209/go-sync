// Package telemetry exposes instance-local Prometheus metrics. Observation never
// participates in checkpointing or changes the delivery protocol.
package telemetry

import (
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go-sync/internal/config"
	"go-sync/internal/queue"
)

// Metrics is concurrency-safe. A nil *Metrics disables observation. Counters
// reset on recreation; queue gauges are read from durable state at scrape time.
type Metrics struct {
	registry                                                          *prometheus.Registry
	handler                                                           http.Handler
	streaming, backpressure                                           prometheus.Gauge
	attempts, captureErrors, reconnects                               prometheus.Counter
	snapshots, snapshotRows, transactions, transactionRows            prometheus.Counter
	walBytes, schemaChanges, queueFull                                prometheus.Counter
	lastReceive, lastCommit, lastAck, lastWALSample                   prometheus.Gauge
	retainedBytes, lagBytes                                           prometheus.Gauge
	requests                                                          *prometheus.CounterVec
	invalidACKs, retries, delivered, deliveredBytes, schemasDelivered prometheus.Counter
	httpDuration, snapshotDuration                                    prometheus.Histogram
	cdcPollDuration                                                   prometheus.Histogram
	cdcRetentionGaps                                                  prometheus.Counter
	sqlSnapshotActive, sqlSnapshotScanned, sqlSnapshotTables          prometheus.Gauge
	sqlSnapshotStarted                                                prometheus.Gauge
	sqlSnapshotFailures, recoveredMessages                            prometheus.Counter
	sqlWaitStarted                                                    *prometheus.GaugeVec
	sqlWaitDuration                                                   *prometheus.HistogramVec
	cdcPolls                                                          *prometheus.CounterVec
	sqlHealth                                                         map[string]prometheus.Gauge
	sqlHealthSampled                                                  *prometheus.GaugeVec
	sqlHealthErrors                                                   prometheus.Counter
}

func New(q *queue.Store, c config.Config) *Metrics {
	m := &Metrics{registry: prometheus.NewRegistry()}
	reg := m.registry
	source := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "go_sync", Name: "source_info", Help: "Configured source engine; use to scope engine-specific alerts.",
	}, []string{"engine"})
	reg.MustRegister(source)
	source.WithLabelValues(c.Engine()).Set(1)
	counter := func(name, help string) prometheus.Counter {
		v := prometheus.NewCounter(prometheus.CounterOpts{Namespace: "go_sync", Name: name, Help: help})
		reg.MustRegister(v)
		return v
	}
	gauge := func(name, help string) prometheus.Gauge {
		v := prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "go_sync", Name: name, Help: help})
		reg.MustRegister(v)
		return v
	}
	histogram := func(name, help string, buckets []float64) prometheus.Histogram {
		v := prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "go_sync", Name: name, Help: help, Buckets: buckets})
		reg.MustRegister(v)
		return v
	}
	m.streaming = gauge("capture_streaming", "One while incremental capture is active; not a liveness guarantee.")
	m.backpressure = gauge("capture_backpressure", "One while capture is waiting for queue capacity.")
	m.attempts = counter("capture_attempts_total", "Capture attempts including initial connection.")
	m.captureErrors = counter("capture_errors_total", "Failed capture attempts excluding shutdown cancellation.")
	// Alert: rate(go_sync_capture_reconnects_total[5m]) > 0
	m.reconnects = counter("capture_reconnects_total", "Retries scheduled after transient errors, including initial connection failures.")
	m.snapshots = counter("capture_snapshots_total", "Snapshots successfully sealed in the local durable queue.")
	m.snapshotRows = counter("capture_snapshot_rows_total", "Rows in successfully sealed snapshots.")
	m.transactions = counter("capture_transactions_total", "Transactions published locally, excluding already durable replay.")
	// Dashboard: rate(go_sync_capture_transaction_rows_total[5m])
	m.transactionRows = counter("capture_transaction_rows_total", "Rows in transactions published locally.")
	m.walBytes = counter("capture_wal_payload_bytes_total", "Received logical payload bytes including replays; not physical WAL bytes.")
	m.schemaChanges = counter("schema_changes_total", "Schema drift detections; does not imply automatic schema adaptation.")
	m.queueFull = counter("queue_full_total", "Queue appends rejected for capacity or free disk reserve.")
	m.lastReceive = gauge("capture_last_receive_timestamp_seconds", "Unix timestamp of last replication frame or successful CDC poll; zero before first.")
	m.lastCommit = gauge("capture_last_commit_timestamp_seconds", "Unix timestamp of last local snapshot or transaction publication; zero before first.")
	m.lastAck = gauge("delivery_last_ack_timestamp_seconds", "Unix timestamp of last successful local queue ACK; zero before first.")
	m.lastWALSample = gauge("wal_sample_timestamp_seconds", "Unix timestamp of last successful WAL sample; zero means unknown.")
	m.retainedBytes = gauge("wal_retained_bytes", "Last sampled current WAL minus slot restart LSN; consult sample timestamp.")
	m.lagBytes = gauge("capture_lag_bytes", "Last sampled current WAL minus local durable LSN; includes unrelated WAL.")
	m.requests = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "go_sync", Name: "http_requests_total", Help: "Delivery attempts by ACK-aware outcome."}, []string{"result"})
	reg.MustRegister(m.requests)
	for _, result := range []string{"success", "retryable_error", "permanent_error", "canceled"} {
		m.requests.WithLabelValues(result)
	}
	// Alert: rate(go_sync_http_retries_total[5m]) > 0
	m.retries = counter("http_retries_total", "Delivery retries scheduled after retryable failures.")
	m.invalidACKs = counter("http_invalid_acks_total", "HTTP 200 responses with oversized, malformed or mismatched ACKs.")
	m.delivered = counter("delivery_messages_total", "Messages acknowledged remotely and removed durably from the local queue.")
	m.deliveredBytes = counter("delivery_bytes_total", "Serialized bytes of locally acknowledged messages; excludes retransmissions.")
	m.schemasDelivered = counter("schema_delivered_total", "Schema messages acknowledged remotely and locally.")
	// Dashboard: histogram_quantile(0.99, sum by (le) (rate(go_sync_http_request_duration_seconds_bucket[5m])))
	m.httpDuration = histogram("http_request_duration_seconds", "Delivery attempt duration including ACK validation.", []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60})
	m.snapshotDuration = histogram("snapshot_duration_seconds", "Duration of successfully sealed snapshot attempts.", []float64{.1, 1, 10, 30, 60, 300, 600, 1800, 3600})
	// Dashboard: histogram_quantile(0.99, rate(go_sync_sqlserver_cdc_poll_duration_seconds_bucket[5m]))
	m.cdcPollDuration = histogram("sqlserver_cdc_poll_duration_seconds", "SQL Server CDC poll duration including local staging.", prometheus.DefBuckets)
	// Alert: increase(go_sync_sqlserver_cdc_retention_gaps_total[5m]) > 0
	m.cdcRetentionGaps = counter("sqlserver_cdc_retention_gaps_total", "Detected SQL Server CDC retention gaps; capture stops without skipping data.")
	// Dashboard: go_sync_sqlserver_snapshot_scanned_rows
	m.sqlSnapshotActive = gauge("sqlserver_snapshot_active", "One during a SQL Server snapshot attempt, including waits.")
	m.sqlSnapshotScanned = gauge("sqlserver_snapshot_scanned_rows", "Rows accepted into the current or last snapshot batch; not necessarily durable or published. Resets per attempt.")
	m.sqlSnapshotTables = gauge("sqlserver_snapshot_completed_tables", "Tables scanned and staged in the current or last attempt; not published. Resets per attempt.")
	m.sqlSnapshotStarted = gauge("sqlserver_snapshot_start_timestamp_seconds", "Local start time of the current or last snapshot attempt; zero before first.")
	m.sqlSnapshotFailures = counter("sqlserver_snapshot_failures_total", "Failed snapshot attempts, excluding shutdown cancellation.")
	m.recoveredMessages = counter("capture_recovery_discarded_messages_total", "Unpublished messages discarded by successful recovery; not delivered messages.")
	// Alert: time() - go_sync_sqlserver_snapshot_wait_start_timestamp_seconds > 60
	m.sqlWaitStarted = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "go_sync", Name: "sqlserver_snapshot_wait_start_timestamp_seconds",
		Help: "Start timestamp while waiting; zero when not waiting. Stages: locks, fence.",
	}, []string{"stage"})
	m.sqlWaitDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "go_sync", Name: "sqlserver_snapshot_wait_duration_seconds",
		Help:    "Snapshot lock acquisition or fence wait duration, including failed attempts.",
		Buckets: []float64{.1, 1, 5, 10, 30, 60, 120, 300, 600, 3600},
	}, []string{"stage", "result"})
	m.cdcPolls = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "go_sync", Name: "sqlserver_cdc_polls_total", Help: "CDC polls by bounded result; idle is not proof of capture job health.",
	}, []string{"result"})
	reg.MustRegister(m.sqlWaitStarted, m.sqlWaitDuration, m.cdcPolls)
	for _, stage := range []string{"locks", "fence"} {
		m.sqlWaitStarted.WithLabelValues(stage).Set(0)
		for _, result := range []string{"success", "error"} {
			m.sqlWaitDuration.WithLabelValues(stage, result)
		}
	}
	for _, result := range []string{"progress", "idle", "error", "canceled"} {
		m.cdcPolls.WithLabelValues(result)
	}
	m.initSQLHealth()
	reg.MustRegister(newQueueCollector(q, c), collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	scrapes := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "go_sync", Name: "metrics_requests_total", Help: "Metrics HTTP requests by status code."}, []string{"code"})
	scrapeDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "go_sync", Name: "metrics_request_duration_seconds", Help: "Metrics HTTP request duration.", Buckets: prometheus.DefBuckets}, nil)
	reg.MustRegister(scrapes, scrapeDuration)
	m.handler = promhttp.InstrumentHandlerCounter(scrapes, promhttp.InstrumentHandlerDuration(scrapeDuration,
		promhttp.HandlerFor(reg, promhttp.HandlerOpts{MaxRequestsInFlight: 4, EnableOpenMetrics: true})))
	return m
}

func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) { m.handler.ServeHTTP(w, r) }

// CDCPoll observes SQL Server polling without treating its LSN as WAL bytes.
func (m *Metrics) CDCPoll(elapsed time.Duration, success, retentionGap bool) {
	if m == nil {
		return
	}
	m.cdcPollDuration.Observe(elapsed.Seconds())
	if success {
		m.lastReceive.SetToCurrentTime()
	}
	if retentionGap {
		m.cdcRetentionGaps.Inc()
	}
}
func (m *Metrics) CaptureAttempt() {
	if m != nil {
		m.attempts.Inc()
	}
}
func (m *Metrics) CaptureError(retry bool) {
	if m == nil {
		return
	}
	m.captureErrors.Inc()
	if retry {
		m.reconnects.Inc()
	}
}
func (m *Metrics) Streaming(active bool) {
	if m != nil {
		m.streaming.Set(bit(active))
	}
}
func (m *Metrics) Received(bytes int) {
	if m == nil {
		return
	}
	m.walBytes.Add(float64(bytes))
	m.lastReceive.SetToCurrentTime()
}
func (m *Metrics) WAL(retained, lag float64) {
	if m == nil {
		return
	}
	m.retainedBytes.Set(retained)
	m.lagBytes.Set(lag)
	m.lastWALSample.SetToCurrentTime()
}
func (m *Metrics) SnapshotCommitted(rows uint64, elapsed time.Duration) {
	if m == nil {
		return
	}
	m.snapshots.Inc()
	m.snapshotRows.Add(float64(rows))
	m.lastCommit.SetToCurrentTime()
	m.snapshotDuration.Observe(elapsed.Seconds())
}
func (m *Metrics) TransactionCommitted(rows uint64) {
	if m == nil {
		return
	}
	m.transactions.Inc()
	m.transactionRows.Add(float64(rows))
	m.lastCommit.SetToCurrentTime()
}
func (m *Metrics) SchemaChanged() {
	if m != nil {
		m.schemaChanges.Inc()
	}
}
func (m *Metrics) QueueFull() {
	if m != nil {
		m.queueFull.Inc()
	}
}
func (m *Metrics) Backpressure(active bool) {
	if m != nil {
		m.backpressure.Set(bit(active))
	}
}
func (m *Metrics) DeliveryRetry() {
	if m != nil {
		m.retries.Inc()
	}
}
func (m *Metrics) InvalidACK() {
	if m != nil {
		m.invalidACKs.Inc()
	}
}
func (m *Metrics) Delivered(bytes int, schema bool) {
	if m == nil {
		return
	}
	m.delivered.Inc()
	m.deliveredBytes.Add(float64(bytes))
	m.lastAck.SetToCurrentTime()
	if schema {
		m.schemasDelivered.Inc()
	}
}

// Delivery includes response reading and ACK validation. Remote ACK success
// does not imply that removing the message from the local queue succeeded.
func (m *Metrics) Delivery(elapsed time.Duration, retry bool, err error, canceled bool) {
	if m == nil {
		return
	}
	result := "success"
	if canceled {
		result = "canceled"
	} else if err != nil {
		if retry {
			result = "retryable_error"
		} else {
			result = "permanent_error"
		}
	}
	m.requests.WithLabelValues(result).Inc()
	m.httpDuration.Observe(elapsed.Seconds())
}

func bit(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

// queueCollector takes one consistent read transaction per scrape, never makes
// remote calls, and redacts local paths/errors from failed exposition.
type queueCollector struct {
	q    *queue.Store
	c    config.Config
	desc []*prometheus.Desc
}

func newQueueCollector(q *queue.Store, c config.Config) *queueCollector {
	x := &queueCollector{q: q, c: c}
	for _, metric := range [][2]string{
		{"queue_bytes", "Serialized queued bytes including unpublished chunks; not database file size."},
		{"queue_limit_bytes", "Configured maximum queued payload bytes."},
		{"queue_pending_messages", "Published messages awaiting receiver acknowledgment."},
		{"queue_staged_messages", "Unpublished messages in an incomplete snapshot or transaction."},
		{"disk_free_bytes", "Available bytes on the queue filesystem."},
		{"disk_reserve_bytes", "Configured minimum free disk reserve."},
		{"queue_oldest_pending_age_seconds", "Enqueue age of the oldest published unacknowledged message; zero if empty."},
	} {
		x.desc = append(x.desc, prometheus.NewDesc("go_sync_"+metric[0], metric[1], nil, nil))
	}
	x.desc = append(x.desc, prometheus.NewDesc("go_sync_capture_phase", "Current durable capture phase as a one-hot gauge.", []string{"phase"}, nil))
	return x
}

func (x *queueCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range x.desc {
		ch <- d
	}
}
func (x *queueCollector) Collect(ch chan<- prometheus.Metric) {
	st, oldest, err := x.q.MetricsSnapshot()
	if err != nil {
		ch <- prometheus.NewInvalidMetric(x.desc[0], errors.New("queue metrics unavailable"))
		return
	}
	free, err := queue.FreeBytes(x.c.DataDir)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(x.desc[4], errors.New("disk metrics unavailable"))
		return
	}
	age := float64(0)
	if !oldest.IsZero() {
		age = max(0, time.Since(oldest).Seconds())
	}
	// Alert: go_sync_queue_bytes / go_sync_queue_limit_bytes > 0.8
	// Alert: go_sync_queue_oldest_pending_age_seconds > 300
	values := []float64{float64(st.Bytes), float64(x.c.QueueBytes), float64(st.ReadySeq - st.DeliveredSeq), float64(st.NextSeq - st.ReadySeq), float64(free), float64(x.c.ReserveBytes), age}
	for i, value := range values {
		ch <- prometheus.MustNewConstMetric(x.desc[i], prometheus.GaugeValue, value)
	}
	phase := st.Phase
	if phase == "" {
		phase = "uninitialized"
	}
	for _, label := range []string{"uninitialized", "snapshot", "stream"} {
		ch <- prometheus.MustNewConstMetric(x.desc[7], prometheus.GaugeValue, bit(phase == label), label)
	}
}
