package telemetry

import (
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func (m *Metrics) initSQLHealth() {
	m.sqlHealth = make(map[string]prometheus.Gauge)
	m.sqlHealthSampled = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "go_sync", Name: "sqlserver_health_sample_timestamp_seconds",
		Help: "Local timestamp of a valid advisory sample; zero for unknown. Consult before using health gauges.",
	}, []string{"signal"})
	m.sqlHealthErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "go_sync", Name: "sqlserver_health_errors_total",
		Help: "Advisory health sampling failures; do not interrupt capture.",
	})
	m.registry.MustRegister(m.sqlHealthSampled, m.sqlHealthErrors)
	for signal, help := range map[string]string{
		"scan_age":         "Source time since latest completed CDC scan, including empty scans; NaN if unknown. Not Agent service status.",
		"capture_latency":  "Latest completed nonempty error-free CDC scan latency; NaN for empty scans or unknown.",
		"checkpoint_lag":   "Captured high-watermark time minus durable checkpoint time; excludes uncaptured log and HTTP delivery. NaN if unknown.",
		"retention_margin": "Durable checkpoint time minus latest table low-watermark time, clamped to zero; NOT time until cleanup. NaN if unknown.",
	} {
		g := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "go_sync", Name: "sqlserver_" + signal + "_seconds", Help: help,
		})
		m.registry.MustRegister(g)
		m.sqlHealth[signal] = g
		g.Set(math.NaN())
		m.sqlHealthSampled.WithLabelValues(signal).Set(0)
	}
}

// SQLHealthValue replaces a health sample, clearing a prior value when unknown.
func (m *Metrics) SQLHealthValue(signal string, value float64, known bool) {
	if m == nil {
		return
	}
	g, ok := m.sqlHealth[signal]
	if !ok {
		return
	}
	if !known || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		g.Set(math.NaN())
		m.sqlHealthSampled.WithLabelValues(signal).Set(0)
		return
	}
	g.Set(value)
	m.sqlHealthSampled.WithLabelValues(signal).SetToCurrentTime()
}

// SQLHealthError counts failed advisory reads, not capture failures.
func (m *Metrics) SQLHealthError() {
	if m != nil {
		m.sqlHealthErrors.Inc()
	}
}

// SQLSnapshotStarted resets attempt-local progress, not durable counters.
func (m *Metrics) SQLSnapshotStarted() {
	if m == nil {
		return
	}
	m.sqlSnapshotActive.Set(1)
	m.sqlSnapshotScanned.Set(0)
	m.sqlSnapshotTables.Set(0)
	m.sqlSnapshotStarted.SetToCurrentTime()
}

// SQLSnapshotScanned counts accepted rows before publication.
func (m *Metrics) SQLSnapshotScanned() {
	if m != nil {
		m.sqlSnapshotScanned.Inc()
	}
}

// SQLSnapshotTableCompleted records a staged table, not a published snapshot.
func (m *Metrics) SQLSnapshotTableCompleted() {
	if m != nil {
		m.sqlSnapshotTables.Inc()
	}
}

// SQLSnapshotFinished clears active state on success, failure or cancellation.
func (m *Metrics) SQLSnapshotFinished(failed bool) {
	if m == nil {
		return
	}
	m.sqlSnapshotActive.Set(0)
	if failed {
		m.sqlSnapshotFailures.Inc()
	}
}

// SQLSnapshotWait returns an observer to call exactly once when a wait ends.
// Only a fixed set of stages is accepted to keep cardinality bounded.
func (m *Metrics) SQLSnapshotWait(stage string) func(bool) {
	if m == nil || (stage != "locks" && stage != "fence") {
		return func(bool) {}
	}
	started := time.Now()
	m.sqlWaitStarted.WithLabelValues(stage).SetToCurrentTime()
	return func(success bool) {
		result := "error"
		if success {
			result = "success"
		}
		m.sqlWaitStarted.WithLabelValues(stage).Set(0)
		m.sqlWaitDuration.WithLabelValues(stage, result).Observe(time.Since(started).Seconds())
	}
}

// RecoveryDiscarded observes only successfully discarded unpublished messages.
func (m *Metrics) RecoveryDiscarded(messages uint64) {
	if m != nil {
		m.recoveredMessages.Add(float64(messages))
	}
}

// CDCPollOutcome separates idle reads from source progress and failures.
func (m *Metrics) CDCPollOutcome(progress, failed, canceled bool) {
	if m == nil {
		return
	}
	result := "idle"
	switch {
	case canceled:
		result = "canceled"
	case failed:
		result = "error"
	case progress:
		result = "progress"
	}
	m.cdcPolls.WithLabelValues(result).Inc()
}
