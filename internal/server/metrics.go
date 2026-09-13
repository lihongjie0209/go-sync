package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/expfmt"
	"go-sync/internal/serverconfig"
)

type serverMetrics struct {
	registry                               *prometheus.Registry
	handler                                http.Handler
	connected, received, applied, pending  *prometheus.GaugeVec
	events, applyErrors, reloads, vmWrites *prometheus.CounterVec
	applyDuration                          *prometheus.HistogramVec
	reconciles                             *prometheus.CounterVec
	reconcileDuration                      *prometheus.HistogramVec
	reconcileBuckets                       *prometheus.GaugeVec

	mu        sync.RWMutex
	collector map[string][]byte
}

func newServerMetrics() *serverMetrics {
	registry := prometheus.NewRegistry()
	m := &serverMetrics{registry: registry, collector: make(map[string][]byte)}
	m.connected = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "go_sync_server", Name: "syncer_connected", Help: "One while a configured syncer has an active gRPC stream."}, []string{"syncer_id"})
	m.received = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "go_sync_server", Name: "received_sequence", Help: "Highest sequence durably received."}, []string{"syncer_id"})
	m.applied = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "go_sync_server", Name: "applied_sequence", Help: "Highest sequence transactionally applied."}, []string{"syncer_id"})
	m.pending = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "go_sync_server", Name: "pending_messages", Help: "Durably received messages not yet applied."}, []string{"syncer_id"})
	m.events = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "go_sync_server", Name: "events_total", Help: "Received event messages by kind."}, []string{"syncer_id", "kind"})
	m.applyErrors = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "go_sync_server", Name: "apply_errors_total", Help: "Target apply errors."}, []string{"syncer_id"})
	m.reloads = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "go_sync_server", Name: "config_reloads_total", Help: "Configuration reload attempts."}, []string{"result"})
	m.vmWrites = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "go_sync_server", Name: "victoriametrics_writes_total", Help: "VictoriaMetrics import attempts."}, []string{"component", "result"})
	m.applyDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "go_sync_server", Name: "apply_duration_seconds", Help: "Event persistence and apply duration.", Buckets: prometheus.DefBuckets}, []string{"syncer_id", "kind"})
	m.reconciles = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "go_sync_server", Name: "reconciliations_total", Help: "Scheduled reconciliation outcomes."}, []string{"syncer_id", "result"})
	m.reconcileDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "go_sync_server", Name: "reconciliation_duration_seconds", Help: "Scheduled reconciliation duration.", Buckets: prometheus.DefBuckets}, []string{"syncer_id"})
	m.reconcileBuckets = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "go_sync_server", Name: "reconciliation_mismatched_buckets", Help: "Mismatched buckets in the latest reconciliation."}, []string{"syncer_id"})
	registry.MustRegister(m.connected, m.received, m.applied, m.pending, m.events, m.applyErrors, m.reloads, m.vmWrites, m.applyDuration,
		m.reconciles, m.reconcileDuration, m.reconcileBuckets,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m.handler = promhttp.HandlerFor(registry, promhttp.HandlerOpts{EnableOpenMetrics: true, MaxRequestsInFlight: 4})
	return m
}

func (m *serverMetrics) ServeHTTP(w http.ResponseWriter, r *http.Request) { m.handler.ServeHTTP(w, r) }

func (m *serverMetrics) setCollector(id string, payload []byte) {
	m.mu.Lock()
	m.collector[id] = append([]byte(nil), payload...)
	m.mu.Unlock()
}

func (m *serverMetrics) gatherText() ([]byte, error) {
	families, err := m.registry.Gather()
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	encoder := expfmt.NewEncoder(&out, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, family := range families {
		if err := encoder.Encode(family); err != nil {
			return nil, err
		}
	}
	return out.Bytes(), nil
}

func (m *serverMetrics) runVM(ctx context.Context, cfg func() serverconfig.VictoriaMetrics) error {
	for {
		current := cfg()
		interval, _ := time.ParseDuration(current.Interval)
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		serverPayload, err := m.gatherText()
		if err == nil {
			err = writeVM(ctx, current, serverPayload, map[string]string{"component": "server"})
		}
		m.vmWrites.WithLabelValues("server", resultLabel(err)).Inc()
		m.mu.RLock()
		copies := make(map[string][]byte, len(m.collector))
		for id, payload := range m.collector {
			copies[id] = append([]byte(nil), payload...)
		}
		m.mu.RUnlock()
		for id, payload := range copies {
			err := writeVM(ctx, current, payload, map[string]string{"component": "collector", "syncer_id": id})
			m.vmWrites.WithLabelValues("collector", resultLabel(err)).Inc()
		}
	}
}

func resultLabel(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}

func writeVM(ctx context.Context, cfg serverconfig.VictoriaMetrics, payload []byte, labels map[string]string) error {
	if len(payload) == 0 {
		return nil
	}
	u, err := url.Parse(cfg.URL)
	if err != nil {
		return err
	}
	query := u.Query()
	for name, value := range labels {
		query.Add("extra_label", name+"="+value)
	}
	u.RawQuery = query.Encode()
	timeout, _ := time.ParseDuration(cfg.Timeout)
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, u.String(), bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "text/plain; version=0.0.4")
	for name, value := range cfg.Headers {
		request.Header.Set(name, value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, _ = io.CopyN(io.Discard, response.Body, 4096)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("VictoriaMetrics returned HTTP %s", strconv.Itoa(response.StatusCode))
	}
	return nil
}
