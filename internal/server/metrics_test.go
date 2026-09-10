package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go-sync/internal/serverconfig"
)

func TestWriteVMRequestAndHTTPBoundaries(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		wantErr    bool
		wantCalled bool
	}{
		{name: "success", status: http.StatusNoContent, wantCalled: true},
		{name: "server error", status: http.StatusServiceUnavailable, wantErr: true, wantCalled: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			called := false
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				if r.Method != http.MethodPost || r.URL.Query().Get("extra_label") == "" || r.Header.Get("Authorization") != "Bearer test" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
				}
				payload, _ := io.ReadAll(r.Body)
				if string(payload) != "metric 1\n" {
					t.Errorf("payload = %q", payload)
				}
				w.WriteHeader(test.status)
			}))
			defer endpoint.Close()
			cfg := serverconfig.VictoriaMetrics{URL: endpoint.URL, Timeout: "1s", Headers: map[string]string{"Authorization": "Bearer test"}}
			err := writeVM(t.Context(), cfg, []byte("metric 1\n"), map[string]string{"component": "server"})
			if (err != nil) != test.wantErr || called != test.wantCalled {
				t.Fatalf("writeVM() error = %v, called = %v", err, called)
			}
		})
	}
	if err := writeVM(t.Context(), serverconfig.VictoriaMetrics{URL: "://bad", Timeout: "1s"}, []byte("x"), nil); err == nil {
		t.Fatal("invalid URL was accepted")
	}
	if err := writeVM(t.Context(), serverconfig.VictoriaMetrics{}, nil, nil); err != nil {
		t.Fatalf("empty payload should be a no-op: %v", err)
	}
}

func TestMetricsExporterConcurrentCollectorUpdatesAndShutdown(t *testing.T) {
	var mu sync.Mutex
	writes := map[string]int{}
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		component := ""
		for _, label := range r.URL.Query()["extra_label"] {
			if strings.HasPrefix(label, "component=") {
				component = strings.TrimPrefix(label, "component=")
			}
		}
		mu.Lock()
		writes[component]++
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer endpoint.Close()

	metrics := newServerMetrics()
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- metrics.runVM(ctx, func() serverconfig.VictoriaMetrics {
			return serverconfig.VictoriaMetrics{URL: endpoint.URL, Interval: "5ms", Timeout: "100ms"}
		})
	}()
	for i := 0; ctx.Err() == nil; i++ {
		metrics.setCollector("client", []byte("collector_metric 1\n"))
		time.Sleep(time.Millisecond)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if writes["server"] < 2 || writes["collector"] < 2 {
		t.Fatalf("insufficient exporter cycles: %+v", writes)
	}
}
