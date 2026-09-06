package telemetry

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestServerRoutesAndShutdown(t *testing.T) {
	s, err := Listen("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "metric 1\n")
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("server did not stop")
		}
		conn, err := net.DialTimeout("tcp", s.Addr().String(), time.Second)
		if err == nil {
			conn.Close()
			t.Error("listener still open")
		}
	})
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	for _, tc := range []struct {
		name, method, path string
		status             int
	}{
		{"metrics", "GET", "/metrics", 200},
		{"head", "HEAD", "/metrics", 200},
		{"no writes", "POST", "/metrics", 405},
		{"no pprof", "GET", "/debug/pprof/", 404},
		{"exact path", "GET", "/metrics/extra", 404},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), tc.method, "http://"+s.Addr().String()+tc.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.status {
				t.Fatalf("status %d, want %d", resp.StatusCode, tc.status)
			}
		})
	}
}

func TestListenFailure(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if s, err := Listen(l.Addr().String(), http.NotFoundHandler()); err == nil {
		s.listener.Close()
		t.Fatal("occupied port accepted")
	}
}
