package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go-sync/internal/config"
	"go-sync/internal/event"
	"go-sync/internal/queue"
	"go-sync/internal/telemetry"
)

func TestACKContract(t *testing.T) {
	for _, tc := range []struct {
		name           string
		status         int
		body           string
		retry, wantErr bool
	}{
		{"durable ack", 200, `{"message_id":"id","ack_seq":1}`, false, false},
		{"wrong id", 200, `{"message_id":"other","ack_seq":1}`, false, true},
		{"ahead ack", 200, `{"message_id":"id","ack_seq":2}`, false, true},
		{"malformed", 200, `ok`, false, true},
		{"not durable", 202, ``, false, true},
		{"rate limit", 429, ``, true, true},
		{"temporary failure", 503, ``, true, true},
		{"bad auth", 401, ``, false, true},
		{"redirect", 307, ``, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Idempotency-Key") != "id" {
					t.Error("missing idempotency key")
				}
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			}))
			defer srv.Close()
			c := config.Defaults()
			c.URL = srv.URL
			s := New(c, slog.New(slog.NewTextHandler(io.Discard, nil)))
			retry, err := s.Send(t.Context(), event.Message{ID: "id", Seq: 1}, []byte(`{}`))
			if retry != tc.retry || (err != nil) != tc.wantErr {
				t.Fatalf("got retry=%v err=%v", retry, err)
			}
		})
	}
}

func TestDeliveryMetricsRespectACK(t *testing.T) {
	for _, tc := range []struct {
		name    string
		invalid bool
	}{{"retry then valid", false}, {"invalid ACK", true}} {
		t.Run(tc.name, func(t *testing.T) {
			c := config.Defaults()
			c.DataDir = t.TempDir()
			c.RetryMin, c.RetryMax = "1ms", "1ms"
			q, err := queue.Open(c.DataDir, c.QueueBytes, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			if err := q.Initialize(queue.State{SourceID: "test", Generation: "g"}); err != nil {
				t.Fatal(err)
			}
			if err := q.Append(event.Message{Kind: "schema"}); err != nil {
				t.Fatal(err)
			}
			if err := q.Publish("0/10", true); err != nil {
				t.Fatal(err)
			}
			var attempts atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if attempts.Add(1) == 1 && !tc.invalid {
					w.WriteHeader(503)
					return
				}
				var msg event.Message
				if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
					t.Error(err)
					return
				}
				if tc.invalid {
					msg.ID = "wrong"
				}
				json.NewEncoder(w).Encode(event.Ack{ID: msg.ID, Seq: msg.Seq})
			}))
			defer srv.Close()
			c.URL = srv.URL
			meter := telemetry.New(q, c)
			sender := New(c, slog.New(slog.NewTextHandler(io.Discard, nil))).WithMetrics(meter)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- sender.Run(ctx, q) }()
			if !tc.invalid {
				for {
					st, err := q.State()
					if err != nil {
						t.Error(err)
						break
					}
					if st.DeliveredSeq == 1 || ctx.Err() != nil {
						break
					}
					time.Sleep(time.Millisecond)
				}
				cancel()
			}
			err = <-done
			if tc.invalid && (err == nil || errors.Is(err, context.DeadlineExceeded)) {
				t.Fatalf("invalid ACK not rejected: %v", err)
			}
			w := httptest.NewRecorder()
			meter.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
			if w.Code != 200 {
				t.Fatal(w.Body.String())
			}
			samples := []string{"go_sync_delivery_messages_total 1", "go_sync_http_retries_total 1", "go_sync_queue_pending_messages 0", "go_sync_schema_delivered_total 1", `go_sync_http_requests_total{result="success"} 1`, `go_sync_http_requests_total{result="retryable_error"} 1`}
			if tc.invalid {
				samples = []string{"go_sync_delivery_messages_total 0", "go_sync_queue_pending_messages 1", "go_sync_http_invalid_acks_total 1", `go_sync_http_requests_total{result="permanent_error"} 1`}
			}
			for _, sample := range samples {
				if !strings.Contains("\n"+w.Body.String(), "\n"+sample+"\n") {
					t.Errorf("missing metric %s", sample)
				}
			}
		})
	}
}

func TestLostResponseResendsSameIdentity(t *testing.T) {
	var seen []event.Message
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m event.Message
		if e := json.NewDecoder(r.Body).Decode(&m); e != nil {
			t.Error(e)
		}
		seen = append(seen, m)
		if len(seen) == 1 {
			conn, _, e := w.(http.Hijacker).Hijack()
			if e != nil {
				t.Error(e)
				return
			}
			conn.Close()
			return
		}
		json.NewEncoder(w).Encode(event.Ack{ID: m.ID, Seq: m.Seq})
	}))
	defer srv.Close()
	c := config.Defaults()
	c.URL = srv.URL
	s := New(c, slog.New(slog.NewTextHandler(io.Discard, nil)))
	m := event.Message{ID: "stable", Seq: 3}
	b, e := json.Marshal(m)
	if e != nil {
		t.Fatal(e)
	}
	retry, e := s.Send(t.Context(), m, b)
	if !retry || e == nil {
		t.Fatalf("lost response: %v %v", retry, e)
	}
	if _, e := s.Send(t.Context(), m, b); e != nil {
		t.Fatal(e)
	}
	if len(seen) != 2 || seen[0].ID != seen[1].ID || seen[0].Seq != seen[1].Seq {
		t.Fatal("retry changed identity")
	}
}
