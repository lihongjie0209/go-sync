// Package delivery sends immutable queue messages and validates durable acknowledgments.
package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"

	"go-sync/internal/config"
	"go-sync/internal/event"
	"go-sync/internal/queue"
	"go-sync/internal/telemetry"
)

type Sender struct {
	client  *http.Client
	cfg     config.Config
	log     *slog.Logger
	metrics *telemetry.Metrics
}

// WithMetrics attaches an observer before Run or Send starts.
func (s *Sender) WithMetrics(m *telemetry.Metrics) *Sender { s.metrics = m; return s }

func New(c config.Config, log *slog.Logger) *Sender {
	d, _ := time.ParseDuration(c.HTTPTimeout)
	return &Sender{cfg: c, log: log, client: &http.Client{Timeout: d, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

// Send reports whether the unchanged request can be retried. Response bodies are
// bounded and never included in errors because they may echo secrets or row data.
func (s *Sender) Send(ctx context.Context, m event.Message, body []byte) (retry bool, result error) {
	start := time.Now()
	defer func() { s.metrics.Delivery(time.Since(start), retry, result, result != nil && ctx.Err() != nil) }()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return false, errors.New("invalid http request")
	}
	for k, v := range s.cfg.Headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", m.ID)
	resp, err := s.client.Do(req)
	if err != nil {
		return true, errors.New("http transport failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		retry := resp.StatusCode == 408 || resp.StatusCode == 429 || resp.StatusCode >= 500
		return retry, fmt.Errorf("receiver returned http %d", resp.StatusCode)
	}
	body, err = io.ReadAll(io.LimitReader(resp.Body, 65537))
	if err != nil {
		return true, errors.New("read ack failed")
	}
	if len(body) > 65536 {
		s.metrics.InvalidACK()
		return false, errors.New("ack exceeds size limit")
	}
	var ack event.Ack
	if err := json.Unmarshal(body, &ack); err != nil {
		s.metrics.InvalidACK()
		return false, errors.New("invalid ack json")
	}
	if ack.ID != m.ID || ack.Seq != m.Seq {
		s.metrics.InvalidACK()
		return false, errors.New("ack does not match message")
	}
	return false, nil
}

func Wait(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (s *Sender) Run(ctx context.Context, q *queue.Store) error {
	defer s.client.CloseIdleConnections()
	minDelay, _ := time.ParseDuration(s.cfg.RetryMin)
	maxDelay, _ := time.ParseDuration(s.cfg.RetryMax)
	delay := minDelay
	var retries uint64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		m, b, err := q.Peek()
		if errors.Is(err, queue.ErrEmpty) {
			if err := Wait(ctx, 200*time.Millisecond); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		retry, err := s.Send(ctx, m, b)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !retry {
				return err
			}
			s.metrics.DeliveryRetry()
			jitter := delay/2 + time.Duration(rand.Int64N(max(1, int64(delay/2))))
			retries++
			s.log.WarnContext(ctx, "http delivery retry", "seq", m.Seq,
				"message_id", m.ID, "kind", m.Kind, "lsn", m.LSN, "error", err,
				"retry", retries, "retry_delay_seconds", jitter.Seconds())
			if err := Wait(ctx, jitter); err != nil {
				return err
			}
			delay = min(delay*2, maxDelay)
			continue
		}
		if err := q.Ack(m.Seq, m.ID); err != nil {
			return err
		}
		s.metrics.Delivered(len(b), m.Kind == "schema")
		if retries > 0 {
			s.log.InfoContext(ctx, "http delivery recovered", "seq", m.Seq,
				"message_id", m.ID, "retries", retries)
		}
		retries = 0
		delay = minDelay
	}
}
