package sqlserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go-sync/internal/config"
	"go-sync/internal/delivery"
	"go-sync/internal/queue"
	"go-sync/internal/telemetry"
)

// Collector owns one source capture and its local queue's unpublished tail.
type Collector struct {
	cfg             config.Config
	q               *queue.Store
	log             *slog.Logger
	metrics         *telemetry.Metrics
	healthWarningAt time.Time
}

// New shares the existing framework queue and HTTP-independent capture lifecycle.
func New(c config.Config, q *queue.Store, log *slog.Logger) *Collector {
	return &Collector{cfg: c, q: q, log: log}
}

// WithMetrics attaches the shared, nil-safe observer before Run.
func (c *Collector) WithMetrics(m *telemetry.Metrics) *Collector { c.metrics = m; return c }

// Run retries transient database outages from the last locally published LSN.
// Retention gaps, schema changes and incomplete update pairs fail closed.
func (c *Collector) Run(ctx context.Context) error {
	if err := c.cfg.Validate(); err != nil {
		return err
	}
	minimum, _ := time.ParseDuration(c.cfg.RetryMin)
	maximum, _ := time.ParseDuration(c.cfg.RetryMax)
	delay := minimum
	var attempt uint64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.metrics.CaptureAttempt()
		attempt++
		err := c.attempt(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		retry := retryable(err)
		if err != nil {
			c.metrics.CaptureError(retry)
		}
		if !retry {
			return err
		}
		c.log.WarnContext(ctx, "sqlserver connection interrupted; replaying from durable checkpoint",
			"attempt", attempt, "retry_delay_seconds", delay.Seconds(), "error", err)
		if err := delivery.Wait(ctx, delay); err != nil {
			return err
		}
		delay = min(delay*2, maximum)
	}
}

func (c *Collector) attempt(ctx context.Context) (result error) {
	before, err := c.q.State()
	if err != nil {
		return err
	}
	if err := c.q.Recover(); err != nil {
		return err
	}
	discarded := before.NextSeq - before.ReadySeq
	c.metrics.RecoveryDiscarded(discarded)
	c.log.InfoContext(ctx, "sqlserver local recovery completed",
		"discarded_staged_messages", discarded, "durable_lsn", before.DurableLSN, "phase", before.Phase)
	connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	db, err := open(connectCtx, c.cfg)
	cancel()
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, db.Close()) }()
	checkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	info, err := inspect(checkCtx, db, c.cfg)
	cancel()
	if err != nil {
		c.observeSchemaError(err)
		return err
	}
	st, err := c.q.State()
	if err != nil {
		return err
	}
	if st.Phase != "" {
		if st.Fingerprint != c.cfg.Fingerprint() || st.SystemID != info.SystemID || st.Database != info.Database {
			return errors.New("sqlserver source identity or capture scope changed; automatic failover is unsupported")
		}
		if st.Phase == "stream" && st.SchemaHash != schemaHash(info.Tables) {
			c.metrics.SchemaChanged()
			return errors.New("sqlserver schema or capture instance changed; explicit reinitialization required")
		}
	}
	if st.Phase != "stream" {
		if err := c.snapshot(ctx, db, info); err != nil {
			return err
		}
	}
	c.metrics.Streaming(true)
	defer c.metrics.Streaming(false)
	c.log.InfoContext(ctx, "sqlserver incremental capture started")
	lastProgressLog := time.Now()
	var lastHealthSample time.Time
	poll, _ := time.ParseDuration(c.cfg.SQLServer.PollInterval)
	queryTimeout, _ := time.ParseDuration(c.cfg.SQLServer.QueryTimeout)
	for {
		if c.metrics != nil && time.Since(lastHealthSample) >= 30*time.Second {
			c.sampleHealth(ctx, db, info.Tables)
			lastHealthSample = time.Now()
		}
		queryCtx, cancel := context.WithTimeout(ctx, queryTimeout)
		progress, err := c.poll(queryCtx, db, info)
		cancel()
		if err != nil {
			return fmt.Errorf("sqlserver incremental capture: %w", err)
		}
		if time.Since(lastProgressLog) >= time.Minute {
			state, err := c.q.State()
			if err != nil {
				return err
			}
			c.log.InfoContext(ctx, "sqlserver capture progress",
				"durable_lsn", state.DurableLSN, "pending_messages", state.ReadySeq-state.DeliveredSeq,
				"staged_messages", state.NextSeq-state.ReadySeq)
			lastProgressLog = time.Now()
		}
		if !progress {
			if err := delivery.Wait(ctx, poll); err != nil {
				return err
			}
		}
	}
}
