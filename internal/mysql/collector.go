package mysql

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"go-sync/internal/config"
	"go-sync/internal/delivery"
	"go-sync/internal/queue"
	"go-sync/internal/telemetry"
)

type Collector struct {
	cfg     config.Config
	q       *queue.Store
	log     *slog.Logger
	metrics *telemetry.Metrics
}

func New(c config.Config, q *queue.Store, log *slog.Logger) *Collector {
	return &Collector{cfg: c, q: q, log: log}
}
func (c *Collector) WithMetrics(m *telemetry.Metrics) *Collector { c.metrics = m; return c }

func (c *Collector) Run(ctx context.Context) error {
	minimum, _ := time.ParseDuration(c.cfg.RetryMin)
	maximum, _ := time.ParseDuration(c.cfg.RetryMax)
	delay := minimum
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.metrics.CaptureAttempt()
		err := c.attempt(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		retry := mysqlRetryable(err)
		if err != nil {
			c.metrics.CaptureError(retry)
		}
		if !retry {
			return err
		}
		c.log.WarnContext(ctx, "mysql connection interrupted; replaying from durable checkpoint", "retry_delay_seconds", delay.Seconds())
		if err := delivery.Wait(ctx, delay); err != nil {
			return err
		}
		delay = min(delay*2, maximum)
	}
}

func (c *Collector) attempt(ctx context.Context) error {
	if err := c.q.Recover(); err != nil {
		return err
	}
	conn, _, err := connect(ctx, c.cfg)
	if err != nil {
		return err
	}
	info, err := inspect(conn, c.cfg)
	conn.Close()
	if err != nil {
		return err
	}
	st, err := c.q.State()
	if err != nil {
		return err
	}
	if st.Phase != "" && (st.Fingerprint != c.cfg.Fingerprint() || st.SystemID != info.ServerUUID || st.Database != info.Database) {
		return errors.New("mysql source identity or capture scope changed")
	}
	if st.Phase != "stream" {
		if err := c.snapshot(ctx, info); err != nil {
			return err
		}
		st, err = c.q.State()
		if err != nil {
			return err
		}
	}
	if st.SchemaHash != schemaHash(info.Tables) {
		return errors.New("mysql schema differs from durable schema; a DDL binlog event may have expired")
	}
	position, err := decodePosition(st.DurableLSN)
	if err != nil {
		return err
	}
	if err := c.validatePosition(ctx, position); err != nil {
		return err
	}
	c.metrics.Streaming(true)
	defer c.metrics.Streaming(false)
	c.log.InfoContext(ctx, "mysql incremental capture started", "checkpoint", st.DurableLSN)
	return c.stream(ctx, info, position)
}

func (c *Collector) validatePosition(ctx context.Context, want gomysql.Position) error {
	conn, _, err := connect(ctx, c.cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	r, err := conn.Execute("SHOW BINARY LOGS")
	if err != nil {
		return fmt.Errorf("mysql list binary logs (REPLICATION CLIENT privilege required): %w", err)
	}
	for i := 0; i < r.RowNumber(); i++ {
		name, _ := r.GetString(i, 0)
		size, scanErr := r.GetUint(i, 1)
		if name == want.Name {
			if scanErr != nil || uint64(want.Pos) > size {
				return errors.New("mysql checkpoint position is beyond retained binlog")
			}
			return nil
		}
	}
	return errors.New("mysql checkpoint binlog has expired; refusing to skip changes")
}

func mysqlRetryable(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.DeadlineExceeded)
}
