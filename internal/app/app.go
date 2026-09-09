package app

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"go-sync/internal/capture"
	"go-sync/internal/config"
	"go-sync/internal/delivery"
	mysqlsource "go-sync/internal/mysql"
	"go-sync/internal/queue"
	"go-sync/internal/sqlserver"
	"go-sync/internal/sqlserverlegacy"
	"go-sync/internal/telemetry"
	"golang.org/x/sync/errgroup"
)

type Status struct {
	UpdatedAt       time.Time   `json:"updated_at"`
	Running         bool        `json:"running"`
	State           queue.State `json:"state"`
	FreeBytes       uint64      `json:"free_bytes"`
	QueueLimit      int64       `json:"queue_limit"`
	OldestPendingAt string      `json:"oldest_pending_at,omitempty"`
	Error           string      `json:"error,omitempty"`
}

func Run(ctx context.Context, c config.Config, log *slog.Logger) (result error) {
	if err := c.Validate(); err != nil {
		return err
	}
	q, err := queue.Open(c.DataDir, c.QueueBytes, c.ReserveBytes)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, q.Close()) }()
	st, err := q.State()
	if err != nil {
		return err
	}
	if st.Phase != "" && st.Fingerprint != c.Fingerprint() {
		return errors.New("configuration differs from persisted capture")
	}
	defer func() {
		s := Status{UpdatedAt: time.Now().UTC(), Running: false, QueueLimit: c.QueueBytes}
		var e error
		s.State, e = q.State()
		if e != nil {
			result = errors.Join(result, e)
		}
		if result != nil && !errors.Is(result, context.Canceled) {
			s.Error = SafeError(result)
		}
		result = errors.Join(result, writeStatus(c.DataDir, s))
	}()
	var metrics *telemetry.Metrics
	var metricsServer *telemetry.Server
	if c.MetricsAddr != "" {
		metrics = telemetry.New(q, c)
		metricsServer, err = telemetry.Listen(c.MetricsAddr, metrics)
		if err != nil {
			return err
		}
		log.Info("metrics listener started", "address", metricsServer.Addr().String(), "path", "/metrics")
	}
	g, groupCtx := errgroup.WithContext(ctx)
	if metricsServer != nil {
		g.Go(func() error { return metricsServer.Run(groupCtx) })
	}
	g.Go(func() error {
		if c.Engine() == "sqlserver" {
			return sqlserver.New(c, q, log).WithMetrics(metrics).Run(groupCtx)
		} else if c.Engine() == "sqlserver_legacy" {
			return sqlserverlegacy.New(c, q, log).WithMetrics(metrics).Run(groupCtx)
		} else if c.Engine() == "mysql" {
			return mysqlsource.New(c, q, log).WithMetrics(metrics).Run(groupCtx)
		}
		return capture.New(c, q, log).WithMetrics(metrics).Run(groupCtx)
	})
	g.Go(func() error { return delivery.New(c, log).WithMetrics(metrics).Run(groupCtx, q) })
	g.Go(func() error {
		var storageWarningAt time.Time
		for {
			st, err := q.State()
			if err != nil {
				return err
			}
			free, err := queue.FreeBytes(c.DataDir)
			if err != nil {
				return err
			}
			s := Status{UpdatedAt: time.Now().UTC(), Running: true, State: st, FreeBytes: free, QueueLimit: c.QueueBytes}
			m, _, err := q.Peek()
			if err == nil {
				s.OldestPendingAt = m.CreatedAt
			} else if !errors.Is(err, queue.ErrEmpty) {
				return err
			}
			if err := writeStatus(c.DataDir, s); err != nil {
				return err
			}
			if st.Bytes >= c.QueueBytes*8/10 || free < c.ReserveBytes*2 {
				if storageWarningAt.IsZero() || time.Since(storageWarningAt) >= time.Minute {
					log.WarnContext(groupCtx, "capture storage nearing capacity", "queued_bytes", st.Bytes, "free_bytes", free)
					storageWarningAt = time.Now()
				}
			} else if !storageWarningAt.IsZero() {
				log.InfoContext(groupCtx, "capture storage capacity recovered", "queued_bytes", st.Bytes, "free_bytes", free)
				storageWarningAt = time.Time{}
			}
			if err := delivery.Wait(groupCtx, 5*time.Second); err != nil {
				return err
			}
		}
	})
	err = g.Wait()
	if ctx.Err() != nil && errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func writeStatus(dir string, s Status) (err error) {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".status-*")
	if err != nil {
		return err
	}
	defer func() {
		if e := os.Remove(f.Name()); e != nil && !errors.Is(e, os.ErrNotExist) {
			err = errors.Join(err, e)
		}
	}()
	if _, err = f.Write(b); err != nil {
		return errors.Join(err, f.Close())
	}
	if err = f.Sync(); err != nil {
		return errors.Join(err, f.Close())
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), filepath.Join(dir, "status.json"))
}

// SafeError redacts database connection strings and server DETAIL fields.
func SafeError(err error) string { return safeError(err) }
