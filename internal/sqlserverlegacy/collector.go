package sqlserverlegacy

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"go-sync/internal/config"
	"go-sync/internal/delivery"
	"go-sync/internal/event"
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

func (c *Collector) WithMetrics(metrics *telemetry.Metrics) *Collector { c.metrics = metrics; return c }

func (c *Collector) Run(ctx context.Context) error {
	if err := c.cfg.Validate(); err != nil {
		return err
	}
	minimum, _ := time.ParseDuration(c.cfg.RetryMin)
	maximum, _ := time.ParseDuration(c.cfg.RetryMax)
	delay := minimum
	for attempt := uint64(1); ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.metrics.CaptureAttempt()
		err := c.attempt(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		isRetryable := retryable(err)
		if err != nil {
			c.metrics.CaptureError(isRetryable)
		}
		if !isRetryable {
			return err
		}
		c.log.WarnContext(ctx, "sqlserver legacy connection interrupted; source outbox will be replayed",
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
	c.metrics.RecoveryDiscarded(before.NextSeq - before.ReadySeq)
	connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	db, err := open(connectCtx, c.cfg)
	cancel()
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, db.Close()) }()
	leaseCtx, leaseCancel := context.WithTimeout(ctx, 30*time.Second)
	lease, err := acquireLease(leaseCtx, db, c.cfg)
	leaseCancel()
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, lease.Close()) }()
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	info, err := inspect(queryCtx, db, c.cfg)
	cancel()
	if err != nil {
		return err
	}
	state, err := c.q.State()
	if err != nil {
		return err
	}
	if state.Phase != "" {
		if !info.Installed || state.Fingerprint != c.cfg.Fingerprint() || state.SystemID != info.SystemID || state.Database != info.Database {
			return errors.New("sqlserver legacy source identity, installation or capture scope changed")
		}
		if state.Phase == "stream" && state.SchemaHash != schemaHash(info.Tables) {
			c.metrics.SchemaChanged()
			return errors.New("sqlserver legacy schema changed; explicit reinitialization is required before trigger replacement")
		}
	}
	if !info.Installed {
		if !c.cfg.SQLServerLegacy.AutoInstall {
			return errors.New("sqlserver legacy outbox is not installed and auto_install is false")
		}
		installCtx, installCancel := context.WithTimeout(ctx, 2*time.Minute)
		err = install(installCtx, db, c.cfg, info.Tables)
		installCancel()
		if err != nil {
			return fmt.Errorf("install sqlserver legacy outbox: %w", err)
		}
		queryCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
		info, err = inspect(queryCtx, db, c.cfg)
		cancel()
		if err != nil {
			return err
		}
		if !info.Installed {
			return errors.New("sqlserver legacy installation did not become ready")
		}
		c.log.InfoContext(ctx, "sqlserver legacy outbox and triggers installed", "tables", len(info.Tables), "schema_version", schemaVersion)
	}
	if state.Phase != "stream" {
		if err := c.snapshot(ctx, db, info); err != nil {
			return err
		}
	} else if err := c.discardSnapshotEvents(ctx, db, state.SnapshotLSN); err != nil {
		return err
	}
	if err := c.finishSourceCleanup(ctx, db); err != nil {
		return err
	}
	c.metrics.Streaming(true)
	defer c.metrics.Streaming(false)
	c.log.InfoContext(ctx, "sqlserver legacy incremental capture started")
	pollInterval, _ := time.ParseDuration(c.cfg.SQLServerLegacy.PollInterval)
	queryTimeout, _ := time.ParseDuration(c.cfg.SQLServerLegacy.QueryTimeout)
	for {
		queryCtx, cancel := context.WithTimeout(ctx, queryTimeout)
		progress, err := c.poll(queryCtx, db, info)
		cancel()
		if err != nil {
			return err
		}
		if !progress {
			if err := delivery.Wait(ctx, pollInterval); err != nil {
				return err
			}
		}
	}
}

func acquireLease(ctx context.Context, db *sql.DB, cfg config.Config) (*sql.Conn, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, dbError(err)
	}
	resource := "go-sync/" + cfg.SourceID + "/" + cfg.SQLServerLegacy.Prefix
	var code int
	err = conn.QueryRowContext(ctx, `DECLARE @result int
 EXEC @result=sp_getapplock @Resource=@p1,@LockMode='Exclusive',@LockOwner='Session',@LockTimeout=0
 SELECT @result`, resource).Scan(&code)
	if err != nil {
		return nil, errors.Join(dbError(err), conn.Close())
	}
	if code < 0 {
		return nil, errors.Join(errors.New("another sqlserver legacy collector owns the source lease"), conn.Close())
	}
	return conn, nil
}

func position(id int64) string { return "legacy:" + fmt.Sprintf("%020d", id) }

func parsePosition(value string) (int64, error) {
	if !strings.HasPrefix(value, "legacy:") {
		return 0, errors.New("invalid sqlserver legacy position")
	}
	id, err := strconv.ParseInt(strings.TrimPrefix(value, "legacy:"), 10, 64)
	if err != nil || id < 0 {
		return 0, errors.New("invalid sqlserver legacy position")
	}
	return id, nil
}

func (c *Collector) snapshot(ctx context.Context, db *sql.DB, info Inspection) (result error) {
	if !c.cfg.SQLServerLegacy.AllowSnapshotLocks {
		return errors.New("initial sqlserver legacy snapshot requires explicit sqlserver_legacy.allow_snapshot_locks=true")
	}
	duration, _ := time.ParseDuration(c.cfg.SQLServerLegacy.SnapshotTimeout)
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	started := time.Now()
	c.metrics.SQLSnapshotStarted()
	defer func() { c.metrics.SQLSnapshotFinished(result != nil && ctx.Err() != context.Canceled) }()
	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	state := queue.State{SourceID: c.cfg.SourceID, Generation: hex.EncodeToString(token[:]), Fingerprint: c.cfg.Fingerprint(),
		SystemID: info.SystemID, Database: info.Database, SchemaHash: schemaHash(info.Tables)}
	for _, table := range info.Tables {
		if table.RowIdentity == "full_row" {
			state.ProtocolVersion = "v2"
		}
	}
	if err := c.q.Initialize(state); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return dbError(err)
	}
	defer rollback(tx, &result)
	for index, table := range info.Tables {
		var one int
		err := tx.QueryRowContext(ctx, "SELECT TOP 1 1 FROM "+table.sqlName()+" WITH (TABLOCKX,HOLDLOCK)").Scan(&one)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("lock legacy snapshot table %d: %w", index+1, dbError(err))
		}
	}
	_, events, _ := names(c.cfg)
	var boundary sql.NullInt64
	if err := tx.QueryRowContext(ctx, "SELECT MAX(change_id) FROM "+events).Scan(&boundary); err != nil {
		return dbError(err)
	}
	end := int64(0)
	if boundary.Valid {
		end = boundary.Int64
	}
	b := batch{cfg: c.cfg, q: c.q, metrics: c.metrics}
	refs := make([]event.TableRef, 0, len(info.Tables))
	for _, table := range info.Tables {
		refs = append(refs, event.TableRef{Schema: table.Schema, Name: table.Name})
	}
	if err := b.append(ctx, event.Message{Kind: "snapshot_begin", Tables: refs}); err != nil {
		return err
	}
	var total uint64
	for index, table := range info.Tables {
		schema, err := json.Marshal(table)
		if err != nil {
			return err
		}
		if err := b.append(ctx, event.Message{Kind: "schema", Schema: schema}); err != nil {
			return err
		}
		b.message = event.Message{Kind: "snapshot_rows"}
		count, err := snapshotRows(ctx, tx, table, &b)
		if err != nil {
			return fmt.Errorf("sqlserver legacy snapshot table %d: %w", index+1, err)
		}
		total += count
		if err := b.flush(ctx); err != nil {
			return err
		}
		c.metrics.SQLSnapshotTableCompleted()
	}
	if err := b.append(ctx, event.Message{Kind: "snapshot_end"}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return dbError(err)
	}
	if err := c.q.FinalizeSnapshotLSN(position(end)); err != nil {
		return err
	}
	if err := c.q.Publish(position(end), true); err != nil {
		return err
	}
	c.metrics.SnapshotCommitted(total, time.Since(started))
	return c.discardSnapshotEvents(ctx, db, position(end))
}
