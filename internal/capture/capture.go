package capture

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"go-sync/internal/config"
	"go-sync/internal/delivery"
	"go-sync/internal/event"
	"go-sync/internal/queue"
	"go-sync/internal/telemetry"
	"go-sync/internal/walplugin"
)

type Collector struct {
	cfg     config.Config
	q       *queue.Store
	log     *slog.Logger
	metrics *telemetry.Metrics
}

var errSchemaRefreshed = errors.New("schema refreshed; replay from durable checkpoint")

// WithMetrics attaches an observer before Run starts.
func (c *Collector) WithMetrics(m *telemetry.Metrics) *Collector { c.metrics = m; return c }

func New(c config.Config, q *queue.Store, log *slog.Logger) *Collector {
	return &Collector{cfg: c, q: q, log: log}
}

func transient(err error) bool {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return strings.HasPrefix(pe.Code, "08") || pe.Code == "57P01" || pe.Code == "57P02" || pe.Code == "57P03" || pe.Code == "53300"
	}
	var ne net.Error
	var opErr *net.OpError
	// os.PathError also satisfies net.Error. A plugin permission/path failure
	// must not be retried indefinitely as a PostgreSQL connection outage.
	isNetwork := errors.As(err, &opErr) || (errors.As(err, &ne) && ne.Timeout())
	return isNetwork || errors.Is(err, context.DeadlineExceeded) || pgconn.SafeToRetry(err) ||
		errors.Is(err, errSchemaRefreshed) ||
		errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func (c *Collector) Run(ctx context.Context) error {
	minDelay, _ := time.ParseDuration(c.cfg.RetryMin)
	maxDelay, _ := time.ParseDuration(c.cfg.RetryMax)
	delay := minDelay
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.metrics.CaptureAttempt()
		err := c.attempt(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		retry := transient(err)
		if err != nil {
			c.metrics.CaptureError(retry)
		}
		if !retry {
			return err
		}
		if errors.Is(err, errSchemaRefreshed) {
			delay = minDelay
			continue
		}
		c.log.Warn("postgres connection interrupted; replaying from durable checkpoint")
		if err := delivery.Wait(ctx, delay); err != nil {
			return err
		}
		delay = min(delay*2, maxDelay)
	}
}

func (c *Collector) attempt(ctx context.Context) error {
	if err := c.q.Recover(); err != nil {
		return err
	}
	checkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	inspection, err := Inspect(checkCtx, c.cfg)
	cancel()
	if err != nil {
		return err
	}
	st, err := c.q.State()
	if err != nil {
		return err
	}
	if st.Phase != "" {
		if st.Fingerprint != c.cfg.Fingerprint() || st.Slot != c.cfg.Slot {
			return errors.New("capture configuration differs from persisted state")
		}
		if st.SystemID != inspection.SystemID || st.Database != inspection.Database || st.Timeline != inspection.Timeline {
			return errors.New("source identity/timeline changed; automatic failover is unsupported")
		}
		if st.Phase == "stream" && st.SchemaHash != schemaHash(inspection.Tables) {
			c.metrics.SchemaChanged()
			return errors.New("schema changed while collector was offline; explicit reinitialization is required")
		}
	}
	if err := walplugin.Ensure(ctx, c.cfg, c.log); err != nil {
		return err
	}
	if st.Phase != "stream" {
		if err := c.snapshot(ctx, inspection, st); err != nil {
			return err
		}
	}
	return c.stream(ctx, inspection.Tables, inspection.Version)
}

func (c *Collector) snapshot(ctx context.Context, info Inspection, previous queue.State) error {
	started := time.Now()
	var totalRows uint64
	conn, err := connect(ctx, c.cfg)
	if err != nil {
		return err
	}
	defer closeConn(ctx, conn)
	var exists bool
	if err := conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", c.cfg.Slot).Scan(&exists); err != nil {
		return err
	}
	if exists && previous.Phase == "" {
		return errors.New("configured slot already exists but has no local ownership record")
	}
	r, err := replication(ctx, c.cfg)
	if err != nil {
		return err
	}
	defer closeReplication(ctx, r)
	if exists {
		var plugin, database, kind string
		var active bool
		if err := conn.QueryRow(ctx, "SELECT plugin,database,slot_type,active FROM pg_replication_slots WHERE slot_name=$1", c.cfg.Slot).Scan(&plugin, &database, &kind, &active); err != nil {
			return err
		}
		if plugin != "wal2json" || database != info.Database || kind != "logical" || active {
			return errors.New("refusing to replace a slot with different ownership or an active consumer")
		}
		if err := pglogrepl.DropReplicationSlot(ctx, r, c.cfg.Slot, pglogrepl.DropReplicationSlotOptions{}); err != nil {
			return err
		}
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	st := queue.State{SourceID: c.cfg.SourceID, Generation: hex.EncodeToString(token[:]), Fingerprint: c.cfg.Fingerprint(), SystemID: info.SystemID, Timeline: info.Timeline, Database: info.Database, SchemaHash: schemaHash(info.Tables), Slot: c.cfg.Slot}
	for _, table := range info.Tables {
		if table.RowIdentity == "full_row" {
			st.ProtocolVersion = "v2"
		}
	}
	if err := c.q.Initialize(st); err != nil {
		return err
	}
	// Empty SnapshotAction uses the 9.4-compatible command and its default export.
	slot, err := pglogrepl.CreateReplicationSlot(ctx, r, c.cfg.Slot, "wal2json", pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
	if err != nil {
		return err
	}
	if slot.SnapshotName == "" {
		return errors.New("slot did not export a snapshot")
	}
	if err := c.q.SnapshotPoint(slot.ConsistentPoint); err != nil {
		return err
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer func() {
		endCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		tx.Rollback(endCtx)
	}()
	if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT '"+strings.ReplaceAll(slot.SnapshotName, "'", "''")+"'"); err != nil {
		return err
	}
	// Hold ACCESS SHARE locks for the entire snapshot; incompatible DDL/TRUNCATE
	// must not change the schema while the exported snapshot is being read.
	for _, t := range info.Tables {
		if _, err := tx.Exec(ctx, "LOCK TABLE "+t.SQL()+" IN ACCESS SHARE MODE"); err != nil {
			return err
		}
	}
	current, err := catalog(ctx, tx, c.cfg.Tables)
	if err != nil {
		return err
	}
	if schemaHash(current) != st.SchemaHash {
		c.metrics.SchemaChanged()
		return errors.New("schema changed while opening snapshot")
	}
	b := batcher{cfg: c.cfg, q: c.q, metrics: c.metrics, msg: event.Message{Kind: "snapshot_rows"}}
	names := make([]event.TableRef, 0, len(c.cfg.Tables))
	for _, t := range c.cfg.Tables {
		names = append(names, event.TableRef{Schema: t.Schema, Name: t.Name})
	}
	if err := b.append(ctx, event.Message{Kind: "snapshot_begin", Tables: names, LSN: slot.ConsistentPoint}); err != nil {
		return err
	}
	for _, t := range current {
		raw, err := json.Marshal(t)
		if err != nil {
			return err
		}
		if err := b.append(ctx, event.Message{Kind: "schema", Schema: raw}); err != nil {
			return err
		}
	}
	for _, t := range current {
		var columns, order []string
		for _, col := range t.Columns {
			columns = append(columns, pgx.Identifier{col.Name}.Sanitize())
			if col.Primary {
				order = append(order, pgx.Identifier{col.Name}.Sanitize())
			}
		}
		query := "SELECT " + strings.Join(columns, ",") + " FROM " + t.SQL()
		if len(order) > 0 {
			query += " ORDER BY " + strings.Join(order, ",")
		}
		rows, err := tx.Query(ctx, query)
		if err != nil {
			return err
		}
		var ordinal uint64
		for rows.Next() {
			ordinal++
			totalRows++
			row := event.Row{Schema: t.Schema, Table: t.Name, Operation: "read", Ordinal: ordinal}
			for i, raw := range rows.RawValues() {
				col := t.Columns[i]
				v := event.Column{Name: col.Name, Type: col.Type}
				if raw != nil {
					value := string(raw)
					if col.OID == 16 {
						if value == "t" {
							value = "true"
						} else {
							value = "false"
						}
					}
					v.Value = &value
				}
				row.Columns = append(row.Columns, v)
				if col.Primary {
					row.Key = append(row.Key, v)
				}
			}
			if t.RowIdentity == "full_row" {
				row.Identity = "full_row"
				row.Key = append([]event.Column(nil), row.Columns...)
			}
			if err := b.add(ctx, row); err != nil {
				rows.Close()
				return err
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
	}
	if err := b.flush(ctx); err != nil {
		return err
	}
	if err := b.append(ctx, event.Message{Kind: "snapshot_end", LSN: slot.ConsistentPoint}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if err := c.q.Publish(slot.ConsistentPoint, true); err != nil {
		return err
	}
	c.metrics.SnapshotCommitted(totalRows, time.Since(started))
	c.log.Info("snapshot sealed", "lsn", slot.ConsistentPoint)
	return nil
}

func (c *Collector) stream(ctx context.Context, tables []Table, version int) error {
	st, err := c.q.State()
	if err != nil {
		return err
	}
	start, err := pglogrepl.ParseLSN(st.DurableLSN)
	if err != nil {
		return err
	}
	normal, err := connect(ctx, c.cfg)
	if err != nil {
		return err
	}
	defer closeConn(ctx, normal)
	var plugin, db string
	var active bool
	if err := normal.QueryRow(ctx, "SELECT plugin,database,active FROM pg_replication_slots WHERE slot_name=$1", c.cfg.Slot).Scan(&plugin, &db, &active); err != nil {
		return fmt.Errorf("required slot unavailable: %w", err)
	}
	if plugin != "wal2json" || db != st.Database || active {
		return errors.New("slot does not match capture or is already active")
	}
	// 9.4 has no confirmed_flush_lsn column. Probe via row_to_json for a
	// cross-version read; a slot ahead of local durability indicates lost state.
	var confirmed *string
	if err := normal.QueryRow(ctx, "SELECT row_to_json(s)->>'confirmed_flush_lsn' FROM pg_replication_slots s WHERE slot_name=$1", c.cfg.Slot).Scan(&confirmed); err != nil {
		return err
	}
	if confirmed != nil {
		lsn, err := pglogrepl.ParseLSN(*confirmed)
		if err != nil {
			return err
		}
		if lsn > start {
			return errors.New("slot is ahead of local durable checkpoint; possible lost local state")
		}
	}
	r, err := replication(ctx, c.cfg)
	if err != nil {
		return err
	}
	defer closeReplication(ctx, r)
	if err := pglogrepl.StartReplication(ctx, r, c.cfg.Slot, start, pglogrepl.StartReplicationOptions{Mode: pglogrepl.LogicalReplication, PluginArgs: pluginArgs(c.cfg)}); err != nil {
		return err
	}
	d := newDecoder(c.cfg, c.q, tables, start)
	d.b.metrics = c.metrics
	c.metrics.Streaming(true)
	defer c.metrics.Streaming(false)
	lastFeedback := time.Time{}
	lastMetrics := time.Time{}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Since(lastFeedback) >= 5*time.Second {
			if time.Since(lastMetrics) >= 30*time.Second {
				currentFn, diffFn := "pg_current_xlog_location", "pg_xlog_location_diff"
				if version >= 100000 {
					currentFn, diffFn = "pg_current_wal_lsn", "pg_wal_lsn_diff"
				}
				metricsCtx, metricsCancel := context.WithTimeout(ctx, 5*time.Second)
				var retained, lag string
				e := normal.QueryRow(metricsCtx, "SELECT "+diffFn+"("+currentFn+"(),restart_lsn)::text, "+diffFn+"("+currentFn+"(),$2::pg_lsn)::text FROM pg_replication_slots WHERE slot_name=$1", c.cfg.Slot, d.durable.String()).Scan(&retained, &lag)
				metricsCancel()
				if e != nil {
					return e
				}
				retainedBytes, e := strconv.ParseFloat(retained, 64)
				if e != nil {
					return errors.New("invalid retained WAL size")
				}
				lagBytes, e := strconv.ParseFloat(lag, 64)
				if e != nil {
					return errors.New("invalid capture WAL lag")
				}
				c.metrics.WAL(max(0, retainedBytes), max(0, lagBytes))
				c.log.Info("replication progress", "durable_lsn", d.durable.String(), "wal_retained_bytes", retained)
				lastMetrics = time.Now()
			}
			if err := feedback(ctx, r, d.durable); err != nil {
				return err
			}
			lastFeedback = time.Now()
		}
		receiveCtx, cancel := context.WithTimeout(ctx, time.Second)
		msg, err := r.ReceiveMessage(receiveCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if pgconn.Timeout(err) {
				continue
			}
			return err
		}
		switch m := msg.(type) {
		case *pgproto3.CopyData:
			if len(m.Data) == 0 {
				return errors.New("empty replication frame")
			}
			switch m.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				if len(m.Data) < 18 {
					return errors.New("short keepalive frame")
				}
				k, e := pglogrepl.ParsePrimaryKeepaliveMessage(m.Data[1:])
				if e != nil {
					return e
				}
				c.metrics.Received(0)
				if k.ReplyRequested {
					if err := feedback(ctx, r, d.durable); err != nil {
						return err
					}
				}
			case pglogrepl.XLogDataByteID:
				if len(m.Data) < 25 {
					return errors.New("short xlog frame")
				}
				x, e := pglogrepl.ParseXLogData(m.Data[1:])
				if e != nil {
					return e
				}
				c.metrics.Received(len(x.WALData))
				if e := d.consume(ctx, x.WALData); e != nil {
					if errors.Is(e, errSchemaMismatch) {
						if refreshErr := c.refreshSchemaForReplay(ctx, normal, d.durable, st.SchemaHash); refreshErr != nil {
							return refreshErr
						}
						return errSchemaRefreshed
					}
					return e
				}
			default:
				return errors.New("unsupported replication frame")
			}
		case *pgproto3.ErrorResponse:
			return pgconn.ErrorResponseToPgError(m)
		case *pgproto3.CopyDone:
			return errors.New("logical replication ended unexpectedly")
		}
	}
}

func (c *Collector) refreshSchemaForReplay(ctx context.Context, conn *pgx.Conn, durable pglogrepl.LSN, oldHash string) error {
	if c.cfg.DeliveryTransport() != "grpc" {
		return errors.New("postgres schema changed; online adaptation requires the standard gRPC server")
	}
	current, err := catalog(ctx, conn, c.cfg.Tables)
	if err != nil {
		return err
	}
	hash := schemaHash(current)
	if hash == oldHash {
		return errSchemaMismatch
	}
	// A schema mismatch is discovered before its source transaction is locally
	// published. Discard only those invisible chunks, publish the full schema at
	// the previous durable LSN, then reconnect so WAL replays under the new map.
	if err := c.q.Recover(); err != nil {
		return err
	}
	for _, table := range current {
		raw, err := json.Marshal(table)
		if err != nil {
			return err
		}
		if err := c.q.Append(event.Message{Kind: "schema", SchemaVersion: hash, Schema: raw}); err != nil {
			return err
		}
	}
	if err := c.q.Append(event.Message{Kind: "schema_end", SchemaVersion: hash}); err != nil {
		return err
	}
	if err := c.q.PublishSchema(durable.String(), hash); err != nil {
		return err
	}
	c.metrics.SchemaChanged()
	c.log.InfoContext(ctx, "postgres schema refresh published; replaying WAL transaction", "schema_version", hash, "durable_lsn", durable.String())
	return nil
}

func feedback(ctx context.Context, r *pgconn.PgConn, lsn pglogrepl.LSN) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// pglogrepl's write ignores context; enforce a socket write deadline.
	if err := r.Conn().SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	err := pglogrepl.SendStandbyStatusUpdate(ctx, r, pglogrepl.StandbyStatusUpdate{WALWritePosition: lsn, WALFlushPosition: lsn, WALApplyPosition: lsn})
	return errors.Join(err, r.Conn().SetWriteDeadline(time.Time{}))
}
