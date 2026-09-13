package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go-sync/internal/event"
	"go-sync/internal/reconcile"
	syncv1 "go-sync/internal/rpc/syncv1"
	"go-sync/internal/serverconfig"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (t *target) reconcileDigests(ctx context.Context, source *syncv1.ReconcileResult, buckets uint32) ([]reconcile.Digest, error) {
	if source.SourceSchema == "" || source.Table == "" || len(source.Columns) == 0 {
		return nil, errors.New("source reconciliation schema is incomplete")
	}
	if len(source.ColumnTypes) != len(source.Columns) {
		return nil, errors.New("source reconciliation column types are incomplete")
	}
	allowed := false
	for _, table := range t.cfg.Tables {
		if table.SourceSchema == source.SourceSchema && table.Name == source.Table {
			allowed = true
			break
		}
	}
	if !allowed {
		return nil, errors.New("source reconciliation table is not configured")
	}
	primary := make(map[string]bool, len(source.PrimaryKeys))
	for _, name := range source.PrimaryKeys {
		primary[name] = true
	}
	selects := make([]string, 0, len(source.Columns))
	for _, name := range source.Columns {
		selects = append(selects, "CASE WHEN "+quote(name)+" IS NULL THEN NULL ELSE "+quote(name)+"::text END")
	}
	tx, err := t.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SET LOCAL TIME ZONE 'UTC'"); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, "SELECT "+strings.Join(selects, ",")+" FROM "+quote(t.cfg.Postgres.TargetSchema)+"."+quote(source.Table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	builder, err := reconcile.NewBuilder(buckets)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		raw := rows.RawValues()
		row := event.Row{Schema: source.SourceSchema, Table: source.Table}
		for index, name := range source.Columns {
			column := event.Column{Name: name, Type: source.ColumnTypes[index]}
			if raw[index] != nil {
				value := string(raw[index])
				column.Value = &value
			}
			row.Columns = append(row.Columns, column)
			if primary[name] {
				row.Key = append(row.Key, column)
			}
		}
		if len(source.PrimaryKeys) == 0 {
			row.Identity = "full_row"
			row.Key = append([]event.Column(nil), row.Columns...)
		}
		if err := builder.Add(row); err != nil {
			return nil, err
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return builder.Digests(), nil
}

func sameDigests(source []*syncv1.BucketDigest, target []reconcile.Digest) (bool, int) {
	if len(source) != len(target) {
		return false, max(len(source), len(target))
	}
	mismatches := 0
	for index := range target {
		if source[index].Bucket != target[index].Bucket || source[index].Rows != target[index].Rows || source[index].Digest != target[index].Hash {
			mismatches++
		}
	}
	return mismatches == 0, mismatches
}

func (s *Service) reconcileDue(id string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.cfg.Reconcile.Enabled {
		return false
	}
	if s.reconcileNext == nil {
		s.reconcileNext = make(map[string]time.Time)
	}
	if next := s.reconcileNext[id]; !next.IsZero() && now.Before(next) {
		return false
	}
	location, _ := time.LoadLocation(s.cfg.Reconcile.Timezone)
	clock, _ := time.Parse("15:04", s.cfg.Reconcile.DailyAt)
	local := now.In(location)
	next := time.Date(local.Year(), local.Month(), local.Day(), clock.Hour(), clock.Minute(), 0, 0, location)
	if s.reconcileNext[id].IsZero() && local.Before(next) {
		s.reconcileNext[id] = next
		return false
	}
	if !next.After(local) {
		next = next.AddDate(0, 0, 1)
	}
	s.reconcileNext[id] = next
	return true
}

func (s *Service) retryReconcile(id string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reconcileNext[id] = now.Add(5 * time.Minute)
}

func (s *Service) runReconcile(stream grpc.BidiStreamingServer[syncv1.CollectorFrame, syncv1.ServerFrame], id string, target *target) (bool, error) {
	s.mu.RLock()
	cfg := s.cfg.Reconcile
	tables := append([]serverconfig.Table(nil), target.cfg.Tables...)
	s.mu.RUnlock()
	requestID := uuid.NewString()
	started := time.Now()
	completed := false
	defer func() {
		s.metrics.reconcileDuration.WithLabelValues(id).Observe(time.Since(started).Seconds())
		if !completed {
			target.recordReconcile(requestID, started, "error", 0, "reconciliation did not complete")
		}
	}()
	s.log.InfoContext(stream.Context(), "scheduled reconciliation started", "syncer_id", id, "request_id", requestID, "tables", len(tables), "buckets", cfg.Buckets)
	totalMismatch := 0
	for _, table := range tables {
		timeout, _ := time.ParseDuration(cfg.Timeout)
		request := &syncv1.ReconcileRequest{RequestId: requestID, SourceSchema: table.SourceSchema, Table: table.Name, Buckets: uint32(cfg.Buckets), AllBuckets: true, TimeoutSeconds: uint32(timeout / time.Second)}
		if err := stream.Send(&syncv1.ServerFrame{Body: &syncv1.ServerFrame_Reconcile{Reconcile: request}}); err != nil {
			return false, err
		}
		frame, err := stream.Recv()
		if err != nil {
			return false, err
		}
		result := frame.GetReconcileResult()
		if result == nil || result.RequestId != requestID || result.SourceSchema != table.SourceSchema || result.Table != table.Name {
			return false, status.Error(codes.InvalidArgument, "invalid reconciliation response")
		}
		if result.Error != "" {
			return false, errors.New("collector reconciliation failed")
		}
		targetDigests, err := target.reconcileDigests(stream.Context(), result, uint32(cfg.Buckets))
		if err != nil {
			return false, err
		}
		_, mismatches := sameDigests(result.Digests, targetDigests)
		totalMismatch += mismatches
	}
	s.metrics.reconcileBuckets.WithLabelValues(id).Set(float64(totalMismatch))
	canRepair := false
	if totalMismatch > 0 && cfg.AutoRepair && !cfg.DryRun {
		minimum, _ := time.ParseDuration(cfg.RepairMinInterval)
		var err error
		canRepair, err = target.repairAllowed(stream.Context(), minimum)
		if err != nil {
			return false, err
		}
	}
	if canRepair {
		if err := target.authorizeRepair(stream.Context()); err != nil {
			return false, err
		}
		if err := stream.Send(&syncv1.ServerFrame{Body: &syncv1.ServerFrame_Repair{Repair: &syncv1.RepairRequest{RequestId: requestID, Reason: fmt.Sprintf("%d mismatched hash buckets", totalMismatch)}}}); err != nil {
			return false, err
		}
		frame, err := stream.Recv()
		if err != nil {
			return false, err
		}
		if accepted := frame.GetRepairAccepted(); accepted == nil || accepted.RequestId != requestID {
			return false, status.Error(codes.InvalidArgument, "invalid repair acknowledgement")
		}
		s.metrics.reconciles.WithLabelValues(id, "repair").Inc()
		target.recordReconcile(requestID, started, "repair", totalMismatch, "full snapshot requested")
		completed = true
		s.log.WarnContext(stream.Context(), "reconciliation requested full snapshot repair", "syncer_id", id, "request_id", requestID, "mismatched_buckets", totalMismatch, "duration_seconds", time.Since(started).Seconds())
		return true, nil
	}
	if err := stream.Send(&syncv1.ServerFrame{Body: &syncv1.ServerFrame_Resume{Resume: &syncv1.ResumeRequest{RequestId: requestID}}}); err != nil {
		return false, err
	}
	frame, err := stream.Recv()
	if err != nil {
		return false, err
	}
	if accepted := frame.GetResumeAccepted(); accepted == nil || accepted.RequestId != requestID {
		return false, status.Error(codes.InvalidArgument, "invalid resume acknowledgement")
	}
	result := "match"
	if totalMismatch > 0 {
		result = "mismatch"
	}
	s.metrics.reconciles.WithLabelValues(id, result).Inc()
	target.recordReconcile(requestID, started, result, totalMismatch, "")
	completed = true
	s.log.InfoContext(stream.Context(), "scheduled reconciliation completed", "syncer_id", id, "request_id", requestID, "result", result, "mismatched_buckets", totalMismatch, "duration_seconds", time.Since(started).Seconds())
	return false, nil
}

func (t *target) recordReconcile(requestID string, started time.Time, result string, mismatches int, detail string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = t.pool.Exec(ctx, "INSERT INTO "+quote(t.cfg.Postgres.MetadataSchema)+`.reconcile_audit
        (syncer_id,request_id,started_at,finished_at,result,mismatched_buckets,detail)
        VALUES($1,$2,$3,clock_timestamp(),$4,$5,$6)`, t.cfg.ID, requestID, started, result, mismatches, detail)
}

func (t *target) repairAllowed(ctx context.Context, minimum time.Duration) (bool, error) {
	var allowed bool
	err := t.pool.QueryRow(ctx, "SELECT NOT EXISTS(SELECT 1 FROM "+quote(t.cfg.Postgres.MetadataSchema)+`.reconcile_audit
        WHERE syncer_id=$1 AND result='repair' AND finished_at > clock_timestamp()-($2::double precision * interval '1 second'))`,
		t.cfg.ID, minimum.Seconds()).Scan(&allowed)
	return allowed, err
}
