package sqlserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
)

// healthSample is advisory, never a checkpoint or permission to consume CDC.
// Invalid values mean unavailable, not zero. Every duration uses source-server
// timestamps, so collector clock skew does not affect these measurements.
type healthSample struct {
	ScanAgeSeconds         sql.NullFloat64
	CaptureLatencySeconds  sql.NullFloat64
	CheckpointLagSeconds   sql.NullFloat64
	RetentionMarginSeconds sql.NullFloat64
}

// readHealth uses only SQL Server 2008-era CDC functions and DMV columns. A DMV
// permission failure can return a useful partial sample alongside a safe error.
// Callers must not interrupt capture because advisory sampling failed.
func readHealth(ctx context.Context, q querier, tables []Table, durable lsn) (healthSample, error) {
	var sample healthSample
	var sampleErr error
	if durable != (lsn{}) {
		query, args := healthPositionQuery(tables, durable)
		err := q.QueryRowContext(ctx, query, args...).Scan(
			&sample.CheckpointLagSeconds, &sample.RetentionMarginSeconds,
		)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			sampleErr = dbError(err)
			sample.CheckpointLagSeconds = sql.NullFloat64{}
			sample.RetentionMarginSeconds = sql.NullFloat64{}
		}
	}

	// Do not use session_id=0: its latency is the last nonzero historical value
	// and therefore remains high even after the capture process catches up.
	// Empty scans establish freshness, but have no transaction latency sample.
	err := q.QueryRowContext(ctx, `SELECT TOP (1)
 CONVERT(float,DATEDIFF(second,end_time,GETDATE())),
 CASE WHEN tran_count>0 AND error_count=0 THEN CONVERT(float,latency) ELSE NULL END
 FROM sys.dm_cdc_log_scan_sessions
 WHERE session_id<>0 AND end_time IS NOT NULL
 ORDER BY session_id DESC`).Scan(&sample.ScanAgeSeconds, &sample.CaptureLatencySeconds)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		sampleErr = errors.Join(sampleErr, dbError(err))
		sample.ScanAgeSeconds = sql.NullFloat64{}
		sample.CaptureLatencySeconds = sql.NullFloat64{}
	}
	// Negative elapsed time can reflect a source clock adjustment; don't report
	// it as healthy zero lag. A negative retention margin instead means the
	// durable position is at/beyond the sampled cleanup boundary: report zero.
	sample.ScanAgeSeconds = healthNonnegative(sample.ScanAgeSeconds)
	sample.CaptureLatencySeconds = healthNonnegative(sample.CaptureLatencySeconds)
	sample.CheckpointLagSeconds = healthNonnegative(sample.CheckpointLagSeconds)
	if sample.RetentionMarginSeconds.Valid && sample.RetentionMarginSeconds.Float64 < 0 {
		sample.RetentionMarginSeconds.Float64 = 0
	}
	sample.RetentionMarginSeconds = healthNonnegative(sample.RetentionMarginSeconds)
	return sample, sampleErr
}

func healthPositionQuery(tables []Table, durable lsn) (string, []any) {
	args := []any{durable[:]}
	minimums := make([]string, 0, len(tables))
	for _, table := range tables {
		args = append(args, table.CaptureInstance)
		minimums = append(minimums, fmt.Sprintf(
			"SELECT sys.fn_cdc_map_lsn_to_time(sys.fn_cdc_get_min_lsn(@p%d)) AS min_time", len(args),
		))
	}
	if len(minimums) == 0 {
		minimums = append(minimums, "SELECT CONVERT(datetime,NULL) AS min_time")
	}
	// One unknown table must make the whole minimum safety margin unknown;
	// MAX alone would silently ignore NULL and overstate the remaining margin.
	// This is distance to the *current* low watermark, not time until cleanup.
	query := `SELECT CONVERT(float,DATEDIFF(second,p.durable_time,p.upper_time)),
 CASE WHEN m.total=m.known THEN CONVERT(float,DATEDIFF(second,m.min_time,p.durable_time))
 ELSE NULL END
 FROM (SELECT sys.fn_cdc_map_lsn_to_time(@p1) AS durable_time,
 sys.fn_cdc_map_lsn_to_time(sys.fn_cdc_get_max_lsn()) AS upper_time) p
 CROSS JOIN (SELECT COUNT(*) AS total,COUNT(min_time) AS known,MAX(min_time) AS min_time
 FROM (` + strings.Join(minimums, " UNION ALL ") + `) bounds) m`
	return query, args
}

func healthNonnegative(value sql.NullFloat64) sql.NullFloat64 {
	if !value.Valid || value.Float64 < 0 || math.IsNaN(value.Float64) || math.IsInf(value.Float64, 0) {
		return sql.NullFloat64{}
	}
	return value
}
