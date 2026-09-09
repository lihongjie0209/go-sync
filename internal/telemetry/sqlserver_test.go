package telemetry

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestSQLServerProgressIsNotPublication(t *testing.T) {
	t.Parallel()
	m, _, _ := metricsFixture(t)
	m.SQLSnapshotStarted()
	m.SQLSnapshotScanned()
	m.SQLSnapshotScanned()
	m.SQLSnapshotTableCompleted()
	finish := m.SQLSnapshotWait("locks")
	finish(false)
	m.SQLSnapshotFinished(true)
	m.RecoveryDiscarded(3)
	body := scrape(t, m)
	for _, sample := range []string{
		"go_sync_sqlserver_snapshot_active 0",
		"go_sync_sqlserver_snapshot_scanned_rows 2",
		"go_sync_sqlserver_snapshot_completed_tables 1",
		"go_sync_sqlserver_snapshot_failures_total 1",
		"go_sync_capture_snapshot_rows_total 0",
		"go_sync_capture_recovery_discarded_messages_total 3",
		`go_sync_sqlserver_snapshot_wait_start_timestamp_seconds{stage="locks"} 0`,
		`go_sync_sqlserver_snapshot_wait_duration_seconds_count{result="error",stage="locks"} 1`,
	} {
		requireSample(t, body, sample)
	}
	m.SQLSnapshotStarted()
	requireSample(t, scrape(t, m), "go_sync_sqlserver_snapshot_scanned_rows 0")
	m.SQLSnapshotFinished(false)
	requireSample(t, scrape(t, m), "go_sync_sqlserver_snapshot_failures_total 1")
}

func TestSQLServerHealthReplacesStaleValues(t *testing.T) {
	t.Parallel()
	m, _, _ := metricsFixture(t)
	m.SQLHealthValue("checkpoint_lag", 12.5, true)
	body := scrape(t, m)
	requireSample(t, body, "go_sync_sqlserver_checkpoint_lag_seconds 12.5")
	if strings.Contains(body, `go_sync_sqlserver_health_sample_timestamp_seconds{signal="checkpoint_lag"} 0`) {
		t.Fatal("known sample timestamp not updated")
	}
	m.SQLHealthValue("checkpoint_lag", math.NaN(), true)
	body = scrape(t, m)
	requireSample(t, body, "go_sync_sqlserver_checkpoint_lag_seconds NaN")
	requireSample(t, body, `go_sync_sqlserver_health_sample_timestamp_seconds{signal="checkpoint_lag"} 0`)
	m.SQLHealthError()
	requireSample(t, scrape(t, m), "go_sync_sqlserver_health_errors_total 1")
	m.SQLHealthValue("unknown-signal", 1, true)
}

func TestSQLServerLabelsAreBounded(t *testing.T) {
	t.Parallel()
	m, q, cfg := metricsFixture(t)
	cfg.SourceType = "sqlserver"
	m = New(q, cfg)
	m.SQLSnapshotWait("secret-table-name")(false)
	m.CDCPollOutcome(true, false, false)
	m.CDCPollOutcome(false, false, false)
	m.CDCPollOutcome(false, true, false)
	m.CDCPollOutcome(false, true, true)
	body := scrape(t, m)
	requireSample(t, body, `go_sync_source_info{engine="sqlserver"} 1`)
	for _, result := range []string{"progress", "idle", "error", "canceled"} {
		requireSample(t, body, `go_sync_sqlserver_cdc_polls_total{result="`+result+`"} 1`)
	}
	if strings.Contains(body, "secret-table-name") {
		t.Fatal("unbounded stage label")
	}
	var disabled *Metrics
	disabled.SQLSnapshotStarted()
	disabled.SQLSnapshotScanned()
	disabled.SQLSnapshotTableCompleted()
	disabled.SQLSnapshotFinished(true)
	disabled.SQLSnapshotWait("fence")(true)
	disabled.CDCPollOutcome(true, false, false)
	disabled.RecoveryDiscarded(1)
}

func TestSQLServerLegacyMetrics(t *testing.T) {
	t.Parallel()
	m, q, cfg := metricsFixture(t)
	cfg.SourceType = "sqlserver_legacy"
	m = New(q, cfg)
	m.LegacyPoll(time.Second, true, nil, false)
	m.LegacyPoll(time.Second, false, errors.New("failed"), false)
	m.LegacyOutboxDelete(true)
	m.LegacyOutboxDelete(false)
	body := scrape(t, m)
	for _, sample := range []string{
		`go_sync_source_info{engine="sqlserver_legacy"} 1`,
		`go_sync_sqlserver_legacy_polls_total{result="progress"} 1`,
		`go_sync_sqlserver_legacy_polls_total{result="error"} 1`,
		"go_sync_sqlserver_legacy_outbox_events_deleted_total 1",
		"go_sync_sqlserver_legacy_outbox_delete_errors_total 1",
	} {
		requireSample(t, body, sample)
	}
}

func TestSQLServerLegacyOutboxMetrics(t *testing.T) {
	t.Parallel()
	m, _, _ := metricsFixture(t)
	m.LegacyOutboxDelete(true)
	m.LegacyOutboxDelete(false)
	body := scrape(t, m)
	requireSample(t, body, "go_sync_sqlserver_legacy_outbox_events_deleted_total 1")
	requireSample(t, body, "go_sync_sqlserver_legacy_outbox_delete_errors_total 1")
	var disabled *Metrics
	disabled.LegacyOutboxDelete(true)
}
