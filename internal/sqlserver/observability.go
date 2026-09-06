package sqlserver

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

func (c *Collector) sampleHealth(ctx context.Context, db *sql.DB, tables []Table) {
	sampleCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var sample healthSample
	st, err := c.q.State()
	if err == nil {
		var durable lsn
		durable, err = parseLSN(st.DurableLSN)
		if err == nil {
			sample, err = readHealth(sampleCtx, db, tables, durable)
		}
	}
	c.publishHealth(sample)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			c.metrics.SQLHealthError()
			if c.healthWarningAt.IsZero() || time.Since(c.healthWarningAt) >= 5*time.Minute {
				c.log.WarnContext(sampleCtx, "sqlserver health sampling unavailable; capture continues",
					"hint", "verify monitoring permissions and connectivity")
				c.healthWarningAt = time.Now()
			}
		}
		return
	}
	if !c.healthWarningAt.IsZero() {
		c.log.InfoContext(ctx, "sqlserver health sampling recovered")
		c.healthWarningAt = time.Time{}
	}
}

func (c *Collector) publishHealth(sample healthSample) {
	for signal, value := range map[string]sql.NullFloat64{
		"scan_age":         sample.ScanAgeSeconds,
		"capture_latency":  sample.CaptureLatencySeconds,
		"checkpoint_lag":   sample.CheckpointLagSeconds,
		"retention_margin": sample.RetentionMarginSeconds,
	} {
		c.metrics.SQLHealthValue(signal, value.Float64, value.Valid)
	}
}

// schemaError classifies catalog mismatches without exposing SQL or row values.
type schemaError string

func (e schemaError) Error() string { return string(e) }

func (c *Collector) observeSchemaError(err error) {
	var mismatch schemaError
	if errors.As(err, &mismatch) {
		c.metrics.SchemaChanged()
	}
}
