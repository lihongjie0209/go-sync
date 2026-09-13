package sqlserverlegacy

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"go-sync/internal/config"
	"go-sync/internal/reconcile"
	syncv1 "go-sync/internal/rpc/syncv1"
)

// Reconcile uses a serializable table scan. SQL Server 2000 has no snapshot
// isolation, so writers may briefly wait behind the scan's range locks.
func Reconcile(ctx context.Context, cfg config.Config, request *syncv1.ReconcileRequest) (result *syncv1.ReconcileResult, resultErr error) {
	allowed := false
	for _, table := range cfg.Tables {
		if table.Schema == request.SourceSchema && table.Name == request.Table {
			allowed = true
			break
		}
	}
	if !allowed {
		return nil, errors.New("reconciliation table is outside the configured capture scope")
	}
	db, err := open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true})
	if err != nil {
		return nil, dbError(err)
	}
	defer rollback(tx, &resultErr)
	info, err := inspect(ctx, tx, cfg)
	if err != nil {
		return nil, err
	}
	var table *Table
	for i := range info.Tables {
		if info.Tables[i].Schema == request.SourceSchema && info.Tables[i].Name == request.Table {
			table = &info.Tables[i]
			break
		}
	}
	if table == nil {
		return nil, errors.New("reconciliation table disappeared")
	}
	result = &syncv1.ReconcileResult{RequestId: request.RequestId, SourceSchema: table.Schema, Table: table.Name}
	for _, column := range table.Columns {
		result.Columns = append(result.Columns, column.Name)
		result.ColumnTypes = append(result.ColumnTypes, column.Type)
		if column.Primary {
			result.PrimaryKeys = append(result.PrimaryKeys, column.Name)
		}
	}
	builder, err := reconcile.NewBuilder(request.Buckets)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT "+strings.Join(snapshotFields(*table), ",")+" FROM "+table.sqlName()+" WITH (HOLDLOCK)")
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	for rows.Next() {
		values := make([][]byte, len(table.Columns))
		args := make([]any, len(values))
		for i := range values {
			args[i] = &values[i]
		}
		if err := rows.Scan(args...); err != nil {
			return nil, dbError(err)
		}
		row, err := snapshotRow(*table, values)
		if err != nil {
			return nil, err
		}
		if err := builder.Add(row); err != nil {
			return nil, err
		}
	}
	if err := dbError(rows.Err()); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, dbError(err)
	}
	for _, digest := range builder.Digests() {
		result.Digests = append(result.Digests, &syncv1.BucketDigest{Bucket: digest.Bucket, Rows: digest.Rows, Digest: digest.Hash})
	}
	return result, nil
}
