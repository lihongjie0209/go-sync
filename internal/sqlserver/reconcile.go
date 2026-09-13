package sqlserver

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"go-sync/internal/config"
	"go-sync/internal/event"
	"go-sync/internal/reconcile"
	syncv1 "go-sync/internal/rpc/syncv1"
)

// Reconcile scans a configured table using SQL Server snapshot isolation.
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
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		return nil, dbError(err)
	}
	defer rollback(tx, &resultErr)
	table, err := readTable(ctx, tx, config.Table{Schema: request.SourceSchema, Name: request.Table})
	if err != nil {
		return nil, err
	}
	result = &syncv1.ReconcileResult{RequestId: request.RequestId, SourceSchema: table.Schema, Table: table.Name}
	fields := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		fields = append(fields, projection(column))
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
	rows, err := tx.QueryContext(ctx, "SELECT "+strings.Join(fields, ",")+" FROM "+table.sqlName())
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	for rows.Next() {
		values := make([]*string, len(table.Columns))
		args := make([]any, len(values))
		for i := range values {
			args[i] = &values[i]
		}
		if err := rows.Scan(args...); err != nil {
			return nil, dbError(err)
		}
		columns, err := toColumns(table, values)
		if err != nil {
			return nil, err
		}
		key, err := rowKey(table, columns)
		if err != nil {
			return nil, err
		}
		if err := builder.Add(event.Row{Schema: table.Schema, Table: table.Name, Identity: table.RowIdentity, Key: key, Columns: columns}); err != nil {
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
