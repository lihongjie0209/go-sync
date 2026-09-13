package mysql

import (
	"context"
	"errors"
	"fmt"
	"strings"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"go-sync/internal/config"
	"go-sync/internal/reconcile"
	syncv1 "go-sync/internal/rpc/syncv1"
)

// Reconcile scans one configured table from a consistent InnoDB snapshot.
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
	conn, _, err := connect(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if _, err := conn.Execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return nil, err
	}
	if _, err := conn.Execute("START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil {
			_, _ = conn.Execute("ROLLBACK")
		}
	}()
	table, err := inspectTable(conn, config.Table{Schema: request.SourceSchema, Name: request.Table})
	if err != nil {
		return nil, err
	}
	result = &syncv1.ReconcileResult{RequestId: request.RequestId, SourceSchema: table.Schema, Table: table.Name}
	projections := make([]string, len(table.Columns))
	for i, column := range table.Columns {
		result.Columns = append(result.Columns, column.Name)
		result.ColumnTypes = append(result.ColumnTypes, column.ColumnType)
		if column.PrimaryKey {
			result.PrimaryKeys = append(result.PrimaryKeys, column.Name)
		}
		if binaryType(column.Type) {
			projections[i] = "IF(" + quote(column.Name) + " IS NULL,NULL,CONCAT('0x',HEX(" + quote(column.Name) + ")))"
		} else {
			projections[i] = "CAST(" + quote(column.Name) + " AS CHAR CHARACTER SET utf8mb4)"
		}
	}
	builder, err := reconcile.NewBuilder(request.Buckets)
	if err != nil {
		return nil, err
	}
	var streamResult gomysql.Result
	err = conn.ExecuteSelectStreaming("SELECT "+strings.Join(projections, ",")+" FROM "+quote(table.Schema)+"."+quote(table.Name), &streamResult, func(values []gomysql.FieldValue) error {
		raw := make([]any, len(values))
		for i := range values {
			if values[i].Type != gomysql.FieldValueTypeNull {
				raw[i] = string(append([]byte(nil), values[i].AsString()...))
			}
		}
		row, err := makeRow(table, "read", nil, raw, 0)
		if err != nil {
			return err
		}
		return builder.Add(row)
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("mysql reconcile %s.%s: %w", table.Schema, table.Name, err)
	}
	if _, err := conn.Execute("COMMIT"); err != nil {
		return nil, err
	}
	for _, digest := range builder.Digests() {
		result.Digests = append(result.Digests, &syncv1.BucketDigest{Bucket: digest.Bucket, Rows: digest.Rows, Digest: digest.Hash})
	}
	return result, nil
}
