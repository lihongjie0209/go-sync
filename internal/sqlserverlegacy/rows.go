package sqlserverlegacy

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"go-sync/internal/event"
)

func normalize(col Column, raw []byte, isNull bool, isBinary bool) (*string, error) {
	if isNull {
		return nil, nil
	}
	value := string(raw)
	if col.BaseType == "float" || col.BaseType == "real" {
		return normalizeFloat(raw, col.BaseType)
	} else if isBinary {
		value = "0x" + hex.EncodeToString(raw)
	} else if col.BaseType == "bit" {
		switch value {
		case "0":
			value = "false"
		case "1":
			value = "true"
		default:
			return nil, errors.New("invalid sqlserver legacy bit value")
		}
	}
	return &value, nil
}

func normalizeFloat(raw []byte, base string) (*string, error) {
	if len(raw) != 4 && len(raw) != 8 {
		return nil, errors.New("invalid sqlserver legacy float width")
	}
	// SQL Server sends CONVERT(varbinary, float) in big-endian network order.
	value, err := strconv.ParseUint(hex.EncodeToString(raw), 16, 64)
	if err != nil {
		return nil, errors.New("invalid sqlserver legacy float bytes")
	}
	var result string
	if base == "real" {
		result = strconv.FormatFloat(float64(math.Float32frombits(uint32(value))), 'g', -1, 32)
	} else {
		result = strconv.FormatFloat(math.Float64frombits(value), 'g', -1, 64)
	}
	return &result, nil
}

func key(table Table, columns []event.Column) ([]event.Column, error) {
	result := []event.Column{}
	for i, col := range table.Columns {
		if table.RowIdentity != "full_row" && !col.Primary {
			continue
		}
		if col.Primary && columns[i].Value == nil {
			return nil, errors.New("sqlserver legacy primary key is null")
		}
		result = append(result, columns[i])
	}
	return result, nil
}

func snapshotRows(ctx context.Context, q querier, table Table, b *batch) (count uint64, result error) {
	fields := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		name := quote(col.Name)
		switch col.BaseType {
		case "binary", "varbinary", "timestamp", "float", "real":
			fields[i] = "CONVERT(varbinary(8000)," + name + ")"
		case "datetime", "smalldatetime":
			fields[i] = "CONVERT(varchar(30)," + name + ",126)"
		case "money", "smallmoney":
			fields[i] = "CONVERT(varchar(100)," + name + ",2)"
		case "bit":
			fields[i] = "CONVERT(varchar(1)," + name + ")"
		case "decimal", "numeric", "bigint", "int", "smallint", "tinyint", "uniqueidentifier":
			fields[i] = "CONVERT(varchar(100)," + name + ")"
		default:
			fields[i] = name
		}
	}
	rows, err := q.QueryContext(ctx, "SELECT "+strings.Join(fields, ",")+" FROM "+table.sqlName())
	if err != nil {
		return 0, dbError(err)
	}
	defer func() { result = errors.Join(result, dbError(rows.Close())) }()
	for rows.Next() {
		values := make([][]byte, len(table.Columns))
		args := make([]any, len(values))
		for i := range values {
			args[i] = &values[i]
		}
		if err := rows.Scan(args...); err != nil {
			return count, dbError(err)
		}
		columns := make([]event.Column, len(table.Columns))
		for i, col := range table.Columns {
			isBinary := col.BaseType == "binary" || col.BaseType == "varbinary" || col.BaseType == "timestamp" || col.BaseType == "float" || col.BaseType == "real"
			value, err := normalize(col, values[i], values[i] == nil, isBinary)
			if err != nil {
				return count, err
			}
			columns[i] = event.Column{Name: col.Name, Type: col.Type, Value: value}
		}
		rowKey, err := key(table, columns)
		if err != nil {
			return count, err
		}
		if err := b.add(ctx, event.Row{Schema: table.Schema, Table: table.Name, Identity: table.RowIdentity,
			Operation: "read", Key: rowKey, Columns: columns}); err != nil {
			return count, err
		}
		count++
		b.metrics.SQLSnapshotScanned()
	}
	return count, dbError(rows.Err())
}

type eventHeader struct {
	id        int64
	objectID  int
	operation string
}

func readEvent(ctx context.Context, q querier, events, values string, tables []Table) (int64, event.Row, error) {
	var header eventHeader
	err := q.QueryRowContext(ctx, "SELECT TOP 1 change_id,object_id,operation FROM "+events+" ORDER BY change_id").Scan(&header.id, &header.objectID, &header.operation)
	if err != nil {
		return 0, event.Row{}, err
	}
	row, err := readEventValues(ctx, q, values, tables, header)
	return header.id, row, err
}

func readEventValues(ctx context.Context, q querier, values string, tables []Table, header eventHeader) (event.Row, error) {
	var table *Table
	for i := range tables {
		if tables[i].ObjectID == header.objectID {
			table = &tables[i]
			break
		}
	}
	if table == nil {
		return event.Row{}, errors.New("sqlserver legacy outbox references an unconfigured table")
	}
	rows, err := q.QueryContext(ctx, "SELECT ordinal,is_null,value_text,value_unicode,value_binary FROM "+values+" WHERE change_id=@p1 ORDER BY ordinal", header.id)
	if err != nil {
		return event.Row{}, dbError(err)
	}
	defer rows.Close()
	columns := make([]event.Column, 0, len(table.Columns))
	for rows.Next() {
		var ordinal int
		var isNull bool
		var textValue, unicodeValue, binaryValue []byte
		if err := rows.Scan(&ordinal, &isNull, &textValue, &unicodeValue, &binaryValue); err != nil {
			return event.Row{}, dbError(err)
		}
		if ordinal != len(columns)+1 || ordinal > len(table.Columns) {
			return event.Row{}, errors.New("sqlserver legacy outbox column sequence is invalid")
		}
		col := table.Columns[ordinal-1]
		raw, binary := textValue, false
		if unicodeValue != nil {
			raw = unicodeValue
		}
		if binaryValue != nil {
			raw, binary = binaryValue, true
		}
		value, err := normalize(col, raw, isNull, binary)
		if err != nil {
			return event.Row{}, err
		}
		columns = append(columns, event.Column{Name: col.Name, Type: col.Type, Value: value})
	}
	if err := rows.Err(); err != nil {
		return event.Row{}, dbError(err)
	}
	if len(columns) != len(table.Columns) {
		return event.Row{}, errors.New("sqlserver legacy outbox row is incomplete")
	}
	rowKey, err := key(*table, columns)
	if err != nil {
		return event.Row{}, err
	}
	row := event.Row{Schema: table.Schema, Table: table.Name, Identity: table.RowIdentity, Key: rowKey}
	switch header.operation {
	case "I":
		row.Operation, row.Columns = "insert", columns
	case "D":
		row.Operation = "delete"
	default:
		return event.Row{}, fmt.Errorf("invalid sqlserver legacy operation %q", header.operation)
	}
	return row, nil
}

func readStatement(ctx context.Context, q querier, events, values string, tables []Table, consume func(string, int64, event.Row) error) (string, int64, uint64, error) {
	var statement string
	err := q.QueryRowContext(ctx, "SELECT TOP 1 CONVERT(varchar(36),statement_id) FROM "+events+" ORDER BY change_id").Scan(&statement)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, 0, err
	}
	if err != nil {
		return "", 0, 0, dbError(err)
	}
	rows, err := q.QueryContext(ctx, "SELECT change_id,object_id,operation FROM "+events+" WHERE statement_id=CONVERT(uniqueidentifier,@p1) ORDER BY change_id", statement)
	if err != nil {
		return "", 0, 0, dbError(err)
	}
	var headers []eventHeader
	for rows.Next() {
		var header eventHeader
		if err := rows.Scan(&header.id, &header.objectID, &header.operation); err != nil {
			rows.Close()
			return "", 0, 0, dbError(err)
		}
		headers = append(headers, header)
	}
	iterationErr := rows.Err()
	closeErr := rows.Close()
	if iterationErr != nil {
		return "", 0, 0, dbError(iterationErr)
	}
	if closeErr != nil {
		return "", 0, 0, dbError(closeErr)
	}
	if len(headers) == 0 {
		return "", 0, 0, errors.New("sqlserver legacy statement disappeared during read")
	}
	var maximum int64
	for _, header := range headers {
		maximum = max(maximum, header.id)
	}
	for _, header := range headers {
		row, err := readEventValues(ctx, q, values, tables, header)
		if err != nil {
			return "", 0, 0, err
		}
		if err := consume(statement, maximum, row); err != nil {
			return "", 0, 0, err
		}
	}
	return statement, maximum, uint64(len(headers)), nil
}
