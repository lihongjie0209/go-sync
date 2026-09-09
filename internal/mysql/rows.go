package mysql

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go-sync/internal/event"
)

func binaryType(t string) bool {
	switch strings.ToLower(t) {
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob", "geometry":
		return true
	}
	return false
}

func valueText(v any, col Column) (*string, error) {
	if v == nil {
		return nil, nil
	}
	var s string
	switch x := v.(type) {
	case []byte:
		if binaryType(col.Type) {
			s = "0x" + hex.EncodeToString(x)
		} else {
			s = string(x)
		}
	case string:
		s = x
	case int:
		s = strconv.Itoa(x)
	case int8:
		s = strconv.FormatInt(int64(x), 10)
	case int16:
		s = strconv.FormatInt(int64(x), 10)
	case int32:
		s = strconv.FormatInt(int64(x), 10)
	case int64:
		s = strconv.FormatInt(x, 10)
	case uint:
		s = strconv.FormatUint(uint64(x), 10)
	case uint8:
		s = strconv.FormatUint(uint64(x), 10)
	case uint16:
		s = strconv.FormatUint(uint64(x), 10)
	case uint32:
		s = strconv.FormatUint(uint64(x), 10)
	case uint64:
		s = strconv.FormatUint(x, 10)
	case float32:
		s = strconv.FormatFloat(float64(x), 'g', -1, 32)
	case float64:
		s = strconv.FormatFloat(x, 'g', -1, 64)
	case time.Time:
		s = x.UTC().Format("2006-01-02 15:04:05.999999")
	case fmt.Stringer:
		s = x.String()
	default:
		return nil, fmt.Errorf("unsupported mysql value %T for %s", v, col.Name)
	}
	return &s, nil
}

func makeRow(table Table, operation string, before, after []any, ordinal uint64) (event.Row, error) {
	if len(table.Columns) != len(before) && before != nil {
		return event.Row{}, errors.New("mysql row before-image does not match current schema")
	}
	if len(table.Columns) != len(after) && after != nil {
		return event.Row{}, errors.New("mysql row after-image does not match current schema")
	}
	r := event.Row{Schema: table.Schema, Table: table.Name, Operation: operation, Ordinal: ordinal}
	if table.RowIdentity == "full_row" {
		r.Identity = "full_row"
	}
	columns := func(values []any) ([]event.Column, error) {
		out := make([]event.Column, 0, len(values))
		for i, v := range values {
			text, err := valueText(v, table.Columns[i])
			if err != nil {
				return nil, err
			}
			out = append(out, event.Column{Name: table.Columns[i].Name, Type: table.Columns[i].ColumnType, Value: text})
		}
		return out, nil
	}
	old, err := columns(before)
	if err != nil {
		return r, err
	}
	current, err := columns(after)
	if err != nil {
		return r, err
	}
	key := func(values []event.Column) []event.Column {
		if table.RowIdentity == "full_row" {
			return values
		}
		var out []event.Column
		for i, c := range table.Columns {
			if c.PrimaryKey {
				out = append(out, values[i])
			}
		}
		return out
	}
	switch operation {
	case "insert", "read":
		r.Columns = current
		r.Key = key(current)
	case "update":
		r.OldKey = key(old)
		r.Key = key(current)
		r.Columns = current
	case "delete":
		r.Key = key(old)
	default:
		return r, errors.New("unknown mysql row operation")
	}
	return r, nil
}
