package sqlserver

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"go-sync/internal/event"
)

func legacyLOB(base string) bool { return base == "text" || base == "ntext" || base == "image" }

// projection makes decimal, money, Unicode and temporal values text in SQL,
// before any driver numeric conversion. Floats cross as binary IEEE values.
func projection(col Column) string {
	name := quote(col.Name)
	switch col.BaseType {
	case "binary", "varbinary", "image", "timestamp", "float", "real":
		return "CONVERT(nvarchar(max),CONVERT(varbinary(max)," + name + "),2)"
	case "money", "smallmoney":
		return "CONVERT(nvarchar(max)," + name + ",2)"
	case "datetime", "smalldatetime", "datetime2", "date", "time", "datetimeoffset":
		return "CONVERT(nvarchar(max)," + name + ",126)"
	default:
		return "CONVERT(nvarchar(max)," + name + ")"
	}
}

func normalize(col Column, value *string) (*string, error) {
	if value == nil {
		return nil, nil
	}
	s := *value
	switch col.BaseType {
	case "binary", "varbinary", "image", "timestamp":
		s = "0x" + strings.ToLower(s)
	case "bit":
		switch s {
		case "0":
			s = "false"
		case "1":
			s = "true"
		default:
			return nil, errors.New("invalid sqlserver bit value")
		}
	case "float", "real":
		b, err := hex.DecodeString(s)
		if err != nil {
			return nil, errors.New("invalid sqlserver float bytes")
		}
		switch len(b) {
		case 8:
			s = strconv.FormatFloat(math.Float64frombits(binary.BigEndian.Uint64(b)), 'g', -1, 64)
		case 4:
			s = strconv.FormatFloat(float64(math.Float32frombits(binary.BigEndian.Uint32(b))), 'g', -1, 32)
		default:
			return nil, errors.New("invalid sqlserver float width")
		}
	}
	return &s, nil
}

func toColumns(table Table, values []*string) ([]event.Column, error) {
	if len(values) != len(table.Columns) {
		return nil, errors.New("cdc row column count differs from schema")
	}
	cols := make([]event.Column, 0, len(values))
	for i, value := range values {
		normalized, err := normalize(table.Columns[i], value)
		if err != nil {
			return nil, err
		}
		cols = append(cols, event.Column{Name: table.Columns[i].Name, Type: table.Columns[i].Type, Value: normalized})
	}
	return cols, nil
}

func rowKey(table Table, cols []event.Column) ([]event.Column, error) {
	key := []event.Column{}
	for i, col := range table.Columns {
		if table.RowIdentity != "full_row" && !col.Primary {
			continue
		}
		if col.Primary && cols[i].Value == nil {
			return nil, errors.New("cdc primary key is null")
		}
		key = append(key, cols[i])
	}
	return key, nil
}

func changed(mask []byte, ordinal int) (bool, error) {
	if ordinal < 1 || ordinal > len(mask)*8 {
		return false, errors.New("invalid cdc update mask")
	}
	index := len(mask) - 1 - (ordinal-1)/8
	return mask[index]&(1<<uint((ordinal-1)%8)) != 0, nil
}

type change struct {
	table     int
	sequence  lsn
	command   int
	operation int
	mask      []byte
	values    []*string
}

func convertChange(table Table, current change, before *change) (event.Row, error) {
	row := event.Row{Schema: table.Schema, Table: table.Name, Identity: table.RowIdentity}
	cols, err := toColumns(table, current.values)
	if err != nil {
		return row, err
	}
	row.Key, err = rowKey(table, cols)
	if err != nil {
		return row, err
	}
	switch current.operation {
	case 1:
		row.Operation = "delete"
	case 2:
		row.Operation = "insert"
		row.Columns = cols
	case 4:
		if before == nil {
			return row, errors.New("cdc update has no before image")
		}
		old, err := toColumns(table, before.values)
		if err != nil {
			return row, err
		}
		row.Operation = "update"
		for i, col := range table.Columns {
			isChanged, err := changed(current.mask, col.Ordinal)
			if err != nil {
				return row, err
			}
			// SQL Server suppresses unchanged MAX before images. Reconstruct
			// only when its update mask explicitly proves the value unchanged.
			if col.MaxLength == -1 && !isChanged {
				old[i] = cols[i]
			}
			if legacyLOB(col.BaseType) && !isChanged {
				continue
			}
			row.Columns = append(row.Columns, cols[i])
		}
		row.OldKey, err = rowKey(table, old)
		if err != nil {
			return row, err
		}
	default:
		return row, fmt.Errorf("invalid cdc operation %d", current.operation)
	}
	return row, nil
}
