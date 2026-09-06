package capture

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/jackc/pglogrepl"
	"go-sync/internal/config"
	"go-sync/internal/event"
	"go-sync/internal/queue"
)

var errSchemaMismatch = errors.New("row schema differs from captured schema")

type wireColumn struct {
	Name  string          `json:"name"`
	Type  string          `json:"type"`
	OID   uint32          `json:"typeoid"`
	Value json.RawMessage `json:"value"`
}
type record struct {
	Action   string       `json:"action"`
	XID      uint32       `json:"xid"`
	LSN      string       `json:"lsn"`
	NextLSN  string       `json:"nextlsn"`
	Schema   string       `json:"schema"`
	Table    string       `json:"table"`
	Columns  []wireColumn `json:"columns"`
	Identity []wireColumn `json:"identity"`
}
type decoder struct {
	b       batcher
	tables  map[config.Table]Table
	durable pglogrepl.LSN
	active  bool
	skip    bool
	end     pglogrepl.LSN
	xid     uint32
	ordinal uint64
}

func newDecoder(c config.Config, q *queue.Store, tables []Table, start pglogrepl.LSN) *decoder {
	d := &decoder{b: batcher{cfg: c, q: q}, tables: make(map[config.Table]Table), durable: start}
	for _, t := range tables {
		d.tables[config.Table{Schema: t.Schema, Name: t.Name}] = t
	}
	return d
}
func (d *decoder) consume(ctx context.Context, data []byte) error {
	// wal2json v2 emits complete records per CopyData; permit multiple JSON
	// objects in a frame, but never treat a malformed/truncated record as success.
	if len(data) > d.b.cfg.MaxRowBytes*2 {
		return errors.New("wal record exceeds hard limit")
	}
	p := json.NewDecoder(bytes.NewReader(data))
	for {
		var r record
		err := p.Decode(&r)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.New("invalid wal2json record")
		}
		if err := d.record(ctx, r); err != nil {
			return err
		}
	}
}
func (d *decoder) record(ctx context.Context, r record) error {
	switch r.Action {
	case "B":
		if d.active {
			return errors.New("nested transaction")
		}
		end, err := pglogrepl.ParseLSN(r.NextLSN)
		if err != nil || end == 0 {
			return errors.New("begin missing transaction end lsn")
		}
		d.active = true
		d.end = end
		d.xid = r.XID
		d.ordinal = 0
		d.skip = end <= d.durable
		d.b.msg = event.Message{Kind: "transaction_rows", Transaction: r.NextLSN, LSN: r.NextLSN}
	case "C":
		if !d.active || r.XID != d.xid {
			return errors.New("commit without matching begin")
		}
		end, err := pglogrepl.ParseLSN(r.NextLSN)
		if err != nil || end != d.end {
			return errors.New("commit lsn mismatch")
		}
		if !d.skip {
			if err := d.b.flush(ctx); err != nil {
				return err
			}
			if err := d.b.append(ctx, event.Message{Kind: "transaction_end", Transaction: r.NextLSN, LSN: r.NextLSN, Chunk: d.b.msg.Chunk}); err != nil {
				return err
			}
			if err := d.b.q.Publish(r.NextLSN, false); err != nil {
				return err
			}
			d.durable = end
			d.b.metrics.TransactionCommitted(d.ordinal)
		}
		d.active = false
	case "I", "U", "D":
		if !d.active || r.XID != d.xid {
			return errors.New("row outside matching transaction")
		}
		if d.skip {
			return nil
		}
		d.ordinal++
		t, ok := d.tables[config.Table{Schema: r.Schema, Name: r.Table}]
		if !ok {
			return errors.New("unexpected table in replication stream")
		}
		row, err := convert(r, t)
		if err != nil {
			if errors.Is(err, errSchemaMismatch) {
				d.b.metrics.SchemaChanged()
			}
			return err
		}
		row.Ordinal = d.ordinal
		return d.b.add(ctx, row)
	case "T":
		return errors.New("truncate requires explicit reinitialization")
	case "M":
		return errors.New("unexpected logical message")
	default:
		return fmt.Errorf("unknown wal2json action %q", r.Action)
	}
	return nil
}

func convert(r record, t Table) (event.Row, error) {
	row := event.Row{Schema: r.Schema, Table: r.Table}
	row.Operation = map[string]string{"I": "insert", "U": "update", "D": "delete"}[r.Action]
	known := map[string]Column{}
	for _, c := range t.Columns {
		known[c.Name] = c
	}
	decode := func(cols []wireColumn) ([]event.Column, error) {
		var out []event.Column
		seen := map[string]bool{}
		for _, col := range cols {
			expected, ok := known[col.Name]
			if !ok || seen[col.Name] || expected.Type != col.Type || expected.OID != col.OID {
				return nil, errSchemaMismatch
			}
			seen[col.Name] = true
			v, err := event.Text(col.Value)
			if err != nil {
				return nil, err
			}
			if v != nil && col.OID == 17 {
				value := "\\x" + *v
				v = &value
			}
			out = append(out, event.Column{Name: col.Name, Type: expected.Type, Value: v})
		}
		return out, nil
	}
	var err error
	row.Columns, err = decode(r.Columns)
	if err != nil {
		return row, err
	}
	old, err := decode(r.Identity)
	if err != nil {
		return row, err
	}
	values := map[string]event.Column{}
	for _, c := range row.Columns {
		values[c.Name] = c
	}
	oldValues := map[string]event.Column{}
	for _, c := range old {
		oldValues[c.Name] = c
	}
	if t.RowIdentity == "full_row" {
		row.Identity = "full_row"
		if r.Action == "U" || r.Action == "D" {
			if len(oldValues) != len(t.Columns) {
				return row, errors.New("full-row identity missing columns; refusing partial row matching")
			}
		}
		for _, col := range t.Columns {
			previous := oldValues[col.Name]
			if r.Action == "D" {
				row.Key = append(row.Key, previous)
				continue
			}
			value, ok := values[col.Name]
			if !ok {
				if r.Action != "U" {
					return row, fmt.Errorf("insert column count differs from schema: %w", errSchemaMismatch)
				}
				// FULL old identities contain detoasted values. Missing UPDATE
				// columns represent unchanged TOAST, not NULL.
				value = previous
			}
			row.Key = append(row.Key, value)
			if r.Action == "U" {
				row.OldKey = append(row.OldKey, previous)
			}
		}
		return row, nil
	}
	for _, name := range t.KeyNames() {
		if r.Action == "D" {
			v, ok := oldValues[name]
			if !ok || v.Value == nil {
				return row, errors.New("delete missing primary key")
			}
			row.Key = append(row.Key, v)
			continue
		}
		v, ok := values[name]
		if !ok || v.Value == nil {
			return row, errors.New("row missing primary key")
		}
		row.Key = append(row.Key, v)
		if r.Action == "U" {
			ov, ok := oldValues[name]
			if !ok || ov.Value == nil {
				return row, errors.New("update missing old primary key")
			}
			row.OldKey = append(row.OldKey, ov)
		}
	}
	if r.Action == "I" && len(row.Columns) != len(t.Columns) {
		return row, fmt.Errorf("insert column count differs from schema: %w", errSchemaMismatch)
	}
	return row, nil
}
