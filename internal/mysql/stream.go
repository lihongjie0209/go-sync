package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"go-sync/internal/event"
)

func (c *Collector) stream(ctx context.Context, info Inspection, start gomysql.Position) error {
	ep, err := parseDSN(c.cfg.DSN)
	if err != nil {
		return err
	}
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{ServerID: c.cfg.MySQL.ServerID, Flavor: "mysql", Host: ep.host, Port: ep.port, User: ep.user, Password: ep.password, Charset: "utf8mb4", TimestampStringLocation: nil})
	defer syncer.Close()
	streamer, err := syncer.StartSync(start)
	if err != nil {
		return fmt.Errorf("mysql start replication: %w", err)
	}
	currentFile := start.Name
	transaction := encodePosition(start)
	b := batch{cfg: c.cfg, q: c.q, metrics: c.metrics, message: event.Message{Kind: "transaction_rows", Transaction: transaction}}
	tables := tableMap(info.Tables)
	var ordinal uint64
	for {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			return fmt.Errorf("mysql read binlog: %w", err)
		}
		c.metrics.Received(len(ev.RawData))
		switch body := ev.Event.(type) {
		case *replication.RotateEvent:
			currentFile = string(body.NextLogName)
		case *replication.RowsEvent:
			key := string(body.Table.Schema) + "\x00" + string(body.Table.Table)
			table, ok := tables[key]
			if !ok {
				continue
			}
			typ := body.Type()
			if typ == replication.EnumRowsEventTypeUnknown {
				return errors.New("mysql unknown row event")
			}
			for _, skipped := range body.SkippedColumns {
				if len(skipped) != 0 {
					return errors.New("mysql partial row image detected; binlog_row_image must remain FULL")
				}
			}
			switch typ {
			case replication.EnumRowsEventTypeInsert:
				for _, values := range body.Rows {
					ordinal++
					row, e := makeRow(table, "insert", nil, values, ordinal)
					if e != nil {
						return e
					}
					if e = b.add(ctx, row); e != nil {
						return e
					}
				}
			case replication.EnumRowsEventTypeDelete:
				for _, values := range body.Rows {
					ordinal++
					row, e := makeRow(table, "delete", values, nil, ordinal)
					if e != nil {
						return e
					}
					if e = b.add(ctx, row); e != nil {
						return e
					}
				}
			case replication.EnumRowsEventTypeUpdate:
				if len(body.Rows)%2 != 0 {
					return errors.New("mysql update event has incomplete before/after pair")
				}
				for i := 0; i < len(body.Rows); i += 2 {
					ordinal++
					row, e := makeRow(table, "update", body.Rows[i], body.Rows[i+1], ordinal)
					if e != nil {
						return e
					}
					if e = b.add(ctx, row); e != nil {
						return e
					}
				}
			}
		case *replication.XIDEvent:
			end := encodePosition(gomysql.Position{Name: currentFile, Pos: ev.Header.LogPos})
			if ordinal != 0 {
				if err := b.flush(ctx); err != nil {
					return err
				}
				if err := b.append(ctx, event.Message{Kind: "transaction_end", Transaction: transaction}); err != nil {
					return err
				}
			}
			if err := c.q.Publish(end, false); err != nil {
				return err
			}
			if ordinal != 0 {
				c.metrics.TransactionCommitted(ordinal)
			}
			ordinal = 0
			transaction = end
			b.message = event.Message{Kind: "transaction_rows", Transaction: transaction}
		case *replication.QueryEvent:
			query := strings.ToUpper(strings.TrimSpace(string(body.Query)))
			switch query {
			case "BEGIN":
				transaction = encodePosition(gomysql.Position{Name: currentFile, Pos: ev.Header.LogPos})
				b.message.Transaction = transaction
			case "COMMIT": // Transactional tables normally use XID_EVENT; tolerate explicit COMMIT without rows.
			case "ROLLBACK":
				if ordinal != 0 {
					return errors.New("mysql rollback encountered after row events")
				}
				checkpoint := encodePosition(gomysql.Position{Name: currentFile, Pos: ev.Header.LogPos})
				if err := c.q.Publish(checkpoint, false); err != nil {
					return err
				}
				transaction = checkpoint
				b.message.Transaction = transaction
			default:
				if ordinal != 0 {
					return errors.New("mysql DDL/implicit commit encountered with staged row events")
				}
				if err := c.refreshSchema(ctx, ev.Header.LogPos, currentFile, &info); err != nil {
					return err
				}
				tables = tableMap(info.Tables)
				transaction = encodePosition(gomysql.Position{Name: currentFile, Pos: ev.Header.LogPos})
				b.message.Transaction = transaction
			}
		}
	}
}

func tableMap(tables []Table) map[string]Table {
	m := make(map[string]Table, len(tables))
	for _, t := range tables {
		m[t.Schema+"\x00"+t.Name] = t
	}
	return m
}

func (c *Collector) refreshSchema(ctx context.Context, pos uint32, file string, info *Inspection) error {
	conn, _, err := connect(ctx, c.cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	next, err := inspect(conn, c.cfg)
	if err != nil {
		return err
	}
	if next.ServerUUID != info.ServerUUID {
		return errors.New("mysql source identity changed")
	}
	hash := schemaHash(next.Tables)
	checkpoint := encodePosition(gomysql.Position{Name: file, Pos: pos})
	if hash == schemaHash(info.Tables) {
		return c.q.Publish(checkpoint, false)
	}
	for _, t := range next.Tables {
		raw, e := json.Marshal(t)
		if e != nil {
			return e
		}
		if e = c.q.Append(event.Message{Kind: "schema", SchemaVersion: hash, Schema: raw}); e != nil {
			return e
		}
	}
	if c.cfg.DeliveryTransport() == "grpc" {
		if err = c.q.Append(event.Message{Kind: "schema_end", SchemaVersion: hash}); err != nil {
			return err
		}
	}
	if err = c.q.PublishSchema(checkpoint, hash); err != nil {
		return err
	}
	c.metrics.SchemaChanged()
	*info = next
	return nil
}
