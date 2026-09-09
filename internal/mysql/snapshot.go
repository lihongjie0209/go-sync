package mysql

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"go-sync/internal/event"
	"go-sync/internal/queue"
)

func (c *Collector) snapshot(ctx context.Context, info Inspection) (result error) {
	started := time.Now()
	if !c.cfg.MySQL.AllowSnapshotLock {
		return errors.New("initial mysql snapshot requires mysql.allow_snapshot_lock=true because FLUSH TABLES WITH READ LOCK briefly blocks writes")
	}
	duration, _ := time.ParseDuration(c.cfg.MySQL.SnapshotTimeout)
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	lockConn, _, err := connect(ctx, c.cfg)
	if err != nil {
		return err
	}
	defer lockConn.Close()
	if _, err = lockConn.Execute("FLUSH TABLES WITH READ LOCK"); err != nil {
		return fmt.Errorf("mysql acquire snapshot lock (RELOAD privilege required): %w", err)
	}
	locked := true
	defer func() {
		if locked {
			_, _ = lockConn.Execute("UNLOCK TABLES")
		}
	}()
	r, err := lockConn.Execute("SHOW MASTER STATUS")
	if err != nil || r.RowNumber() != 1 {
		return errors.New("mysql SHOW MASTER STATUS returned no binlog position")
	}
	file, _ := r.GetString(0, 0)
	pos64, err := r.GetUint(0, 1)
	if err != nil {
		return err
	}
	boundary := gomysql.Position{Name: file, Pos: uint32(pos64)}
	snapshotConn, _, err := connect(ctx, c.cfg)
	if err != nil {
		return err
	}
	defer snapshotConn.Close()
	if _, err = snapshotConn.Execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return err
	}
	if _, err = snapshotConn.Execute("START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
		return err
	}
	defer func() {
		if result != nil {
			_, _ = snapshotConn.Execute("ROLLBACK")
		}
	}()
	current, err := inspect(snapshotConn, c.cfg)
	if err != nil {
		return err
	}
	if current.ServerUUID != info.ServerUUID || current.Database != info.Database || schemaHash(current.Tables) != schemaHash(info.Tables) {
		return errors.New("mysql source or schema changed while acquiring snapshot boundary")
	}
	info = current
	if _, err = lockConn.Execute("UNLOCK TABLES"); err != nil {
		return err
	}
	locked = false
	var token [32]byte
	if _, err = rand.Read(token[:]); err != nil {
		return err
	}
	generation := hex.EncodeToString(token[:])
	st := queue.State{ProtocolVersion: "v1", SourceID: c.cfg.SourceID, Generation: generation, Fingerprint: c.cfg.Fingerprint(), SystemID: info.ServerUUID, Database: info.Database, SchemaHash: schemaHash(info.Tables)}
	for _, t := range info.Tables {
		if t.RowIdentity == "full_row" {
			st.ProtocolVersion = "v2"
		}
	}
	if err = c.q.Initialize(st); err != nil {
		return err
	}
	b := batch{cfg: c.cfg, q: c.q, metrics: c.metrics}
	refs := make([]event.TableRef, 0, len(info.Tables))
	for _, t := range info.Tables {
		refs = append(refs, event.TableRef{Schema: t.Schema, Name: t.Name})
	}
	if err = b.append(ctx, event.Message{Kind: "snapshot_begin", Tables: refs}); err != nil {
		return err
	}
	var total uint64
	for _, t := range info.Tables {
		schema, _ := json.Marshal(t)
		if err = b.append(ctx, event.Message{Kind: "schema", Schema: schema}); err != nil {
			return err
		}
		b.message = event.Message{Kind: "snapshot_rows"}
		projections := make([]string, len(t.Columns))
		for i, col := range t.Columns {
			if binaryType(col.Type) {
				projections[i] = "IF(" + quote(col.Name) + " IS NULL,NULL,CONCAT('0x',HEX(" + quote(col.Name) + ")))"
			} else {
				projections[i] = "CAST(" + quote(col.Name) + " AS CHAR CHARACTER SET utf8mb4)"
			}
		}
		var streamResult gomysql.Result
		err = snapshotConn.ExecuteSelectStreaming("SELECT "+strings.Join(projections, ",")+" FROM "+quote(t.Schema)+"."+quote(t.Name), &streamResult, func(values []gomysql.FieldValue) error {
			raw := make([]any, len(values))
			for i := range values {
				if values[i].Type != gomysql.FieldValueTypeNull {
					raw[i] = string(append([]byte(nil), values[i].AsString()...))
				}
			}
			row, e := makeRow(t, "read", nil, raw, total+1)
			if e != nil {
				return e
			}
			if e = b.add(ctx, row); e != nil {
				return e
			}
			total++
			return nil
		}, nil)
		if err != nil {
			return fmt.Errorf("mysql snapshot %s.%s: %w", t.Schema, t.Name, err)
		}
		if err = b.flush(ctx); err != nil {
			return err
		}
	}
	if err = b.append(ctx, event.Message{Kind: "snapshot_end"}); err != nil {
		return err
	}
	if _, err = snapshotConn.Execute("COMMIT"); err != nil {
		return err
	}
	checkpoint := encodePosition(boundary)
	if err = c.q.FinalizeSnapshotLSN(checkpoint); err != nil {
		return err
	}
	if err = c.q.Publish(checkpoint, true); err != nil {
		return err
	}
	c.metrics.SnapshotCommitted(total, time.Since(started))
	return nil
}
