//go:build integration

package capture

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"go-sync/internal/config"
	"go-sync/internal/queue"
	"go-sync/internal/testpg"
)

func TestIncompleteSnapshotRestartAndConcurrentWrites(t *testing.T) {
	dsn := testpg.Start(t).DSN
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer closeConn(ctx, conn)
	name := fmt.Sprintf("snapshot_%x", time.Now().UnixNano())
	ident := pgx.Identifier{"public", name}.Sanitize()
	if _, err := conn.Exec(ctx, "CREATE TABLE "+ident+" (id integer PRIMARY KEY, body text); INSERT INTO "+ident+" SELECT n, repeat('old',1000) FROM generate_series(1,200) n"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		conn.Exec(cleanupCtx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1", name)
		conn.Exec(cleanupCtx, "DROP TABLE "+ident)
	}()
	c := config.Defaults()
	c.DSN = dsn
	c.SourceID = name
	c.Slot = name
	c.Tables = []config.Table{{Schema: "public", Name: name}}
	c.DataDir = t.TempDir()
	c.URL = "http://127.0.0.1:1"
	c.BatchRows = 1
	c.BatchBytes = 1024
	c.MaxRowBytes = 8192
	c.ReserveBytes = 0
	info, err := Inspect(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	q, err := queue.Open(c.DataDir, 2000, 0)
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	err = New(c, q, log).snapshot(ctx, info, queue.State{})
	if err == nil || !strings.Contains(err.Error(), "capacity") {
		q.Close()
		t.Fatalf("expected capacity failure, got %v", err)
	}
	previous, err := q.State()
	if err != nil {
		t.Fatal(err)
	}
	if previous.Phase != "snapshot" || previous.ReadySeq != 0 {
		t.Fatalf("incomplete snapshot published: %+v", previous)
	}
	if _, _, err := q.Peek(); !errors.Is(err, queue.ErrEmpty) {
		t.Fatal("partial snapshot exposed")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = queue.Open(c.DataDir, 4<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	// Write after the replacement slot exported its snapshot. These changes
	// must appear only in the subsequent WAL stream, regardless of scan timing.
	written := make(chan error, 1)
	go func() {
		for {
			st, e := q.State()
			if e != nil {
				written <- e
				return
			}
			if st.Generation != previous.Generation && st.SnapshotLSN != "" {
				break
			}
			select {
			case <-ctx.Done():
				written <- ctx.Err()
				return
			case <-time.After(time.Millisecond):
			}
		}
		_, e := conn.Exec(ctx, "BEGIN; UPDATE "+ident+" SET body='changed' WHERE id=1; DELETE FROM "+ident+" WHERE id=2; INSERT INTO "+ident+" VALUES(201,'new'); COMMIT")
		written <- e
	}()
	if err := New(c, q, log).snapshot(ctx, info, previous); err != nil {
		cancel()
		<-written
		t.Fatal(err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	st, err := q.State()
	if err != nil {
		t.Fatal(err)
	}
	if st.Phase != "stream" || st.Generation == previous.Generation {
		t.Fatal("snapshot was not restarted with a new generation")
	}
	rows := 0
	for {
		m, _, e := q.Peek()
		if errors.Is(e, queue.ErrEmpty) {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		for _, row := range m.Rows {
			rows++
			if *row.Key[0].Value == "201" || *row.Columns[1].Value == "changed" {
				t.Fatal("snapshot included post-boundary data")
			}
		}
		if e := q.Ack(m.Seq, m.ID); e != nil {
			t.Fatal(e)
		}
	}
	if rows != 200 {
		t.Fatalf("snapshot rows=%d", rows)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- New(c, q, log).Run(runCtx) }()
	defer func() { stop(); <-done }()
	changes := 0
	for changes < 3 {
		m, _, e := q.Peek()
		if errors.Is(e, queue.ErrEmpty) {
			select {
			case e := <-done:
				done <- e
				t.Fatalf("capture exited: %v", e)
			case <-ctx.Done():
				t.Fatal("incremental changes missing")
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}
		if e != nil {
			t.Fatal(e)
		}
		changes += len(m.Rows)
		if e := q.Ack(m.Seq, m.ID); e != nil {
			t.Fatal(e)
		}
	}
}
