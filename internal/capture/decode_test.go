package capture

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"go-sync/internal/config"
	"go-sync/internal/queue"
	"go-sync/internal/telemetry"
)

func fixture(t *testing.T) (*decoder, *queue.Store) {
	t.Helper()
	dir := t.TempDir()
	q, e := queue.Open(dir, 1<<20, 0)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { q.Close() })
	if e = q.Initialize(queue.State{SourceID: "test", Generation: "g"}); e != nil {
		t.Fatal(e)
	}
	if e = q.Publish("0/10", true); e != nil {
		t.Fatal(e)
	}
	c := config.Defaults()
	c.DataDir = dir
	c.BatchRows = 1
	d := newDecoder(c, q, []Table{{Schema: "public", Name: "items", Columns: []Column{{Name: "id", Type: "bigint", OID: 20, Primary: true}, {Name: "body", Type: "text", OID: 25}}}}, 16)
	return d, q
}

func TestCommitGatesDelivery(t *testing.T) {
	d, q := fixture(t)
	meter := telemetry.New(q, d.b.cfg)
	d.b.metrics = meter
	checkCount := func(count string) {
		t.Helper()
		w := httptest.NewRecorder()
		meter.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
		if w.Code != 200 || !strings.Contains(w.Body.String(), "\ngo_sync_capture_transaction_rows_total "+count+"\n") {
			t.Fatalf("unexpected committed-row metric: %d %s", w.Code, w.Body.String())
		}
	}
	frames := []string{`{"action":"B","xid":1,"nextlsn":"0/30"}`, `{"action":"I","xid":1,"schema":"public","table":"items","columns":[{"name":"id","type":"bigint","typeoid":20,"value":"9007199254740993"},{"name":"body","type":"text","typeoid":25,"value":null}]}`}
	for _, f := range frames {
		if e := d.consume(t.Context(), []byte(f)); e != nil {
			t.Fatal(e)
		}
	}
	if _, _, e := q.Peek(); !errors.Is(e, queue.ErrEmpty) {
		t.Fatal("uncommitted rows visible")
	}
	checkCount("0")
	if e := d.consume(t.Context(), []byte(`{"action":"C","xid":1,"nextlsn":"0/30"}`)); e != nil {
		t.Fatal(e)
	}
	m, _, e := q.Peek()
	if e != nil {
		t.Fatal(e)
	}
	if *m.Rows[0].Key[0].Value != "9007199254740993" || m.Rows[0].Columns[1].Value != nil {
		t.Fatal("lossy values")
	}
	st, e := q.State()
	if e != nil {
		t.Fatal(e)
	}
	if st.DurableLSN != "0/30" || st.ReadySeq != 2 {
		t.Fatalf("state %+v", st)
	}
	// PostgreSQL can replay already committed transactions after a server crash.
	for _, f := range append(frames, `{"action":"C","xid":1,"nextlsn":"0/30"}`) {
		if e := d.consume(t.Context(), []byte(f)); e != nil {
			t.Fatal(e)
		}
	}
	next, e := q.State()
	if e != nil {
		t.Fatal(e)
	}
	if next.NextSeq != st.NextSeq {
		t.Fatal("duplicate source transaction appended")
	}
	checkCount("1")
}

func TestWholeTransactionDurabilityGate(t *testing.T) {
	for _, tc := range []struct {
		name       string
		commit     string
		full       bool
		wantCommit bool
	}{
		{name: "complete transaction", commit: `{"action":"C","xid":1,"nextlsn":"0/30"}`, wantCommit: true},
		{name: "interrupted before commit"},
		{name: "wrong xid", commit: `{"action":"C","xid":2,"nextlsn":"0/30"}`},
		{name: "wrong lsn", commit: `{"action":"C","xid":1,"nextlsn":"0/40"}`},
		{name: "no room for commit marker", commit: `{"action":"C","xid":1,"nextlsn":"0/30"}`, full: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d, q := fixture(t)
			if err := d.consume(t.Context(), []byte(`{"action":"B","xid":1,"nextlsn":"0/30"}`)); err != nil {
				t.Fatal(err)
			}
			for range 3 {
				if err := d.consume(t.Context(), []byte(`{"action":"I","xid":1,"schema":"public","table":"items","columns":[{"name":"id","type":"bigint","typeoid":20,"value":1},{"name":"body","type":"text","typeoid":25,"value":"payload"}]}`)); err != nil {
					t.Fatal(err)
				}
				if _, _, err := q.Peek(); !errors.Is(err, queue.ErrEmpty) {
					t.Fatalf("partial transaction became deliverable: %v", err)
				}
			}
			st, err := q.State()
			if err != nil {
				t.Fatal(err)
			}
			if st.NextSeq != 3 || st.ReadySeq != 0 || st.DurableLSN != "0/10" || d.durable != 16 {
				t.Fatalf("staging advanced publication/checkpoint: %+v", st)
			}
			reopen := func(limit int64) {
				t.Helper()
				if err := q.Close(); err != nil {
					t.Fatal(err)
				}
				next, err := queue.Open(d.b.cfg.DataDir, limit, 0)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = next.Close() })
				q = next
				d.b.q = next
			}
			if tc.full {
				// Existing row chunks fit, but the transaction_end append must fail.
				reopen(st.Bytes)
			}
			if tc.commit != "" {
				err := d.consume(t.Context(), []byte(tc.commit))
				if (err == nil) != tc.wantCommit {
					t.Fatalf("commit error = %v, want success %v", err, tc.wantCommit)
				}
			}
			if !tc.wantCommit {
				if _, _, err := q.Peek(); !errors.Is(err, queue.ErrEmpty) {
					t.Fatalf("failed/incomplete transaction became deliverable: %v", err)
				}
				if d.durable != 16 {
					t.Fatal("failed transaction advanced replication feedback position")
				}
			}
			reopen(1 << 20)
			if err := q.Recover(); err != nil {
				t.Fatal(err)
			}
			st, err = q.State()
			if err != nil {
				t.Fatal(err)
			}
			if !tc.wantCommit {
				if st.NextSeq != 0 || st.ReadySeq != 0 || st.Bytes != 0 || st.DurableLSN != "0/10" {
					t.Fatalf("incomplete transaction survived recovery: %+v", st)
				}
			} else {
				if st.NextSeq != 4 || st.ReadySeq != 4 || st.DurableLSN != "0/30" {
					t.Fatalf("complete transaction not durable: %+v", st)
				}
				for chunk := uint64(0); chunk < 4; chunk++ {
					m, _, err := q.Peek()
					if err != nil {
						t.Fatal(err)
					}
					if m.Transaction != "0/30" || m.Chunk != chunk {
						t.Fatalf("wrong transaction/chunk: %+v", m)
					}
					if chunk < 3 {
						if m.Kind != "transaction_rows" || len(m.Rows) != 1 || m.Rows[0].Ordinal != chunk+1 {
							t.Fatalf("missing/out-of-order transaction rows: %+v", m)
						}
					} else if m.Kind != "transaction_end" || len(m.Rows) != 0 {
						t.Fatalf("missing transaction end: %+v", m)
					}
					if err := q.Ack(m.Seq, m.ID); err != nil {
						t.Fatal(err)
					}
				}
			}
			if _, _, err := q.Peek(); !errors.Is(err, queue.ErrEmpty) {
				t.Fatalf("unexpected remaining message: %v", err)
			}
		})
	}
}

func TestUpdateKeyAndMissingToast(t *testing.T) {
	table := Table{Schema: "public", Name: "items", Columns: []Column{{Name: "id", Type: "integer", OID: 23, Primary: true}, {Name: "body", Type: "text", OID: 25}}}
	var r record
	if e := json.Unmarshal([]byte(`{"action":"U","schema":"public","table":"items","columns":[{"name":"id","type":"integer","typeoid":23,"value":2}],"identity":[{"name":"id","type":"integer","typeoid":23,"value":1}]}`), &r); e != nil {
		t.Fatal(e)
	}
	row, e := convert(r, table)
	if e != nil {
		t.Fatal(e)
	}
	if *row.Key[0].Value != "2" || *row.OldKey[0].Value != "1" || len(row.Columns) != 1 {
		t.Fatalf("wrong patch: %+v", row)
	}
}

func TestInvalidRecordsDoNotAdvance(t *testing.T) {
	for _, tc := range []struct{ name, frame string }{
		{"malformed", `{"action":`}, {"no begin", `{"action":"C","xid":1,"nextlsn":"0/30"}`},
		{"no lsn", `{"action":"B","xid":1}`}, {"truncate", `{"action":"T"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, q := fixture(t)
			if e := d.consume(t.Context(), []byte(tc.frame)); e == nil {
				t.Fatal("invalid record accepted")
			}
			st, e := q.State()
			if e != nil {
				t.Fatal(e)
			}
			if st.DurableLSN != "0/10" {
				t.Fatal("advanced checkpoint")
			}
		})
	}
}

func TestByteaAndSpecialNumbers(t *testing.T) {
	table := Table{Columns: []Column{{Name: "id", Type: "numeric", OID: 1700, Primary: true}, {Name: "data", Type: "bytea", OID: 17}}}
	r := record{Action: "I", Columns: []wireColumn{{Name: "id", Type: "numeric", OID: 1700, Value: json.RawMessage(`"NaN"`)}, {Name: "data", Type: "bytea", OID: 17, Value: json.RawMessage(`"00ff"`)}}}
	row, e := convert(r, table)
	if e != nil {
		t.Fatal(e)
	}
	if *row.Key[0].Value != "NaN" || *row.Columns[1].Value != "\\x00ff" {
		t.Fatal("wrong text values")
	}
}

func FuzzRowDecoder(f *testing.F) {
	f.Add([]byte(`{"action":"I","columns":[{"name":"id","type":"bigint","typeoid":20,"value":1}]}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 4096 {
			return
		}
		var r record
		if json.Unmarshal(b, &r) != nil {
			return
		}
		table := Table{Columns: []Column{{Name: "id", Type: "bigint", OID: 20, Primary: true}}}
		_, _ = convert(r, table)
	})
}
