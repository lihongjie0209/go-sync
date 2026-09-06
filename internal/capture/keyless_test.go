package capture

import (
	"encoding/json"
	"errors"
	"testing"

	"go-sync/internal/config"
	"go-sync/internal/queue"
)

func TestFullRowIdentity(t *testing.T) {
	table := Table{RowIdentity: "full_row", Identity: "f", Columns: []Column{{Name: "n", Type: "integer", OID: 23}, {Name: "body", Type: "text", OID: 25}}}
	old := []wireColumn{{Name: "n", Type: "integer", OID: 23, Value: json.RawMessage(`null`)}, {Name: "body", Type: "text", OID: 25, Value: json.RawMessage(`"large old text"`)}}
	for _, tc := range []struct {
		name, action      string
		columns, identity []wireColumn
		wantErr           bool
	}{
		{"insert with NULL", "I", old, nil, false},
		{"delete with NULL", "D", nil, old, false},
		{"update missing unchanged toast", "U", []wireColumn{{Name: "n", Type: "integer", OID: 23, Value: json.RawMessage(`2`)}}, old, false},
		{"partial delete", "D", nil, old[:1], true},
		{"missing old update", "U", old, nil, true},
		{"partial insert", "I", old[:1], nil, true},
		{"duplicate old column", "D", nil, []wireColumn{old[0], old[0]}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row, err := convert(record{Action: tc.action, Columns: tc.columns, Identity: tc.identity}, table)
			if (err != nil) != tc.wantErr {
				t.Fatalf("row=%+v err=%v", row, err)
			}
			if tc.wantErr {
				return
			}
			if row.Identity != "full_row" || len(row.Key) != 2 {
				t.Fatalf("incorrect identity: %+v", row)
			}
			if tc.action == "U" {
				if len(row.OldKey) != 2 || row.OldKey[0].Value != nil || *row.Key[1].Value != "large old text" {
					t.Fatalf("lost old/unchanged value: %+v", row)
				}
			} else if row.Key[0].Value != nil {
				t.Fatal("NULL identity was lost")
			}
		})
	}
}

func TestPartialFullIdentityDoesNotAdvanceCheckpoint(t *testing.T) {
	d, q := fixture(t)
	table := d.tables[config.Table{Schema: "public", Name: "items"}]
	table.RowIdentity, table.Identity = "full_row", "f"
	for i := range table.Columns {
		table.Columns[i].Primary = false
	}
	d.tables[config.Table{Schema: "public", Name: "items"}] = table
	if err := d.consume(t.Context(), []byte(`{"action":"B","xid":1,"nextlsn":"0/30"}`)); err != nil {
		t.Fatal(err)
	}
	if err := d.consume(t.Context(), []byte(`{"action":"D","xid":1,"schema":"public","table":"items","identity":[{"name":"id","type":"bigint","typeoid":20,"value":1}]}`)); err == nil {
		t.Fatal("partial identity accepted")
	}
	st, err := q.State()
	if err != nil {
		t.Fatal(err)
	}
	if st.DurableLSN != "0/10" {
		t.Fatal("checkpoint advanced")
	}
	if _, _, err := q.Peek(); !errors.Is(err, queue.ErrEmpty) {
		t.Fatal("invalid transaction visible")
	}
}
