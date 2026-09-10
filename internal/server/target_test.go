package server

import (
	"math"
	"strings"
	"testing"

	"go-sync/internal/event"
	"go-sync/internal/serverconfig"
)

func TestQuoteEscapesIdentifiers(t *testing.T) {
	if got, want := quote(`public"; DROP TABLE x; --`), `"public""; DROP TABLE x; --"`; got != want {
		t.Fatalf("quote() = %q, want %q", got, want)
	}
}

func TestExpressionsRejectInvalidTargetValues(t *testing.T) {
	invalidHex := "0xnot-hex"
	value := "value"
	tests := []struct {
		name    string
		column  appliedColumn
		types   map[string]string
		wantErr string
	}{
		{name: "unknown column", column: appliedColumn{name: "missing", value: &value}, types: map[string]string{}, wantErr: "does not exist"},
		{name: "file target is not bytea", column: appliedColumn{name: "content", value: []byte("x"), binary: true}, types: map[string]string{"content": "text"}, wantErr: "must be bytea"},
		{name: "invalid hex bytea", column: appliedColumn{name: "content", value: &invalidHex}, types: map[string]string{"content": "bytea"}, wantErr: "invalid binary value"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, _, err := expressions([]appliedColumn{test.column}, test.types, 1)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("expressions() error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}

func TestTransformPreservesFilePathAndRejectsBadBase64(t *testing.T) {
	target := &target{cfg: serverconfig.Syncer{FileColumns: []serverconfig.FileColumn{{SourceSchema: "src", Table: "documents", SourceColumn: "path", ContentColumn: "content"}}}}
	path, encoded := "C:/files/a.bin", "AQID"
	columns, err := target.transform(event.Row{Schema: "src", Table: "documents"}, []event.Column{{Name: "path", Value: &encoded, Encoding: "base64", SourceValue: &path}})
	if err != nil {
		t.Fatal(err)
	}
	if len(columns) != 2 || columns[0].value != &path || string(columns[1].value.([]byte)) != "\x01\x02\x03" {
		t.Fatalf("unexpected transformed columns: %+v", columns)
	}
	bad := "%%%"
	if _, err := target.transform(event.Row{Schema: "src", Table: "documents"}, []event.Column{{Name: "path", Value: &bad, Encoding: "base64"}}); err == nil {
		t.Fatal("invalid base64 was accepted")
	}
}

func TestSequenceLargerThanPostgresBigintIsRejectedBeforeDatabaseAccess(t *testing.T) {
	target := &target{}
	err := target.Store(t.Context(), event.Message{Seq: uint64(math.MaxInt64) + 1}, nil)
	if err == nil || !strings.Contains(err.Error(), "bigint") {
		t.Fatalf("Store() error = %v", err)
	}
}
