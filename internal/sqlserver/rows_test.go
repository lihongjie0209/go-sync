package sqlserver

import (
	"errors"
	"testing"
)

func ptr(s string) *string { return &s }

func TestLSN(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, value string
		valid       bool
	}{
		{name: "valid", value: "0x0000001200000a010003", valid: true},
		{name: "zero", value: "0x00000000000000000000", valid: true},
		{name: "postgres", value: "0/30"},
		{name: "short", value: "0x01"},
		{name: "invalid hex", value: "0x0000001200000a01000z"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseLSN(tc.value)
			if (err == nil) != tc.valid {
				t.Fatalf("parse error = %v", err)
			}
			if tc.valid && got.String() != tc.value {
				t.Fatalf("round trip = %s", got.String())
			}
		})
	}
}

func TestLosslessValues(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, base, raw, want string }{
		{name: "bigint", base: "bigint", raw: "9007199254740993", want: "9007199254740993"},
		{name: "decimal", base: "decimal", raw: "12345678901234567890.123456789012345678", want: "12345678901234567890.123456789012345678"},
		{name: "unicode", base: "nvarchar", raw: "中文 😀  ", want: "中文 😀  "},
		{name: "binary", base: "varbinary", raw: "00FF", want: "0x00ff"},
		{name: "float", base: "float", raw: "3FB999999999999A", want: "0.1"},
		{name: "real", base: "real", raw: "3DCCCCCD", want: "0.1"},
		{name: "bit", base: "bit", raw: "1", want: "true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			col := Column{BaseType: tc.base}
			got, err := normalize(col, &tc.raw)
			if err != nil || got == nil || *got != tc.want {
				t.Fatalf("value = %v, error = %v", got, err)
			}
			got, err = normalize(col, nil)
			if err != nil || got != nil {
				t.Fatal("NULL changed")
			}
		})
	}
}

func TestKeylessUpdateReconstructsUnchangedMaxOldValue(t *testing.T) {
	t.Parallel()
	table := Table{Schema: "dbo", Name: "bag", RowIdentity: "full_row", Columns: []Column{
		{Name: "n", Type: "int", BaseType: "int", Ordinal: 1},
		{Name: "body", Type: "nvarchar(max)", BaseType: "nvarchar", MaxLength: -1, Ordinal: 2},
	}}
	before := change{operation: 3, values: []*string{ptr("1"), nil}}
	current := change{operation: 4, mask: []byte{1}, values: []*string{ptr("2"), ptr("unchanged")}}
	row, err := convertChange(table, current, &before)
	if err != nil {
		t.Fatal(err)
	}
	if row.Identity != "full_row" || *row.OldKey[0].Value != "1" || *row.OldKey[1].Value != "unchanged" {
		t.Fatalf("wrong old identity: %+v", row)
	}
	current.mask = []byte{3}
	row, err = convertChange(table, current, &before)
	if err != nil || row.OldKey[1].Value != nil {
		t.Fatal("changed NULL old value was not preserved")
	}
}

func TestRetentionAndMask(t *testing.T) {
	t.Parallel()
	from := lsn{9: 1}
	if !errors.Is(validateRange(from, lsn{9: 2}), errRetentionGap) {
		t.Fatal("retention gap accepted")
	}
	if err := validateRange(from, from); err != nil {
		t.Fatal(err)
	}
	if err := validateRange(from, lsn{}); err == nil {
		t.Fatal("disabled capture accepted")
	}
	for _, tc := range []struct {
		name    string
		ordinal int
		want    bool
		valid   bool
	}{
		{name: "low bit", ordinal: 1, want: true, valid: true},
		{name: "unset", ordinal: 2, valid: true},
		{name: "next byte", ordinal: 9, want: true, valid: true},
		{name: "zero", ordinal: 0},
		{name: "outside", ordinal: 17},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := changed([]byte{1, 1}, tc.ordinal)
			if (err == nil) != tc.valid || got != tc.want {
				t.Fatalf("mask = %v, %v", got, err)
			}
		})
	}
}

func TestIdentifierQuoting(t *testing.T) {
	t.Parallel()
	if got := quote("x]; DROP TABLE dbo.t;--"); got != "[x]]; DROP TABLE dbo.t;--]" {
		t.Fatal(got)
	}
}
