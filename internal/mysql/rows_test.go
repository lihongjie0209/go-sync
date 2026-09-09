package mysql

import "testing"

func TestMakeKeylessRowPreservesUnsignedAndBinary(t *testing.T) {
	table := Table{Schema: "db", Name: "bag", RowIdentity: "full_row", Columns: []Column{
		{Name: "n", Type: "bigint", ColumnType: "bigint unsigned"},
		{Name: "b", Type: "blob", ColumnType: "blob"},
	}}
	row, err := makeRow(table, "delete", []any{uint64(18446744073709551615), []byte{0, 255}}, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if row.Identity != "full_row" || len(row.Key) != 2 || *row.Key[0].Value != "18446744073709551615" || *row.Key[1].Value != "0x00ff" {
		t.Fatalf("unexpected row: %+v", row)
	}
}
