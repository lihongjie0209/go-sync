package server

import "testing"

func TestSQLServerLegacyTargetTypes(t *testing.T) {
	t.Parallel()
	table := sourceSchemaTable{SourceType: "sqlserver_legacy"}
	tests := []struct {
		column sourceSchemaColumn
		want   string
	}{
		{sourceSchemaColumn{BaseType: "numeric", Type: "numeric(18,4)"}, "numeric(18,4)"},
		{sourceSchemaColumn{BaseType: "money", Type: "money"}, "numeric(19,4)"},
		{sourceSchemaColumn{BaseType: "uniqueidentifier", Type: "uniqueidentifier"}, "uuid"},
		{sourceSchemaColumn{BaseType: "image", Type: "image"}, "bytea"},
	}
	for _, test := range tests {
		got, err := safeTargetType(table, test.column)
		if err != nil || got != test.want {
			t.Fatalf("safeTargetType(%q) = %q, %v; want %q", test.column.BaseType, got, err, test.want)
		}
	}
}
