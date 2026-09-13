package sqlserverlegacy

import (
	"math"
	"strconv"
	"strings"
	"testing"

	"go-sync/internal/config"
)

func TestPositionRoundTrip(t *testing.T) {
	t.Parallel()
	for _, id := range []int64{0, 1, 9223372036854775807} {
		value := position(id)
		got, err := parsePosition(value)
		if err != nil || got != id {
			t.Fatalf("parsePosition(%q) = %d, %v", value, got, err)
		}
	}
	for _, value := range []string{"", "0x01", "legacy:-1", "legacy:not-a-number"} {
		if _, err := parsePosition(value); err == nil {
			t.Fatalf("parsePosition(%q) accepted invalid value", value)
		}
	}
}

func TestTriggerSQLCapturesBothImagesAndQuotesIdentifiers(t *testing.T) {
	t.Parallel()
	c := config.Defaults()
	c.SourceType = "sqlserver_legacy"
	c.SQLServerLegacy.Owner = "dbo"
	table := Table{Schema: "dbo", Name: "order", ObjectID: 42, TriggerName: triggerName(c, 42), Columns: []Column{
		{Name: "id", BaseType: "int", Type: "int", Primary: true},
		{Name: "label", BaseType: "nvarchar", Type: "nvarchar(20)"},
	}}
	sql := triggerSQL(c, table)
	for _, fragment := range []string{
		"/* go-sync legacy managed v1 */", "SELECT [id],[label] FROM deleted", "SELECT [id],[label] FROM inserted",
		"operation) VALUES (@statement,42,'D')", "operation) VALUES (@statement,42,'I')",
		"CASE WHEN @v2 IS NULL THEN 1 ELSE 0 END", "[dbo].[order]",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("generated trigger is missing %q\n%s", fragment, sql)
		}
	}
	if strings.Contains(sql, "SELECT * FROM inserted") || strings.Contains(sql, "SELECT * FROM deleted") {
		t.Fatal("generated trigger references legacy lob columns through select star")
	}
}

func TestModernTriggerCapturesLOBWithoutReadingDeletedLOB(t *testing.T) {
	t.Parallel()
	c := config.Defaults()
	c.SourceType = "sqlserver_legacy"
	c.SQLServerLegacy.Owner = "dbo"
	table := Table{Schema: "dbo", Name: "photos", ObjectID: 43, TriggerName: triggerName(c, 43), Modern: true, Columns: []Column{
		{Name: "id", BaseType: "bigint", Type: "bigint", Primary: true},
		{Name: "body", BaseType: "ntext", Type: "ntext"},
		{Name: "photo", BaseType: "image", Type: "image"},
	}}
	sql := triggerSQL(c, table)
	for _, fragment := range []string{
		"SELECT d.[id],CAST(NULL AS nvarchar(max)),CAST(NULL AS varbinary(max)) FROM deleted AS d",
		"SELECT b.[id],b.[body],b.[photo] FROM [dbo].[photos] AS b INNER JOIN inserted AS i ON b.[id]=i.[id]",
		"CONVERT(nvarchar(max),@v2)", "CONVERT(varbinary(max),@v3)",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("generated modern trigger is missing %q\n%s", fragment, sql)
		}
	}
	if strings.Contains(sql, "d.[body]") || strings.Contains(sql, "d.[photo]") {
		t.Fatal("generated modern trigger reads a legacy LOB from deleted")
	}
}

func TestNormalizeLegacyValues(t *testing.T) {
	t.Parallel()
	floatBytes := []byte{0x3f, 0xb9, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9a}
	value, err := normalize(Column{BaseType: "float"}, floatBytes, false, true)
	if err != nil || value == nil {
		t.Fatal(err)
	}
	parsed, err := strconv.ParseFloat(*value, 64)
	if err != nil || math.Abs(parsed-0.1) > 1e-15 {
		t.Fatalf("float = %q", *value)
	}
	binary, err := normalize(Column{BaseType: "varbinary"}, []byte{0, 255}, false, true)
	if err != nil || binary == nil || *binary != "0x00ff" {
		t.Fatalf("binary = %v, %v", binary, err)
	}
	bit, err := normalize(Column{BaseType: "bit"}, []byte("1"), false, false)
	if err != nil || bit == nil || *bit != "true" {
		t.Fatalf("bit = %v, %v", bit, err)
	}
}

func TestTypeNameRejectsLegacyLOB(t *testing.T) {
	t.Parallel()
	for _, typ := range []string{"text", "ntext", "image"} {
		if !legacyLOB(typ) {
			t.Fatalf("%s was not classified as legacy lob", typ)
		}
	}
}
