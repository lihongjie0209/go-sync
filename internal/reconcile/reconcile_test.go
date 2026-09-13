package reconcile

import (
	"reflect"
	"testing"

	"go-sync/internal/event"
)

func TestDigestsAreOrderIndependentAndDuplicateSensitive(t *testing.T) {
	t.Parallel()
	value := func(v string) *string { return &v }
	a := event.Row{Schema: "s", Table: "t", Key: []event.Column{{Name: "id", Value: value("1")}}, Columns: []event.Column{{Name: "id", Value: value("1")}, {Name: "body", Value: value("x")}}}
	b := event.Row{Schema: "s", Table: "t", Key: []event.Column{{Name: "id", Value: value("2")}}, Columns: []event.Column{{Name: "id", Value: value("2")}, {Name: "body", Value: nil}}}
	left, err := Digests([]event.Row{a, b}, 8)
	if err != nil {
		t.Fatal(err)
	}
	right, err := Digests([]event.Row{b, a}, 8)
	if err != nil {
		t.Fatal(err)
	}
	for i := range left {
		if left[i] != right[i] {
			t.Fatal("row order changed reconciliation digest")
		}
	}
	duplicate, err := Digests([]event.Row{a, b, b}, 8)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate[BucketForTest(t, b, 8)] == left[BucketForTest(t, b, 8)] {
		t.Fatal("duplicate keyless multiplicity was not represented")
	}
}

func TestDigestsNormalizeCrossDatabaseRepresentations(t *testing.T) {
	pointer := func(value string) *string { return &value }
	left := event.Row{Schema: "src", Table: "items", Key: []event.Column{{Name: "id", Type: "int", Value: pointer("1")}}, Columns: []event.Column{
		{Name: "id", Type: "int", Value: pointer("1")},
		{Name: "payload", Type: "varbinary(10)", Value: pointer("0x00FF")},
		{Name: "enabled", Type: "bit", Value: pointer("1")},
		{Name: "amount", Type: "decimal(10,2)", Value: pointer("1.00")},
		{Name: "document", Type: "json", Value: pointer(`{"b":2,"a":1}`)},
		{Name: "changed_at", Type: "datetime", Value: pointer("2026-09-13T02:03:04")},
	}}
	right := event.Row{Schema: "src", Table: "items", Key: []event.Column{{Name: "id", Type: "int", Value: pointer("1")}}, Columns: []event.Column{
		{Name: "id", Type: "int", Value: pointer("1")},
		{Name: "payload", Type: "varbinary(10)", Value: pointer(`\x00ff`)},
		{Name: "enabled", Type: "bit", Value: pointer("true")},
		{Name: "amount", Type: "decimal(10,2)", Value: pointer("1")},
		{Name: "document", Type: "json", Value: pointer(`{"a":1,"b":2}`)},
		{Name: "changed_at", Type: "datetime", Value: pointer("2026-09-13 02:03:04")},
	}}
	a, err := Digests([]event.Row{left}, 8)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Digests([]event.Row{right}, 8)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("normalized digests differ:\n%v\n%v", a, b)
	}
}

func BucketForTest(t *testing.T, row event.Row, buckets uint32) uint32 {
	t.Helper()
	bucket, err := Bucket(row, buckets)
	if err != nil {
		t.Fatal(err)
	}
	return bucket
}
