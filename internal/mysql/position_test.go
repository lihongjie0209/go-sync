package mysql

import (
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

func TestPositionRoundTrip(t *testing.T) {
	want := gomysql.Position{Name: "mysql-bin.000123", Pos: 4294967290}
	got, err := decodePosition(encodePosition(want))
	if err != nil || got != want {
		t.Fatalf("position round trip = %+v, %v", got, err)
	}
	for _, raw := range []string{"", "mysql:v1::00000000000000000004", "mysql:v2:eA:4", "mysql:v1:eA:3"} {
		if _, err := decodePosition(raw); err == nil {
			t.Fatalf("accepted invalid position %q", raw)
		}
	}
}
