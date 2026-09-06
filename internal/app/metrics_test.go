package app

import (
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"

	"go-sync/internal/config"
)

func TestMetricsBindFailurePrecedesCapture(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	c := config.Defaults()
	c.SourceID, c.Slot = "test", "test"
	c.DSN = "postgres://127.0.0.1:1/postgres?sslmode=disable"
	c.URL = "http://127.0.0.1:1/cdc"
	c.Tables = []config.Table{{Schema: "public", Name: "items"}}
	c.DataDir = t.TempDir()
	c.MetricsAddr = l.Addr().String()
	err = Run(t.Context(), c, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil || !strings.Contains(err.Error(), "listen for metrics") {
		t.Fatalf("expected bind failure before connecting to PostgreSQL: %v", err)
	}
}
