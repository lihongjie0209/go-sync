package logging

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go-sync/internal/config"
)

func TestOpenWritesJSONFileAndFiltersLevel(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	c := config.Defaults()
	c.Log.File = filepath.Join("logs", "collector.log")
	c.Log.Level = "warn"
	logger, closeLog, err := Open(c, dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	logger.Info("hidden")
	logger.Warn("visible", "source_id", "test")
	if err := closeLog(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, c.Log.File))
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	if strings.Contains(text, "hidden") || !strings.Contains(text, `"msg":"visible"`) || !strings.Contains(text, `"source_id":"test"`) {
		t.Fatalf("unexpected log: %s", text)
	}
}

func TestOpenRotatesBySize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	c := config.Defaults()
	c.Log.File = "collector.log"
	c.Log.MaxSizeMB = 1
	c.Log.MaxBackups = 2
	c.Log.MaxAgeDays = 0
	c.Log.Compress = false
	logger, closeLog, err := Open(c, dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	payload := strings.Repeat("x", 64<<10)
	for range 40 {
		logger.Info("rotation fixture", "payload", payload)
	}
	if err := closeLog(); err != nil {
		t.Fatal(err)
	}
	backups, err := filepath.Glob(filepath.Join(dir, "collector-*.log"))
	if err != nil {
		t.Fatal(err)
	}
	if len(backups) == 0 || len(backups) > c.Log.MaxBackups {
		t.Fatalf("backup count = %d", len(backups))
	}
}
