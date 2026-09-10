package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go-sync/internal/config"
)

func TestCLIUsage(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		code int
	}{
		{"help", []string{"--help"}, 0},
		{"version", []string{"version"}, 0},
		{"unknown command", []string{"delete-slot"}, 2},
		{"unknown flag", []string{"run", "--unknown"}, 2},
		{"extra arguments", []string{"run", "extra"}, 2},
		{"service action required", []string{"service"}, 2},
		{"unknown service action", []string{"service", "restart"}, 2},
		{"service extra arguments", []string{"service", "status", "extra"}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			if code := execute(t.Context(), tc.args, &out, &errOut); code != tc.code {
				t.Fatalf("exit %d: %s", code, errOut.String())
			}
		})
	}
}

func TestRunFatalErrorIsStructured(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	c := config.Defaults()
	c.SourceID, c.Slot, c.DSN = "test", "test", "postgres://user:secret@localhost/db"
	c.URL = "http://localhost/cdc"
	c.Tables = []config.Table{{Schema: "public", Name: "items"}}
	c.DataDir = filepath.Join(dir, "not-a-directory")
	if err := os.WriteFile(c.DataDir, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if code := execute(t.Context(), []string{"run", "--config", path}, &out, &errOut); code != 1 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	lines := strings.Split(strings.TrimSpace(errOut.String()), "\n")
	if len(lines) != 2 || out.Len() != 0 {
		t.Fatalf("unexpected output: %s", errOut.String())
	}
	for _, line := range lines {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("non-JSON runtime log: %s", line)
		}
		if entry["source_type"] != "postgres" || entry["source_id"] != "test" {
			t.Fatal("missing source context")
		}
	}
	if strings.Contains(errOut.String(), "secret") || !strings.Contains(lines[1], `"level":"ERROR"`) {
		t.Fatal("unsafe or missing fatal log")
	}
}
